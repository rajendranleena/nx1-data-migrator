"""
Hive-to-Hive metadata migration strategy.

Source: Hive tables (discovered via Spark SQL against source metastore)
Destination: Hive external tables at destination S3 paths
"""

import contextlib
import logging

from utils.migrations.metadata_strategies import cell_str, normalize_s3
from utils.migrations.shared import compute_dest_path

logger = logging.getLogger(__name__)


def parse_excel_rows(df, config, run_id):
    """Group Excel rows into database-level configs for Hive-to-Hive migration."""
    grouped = {}
    for _, row in df.iterrows():
        src_db = cell_str(row.get('database'))
        if not src_db:
            continue

        raw_table = cell_str(row.get('table'), '*')
        dest_db = cell_str(row.get('dest_database'), src_db)

        raw_bucket = cell_str(row.get('dest_bucket'))
        src_prefix = normalize_s3(cell_str(row.get('source_s3_prefix')))
        dest_prefix = normalize_s3(cell_str(row.get('dest_s3_prefix')))

        has_prefix = bool(src_prefix and dest_prefix)
        has_explicit_bucket = bool(raw_bucket)

        if has_prefix and has_explicit_bucket:
            logger.warning(
                f"[parse_s3_excel] Skipping row for database '{src_db}' — "
                f"specifies both dest_bucket ('{raw_bucket}') and s3 prefix pair. "
                f"These build destination paths differently and cannot be combined."
            )
            continue
        if not has_prefix and not has_explicit_bucket:
            logger.warning(
                f"[parse_s3_excel] Skipping row for database '{src_db}' — "
                f"must supply either (source_s3_prefix + dest_s3_prefix) or dest_bucket"
            )
            continue

        dest_bucket_norm = normalize_s3(raw_bucket) if raw_bucket else config['default_s3_bucket']

        key = (src_db, dest_db, dest_bucket_norm, src_prefix, dest_prefix)
        if key not in grouped:
            grouped[key] = {'tokens': []}

        for tok in raw_table.split(','):
            tok = tok.strip()
            if tok:
                grouped[key]['tokens'].append(tok)

    configs = []
    for (src_db, dest_db, dest_bucket, src_prefix, dest_prefix), group in grouped.items():
        unique_tokens = list(dict.fromkeys(group['tokens']))
        if '*' in unique_tokens:
            unique_tokens = ['*']

        configs.append({
            'source_database': src_db,
            'dest_database': dest_db,
            'dest_bucket': dest_bucket,
            'source_s3_prefix': src_prefix,
            'dest_s3_prefix': dest_prefix,
            'table_tokens': unique_tokens,
            'run_id': run_id,
        })
        logger.info(
            f"[parse_s3_excel] {src_db} → {dest_db} | bucket={dest_bucket} | "
            f"src_prefix={src_prefix or 'N/A'} | dest_prefix={dest_prefix or 'N/A'} | "
            f"tokens={unique_tokens[:5]}"
        )

    return configs


def discover_tables(db_config, spark, config):
    """Discover Hive table metadata via Spark SQL."""
    import fnmatch

    src_db = db_config['source_database']
    dest_db = db_config['dest_database']
    dest_bucket = db_config['dest_bucket']
    src_prefix = db_config.get('source_s3_prefix', '')
    dest_prefix = db_config.get('dest_s3_prefix', '')
    tokens = db_config.get('table_tokens', ['*'])

    all_tables = [r.tableName for r in spark.sql(f"SHOW TABLES IN {src_db}").collect()]
    matched = []
    for tok in tokens:
        if tok == '*':
            matched = all_tables
            break
        matched += [t for t in all_tables if fnmatch.fnmatch(t, tok) and t not in matched]

    logger.info(f"[discover_hive_tables] '{src_db}': {len(matched)} table(s) matched")

    metadata = []
    for tbl in matched:
        try:
            desc_rows = spark.sql(f"DESCRIBE FORMATTED {src_db}.{tbl}").collect()

            location = None
            input_format = None
            table_type = 'EXTERNAL_TABLE'

            for r in desc_rows:
                col = (r.col_name or '').strip().rstrip(':').lower()
                val = (r.data_type or '').strip()
                if col == 'location':
                    location = val
                elif col in ('type', 'table type'):
                    table_type = val.replace('_TABLE', '')
                elif col == 'inputformat':
                    input_format = val

            file_format = 'PARQUET'
            if input_format:
                lf = input_format.lower()
                if 'parquet' in lf:
                    file_format = 'PARQUET'
                elif 'orc' in lf:
                    file_format = 'ORC'
                elif 'avro' in lf:
                    file_format = 'AVRO'
                elif 'text' in lf:
                    file_format = 'TEXTFILE'
                else:
                    logger.warning(
                        f"[discover_hive_tables] Unrecognised InputFormat "
                        f"'{input_format}' for {src_db}.{tbl} — defaulting to PARQUET"
                    )

            part_cols = []
            in_part_section = False
            for r in desc_rows:
                cn = (r.col_name or '').strip()
                if cn == '# Partition Information':
                    in_part_section = True
                    continue
                if in_part_section and cn == '# col_name':
                    continue
                if in_part_section and cn.startswith('#'):
                    break
                if in_part_section and cn:
                    part_cols.append(cn)

            is_partitioned = len(part_cols) > 0

            partitions = []
            with contextlib.suppress(Exception):
                partitions = [r.partition for r in spark.sql(f"SHOW PARTITIONS {src_db}.{tbl}").collect()]

            row_count = 0
            try:
                row_count = spark.sql(f"SELECT COUNT(*) as c FROM {src_db}.{tbl}").collect()[0]['c']
            except Exception as e:
                logger.warning(f"[discover_hive_tables] Could not count rows for {src_db}.{tbl}: {e}")

            source_file_count = 0
            source_total_size = 0
            if location:
                try:
                    from py4j.java_gateway import java_import
                    java_import(spark._jvm, 'org.apache.hadoop.fs.*')
                    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                        spark._jvm.java.net.URI(location),
                        spark._jsc.hadoopConfiguration()
                    )
                    path_obj = spark._jvm.org.apache.hadoop.fs.Path(location)
                    if fs.exists(path_obj):
                        summary = fs.getContentSummary(path_obj)
                        source_total_size = int(summary.getLength())
                        source_file_count = int(summary.getFileCount())
                except Exception as e:
                    logger.warning(f"[discover_hive_tables] Could not get FS summary for {src_db}.{tbl}: {e}")

            schema = []
            for r in spark.sql(f"DESCRIBE {src_db}.{tbl}").collect():
                cn = (r.col_name or '').strip()
                if cn.startswith('#') or cn == '' or cn == 'col_name':
                    break
                schema.append({'name': cn, 'type': (r.data_type or '').strip()})

            dest_path = compute_dest_path(
                source_location=location or '',
                dest_database=dest_db,
                table_name=tbl,
                dest_bucket=dest_bucket,
                source_s3_prefix=src_prefix,
                dest_s3_prefix=dest_prefix,
            )

            logger.info(
                f"[discover_hive_tables] {src_db}.{tbl} | fmt={file_format} | "
                f"parts={len(partitions)} | rows={row_count} | "
                f"size={source_total_size / (1024 ** 2):.1f}MB | dest={dest_path}"
            )

            metadata.append({
                'source_database': src_db,
                'source_table': tbl,
                'dest_database': dest_db,
                'dest_bucket': dest_bucket,
                'source_location': location or '',
                'dest_location': dest_path,
                'file_format': file_format,
                'table_type': table_type,
                'schema': schema,
                'partition_columns': ','.join(part_cols),
                'partitions': partitions,
                'partition_count': len(partitions),
                'is_partitioned': is_partitioned,
                'source_row_count': row_count,
                'source_file_count': source_file_count,
                'source_total_size_bytes': source_total_size,
            })

        except Exception as e:
            logger.error(f"[discover_hive_tables] FAILED for {src_db}.{tbl}: {e}")
            metadata.append({
                'source_database': src_db,
                'source_table': tbl,
                'dest_database': dest_db,
                'dest_bucket': dest_bucket,
                'source_location': '',
                'dest_location': '',
                'file_format': 'PARQUET',
                'table_type': 'UNKNOWN',
                'schema': [],
                'partition_columns': '',
                'partitions': [],
                'partition_count': 0,
                'is_partitioned': False,
                'source_row_count': 0,
                'source_file_count': 0,
                'source_total_size_bytes': 0,
                'error': str(e)[:500],
            })

    return metadata


def create_dest_table(table_info, dest_db, spark, config):
    """Create or repair a destination Hive external table."""
    from utils.migrations.shared import apply_bucket_credentials

    tbl = table_info['source_table']
    dest_path = table_info['dest_location']
    fmt = table_info.get('file_format', 'PARQUET')
    schema_list = table_info.get('schema', [])
    part_cols_str = table_info.get('partition_columns', '')
    is_part = table_info.get('is_partitioned', False)
    full_name = f"{dest_db}.{tbl}"

    apply_bucket_credentials(
        spark, dest_path,
        config.get('_dest_endpoint', ''),
        config.get('_dest_access_key', ''),
        config.get('_dest_secret_key', ''),
    )

    logger.info(f"[create_hive_table] Processing {full_name} | fmt={fmt} | partitioned={is_part}")

    try:
        exists = False
        try:
            spark.sql(f"DESCRIBE {full_name}")
            exists = True
        except Exception:
            pass

        if exists:
            if is_part:
                spark.sql(f"MSCK REPAIR TABLE {full_name}")
            spark.sql(f"REFRESH TABLE {full_name}")
            logger.info(f"[create_hive_table] REPAIRED (already existed): {full_name}")
            return {'source_table': tbl, 'status': 'COMPLETED', 'existed': True, 'error': None}

        part_col_list = [p.strip() for p in part_cols_str.split(',') if p.strip()]

        if schema_list:
            cols = [
                f"`{c['name']}` {c['type']}"
                for c in schema_list
                if c.get('name') and c['name'] not in part_col_list
            ]
            col_def = ', '.join(cols)
        else:
            infer_df = spark.read.format(fmt.lower()).load(dest_path)
            col_def = ', '.join([
                f"`{f.name}` {f.dataType.simpleString()}"
                for f in infer_df.schema.fields
                if f.name not in part_col_list
            ])

        part_clause = ''
        if is_part and part_col_list:
            pdefs = []
            for pc in part_col_list:
                ptype = 'STRING'
                for c in schema_list:
                    if c.get('name') == pc:
                        ptype = c.get('type', 'STRING')
                        break
                pdefs.append(f"`{pc}` {ptype}")
            part_clause = f"PARTITIONED BY ({', '.join(pdefs)})"

        ddl = f"""
            CREATE EXTERNAL TABLE IF NOT EXISTS {full_name} ({col_def})
            {part_clause}
            STORED AS {fmt}
            LOCATION '{dest_path}'
        """
        spark.sql(ddl)

        if is_part:
            spark.sql(f"MSCK REPAIR TABLE {full_name}")
        spark.sql(f"REFRESH TABLE {full_name}")

        logger.info(f"[create_hive_table] CREATED: {full_name} at {dest_path}")
        return {'source_table': tbl, 'status': 'COMPLETED', 'existed': False, 'error': None}

    except Exception as e:
        error_msg = str(e)[:2000]
        logger.error(f"[create_hive_table] FAILED for {full_name}: {error_msg}")
        return {'source_table': tbl, 'status': 'FAILED', 'existed': False, 'error': error_msg}
