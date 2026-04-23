"""
Iceberg-to-Iceberg migration strategy.

Source: Iceberg table with Hadoop catalog on S3 (metadata.json files)
Destination: Iceberg table registered in HMS via CREATE TABLE + add_files

Reads schema and partition spec from the destination's metadata.json.
If the table already exists in HMS it is dropped and recreated so that
add_files can re-import all data files (add_files has no skip-duplicates
mode — it rejects the entire import when any source file is already tracked).

Limitation: add_files only supports identity partition transforms.
Tables with year(), month(), bucket(), truncate() partitioning will fail.
"""

import contextlib
import logging

from utils.migrations.shared import cell_str, normalize_s3
from utils.migrations.shared import (
    _extract_partition_spec,
    _extract_row_count,
    _extract_schema,
    _get_fs_stats,
    _list_iceberg_tables,
    _map_iceberg_type,
    _match_tokens,
    _read_iceberg_metadata,
    apply_bucket_credentials,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# DDL builders
# ─────────────────────────────────────────────────────────────────────────────

_TRANSFORM_MAP = {
    'identity': lambda col, _: col,
    'year': lambda col, _: f'years({col})',
    'month': lambda col, _: f'months({col})',
    'day': lambda col, _: f'days({col})',
    'hour': lambda col, _: f'hours({col})',
    'bucket': lambda col, p: f'bucket({p}, {col})',
    'truncate': lambda col, p: f'truncate({p}, {col})',
}


def _build_iceberg_partition_clause(partition_spec):
    """Convert a partition spec detail list into a Spark SQL PARTITIONED BY clause body."""
    if not partition_spec:
        return ''

    parts = []
    for field in partition_spec:
        col = f"`{field['source_column']}`"
        transform = field['transform']
        param = field.get('param')
        formatter = _TRANSFORM_MAP.get(transform)
        if formatter:
            parts.append(formatter(col, param))
        else:
            parts.append(col)
    return ', '.join(parts)


def _build_column_defs(schema):
    """Build a column definition string from a schema list for CREATE TABLE DDL."""
    if not schema:
        return ''
    return ', '.join(f"`{col['name']}` {col['type']}" for col in schema)


# ─────────────────────────────────────────────────────────────────────────────
# HMS helpers
# ─────────────────────────────────────────────────────────────────────────────

def _describe_schema(spark, full_name):
    """Extract schema from an HMS table via DESCRIBE."""
    rows = spark.sql(f"DESCRIBE {full_name}").collect()
    schema = []
    for r in rows:
        cn = (r.col_name or '').strip()
        if cn.startswith('#') or cn == '' or cn == 'col_name':
            break
        schema.append({'name': cn, 'type': (r.data_type or '').strip()})
    return schema


def _describe_table_info(spark, full_name):
    """Extract location and file format from DESCRIBE FORMATTED."""
    location = None
    file_format = 'PARQUET'
    for row in spark.sql(f"DESCRIBE FORMATTED {full_name}").collect():
        col = (row.col_name or '').strip().rstrip(':').lower()
        val = (row.data_type or '').strip()
        if col == 'location':
            location = val
        elif col == 'write.format.default':
            file_format = val.upper()
    return location, file_format


def _resolve_data_path(spark, table_path):
    """Return {table_path}/data if it exists, otherwise table_path."""
    try:
        from py4j.java_gateway import java_import

        java_import(spark._jvm, 'org.apache.hadoop.fs.*')
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(table_path),
            spark._jsc.hadoopConfiguration()
        )
        data_path = f"{table_path}/data"
        if fs.exists(spark._jvm.org.apache.hadoop.fs.Path(data_path)):
            return data_path
    except Exception as e:
        logger.warning(f"[iceberg_create] Could not check data/ subdir for {table_path}: {e}")
    return table_path


# ─────────────────────────────────────────────────────────────────────────────
# Strategy interface functions
# ─────────────────────────────────────────────────────────────────────────────

def parse_excel_rows(df, config, run_id):
    """Parse Excel rows for Iceberg-to-Iceberg migration.

    Required columns: database, table, dest_s3_prefix.
    The 'table' column supports single names, comma-separated lists,
    or wildcards (fnmatch patterns). Rows are grouped by (database, dest_s3_prefix).
    """
    grouped = {}
    for _, row in df.iterrows():
        database = cell_str(row.get('database'))
        if not database:
            continue

        raw_table = cell_str(row.get('table'), '*')
        dest_s3_prefix = normalize_s3(cell_str(row.get('dest_s3_prefix')))

        if not dest_s3_prefix:
            logger.warning(
                f"[parse_iceberg_excel] Skipping row for database '{database}' — "
                f"missing 'dest_s3_prefix'"
            )
            continue

        key = (database, dest_s3_prefix.rstrip('/'))
        if key not in grouped:
            grouped[key] = {'tokens': []}

        for tok in raw_table.split(','):
            tok = tok.strip()
            if tok:
                grouped[key]['tokens'].append(tok)

    configs = []
    for (database, dest_s3_prefix), group in grouped.items():
        unique_tokens = list(dict.fromkeys(group['tokens']))
        if '*' in unique_tokens:
            unique_tokens = ['*']

        configs.append({
            'source_database': database,
            'dest_database': database,
            'dest_s3_prefix': dest_s3_prefix,
            'table_tokens': unique_tokens,
            'run_id': run_id,
        })
        logger.info(
            f"[parse_iceberg_excel] {database} | "
            f"prefix={dest_s3_prefix} | tokens={unique_tokens[:5]}"
        )

    return configs


def discover_tables(db_config, spark, config):
    """Discover Iceberg tables at destination and collect metadata.

    Lists S3 subdirectories under {dest_s3_prefix}/{database}/ to find
    tables, then matches against table_tokens. For each matched table,
    queries HMS if the table is already registered, otherwise falls back
    to reading metadata.json from the destination.
    """
    database = db_config['dest_database']
    dest_prefix = db_config['dest_s3_prefix']
    tokens = db_config.get('table_tokens', ['*'])
    db_path = f"{dest_prefix.rstrip('/')}/{database}"

    apply_bucket_credentials(
        spark, dest_prefix,
        config.get('_dest_endpoint', ''),
        config.get('_dest_access_key', ''),
        config.get('_dest_secret_key', ''),
    )

    available_tables = _list_iceberg_tables(spark, db_path)
    matched = _match_tokens(available_tables, tokens)

    logger.info(
        f"[iceberg_discover] '{database}': {len(available_tables)} table(s) found, "
        f"{len(matched)} matched tokens {tokens[:5]}"
    )

    metadata_list = []
    for tbl_name in matched:
        dest_path = f"{db_path}/{tbl_name}"
        full_name = f"{database}.{tbl_name}"

        try:
            table_in_hms = False
            try:
                spark.sql(f"DESCRIBE {full_name}")
                table_in_hms = True
            except Exception:
                pass

            if table_in_hms:
                schema = _describe_schema(spark, full_name)
                row_count = spark.sql(
                    f"SELECT COUNT(*) as c FROM {full_name}"
                ).collect()[0]['c']

                # Partition spec from metadata.json — the .partitions metadata
                # table columns (partition, record_count, file_count, …) are
                # NOT the actual partition columns, and HMS doesn't preserve
                # transforms (year, month, bucket, etc.).
                try:
                    iceberg_meta = _read_iceberg_metadata(spark, dest_path)
                    partition_spec_detail, is_partitioned = (
                        _extract_partition_spec(iceberg_meta)
                    )
                except Exception:
                    partition_spec_detail, is_partitioned = [], False

                partition_columns = ','.join(
                    p['source_column'] for p in partition_spec_detail
                )

                partition_count = 0
                if is_partitioned:
                    with contextlib.suppress(Exception):
                        partition_count = spark.sql(
                            f"SELECT * FROM {full_name}.partitions"
                        ).count()

                location, file_format = _describe_table_info(spark, full_name)

                logger.info(
                    f"[iceberg_discover] {full_name} (HMS) | fmt={file_format} | "
                    f"rows={row_count} | parts={partition_count}"
                )

                file_count, total_size = _get_fs_stats(spark, dest_path)

                metadata_list.append({
                    'source_database': database,
                    'source_table': tbl_name,
                    'dest_database': database,
                    'source_location': location or dest_path,
                    'dest_location': dest_path,
                    'file_format': file_format,
                    'table_type': 'ICEBERG',
                    'schema': schema,
                    'partition_columns': partition_columns,
                    'partition_spec_detail': partition_spec_detail,
                    'partitions': [],
                    'partition_count': partition_count,
                    'is_partitioned': is_partitioned,
                    'source_row_count': row_count,
                    'source_file_count': file_count,
                    'source_total_size_bytes': total_size,
                    'format_version': '2',
                })

            else:
                iceberg_meta = _read_iceberg_metadata(spark, dest_path)
                schema = _extract_schema(iceberg_meta)
                partition_spec, is_partitioned = _extract_partition_spec(iceberg_meta)
                row_count = _extract_row_count(iceberg_meta)
                file_format = iceberg_meta.get('properties', {}).get(
                    'write.format.default', 'parquet'
                ).upper()
                format_version = str(iceberg_meta.get('format-version', 2))

                file_count, total_size = _get_fs_stats(spark, dest_path)

                logger.info(
                    f"[iceberg_discover] {full_name} (metadata.json) | fmt={file_format} | "
                    f"rows={row_count} | size={total_size / (1024 ** 2):.1f}MB"
                )

                metadata_list.append({
                    'source_database': database,
                    'source_table': tbl_name,
                    'dest_database': database,
                    'source_location': dest_path,
                    'dest_location': dest_path,
                    'file_format': file_format,
                    'table_type': 'ICEBERG',
                    'schema': schema,
                    'partition_columns': ','.join(
                        p['source_column'] for p in partition_spec
                    ),
                    'partition_spec_detail': partition_spec,
                    'partitions': [],
                    'partition_count': 0,
                    'is_partitioned': is_partitioned,
                    'source_row_count': row_count,
                    'source_file_count': file_count,
                    'source_total_size_bytes': total_size,
                    'format_version': format_version,
                })

        except Exception as e:
            logger.error(f"[iceberg_discover] FAILED for {full_name}: {e}")
            metadata_list.append({
                'source_database': database,
                'source_table': tbl_name,
                'dest_database': database,
                'source_location': dest_path,
                'dest_location': '',
                'file_format': 'UNKNOWN',
                'table_type': 'UNKNOWN',
                'schema': [],
                'partition_columns': '',
                'partition_spec_detail': [],
                'partitions': [],
                'partition_count': 0,
                'is_partitioned': False,
                'source_row_count': 0,
                'source_file_count': 0,
                'source_total_size_bytes': 0,
                'format_version': '2',
                'error': str(e)[:500],
            })

    return metadata_list


def create_dest_table(table_info, dest_db, spark, config):
    """Create an Iceberg table at dest_location and register data via add_files.

    - Table doesn't exist: CREATE TABLE + add_files
    - Table already exists: DROP TABLE + CREATE TABLE + add_files

    The table is always dropped and recreated because add_files rejects the
    entire import when any source file is already tracked (there is no
    skip-duplicates mode).  DROP + CREATE + add_files re-imports everything
    cleanly and handles both re-runs and incremental data loads.
    """
    tbl = table_info['source_table']
    dest_path = table_info['dest_location']
    full_name = f"{dest_db}.{tbl}"
    fmt = table_info.get('file_format', 'PARQUET')
    format_version = table_info.get('format_version', '2')
    schema = table_info.get('schema', [])
    partition_spec = table_info.get('partition_spec_detail', [])

    apply_bucket_credentials(
        spark, dest_path,
        config.get('_dest_endpoint', ''),
        config.get('_dest_access_key', ''),
        config.get('_dest_secret_key', ''),
    )

    try:
        exists = False
        existing_location = None
        try:
            for row in spark.sql(f"DESCRIBE FORMATTED {full_name}").collect():
                if (row.col_name or '').strip() == 'Location':
                    existing_location = (row.data_type or '').strip()
            exists = True
        except Exception:
            pass

        # Drop existing table so add_files can re-import all data files.
        # add_files rejects the entire import when ANY source file is already
        # tracked (check_duplicate_files has no skip mode), so the only way to
        # handle incremental data is DROP + CREATE + add_files.
        if exists:
            spark.sql(f"DROP TABLE {full_name}")
            logger.info(f"[iceberg_create] Dropped {full_name} (was at {existing_location})")

        # CREATE TABLE at dest
        col_defs = _build_column_defs(schema)
        if not col_defs:
            raise ValueError(f"No schema available for {full_name} — cannot create table")

        partition_clause = _build_iceberg_partition_clause(partition_spec)
        partitioned_by = f"PARTITIONED BY ({partition_clause})" if partition_clause else ""

        ddl = (
            f"CREATE TABLE {full_name} ({col_defs}) "
            f"USING iceberg "
            f"{partitioned_by} "
            f"LOCATION '{dest_path}' "
            f"TBLPROPERTIES ("
            f"'write.format.default' = '{fmt.lower()}', "
            f"'format-version' = '{format_version}')"
        )
        spark.sql(ddl)
        logger.info(f"[iceberg_create] CREATED: {full_name} at {dest_path}")

        # add_files
        data_path = _resolve_data_path(spark, dest_path)
        spark.sql(
            f"CALL spark_catalog.system.add_files("
            f"table => '{full_name}', "
            f"source_table => '`{fmt.lower()}`.`{data_path}`')"
        )
        logger.info(f"[iceberg_create] add_files complete for {full_name}")

        # Row and partition counts reflecting what add_files actually
        # imported. These become the authoritative source_* values for
        # validation — the discover-time metrics reflect only metadata.json-
        # tracked state, which is stale when incremental Parquet files sit
        # in data/ untracked by the source Iceberg metadata.
        imported_row_count = spark.sql(
            f"SELECT COUNT(*) as c FROM {full_name}"
        ).collect()[0]['c']

        imported_partition_count = 0
        if partition_spec:
            with contextlib.suppress(Exception):
                imported_partition_count = spark.sql(
                    f"SELECT * FROM {full_name}.partitions"
                ).count()

        logger.info(
            f"[iceberg_create] {full_name} imported "
            f"{imported_row_count} rows, {imported_partition_count} partitions"
        )

        return {
            'source_table': tbl,
            'status': 'COMPLETED',
            'existed': exists,
            'imported_row_count': imported_row_count,
            'imported_partition_count': imported_partition_count,
            'error': None,
        }

    except Exception as e:
        error_msg = str(e)[:2000]
        logger.error(f"[iceberg_create] FAILED for {full_name}: {error_msg}")
        return {'source_table': tbl, 'status': 'FAILED', 'existed': False, 'error': error_msg}
