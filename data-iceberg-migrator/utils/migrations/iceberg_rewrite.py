"""
Iceberg-to-Iceberg migration strategy using rewrite_table_path.

Source: Iceberg table whose data AND metadata have been pre-copied to S3.
        The metadata files still contain the original (source) S3 path prefix.
Destination: Iceberg table registered in HMS after in-place prefix rewrite.

How it works (pre-copy scenario):
  1. Temporarily register the table in HMS so the procedure can locate it.
  2. Call CALL spark_catalog.system.rewrite_table_path — rewrites every absolute
     path reference in the metadata from source_prefix to dest_prefix and writes
     the result to a staging directory.
  3. Drop the temporary HMS entry (still points to old metadata).
  4. Replace the metadata directory at the table location with the staged files.
  5. Delete the staging directory.
  6. Permanently register via CALL spark_catalog.system.register_table using
     the new metadata.json at the table location.

Advantages over CREATE TABLE + add_files:
  - Full snapshot history and time-travel preserved.
  - All partition transforms (year, month, bucket, truncate) preserved.
  - Exact schema fidelity — no DDL type translation.
  - Full table properties retained.

Requirements:
  - Both data and metadata files must be pre-copied to the destination S3 path.
  - The destination Spark/Iceberg environment must support rewrite_table_path
    (Apache Iceberg 1.4+).
  - source_s3_prefix and dest_s3_prefix must be provided in the Excel config.
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
    _match_tokens,
    _resolve_metadata_file,
)

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Hadoop FS helpers (staging cleanup)
# ─────────────────────────────────────────────────────────────────────────────

def _delete_hadoop_dir(spark, path: str) -> None:
    """Recursively delete a path via Hadoop FS. No-op if it does not exist."""
    from py4j.java_gateway import java_import

    java_import(spark._jvm, 'org.apache.hadoop.fs.*')
    conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jvm.java.net.URI(path), conf
    )
    path_obj = spark._jvm.org.apache.hadoop.fs.Path(path)
    if fs.exists(path_obj):
        fs.delete(path_obj, True)
        logger.info(f"[iceberg_rewrite] Deleted {path}")


def _copy_hadoop_dir(spark, src_dir: str, dest_dir: str) -> None:
    """Copy all files directly under src_dir into dest_dir (non-recursive)."""
    from py4j.java_gateway import java_import

    java_import(spark._jvm, 'org.apache.hadoop.fs.*')
    conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jvm.java.net.URI(src_dir), conf
    )

    src = spark._jvm.org.apache.hadoop.fs.Path(src_dir)
    dst = spark._jvm.org.apache.hadoop.fs.Path(dest_dir)
    fs.mkdirs(dst)

    items = fs.listStatus(src)
    for i in range(len(items)):
        item_path = items[i].getPath()
        dest_file = spark._jvm.org.apache.hadoop.fs.Path(
            f"{dest_dir}/{item_path.getName()}"
        )
        spark._jvm.org.apache.hadoop.fs.FileUtil.copy(
            fs, item_path, fs, dest_file, False, conf
        )
    logger.info(f"[iceberg_rewrite] Copied {src_dir} → {dest_dir}")


# ─────────────────────────────────────────────────────────────────────────────
# Strategy interface functions
# ─────────────────────────────────────────────────────────────────────────────

def parse_excel_rows(df, config, run_id):
    """Parse Excel rows for the rewrite_table_path migration strategy.

    Required columns: database, table, source_s3_prefix, dest_s3_prefix.
    The 'table' column supports a single name, a comma-separated list, or a
    wildcard (fnmatch pattern). Rows are grouped by
    (database, source_s3_prefix, dest_s3_prefix).
    """
    grouped = {}
    for _, row in df.iterrows():
        database = cell_str(row.get('database'))
        if not database:
            continue

        raw_table = cell_str(row.get('table'), '*')
        source_s3_prefix = normalize_s3(cell_str(row.get('source_s3_prefix')))
        dest_s3_prefix = normalize_s3(cell_str(row.get('dest_s3_prefix')))

        if not dest_s3_prefix:
            logger.warning(
                f"[parse_iceberg_rewrite_excel] Skipping '{database}' — "
                "missing 'dest_s3_prefix'"
            )
            continue

        if not source_s3_prefix:
            logger.warning(
                f"[parse_iceberg_rewrite_excel] Skipping '{database}' — "
                "missing 'source_s3_prefix'"
            )
            continue

        key = (database, source_s3_prefix.rstrip('/'), dest_s3_prefix.rstrip('/'))
        if key not in grouped:
            grouped[key] = {'tokens': []}

        for tok in raw_table.split(','):
            tok = tok.strip()
            if tok:
                grouped[key]['tokens'].append(tok)

    configs = []
    for (database, source_s3_prefix, dest_s3_prefix), group in grouped.items():
        unique_tokens = list(dict.fromkeys(group['tokens']))
        if '*' in unique_tokens:
            unique_tokens = ['*']

        configs.append({
            'source_database': database,
            'dest_database': database,
            'source_s3_prefix': source_s3_prefix,
            'dest_s3_prefix': dest_s3_prefix,
            'table_tokens': unique_tokens,
            'run_id': run_id,
        })
        logger.info(
            f"[parse_iceberg_rewrite_excel] {database} | "
            f"src={source_s3_prefix} | dest={dest_s3_prefix} | "
            f"tokens={unique_tokens[:5]}"
        )

    return configs


def discover_tables(db_config, spark, config):
    """Discover Iceberg tables at the destination and read their metadata.

    Always reads schema/partition spec from metadata.json rather than HMS
    because HMS may not have the table registered yet and would not preserve
    partition transform details (year, month, bucket, etc.).
    """
    from utils.migrations.shared import apply_bucket_credentials

    database = db_config['dest_database']
    dest_prefix = db_config['dest_s3_prefix']
    source_prefix = db_config['source_s3_prefix']
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
        f"[iceberg_rewrite_discover] '{database}': {len(available_tables)} table(s) found, "
        f"{len(matched)} matched tokens {tokens[:5]}"
    )

    metadata_list = []
    for tbl_name in matched:
        dest_path = f"{db_path}/{tbl_name}"
        full_name = f"{database}.{tbl_name}"

        try:
            from utils.migrations.shared import _read_iceberg_metadata

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
                f"[iceberg_rewrite_discover] {full_name} | fmt={file_format} | "
                f"rows={row_count} | size={total_size / (1024 ** 2):.1f}MB | "
                f"partitioned={is_partitioned}"
            )

            metadata_list.append({
                'source_database': database,
                'source_table': tbl_name,
                'dest_database': database,
                'source_location': dest_path,
                'dest_location': dest_path,
                'source_s3_prefix': source_prefix,
                'dest_s3_prefix': dest_prefix,
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
            logger.error(f"[iceberg_rewrite_discover] FAILED for {full_name}: {e}")
            metadata_list.append({
                'source_database': database,
                'source_table': tbl_name,
                'dest_database': database,
                'source_location': dest_path,
                'dest_location': '',
                'source_s3_prefix': source_prefix,
                'dest_s3_prefix': dest_prefix,
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
    """Register an Iceberg table at dest_location using rewrite_table_path.

    Pipeline (pre-copy scenario — data and metadata already at destination):
      1. Drop table from HMS if already registered.
      2. Register temporarily so rewrite_table_path can access the table.
      3. Call rewrite_table_path — writes rewritten metadata to a staging dir.
      4. Drop the temporary HMS registration (still points to old metadata).
      5. Delete old metadata directory at the table location.
      6. Copy rewritten metadata from staging into the table location.
      7. Delete staging directory.
      8. Register permanently via register_table using the new metadata.json.
    """
    from utils.migrations.shared import apply_bucket_credentials

    tbl = table_info['source_table']
    dest_path = table_info['dest_location']
    source_prefix = table_info.get('source_s3_prefix', '').rstrip('/')
    dest_prefix = table_info.get('dest_s3_prefix', '').rstrip('/')
    full_name = f"{dest_db}.{tbl}"
    staging_path = f"{dest_prefix}/{dest_db}/_staging_rewrite/{tbl}"

    apply_bucket_credentials(
        spark, dest_path,
        config.get('_dest_endpoint', ''),
        config.get('_dest_access_key', ''),
        config.get('_dest_secret_key', ''),
    )

    try:
        # Step 1: Drop from HMS if already registered
        exists = False
        try:
            for row in spark.sql(f"DESCRIBE FORMATTED {full_name}").collect():
                pass
            exists = True
        except Exception:
            pass

        if exists:
            spark.sql(f"DROP TABLE {full_name}")
            logger.info(f"[iceberg_rewrite] Dropped {full_name} from HMS")

        # Step 2: Temporary HMS registration so the procedure can find the table
        spark.sql(f"CREATE TABLE {full_name} USING iceberg LOCATION '{dest_path}'")
        logger.info(f"[iceberg_rewrite] Temporarily registered {full_name} at {dest_path}")

        # Step 3: Rewrite metadata — writes to staging, does NOT update HMS
        if not source_prefix or not dest_prefix:
            raise ValueError(
                f"source_s3_prefix and dest_s3_prefix are required for "
                f"iceberg_rewrite_table_path strategy (got: '{source_prefix}', '{dest_prefix}')"
            )

        logger.info(
            f"[iceberg_rewrite] rewrite_table_path: "
            f"'{source_prefix}' → '{dest_prefix}' | staging={staging_path}"
        )
        spark.sql(f"""
            CALL spark_catalog.system.rewrite_table_path(
                table => '{full_name}',
                source_prefix => '{source_prefix}',
                target_prefix => '{dest_prefix}',
                staging_location => '{staging_path}'
            )
        """)

        # Step 4: Drop temporary registration (HMS still points to old metadata)
        spark.sql(f"DROP TABLE {full_name}")

        # Step 5–7: Swap old metadata with staged metadata
        _delete_hadoop_dir(spark, f"{dest_path}/metadata")
        _copy_hadoop_dir(spark, f"{staging_path}/metadata", f"{dest_path}/metadata")
        _delete_hadoop_dir(spark, staging_path)
        logger.info(f"[iceberg_rewrite] Swapped metadata at {dest_path}/metadata")

        # Step 8: Permanent HMS registration via register_table
        new_metadata_file = _resolve_metadata_file(spark, dest_path)
        spark.sql(f"""
            CALL spark_catalog.system.register_table(
                table => '{full_name}',
                metadata_file => '{new_metadata_file}'
            )
        """)
        logger.info(f"[iceberg_rewrite] Registered {full_name} via {new_metadata_file}")

        # Validate counts
        imported_row_count = spark.sql(
            f"SELECT COUNT(*) as c FROM {full_name}"
        ).collect()[0]['c']

        imported_partition_count = 0
        if table_info.get('partition_spec_detail'):
            with contextlib.suppress(Exception):
                imported_partition_count = spark.sql(
                    f"SELECT * FROM {full_name}.partitions"
                ).count()

        logger.info(
            f"[iceberg_rewrite] {full_name} registered: "
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
        logger.error(f"[iceberg_rewrite] FAILED for {full_name}: {error_msg}")
        # Clean up staging on failure to avoid leaving orphaned files
        with contextlib.suppress(Exception):
            _delete_hadoop_dir(spark, staging_path)
        return {
            'source_table': tbl,
            'status': 'FAILED',
            'existed': False,
            'error': error_msg,
        }
