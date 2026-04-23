"""
DAG 5: Iceberg Rewrite Table Path Migration

Iceberg-to-Iceberg migration using the rewrite_table_path procedure.
Use this DAG when data AND metadata have already been copied to the destination
S3 bucket and snapshot history / partition transform fidelity must be preserved.

Strategy: iceberg_rewrite_table_path
  - Reads existing Iceberg metadata at the destination (which still references
    old source S3 paths) and rewrites every path prefix via the Iceberg
    rewrite_table_path stored procedure.
  - Registers the rewritten table in HMS via register_table.
  - Preserves full snapshot history, time-travel, all partition transforms
    (year, month, bucket, truncate), exact schema types, and table properties.

When to use this DAG vs DAG 4:
  - Use DAG 5 (this DAG) when: metadata is pre-copied to destination, snapshot
    history is required, tables use non-identity partition transforms, or schema
    contains complex types (struct, list, map).
  - Use DAG 4 when: only Parquet data files are copied (no metadata), tables
    use identity partitioning only, or snapshot history is not needed.

Requirements:
  - Both data AND metadata files must be present at the destination S3 path.
  - Destination Spark/Iceberg environment must support rewrite_table_path
    (Apache Iceberg 1.4+).

Pipeline stages:
  1. Init tracking tables & create run record
  2. Parse Excel config (database, table tokens, source_s3_prefix, dest_s3_prefix)
  3. Discover source table metadata (schema, partitions, row counts from metadata.json)
  4. Validate data presence at destination S3 paths
  5. Create destination tables (rewrite_table_path + register_table)
  6. Validate destination tables (row count, partition, schema comparison)
  7. Generate HTML report & send email

Excel columns: database | table | source_s3_prefix | dest_s3_prefix
"""

import contextlib
import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from dotenv import load_dotenv
from utils.migrations.shared import (
    apply_bucket_credentials,
    configure_spark_s3,
    execute_with_iceberg_retry,
    get_config,
    track_duration,
)

_dag_stem = Path(__file__).stem
logger = logging.getLogger(__name__)

_dag_dir = Path(__file__).resolve().parent
_config_dir = str(_dag_dir / 'utils' / 'migration_configs')
if os.path.isdir(_config_dir):
    load_dotenv(os.path.join(_config_dir, 'env.shared'))
    load_dotenv(os.path.join(_config_dir, f'env.{_dag_stem}'), override=True)
else:
    logger.warning(
        f"Config directory {_config_dir} not found — "
        "env files not loaded, using Airflow Variables / defaults"
    )

default_args = {
    'owner': 'data-migration',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


# =============================================================================
# DAG 5: ICEBERG REWRITE TABLE PATH MIGRATION TASKS
# =============================================================================

@task.pyspark(conn_id='spark_default')
def init_rewrite_tracking_tables(spark) -> dict:
    """Create Iceberg tracking tables for the rewrite migration if they don't exist."""
    config = get_config()
    tracking_db = config['tracking_database']
    tracking_loc = config['tracking_location']

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {tracking_db} LOCATION '{tracking_loc}'")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tracking_db}.rewrite_migration_runs (
            run_id              STRING,
            dag_run_id          STRING,
            excel_file_path     STRING,
            started_at          TIMESTAMP,
            completed_at        TIMESTAMP,
            status              STRING,
            total_tables        INT,
            successful_tables   INT,
            failed_tables       INT,
            missing_tables      INT,
            config_json         STRING
        )
        USING iceberg
        LOCATION '{tracking_loc}/rewrite_migration_runs'
    """)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tracking_db}.rewrite_migration_table_status (
            run_id                      STRING,
            source_database             STRING,
            source_table                STRING,
            dest_database               STRING,
            dest_bucket                 STRING,
            source_s3_location          STRING,
            dest_s3_location            STRING,
            source_s3_prefix            STRING,
            dest_s3_prefix              STRING,
            file_format                 STRING,
            is_partitioned              BOOLEAN,
            partition_columns           STRING,
            partition_count             INT,
            schema_json                 STRING,
            partitions_json             STRING,
            table_type                  STRING,
            source_row_count            BIGINT,
            source_file_count           BIGINT,
            source_total_size_bytes     BIGINT,
            data_presence_status        STRING,
            data_presence_checked_at    TIMESTAMP,
            data_presence_file_count    BIGINT,
            data_presence_size_bytes    BIGINT,
            discovery_status            STRING,
            discovery_completed_at      TIMESTAMP,
            discovery_duration_seconds  DOUBLE,
            table_create_status             STRING,
            table_create_completed_at       TIMESTAMP,
            table_create_duration_seconds   DOUBLE,
            table_already_existed           BOOLEAN,
            validation_status               STRING,
            validation_completed_at         TIMESTAMP,
            validation_duration_seconds     DOUBLE,
            dest_hive_row_count             BIGINT,
            dest_partition_count            INT,
            source_partition_count          INT,
            row_count_match                 BOOLEAN,
            partition_count_match           BOOLEAN,
            schema_match                    BOOLEAN,
            schema_differences              STRING,
            overall_status  STRING,
            error_message   STRING,
            updated_at      TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (source_database)
        LOCATION '{tracking_loc}/rewrite_migration_table_status'
    """)

    logger.info(f"[init_rewrite_tracking_tables] Tracking tables ready in '{tracking_db}'")
    return {'status': 'initialized', 'database': tracking_db}


@task.pyspark(conn_id='spark_default')
def create_rewrite_migration_run(excel_file_path: str, dag_run_id: str, spark) -> str:
    """Create run record and return run_id."""
    import uuid
    config = get_config()
    tracking_db = config['tracking_database']
    run_id = f"rewrite_run_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

    spark.sql(f"""
        INSERT INTO {tracking_db}.rewrite_migration_runs
        VALUES (
            '{run_id}', '{dag_run_id}', '{excel_file_path}',
            current_timestamp(), NULL, 'RUNNING',
            0, 0, 0, 0,
            '{json.dumps(config).replace("'", "''")}'
        )
    """)
    logger.info(f"[create_rewrite_migration_run] Run created: {run_id}")
    return run_id


@task.pyspark(conn_id='spark_default')
def parse_rewrite_excel(excel_file_path: str, run_id: str, spark) -> list:
    """Read Excel config and parse rows for the rewrite_table_path migration.

    Required columns: database, table, source_s3_prefix, dest_s3_prefix.
    Rows are grouped by (database, source_s3_prefix, dest_s3_prefix).
    """
    from io import BytesIO

    import pandas as ps
    from utils.migrations.shared import cell_str, normalize_s3

    binary_df = spark.read.format("binaryFile").load(excel_file_path)
    row = binary_df.select("content").first()
    excel_bytes = bytes(row.content)
    df = ps.read_excel(BytesIO(excel_bytes), engine='openpyxl')
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

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
                f"[parse_rewrite_excel] Skipping '{database}' — "
                "missing 'dest_s3_prefix'"
            )
            continue

        if not source_s3_prefix:
            logger.warning(
                f"[parse_rewrite_excel] Skipping '{database}' — "
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
            f"[parse_rewrite_excel] {database} | "
            f"src={source_s3_prefix} | dest={dest_s3_prefix} | "
            f"tokens={unique_tokens[:5]}"
        )

    if not configs:
        logger.error("[parse_rewrite_excel] No valid rows found in Excel config")
        return []

    logger.info(f"[parse_rewrite_excel] Emitting {len(configs)} database config(s)")
    return configs


@task.pyspark(conn_id='spark_default')
@track_duration
def discover_rewrite_tables(db_config: dict, spark, **context) -> dict:
    """Discover Iceberg tables at the destination and read their metadata.

    Always reads schema/partition spec from metadata.json rather than HMS
    because HMS may not have the table registered yet and would not preserve
    partition transform details (year, month, bucket, etc.).
    """
    from utils.migrations.shared import (
        _extract_partition_spec,
        _extract_row_count,
        _extract_schema,
        _get_fs_stats,
        _list_iceberg_tables,
        _match_tokens,
        _read_iceberg_metadata,
    )

    config = get_config()
    configure_spark_s3(spark, config)

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
        f"[discover_rewrite_tables] '{database}': {len(available_tables)} table(s) found, "
        f"{len(matched)} matched tokens {tokens[:5]}"
    )

    metadata_list = []
    for tbl_name in matched:
        dest_path = f"{db_path}/{tbl_name}"
        full_name = f"{database}.{tbl_name}"

        try:
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
                f"[discover_rewrite_tables] {full_name} | fmt={file_format} | "
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
            logger.error(f"[discover_rewrite_tables] FAILED for {full_name}: {e}")
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

    result_dict = {
        'run_id': db_config['run_id'],
        'source_database': db_config['source_database'],
        'dest_database': db_config['dest_database'],
        'dest_bucket': db_config.get('dest_bucket', ''),
        'dest_s3_prefix': db_config.get('dest_s3_prefix', ''),
        'source_s3_prefix': db_config.get('source_s3_prefix', ''),
        'tables': metadata_list,
    }

    failed = [t for t in metadata_list if 'error' in t]
    if failed:
        context['ti'].xcom_push(key='return_value', value=result_dict)
        raise Exception(
            f"Discovery failed for {len(failed)}/{len(metadata_list)} table(s) in "
            f"'{database}': "
            + ', '.join(t['source_table'] for t in failed[:3])
        )

    return result_dict


@task.pyspark(conn_id='spark_default')
def record_rewrite_discovered_tables(discovery: dict, spark) -> dict:
    """Persist discovered table metadata into the rewrite_migration_table_status table."""
    if not isinstance(discovery, dict) or 'tables' not in discovery:
        logger.warning(f"[record_rewrite_discovered_tables] Skipping invalid input: {type(discovery)}")
        return {}

    config = get_config()
    configure_spark_s3(spark, config)
    tracking_db = config['tracking_database']
    run_id = discovery['run_id']
    duration = discovery.get('_task_duration', 0.0)

    for t in discovery['tables']:
        schema_json = json.dumps(t.get('schema', [])).replace("'", "''")
        parts_json = json.dumps(t.get('partitions', [])).replace("'", "''")

        existing = spark.sql(f"""
            SELECT COUNT(*) as cnt
            FROM {tracking_db}.rewrite_migration_table_status
            WHERE run_id = '{run_id}'
              AND source_database = '{t['source_database']}'
              AND source_table = '{t['source_table']}'
        """).collect()[0]['cnt']

        if existing > 0:
            execute_with_iceberg_retry(spark, f"""
                UPDATE {tracking_db}.rewrite_migration_table_status
                SET discovery_status = 'COMPLETED',
                    discovery_completed_at = current_timestamp(),
                    discovery_duration_seconds = {duration},
                    source_s3_location = '{t['source_location']}',
                    dest_s3_location = '{t['dest_location']}',
                    file_format = '{t['file_format']}',
                    table_type = '{t['table_type']}',
                    source_row_count = {t['source_row_count']},
                    source_file_count = {t['source_file_count']},
                    source_total_size_bytes = {t['source_total_size_bytes']},
                    partition_count = {t['partition_count']},
                    source_partition_count = {t['partition_count']},
                    updated_at = current_timestamp()
                WHERE run_id = '{run_id}'
                  AND source_database = '{t['source_database']}'
                  AND source_table = '{t['source_table']}'
            """, task_label=f"record_rewrite_discovered_tables:{t['source_table']}")
        else:
            execute_with_iceberg_retry(spark, f"""
                INSERT INTO {tracking_db}.rewrite_migration_table_status (
                    run_id, source_database, source_table, dest_database, dest_bucket,
                    source_s3_location, dest_s3_location,
                    source_s3_prefix, dest_s3_prefix,
                    file_format, is_partitioned, partition_columns, partition_count,
                    schema_json, partitions_json, table_type,
                    source_row_count, source_file_count, source_total_size_bytes,
                    data_presence_status, data_presence_checked_at,
                    data_presence_file_count, data_presence_size_bytes,
                    discovery_status, discovery_completed_at, discovery_duration_seconds,
                    table_create_status, table_create_completed_at, table_create_duration_seconds,
                    table_already_existed,
                    validation_status, validation_completed_at, validation_duration_seconds,
                    dest_hive_row_count, dest_partition_count, source_partition_count,
                    row_count_match, partition_count_match, schema_match, schema_differences,
                    overall_status, error_message, updated_at
                ) VALUES (
                    '{run_id}', '{t['source_database']}', '{t['source_table']}',
                    '{t['dest_database']}', '{t.get('dest_bucket', '')}',
                    '{t['source_location']}', '{t['dest_location']}',
                    '{t.get('source_s3_prefix', '')}', '{t.get('dest_s3_prefix', '')}',
                    '{t['file_format']}',
                    {str(t['is_partitioned']).lower()}, '{t['partition_columns']}', {t['partition_count']},
                    '{schema_json}', '{parts_json}', '{t['table_type']}',
                    {t['source_row_count']}, {t['source_file_count']}, {t['source_total_size_bytes']},
                    NULL, NULL, NULL, NULL,
                    'COMPLETED', current_timestamp(), {duration},
                    NULL, NULL, NULL, NULL,
                    NULL, NULL, NULL,
                    NULL, NULL, {t['partition_count']},
                    NULL, NULL, NULL, NULL,
                    'DISCOVERED', NULL, current_timestamp()
                )
            """, task_label=f"record_rewrite_discovered_tables:insert:{t['source_table']}")

    return discovery


@task.pyspark(conn_id='spark_default')
def validate_rewrite_data_presence(discovery: dict, spark, **context) -> dict:
    """Check that destination S3 paths contain both data and metadata files."""
    if not isinstance(discovery, dict) or 'tables' not in discovery:
        logger.warning(f"[validate_rewrite_data_presence] Skipping invalid input: {type(discovery)}")
        return {}

    config = get_config()
    configure_spark_s3(spark, config)

    results = []
    for t in discovery['tables']:
        dest_path = t.get('dest_location', '')
        tbl = t['source_table']
        src_db = t['source_database']

        if not dest_path:
            results.append({
                'source_database': src_db, 'source_table': tbl,
                'status': 'MISSING', 'file_count': 0, 'size_bytes': 0,
                'error': 'No destination S3 path computed',
            })
            continue

        try:
            from py4j.java_gateway import java_import
            java_import(spark._jvm, 'org.apache.hadoop.fs.*')

            apply_bucket_credentials(
                spark, dest_path,
                config.get('_dest_endpoint', ''),
                config.get('_dest_access_key', ''),
                config.get('_dest_secret_key', ''),
            )

            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark._jvm.java.net.URI(dest_path),
                spark._jsc.hadoopConfiguration()
            )
            path_obj = spark._jvm.org.apache.hadoop.fs.Path(dest_path)

            if not fs.exists(path_obj):
                results.append({
                    'source_database': src_db, 'source_table': tbl,
                    'status': 'MISSING', 'file_count': 0, 'size_bytes': 0,
                    'error': f'Destination path does not exist: {dest_path}',
                })
                continue

            # Verify metadata directory is present (required for rewrite_table_path)
            metadata_path = spark._jvm.org.apache.hadoop.fs.Path(f"{dest_path}/metadata")
            if not fs.exists(metadata_path):
                results.append({
                    'source_database': src_db, 'source_table': tbl,
                    'status': 'MISSING', 'file_count': 0, 'size_bytes': 0,
                    'error': (
                        f'Metadata directory missing at {dest_path}/metadata — '
                        'rewrite_table_path requires pre-copied metadata files'
                    ),
                })
                continue

            summary = fs.getContentSummary(path_obj)
            file_count = int(summary.getFileCount())
            size_bytes = int(summary.getLength())

            if file_count == 0:
                results.append({
                    'source_database': src_db, 'source_table': tbl,
                    'status': 'MISSING', 'file_count': 0, 'size_bytes': 0,
                    'error': f'Destination path exists but contains 0 files: {dest_path}',
                })
            else:
                logger.info(
                    f"[validate_rewrite_data_presence] CONFIRMED: {src_db}.{tbl} | "
                    f"files={file_count} | size={size_bytes / (1024 ** 2):.1f}MB"
                )
                results.append({
                    'source_database': src_db, 'source_table': tbl,
                    'status': 'CONFIRMED', 'file_count': file_count,
                    'size_bytes': size_bytes, 'error': None,
                })

        except Exception as e:
            logger.error(f"[validate_rewrite_data_presence] FAILED for {src_db}.{tbl}: {e}")
            results.append({
                'source_database': src_db, 'source_table': tbl,
                'status': 'FAILED', 'file_count': 0, 'size_bytes': 0,
                'error': str(e)[:500],
            })

    failed = [r for r in results if r['status'] == 'FAILED']
    result_dict = {**discovery, 'presence_results': results}

    if failed:
        context['ti'].xcom_push(key='return_value', value=result_dict)
        raise Exception(
            f"[validate_rewrite_data_presence] Data presence check FAILED for "
            f"{len(failed)}/{len(results)} table(s) in '{discovery['source_database']}'"
        )

    return result_dict


@task.pyspark(conn_id='spark_default')
def update_rewrite_data_presence_status(presence_result: dict, spark) -> dict:
    """Update tracking table with data presence check results."""
    if not isinstance(presence_result, dict) or 'run_id' not in presence_result:
        logger.warning("[update_rewrite_data_presence_status] Skipping invalid input")
        return {}

    config = get_config()
    tracking_db = config['tracking_database']
    run_id = presence_result['run_id']
    src_db = presence_result.get('source_database', '')

    for r in presence_result.get('presence_results', []):
        overall = {
            'CONFIRMED': 'DATA_CONFIRMED',
            'MISSING': 'DATA_MISSING',
            'FAILED': 'FAILED',
        }.get(r['status'], 'FAILED')
        error_msg = (r.get('error') or '').replace("'", "''")[:2000]

        execute_with_iceberg_retry(spark, f"""
            UPDATE {tracking_db}.rewrite_migration_table_status
            SET data_presence_status = '{r['status']}',
                data_presence_checked_at = current_timestamp(),
                data_presence_file_count = {r['file_count']},
                data_presence_size_bytes = {r['size_bytes']},
                overall_status = '{overall}',
                error_message = CASE WHEN '{r['status']}' != 'CONFIRMED'
                                     THEN '{error_msg}'
                                     ELSE error_message END,
                updated_at = current_timestamp()
            WHERE run_id = '{run_id}'
              AND source_database = '{r['source_database']}'
              AND source_table = '{r['source_table']}'
        """, task_label=f"update_rewrite_data_presence_status:{r['source_table']}")

    if src_db:
        execute_with_iceberg_retry(spark, f"""
            UPDATE {tracking_db}.rewrite_migration_table_status
            SET data_presence_status = 'FAILED',
                overall_status = 'FAILED',
                error_message = COALESCE(error_message, 'Data presence check did not process this table'),
                updated_at = current_timestamp()
            WHERE run_id = '{run_id}'
              AND source_database = '{src_db}'
              AND discovery_status = 'COMPLETED'
              AND data_presence_status IS NULL
        """, task_label="update_rewrite_data_presence_status:catchall")

    return presence_result


@task.pyspark(conn_id='spark_default')
@track_duration
def create_rewrite_dest_tables(presence_result: dict, spark, **context) -> dict:
    """Register destination tables using rewrite_table_path + register_table.

    Pipeline per table:
      1. Drop from HMS if already registered.
      2. Temporarily register so rewrite_table_path can locate the table.
      3. Call rewrite_table_path — rewrites path prefixes into staging dir.
      4. Drop temporary registration.
      5. Delete old metadata directory at table location.
      6. Copy rewritten metadata from staging into table location.
      7. Delete staging directory.
      8. Permanently register via register_table using the new metadata file.
    """
    from utils.migrations.shared import _resolve_metadata_file

    if not isinstance(presence_result, dict) or 'tables' not in presence_result:
        logger.warning("[create_rewrite_dest_tables] Skipping invalid input")
        return {}

    def _delete_hadoop_dir(path):
        from py4j.java_gateway import java_import
        java_import(spark._jvm, 'org.apache.hadoop.fs.*')
        conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(path), conf
        )
        path_obj = spark._jvm.org.apache.hadoop.fs.Path(path)
        if fs.exists(path_obj):
            fs.delete(path_obj, True)
            logger.info(f"[create_rewrite_dest_tables] Deleted {path}")

    def _copy_hadoop_dir(src_dir, dest_dir):
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
        logger.info(f"[create_rewrite_dest_tables] Copied {src_dir} → {dest_dir}")

    config = get_config()
    configure_spark_s3(spark, config)
    dest_db = presence_result['dest_database']
    dest_prefix = presence_result.get('dest_s3_prefix', '').rstrip('/')
    source_prefix = presence_result.get('source_s3_prefix', '').rstrip('/')
    tables = presence_result['tables']

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {dest_db}")

    presence_map = {
        r['source_table']: r
        for r in presence_result.get('presence_results', [])
    }

    results = []
    for t in tables:
        tbl = t['source_table']
        p_status = presence_map.get(tbl, {}).get('status', 'UNKNOWN')

        if p_status != 'CONFIRMED':
            logger.info(
                f"[create_rewrite_dest_tables] Skipping {dest_db}.{tbl}, "
                f"data_presence={p_status}"
            )
            results.append({
                'source_table': tbl,
                'status': 'SKIPPED',
                'existed': False,
                'error': f'Data not present at destination (status={p_status})',
            })
            continue

        dest_path = t['dest_location']
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
                for _ in spark.sql(f"DESCRIBE FORMATTED {full_name}").collect():
                    pass
                exists = True
            except Exception:
                pass

            if exists:
                spark.sql(f"DROP TABLE {full_name}")
                logger.info(f"[create_rewrite_dest_tables] Dropped {full_name} from HMS")

            # Step 2: Temporary HMS registration
            spark.sql(
                f"CREATE TABLE {full_name} USING iceberg LOCATION '{dest_path}'"
            )
            logger.info(
                f"[create_rewrite_dest_tables] Temporarily registered "
                f"{full_name} at {dest_path}"
            )

            # Step 3: Rewrite metadata to staging
            if not source_prefix or not dest_prefix:
                raise ValueError(
                    f"source_s3_prefix and dest_s3_prefix are required "
                    f"(got: '{source_prefix}', '{dest_prefix}')"
                )

            logger.info(
                f"[create_rewrite_dest_tables] rewrite_table_path: "
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

            # Step 4: Drop temporary registration
            spark.sql(f"DROP TABLE {full_name}")

            # Steps 5–7: Swap old metadata with rewritten metadata
            _delete_hadoop_dir(f"{dest_path}/metadata")
            _copy_hadoop_dir(f"{staging_path}/metadata", f"{dest_path}/metadata")
            _delete_hadoop_dir(staging_path)
            logger.info(
                f"[create_rewrite_dest_tables] Swapped metadata at {dest_path}/metadata"
            )

            # Step 8: Permanent HMS registration
            new_metadata_file = _resolve_metadata_file(spark, dest_path)
            spark.sql(f"""
                CALL spark_catalog.system.register_table(
                    table => '{full_name}',
                    metadata_file => '{new_metadata_file}'
                )
            """)
            logger.info(
                f"[create_rewrite_dest_tables] Registered {full_name} "
                f"via {new_metadata_file}"
            )

            imported_row_count = spark.sql(
                f"SELECT COUNT(*) as c FROM {full_name}"
            ).collect()[0]['c']

            imported_partition_count = 0
            if t.get('partition_spec_detail'):
                with contextlib.suppress(Exception):
                    imported_partition_count = spark.sql(
                        f"SELECT * FROM {full_name}.partitions"
                    ).count()

            logger.info(
                f"[create_rewrite_dest_tables] {full_name} registered: "
                f"{imported_row_count} rows, {imported_partition_count} partitions"
            )

            results.append({
                'source_table': tbl,
                'status': 'COMPLETED',
                'existed': exists,
                'imported_row_count': imported_row_count,
                'imported_partition_count': imported_partition_count,
                'error': None,
            })

        except Exception as e:
            error_msg = str(e)[:2000]
            logger.error(f"[create_rewrite_dest_tables] FAILED for {full_name}: {error_msg}")
            with contextlib.suppress(Exception):
                _delete_hadoop_dir(staging_path)
            results.append({
                'source_table': tbl,
                'status': 'FAILED',
                'existed': False,
                'error': error_msg,
            })

    failed = [r for r in results if r['status'] == 'FAILED']
    result_dict = {**presence_result, 'table_results': results, '_has_failures': bool(failed)}
    context['ti'].xcom_push(key='return_value', value=result_dict)

    if failed:
        raise Exception(
            f"Table creation failed for {len(failed)}/{len(results)} "
            f"table(s) in '{dest_db}'"
        )
    return result_dict


@task.pyspark(conn_id='spark_default')
def update_rewrite_table_create_status(table_result: dict, spark) -> dict:
    """Update tracking table with table creation results."""
    if not isinstance(table_result, dict) or 'run_id' not in table_result:
        logger.warning("[update_rewrite_table_create_status] Skipping invalid input")
        return {}

    config = get_config()
    tracking_db = config['tracking_database']
    run_id = table_result['run_id']
    dest_db = table_result['dest_database']
    src_db = table_result['source_database']
    table_dur = table_result.get('_task_duration', 0.0)

    for r in table_result.get('table_results', []):
        overall = {
            'COMPLETED': 'TABLE_CREATED',
            'SKIPPED': 'DATA_MISSING',
            'FAILED': 'FAILED',
        }.get(r['status'], 'FAILED')
        error_msg = (r.get('error') or '').replace("'", "''")[:2000]

        imported_rc = r.get('imported_row_count')
        imported_pc = r.get('imported_partition_count')
        metric_updates = []
        if imported_rc is not None:
            metric_updates.append(f"source_row_count = {int(imported_rc)}")
        if imported_pc is not None:
            metric_updates.append(f"source_partition_count = {int(imported_pc)}")
        metric_sql = ",".join(metric_updates) + "," if metric_updates else ""

        execute_with_iceberg_retry(spark, f"""
            UPDATE {tracking_db}.rewrite_migration_table_status
            SET table_create_status = '{r['status']}',
                table_create_completed_at = current_timestamp(),
                table_create_duration_seconds = {table_dur},
                table_already_existed = {str(r.get('existed', False)).lower()},
                {metric_sql}
                overall_status = CASE
                    WHEN overall_status = 'FAILED' THEN overall_status
                    WHEN overall_status = 'DATA_MISSING' THEN overall_status
                    ELSE '{overall}'
                END,
                error_message = CASE
                    WHEN '{r['status']}' = 'FAILED' THEN '{error_msg}'
                    ELSE error_message
                END,
                updated_at = current_timestamp()
            WHERE run_id = '{run_id}'
              AND dest_database = '{dest_db}'
              AND source_table = '{r['source_table']}'
        """, task_label=f"update_rewrite_table_create_status:{r['source_table']}")

    execute_with_iceberg_retry(spark, f"""
        UPDATE {tracking_db}.rewrite_migration_table_status
        SET table_create_status = 'FAILED',
            overall_status = 'FAILED',
            error_message = COALESCE(error_message, 'Table creation task did not process this table'),
            updated_at = current_timestamp()
        WHERE run_id = '{run_id}'
          AND source_database = '{src_db}'
          AND table_create_status IS NULL
          AND data_presence_status = 'CONFIRMED'
    """, task_label="update_rewrite_table_create_status:catchall")

    return table_result


@task.pyspark(conn_id='spark_default')
@track_duration
def validate_rewrite_destination_tables(table_result: dict, spark, **context) -> dict:
    """Validate destination tables — row counts, partition counts, schema comparison."""
    if not isinstance(table_result, dict) or 'tables' not in table_result:
        logger.warning("[validate_rewrite_destination_tables] Skipping invalid input")
        return {}

    config = get_config()
    configure_spark_s3(spark, config)
    tracking_db = config['tracking_database']
    run_id = table_result['run_id']
    src_db = table_result['source_database']
    dest_db = table_result['dest_database']

    validation_results = []

    for t in table_result['tables']:
        tbl = t['source_table']
        dest_tbl = f"{dest_db}.{tbl}"

        upstream = spark.sql(f"""
            SELECT table_create_status, data_presence_status, overall_status
            FROM {tracking_db}.rewrite_migration_table_status
            WHERE run_id = '{run_id}'
              AND source_database = '{src_db}'
              AND source_table = '{tbl}'
        """).collect()

        if upstream:
            row = upstream[0]
            if row['table_create_status'] in ('FAILED', 'SKIPPED') or row['data_presence_status'] != 'CONFIRMED':
                validation_results.append({
                    'source_table': tbl, 'status': 'SKIPPED',
                    'error': f"Skipped — upstream status: {row['overall_status']}",
                })
                continue

        logger.info(f"[validate_rewrite_destination_tables] Validating {dest_tbl}")

        try:
            src_metrics = spark.sql(f"""
                SELECT source_row_count, source_partition_count
                FROM {tracking_db}.rewrite_migration_table_status
                WHERE run_id = '{run_id}'
                  AND source_database = '{src_db}'
                  AND source_table = '{tbl}'
            """).collect()

            if not src_metrics:
                validation_results.append({
                    'source_table': tbl, 'status': 'SKIPPED',
                    'error': 'Source metrics not found in tracking table',
                })
                continue

            src_row_count = src_metrics[0]['source_row_count'] or 0
            src_partition_count = src_metrics[0]['source_partition_count'] or t.get('partition_count', 0)

            dest_row_count = spark.sql(
                f"SELECT COUNT(*) as c FROM {dest_tbl}"
            ).collect()[0]['c']

            dest_partition_count = 0
            with contextlib.suppress(Exception):
                dest_partition_count = spark.sql(
                    f"SELECT * FROM {dest_tbl}.partitions"
                ).count()

            src_schema = {c['name'].lower(): c['type'].lower() for c in t.get('schema', [])}
            dest_schema = {
                r.col_name.lower(): (r.data_type or '').lower()
                for r in spark.sql(f"DESCRIBE {dest_tbl}").collect()
                if r.col_name and not r.col_name.startswith('#')
            }

            schema_match = True
            schema_diffs = []
            for cn, ct in src_schema.items():
                if cn not in dest_schema:
                    schema_match = False
                    schema_diffs.append(f"Missing column: {cn}")
                elif dest_schema[cn] != ct:
                    schema_match = False
                    schema_diffs.append(f"Type mismatch {cn}: source={ct} dest={dest_schema[cn]}")
            for cn in dest_schema:
                if cn not in src_schema:
                    schema_match = False
                    schema_diffs.append(f"Extra column in dest: {cn}")

            row_count_match = (src_row_count == dest_row_count)
            partition_count_match = (
                src_partition_count == dest_partition_count
                if src_partition_count > 0 else True
            )

            match_str = (
                f"rows={'✓' if row_count_match else '✗'} "
                f"parts={'✓' if partition_count_match else '⚠'} "
                f"schema={'✓' if schema_match else '✗'}"
            )
            logger.info(f"[validate_rewrite_destination_tables] {dest_tbl} | {match_str}")

            mismatch_parts = []
            if not row_count_match:
                mismatch_parts.append(f"Row count mismatch: source={src_row_count} dest={dest_row_count}")
            if not partition_count_match:
                mismatch_parts.append(f"Partition mismatch: source={src_partition_count} dest={dest_partition_count}")
            if not schema_match:
                mismatch_parts.append(f"Schema differences: {'; '.join(schema_diffs[:3])}")

            validation_results.append({
                'source_table': tbl,
                'status': 'COMPLETED',
                'source_row_count': src_row_count,
                'dest_hive_row_count': dest_row_count,
                'source_partition_count': src_partition_count,
                'dest_partition_count': dest_partition_count,
                'row_count_match': row_count_match,
                'partition_count_match': partition_count_match,
                'schema_match': schema_match,
                'schema_differences': '; '.join(schema_diffs),
                'error': '; '.join(mismatch_parts) if mismatch_parts else None,
            })

        except Exception as e:
            logger.error(f"[validate_rewrite_destination_tables] FAILED for {dest_tbl}: {e}")
            validation_results.append({
                'source_table': tbl, 'status': 'FAILED', 'error': str(e)[:2000],
            })

    failed = [v for v in validation_results if v['status'] == 'FAILED']
    result_dict = {**table_result, 'validation_results': validation_results, '_has_failures': bool(failed)}
    context['ti'].xcom_push(key='return_value', value=result_dict)

    if failed:
        raise Exception(
            f"Validation failed for {len(failed)}/{len(validation_results)} table(s)"
        )
    return result_dict


@task.pyspark(conn_id='spark_default')
def update_rewrite_validation_status(validation_result: dict, spark) -> dict:
    """Update tracking table with validation results."""
    if not isinstance(validation_result, dict) or 'run_id' not in validation_result:
        logger.warning("[update_rewrite_validation_status] Skipping invalid input")
        return {}

    config = get_config()
    configure_spark_s3(spark, config)
    tracking_db = config['tracking_database']
    run_id = validation_result['run_id']
    src_db = validation_result['source_database']
    dest_db = validation_result['dest_database']
    val_dur = validation_result.get('_task_duration', 0.0)

    for v in validation_result.get('validation_results', []):
        if v['status'] != 'COMPLETED':
            continue

        schema_diffs = (v.get('schema_differences') or '').replace("'", "''")[:2000]
        error_msg = (v.get('error') or '').replace("'", "''")[:2000]

        is_validated = (
            v.get('row_count_match', False) and
            v.get('partition_count_match', False) and
            v.get('schema_match', False)
        )
        final_status = 'VALIDATED' if is_validated else 'VALIDATION_FAILED'
        error_message_sql = 'NULL' if is_validated else f"'{error_msg}'"

        execute_with_iceberg_retry(spark, f"""
            UPDATE {tracking_db}.rewrite_migration_table_status
            SET validation_status = '{v['status']}',
                validation_completed_at = current_timestamp(),
                validation_duration_seconds = {val_dur},
                dest_hive_row_count = {v.get('dest_hive_row_count', 0)},
                dest_partition_count = {v.get('dest_partition_count', 0)},
                source_partition_count = {v.get('source_partition_count', 0)},
                row_count_match = {str(v.get('row_count_match', False)).lower()},
                partition_count_match = {str(v.get('partition_count_match', False)).lower()},
                schema_match = {str(v.get('schema_match', False)).lower()},
                schema_differences = '{schema_diffs}',
                overall_status = CASE
                    WHEN overall_status = 'FAILED' THEN overall_status
                    ELSE '{final_status}'
                END,
                error_message = CASE
                    WHEN overall_status = 'FAILED' THEN error_message
                    ELSE {error_message_sql}
                END,
                updated_at = current_timestamp()
            WHERE run_id = '{run_id}'
              AND source_database = '{src_db}'
              AND dest_database = '{dest_db}'
              AND source_table = '{v['source_table']}'
        """, task_label=f"update_rewrite_validation_status:{v['source_table']}")

    for v in validation_result.get('validation_results', []):
        if v.get('status') == 'FAILED' and v.get('error'):
            per_err = str(v['error'])[:2000].replace("'", "''")
            execute_with_iceberg_retry(spark, f"""
                UPDATE {tracking_db}.rewrite_migration_table_status
                SET validation_status = 'FAILED',
                    overall_status = 'VALIDATION_FAILED',
                    error_message = '{per_err}',
                    updated_at = current_timestamp()
                WHERE run_id = '{run_id}'
                  AND source_database = '{src_db}'
                  AND source_table = '{v['source_table']}'
                  AND validation_status IS NULL
            """, task_label=f"update_rewrite_validation_status:failure_patch:{v['source_table']}")

    execute_with_iceberg_retry(spark, f"""
        UPDATE {tracking_db}.rewrite_migration_table_status
        SET validation_status = 'SKIPPED',
            overall_status = CASE
                WHEN overall_status = 'FAILED' THEN 'FAILED'
                ELSE 'VALIDATION_FAILED'
            END,
            error_message = COALESCE(error_message, 'Validation task did not process this table'),
            updated_at = current_timestamp()
        WHERE run_id  = '{run_id}'
          AND source_database = '{src_db}'
          AND data_presence_status = 'CONFIRMED'
          AND table_create_status = 'COMPLETED'
          AND validation_status IS NULL
    """, task_label="update_rewrite_validation_status:catchall")

    return validation_result


@task.pyspark(conn_id='spark_default')
def generate_rewrite_html_report(run_id: str, spark) -> dict:
    """Generate HTML report for the rewrite_table_path migration."""
    from datetime import datetime

    config = get_config()
    tracking_db = config['tracking_database']
    report_location = config['report_output_location']

    run_info = spark.sql(f"""
        SELECT * FROM {tracking_db}.rewrite_migration_runs WHERE run_id = '{run_id}'
    """).collect()
    run_row = run_info[0] if run_info else None

    table_status = spark.sql(f"""
        SELECT * FROM {tracking_db}.rewrite_migration_table_status
        WHERE run_id = '{run_id}'
        ORDER BY source_database, source_table
    """).collect()

    total_tables = len(table_status)
    successful_tables = sum(1 for t in table_status if t.overall_status == 'VALIDATED')
    failed_tables = sum(1 for t in table_status if 'FAILED' in (t.overall_status or ''))
    missing_tables = sum(1 for t in table_status if t.overall_status == 'DATA_MISSING')
    total_rows = sum(t.source_row_count or 0 for t in table_status)
    total_dest_size = sum(t.data_presence_size_bytes or 0 for t in table_status) / (1024 ** 3)
    dag_run_id = run_row.dag_run_id if run_row else 'N/A'

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>Iceberg Rewrite Migration Report — {run_id}</title>
  <style>
    body {{font-family:'Segoe UI',sans-serif;margin:0;padding:20px;background:#f5f5f5}}
    .container {{max-width:1400px;margin:0 auto;background:white;padding:30px;border-radius:8px;box-shadow:0 2px 10px rgba(0,0,0,.1)}}
    h1 {{color:#2c3e50;border-bottom:3px solid #9b59b6;padding-bottom:10px}}
    h2 {{color:#34495e;margin-top:30px;border-bottom:2px solid #ecf0f1;padding-bottom:8px}}
    .grid {{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:20px;margin:20px 0}}
    .card {{color:white;padding:20px;border-radius:8px;box-shadow:0 4px 6px rgba(0,0,0,.1)}}
    .c1{{background:linear-gradient(135deg,#8e44ad,#9b59b6)}}
    .c2{{background:linear-gradient(135deg,#11998e,#38ef7d)}}
    .c3{{background:linear-gradient(135deg,#f093fb,#f5576c)}}
    .c4{{background:linear-gradient(135deg,#4facfe,#00f2fe)}}
    .c5{{background:linear-gradient(135deg,#f7971e,#ffd200)}}
    .card h3{{margin:0 0 10px;font-size:13px;opacity:.9}}
    .card .v{{font-size:30px;font-weight:bold;margin:0}}
    table {{width:100%;border-collapse:collapse;margin:20px 0;font-size:14px}}
    th {{background:#34495e;color:white;padding:12px;text-align:left}}
    td {{padding:10px 12px;border-bottom:1px solid #ecf0f1}}
    tr:hover{{background:#f8f9fa}}
    .badge {{padding:4px 12px;border-radius:12px;font-size:12px;font-weight:bold;display:inline-block}}
    .ok{{background:#d4edda;color:#155724}} .fail{{background:#f8d7da;color:#721c24}}
    .warn{{background:#fff3cd;color:#856404}}
    .vp{{color:#27ae60;font-weight:bold}} .vf{{color:#e74c3c;font-weight:bold}}
    .vw{{color:#856404;font-weight:bold}} .metric{{font-weight:bold;color:#2980b9}}
    .ts{{color:#95a5a6;font-size:12px}} .divider{{margin:40px 0;border-top:2px dashed #ecf0f1}}
    .strategy-badge {{background:#9b59b6;color:white;padding:4px 10px;border-radius:4px;font-size:12px}}
  </style>
</head>
<body><div class="container">
<h1>Iceberg Rewrite Table Path Migration Report
  <span class="strategy-badge">rewrite_table_path</span>
</h1>
<div class="ts">
  Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC<br>
  Run ID: <strong>{run_id}</strong>&nbsp;|&nbsp;DAG Run: <strong>{dag_run_id}</strong>
</div>

<h2>Migration Summary</h2>
<div class="grid">
  <div class="card c1"><h3>TOTAL TABLES</h3><p class="v">{total_tables}</p></div>
  <div class="card c2"><h3>VALIDATED</h3><p class="v">{successful_tables}</p></div>
  <div class="card c3"><h3>FAILED</h3><p class="v">{failed_tables}</p></div>
  <div class="card c5"><h3>DATA MISSING</h3><p class="v">{missing_tables}</p></div>
  <div class="card c4"><h3>SOURCE ROWS</h3><p class="v">{total_rows:,}</p></div>
  <div class="card c4"><h3>DEST DATA SIZE</h3><p class="v">{total_dest_size:.3f} GB</p></div>
</div>

<div class="divider"></div>
<h2>Data Presence & Metadata Check</h2>
<table>
  <thead><tr>
    <th>Database</th><th>Table</th><th>Dest S3 Path</th>
    <th>Presence Status</th><th>Files Found</th><th>Size (MB)</th>
  </tr></thead><tbody>
"""
    for t in table_status:
        p_status = t.data_presence_status or 'N/A'
        p_class = 'ok' if p_status == 'CONFIRMED' else ('warn' if p_status == 'MISSING' else 'fail')
        html += f"""
  <tr>
    <td>{t.source_database}</td>
    <td><strong>{t.source_table}</strong></td>
    <td class="ts">{(t.dest_s3_location or '')[:80]}</td>
    <td><span class="badge {p_class}">{p_status}</span></td>
    <td class="metric">{(t.data_presence_file_count or 0):,}</td>
    <td class="metric">{(t.data_presence_size_bytes or 0) / (1024 ** 2):.2f}</td>
  </tr>"""

    html += """
  </tbody></table>

<div class="divider"></div>
<h2>Table Migration Details</h2>
<table>
  <thead><tr>
    <th>Database</th><th>Table</th><th>Format</th><th>Partitioned</th>
    <th>Overall Status</th><th>Discovery</th><th>Table Create</th><th>Validation</th>
  </tr></thead><tbody>
"""
    for t in table_status:
        os_val = t.overall_status or ''
        os_class = 'ok' if os_val == 'VALIDATED' else ('warn' if os_val in ('DATA_MISSING', 'TABLE_CREATED') else 'fail')
        d_dur = f"{t.discovery_duration_seconds:.1f}s" if t.discovery_duration_seconds else 'N/A'
        c_dur = f"{t.table_create_duration_seconds:.1f}s" if t.table_create_duration_seconds else 'N/A'
        v_dur = f"{t.validation_duration_seconds:.1f}s" if t.validation_duration_seconds else 'N/A'
        part_str = f"Yes ({t.partition_count or 0})" if t.is_partitioned else 'No'
        html += f"""
  <tr>
    <td>{t.source_database}</td>
    <td><strong>{t.source_table}</strong></td>
    <td>{t.file_format}</td>
    <td>{part_str}</td>
    <td><span class="badge {os_class}">{os_val}</span></td>
    <td class="ts">{d_dur}</td>
    <td class="ts">{c_dur}{'&nbsp;<small>(existed)</small>' if t.table_already_existed else ''}</td>
    <td class="ts">{v_dur}</td>
  </tr>"""

    html += """
  </tbody></table>

<div class="divider"></div>
<h2>Validation Results</h2>
<table>
  <thead><tr>
    <th>Database</th><th>Table</th>
    <th>Source Rows</th><th>Dest Rows</th><th>Row Match</th>
    <th>Src Parts</th><th>Dest Parts</th><th>Part Match</th>
    <th>Schema Match</th>
  </tr></thead><tbody>
"""
    for t in table_status:
        if not t.validation_status or t.validation_status == 'SKIPPED':
            continue
        rm, pm, sm = t.row_count_match, t.partition_count_match, t.schema_match
        html += f"""
  <tr>
    <td>{t.source_database}</td>
    <td><strong>{t.source_table}</strong></td>
    <td class="metric">{(t.source_row_count or 0):,}</td>
    <td class="metric">{(t.dest_hive_row_count or 0):,}</td>
    <td class="{'vp' if rm else 'vf'}">{'✓ PASS' if rm else '✗ FAIL'}</td>
    <td class="metric">{t.source_partition_count or 0}</td>
    <td class="metric">{t.dest_partition_count or 0}</td>
    <td class="{'vp' if pm else 'vw'}">{'✓ PASS' if pm else '⚠ WARN'}</td>
    <td class="{'vp' if sm else 'vf'}">{'✓ PASS' if sm else '✗ FAIL'}</td>
  </tr>"""

    html += """
  </tbody></table>
<div style="margin-top:50px;padding-top:20px;border-top:1px solid #ecf0f1;color:#95a5a6;font-size:12px">
  <p>Auto-generated by the Iceberg Rewrite Table Path Migration DAG (Approach 2).</p>
</div></div></body></html>"""

    report_path = f"{report_location}/{run_id}_rewrite_report.html"
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jvm.java.net.URI(report_path), hadoop_conf
    )
    stream = fs.create(spark._jvm.org.apache.hadoop.fs.Path(report_path), True)
    stream.write(html.encode('utf-8'))
    stream.close()
    logger.info(f"[generate_rewrite_html_report] Report written to {report_path}")
    return {'report_path': report_path}


@task.pyspark(conn_id='spark_default')
def send_rewrite_report_email(report_result: dict, run_id: str, spark) -> dict:
    """Send rewrite migration HTML report via email."""
    import os
    import tempfile

    from airflow.utils.email import send_email

    config = get_config()
    smtp_conn_id = config.get('smtp_conn_id', 'smtp_default')
    recipients_str = config.get('email_recipients', '')

    if not recipients_str:
        logger.warning("[send_rewrite_report_email] No recipients configured. Skipping.")
        return {'sent': False, 'reason': 'no_recipients'}

    recipients = [r.strip() for r in recipients_str.split(',') if r.strip()]
    report_path = report_result.get('report_path', '')

    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(report_path), hadoop_conf
        )
        reader = spark._jvm.java.io.BufferedReader(
            spark._jvm.java.io.InputStreamReader(
                fs.open(spark._jvm.org.apache.hadoop.fs.Path(report_path)), "UTF-8"
            )
        )
        lines, line = [], reader.readLine()
        while line is not None:
            lines.append(line)
            line = reader.readLine()
        reader.close()
        html_content = "\n".join(lines)

        tmp = tempfile.NamedTemporaryFile(
            mode='w', suffix='.html',
            prefix=f'{run_id}_rewrite_report_', delete=False
        )
        tmp.write(html_content)
        tmp.close()

        send_email(
            to=recipients,
            subject=f"Iceberg Rewrite Migration Report — {run_id}",
            html_content=(
                f"<p>Iceberg rewrite_table_path migration report for run "
                f"<strong>{run_id}</strong> is attached.</p>"
            ),
            files=[tmp.name],
            conn_id=smtp_conn_id,
        )
        os.unlink(tmp.name)
        logger.info(f"[send_rewrite_report_email] Report sent to: {recipients}")
        return {'sent': True, 'recipients': recipients, 'report_path': report_path}
    except Exception as e:
        logger.error(f"[send_rewrite_report_email] Failed: {e}")
        raise Exception(f"Failed to send rewrite migration report email: {e}") from e


@task.pyspark(conn_id='spark_default')
def finalize_rewrite_run(run_id: str, spark) -> dict:
    """Aggregate final statistics and mark the run complete."""
    config = get_config()
    tracking_db = config['tracking_database']

    stats = {'total': 0, 'successful': 0, 'failed': 0, 'missing': 0}
    final_status = 'FAILED'

    try:
        result = spark.sql(f"""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN overall_status = 'VALIDATED' THEN 1 ELSE 0 END) as successful,
                SUM(CASE WHEN overall_status LIKE '%FAIL%' THEN 1 ELSE 0 END) as failed,
                SUM(CASE WHEN overall_status = 'DATA_MISSING' THEN 1 ELSE 0 END) as missing
            FROM {tracking_db}.rewrite_migration_table_status
            WHERE run_id = '{run_id}'
        """).collect()

        if result and result[0]['total']:
            stats = {
                'total': result[0]['total'] or 0,
                'successful': result[0]['successful'] or 0,
                'failed': result[0]['failed'] or 0,
                'missing': result[0]['missing'] or 0,
            }
            if stats['failed'] == 0 and stats['missing'] == 0:
                final_status = 'COMPLETED'
            elif stats['failed'] == 0:
                final_status = 'COMPLETED_WITH_MISSING'
            else:
                final_status = 'COMPLETED_WITH_FAILURES'
    except Exception as e:
        logger.error(f"[finalize_rewrite_run] Error querying tracking table: {e}")

    execute_with_iceberg_retry(spark, f"""
        UPDATE {tracking_db}.rewrite_migration_runs
        SET status = '{final_status}',
            completed_at = current_timestamp(),
            total_tables = {stats['total']},
            successful_tables = {stats['successful']},
            failed_tables = {stats['failed']},
            missing_tables = {stats['missing']}
        WHERE run_id = '{run_id}'
    """, task_label="finalize_rewrite_run:update")

    logger.info(
        f"[finalize_rewrite_run] Run '{run_id}' → {final_status} | "
        f"total={stats['total']} validated={stats['successful']} "
        f"failed={stats['failed']} missing={stats['missing']}"
    )
    return {'run_id': run_id, 'status': final_status, **stats}


# =============================================================================
# DAG 5 DEFINITION
# =============================================================================

with DAG(
    dag_id='iceberg_rewrite_table_path_migration',
    default_args=default_args,
    description=(
        'Iceberg-to-Iceberg migration via rewrite_table_path. '
        'Preserves full snapshot history, partition transforms, and schema fidelity. '
        'Requires data AND metadata pre-copied to destination S3.'
    ),
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=5,
    tags=['migration', 'iceberg', 'rewrite-table-path', 'approach-2'],
    params={
        'excel_file_path': Param(
            default='s3a://config-bucket/iceberg_rewrite_migration.xlsx',
            type='string',
            description=(
                'S3 path to Excel config file. '
                'Required columns: database | table | source_s3_prefix | dest_s3_prefix'
            ),
        ),
    },
    render_template_as_native_obj=True,
) as dag_iceberg_rewrite:

    t_init = init_rewrite_tracking_tables()
    t_run_id = create_rewrite_migration_run(
        excel_file_path="{{ params.excel_file_path }}",
        dag_run_id="{{ run_id }}",
    )
    t_excel = parse_rewrite_excel(
        excel_file_path="{{ params.excel_file_path }}",
        run_id=t_run_id,
    )

    # Dynamic task mapping (one set of tasks per database group)
    t_discover = discover_rewrite_tables.expand(db_config=t_excel)
    t_record = record_rewrite_discovered_tables.expand(discovery=t_discover)
    t_record.operator.trigger_rule = 'all_done'

    t_presence = validate_rewrite_data_presence.expand(discovery=t_record)
    t_presence.operator.trigger_rule = 'all_done'

    t_pres_status = update_rewrite_data_presence_status.expand(presence_result=t_presence)
    t_pres_status.operator.trigger_rule = 'all_done'

    t_tables = create_rewrite_dest_tables.expand(presence_result=t_pres_status)
    t_tables.operator.trigger_rule = 'all_done'

    t_tbl_status = update_rewrite_table_create_status.expand(table_result=t_tables)
    t_tbl_status.operator.trigger_rule = 'all_done'

    t_validate = validate_rewrite_destination_tables.expand(table_result=t_tbl_status)
    t_validate.operator.max_active_tis_per_dagrun = 3
    t_validate.operator.trigger_rule = 'all_done'

    t_val_status = update_rewrite_validation_status.expand(validation_result=t_validate)
    t_val_status.operator.trigger_rule = 'all_done'

    t_report = generate_rewrite_html_report(run_id=t_run_id)
    t_report.operator.trigger_rule = 'all_done'

    t_email = send_rewrite_report_email(run_id=t_run_id, report_result=t_report)
    t_email.operator.trigger_rule = 'all_done'

    t_final = finalize_rewrite_run(run_id=t_run_id)
    t_final.operator.trigger_rule = 'all_done'

    # Dependency chain
    t_init >> t_run_id >> t_excel >> t_discover >> t_record
    t_record >> t_presence >> t_pres_status >> t_tables >> t_tbl_status
    t_tbl_status >> t_validate >> t_val_status >> t_report >> t_email >> t_final
