"""
DAG 4: S3-to-S3 Metadata Migration

Metadata-only migration: discovers source Hive table schemas and recreates them
as external tables pointing to destination S3 paths. No data is copied — only
Hive metadata (DDL) is migrated.

Pipeline stages:
  1. Init tracking tables & create run record
  2. Parse Excel config (database, table tokens, prefix mapping)
  3. Discover source Hive table metadata (schema, partitions, row counts)
  4. Validate data presence at destination S3 paths
  5. Create destination Hive external tables (or repair existing)
  6. Validate destination tables (row count, partition, schema comparison)
  7. Generate HTML report & send email

Excel columns: database | table | dest_database | dest_bucket |
               source_s3_prefix | dest_s3_prefix
"""

import contextlib
import json
import logging
import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from dotenv import load_dotenv

from utils.shared import (
    DEFAULT_ARGS,
    apply_bucket_credentials,
    compute_dest_path,
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
    logger.warning(f"Config directory {_config_dir} not found — env files not loaded, using Airflow Variables / defaults")


# =============================================================================
# DAG 4: METADATA COPY / MIGRATION ONLY TASKS
# =============================================================================

@task.pyspark(conn_id='spark_default')
def init_s3_tracking_tables(spark) -> dict:
    """Create Iceberg tracking tables for S3-to-S3 metadata migration."""
    config = get_config()
    tracking_db  = config['tracking_database']
    tracking_loc = config['tracking_location']

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {tracking_db} LOCATION '{tracking_loc}'")

    # Run-level table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tracking_db}.s3_migration_runs (
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
        LOCATION '{tracking_loc}/s3_migration_runs'
    """)

    # Table-level tracking
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tracking_db}.s3_migration_table_status (
            run_id                      STRING,
            source_database             STRING,
            source_table                STRING,
            dest_database               STRING,
            dest_bucket                 STRING,
            source_s3_location          STRING,
            dest_s3_location            STRING,
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
        LOCATION '{tracking_loc}/s3_migration_table_status'
    """)

    logger.info(f"[init_s3_tracking_tables] Tracking tables ready in '{tracking_db}'")
    return {'status': 'initialized', 'database': tracking_db}


@task.pyspark(conn_id='spark_default')
def create_s3_migration_run(excel_file_path: str, dag_run_id: str, spark) -> str:
    """Create run record and return run_id."""
    import uuid
    config       = get_config()
    tracking_db  = config['tracking_database']
    run_id       = f"s3_run_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

    spark.sql(f"""
        INSERT INTO {tracking_db}.s3_migration_runs
        VALUES (
            '{run_id}', '{dag_run_id}', '{excel_file_path}',
            current_timestamp(), NULL, 'RUNNING',
            0, 0, 0, 0,
            '{json.dumps(config).replace("'", "''")}'
        )
    """)
    logger.info(f"[create_s3_migration_run] Run created: {run_id}")
    return run_id


@task.pyspark(conn_id='spark_default')
def parse_s3_excel(excel_file_path: str, run_id: str, spark) -> list:
    """Read and parse the S3 migration Excel config."""
    import math
    from io import BytesIO

    import pandas as ps

    config = get_config()

    binary_df = spark.read.format("binaryFile").load(excel_file_path)
    row = binary_df.select("content").first()
    excel_bytes = bytes(row.content)
    df = ps.read_excel(BytesIO(excel_bytes), engine='openpyxl')
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

    def _str(val, default=''):
        if val is None or (isinstance(val, float) and math.isnan(val)):
            return default
        return str(val).strip() or default

    def _normalize_s3(path: str) -> str:
        if not path:
            return path
        if path.startswith('s3n://'):
            return 's3a://' + path[6:]
        if path.startswith('s3://'):
            return 's3a://' + path[5:]
        if not path.startswith('s3a://'):
            return 's3a://' + path
        return path

    # Group rows by (src_db, dest_db, dest_bucket, src_prefix, dest_prefix)
    grouped = {}
    for _, row in df.iterrows():
        src_db = _str(row.get('database'))
        if not src_db:
            continue

        raw_table = _str(row.get('table'), '*')
        dest_db   = _str(row.get('dest_database'), src_db)

        raw_bucket       = _str(row.get('dest_bucket'))
        src_prefix       = _normalize_s3(_str(row.get('source_s3_prefix')))
        dest_prefix      = _normalize_s3(_str(row.get('dest_s3_prefix')))
        dest_bucket_norm = _normalize_s3(raw_bucket) if raw_bucket else config['default_s3_bucket']

        has_prefix  = bool(src_prefix and dest_prefix)
        has_bucket  = bool(dest_bucket_norm)
        if not has_prefix and not has_bucket:
            logger.warning(
                f"[parse_s3_excel] Skipping row for database '{src_db}' — "
                f"must supply either (source_s3_prefix + dest_s3_prefix) or dest_bucket"
            )
            continue

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
            'source_database':  src_db,
            'dest_database':    dest_db,
            'dest_bucket':      dest_bucket,
            'source_s3_prefix': src_prefix,
            'dest_s3_prefix':   dest_prefix,
            'table_tokens':     unique_tokens,
            'run_id':           run_id,
        })
        logger.info(
            f"[parse_s3_excel] {src_db} → {dest_db} | bucket={dest_bucket} | "
            f"src_prefix={src_prefix or 'N/A'} | dest_prefix={dest_prefix or 'N/A'} | "
            f"tokens={unique_tokens[:5]}"
        )

    if not configs:
        logger.error("[parse_s3_excel] No valid rows found in Excel config")
        return []

    logger.info(f"[parse_s3_excel] Emitting {len(configs)} database config(s)")
    return configs


@task.pyspark(conn_id='spark_default')
@track_duration
def discover_source_hive_tables(db_config: dict, spark, **context) -> dict:
    """Discover source Hive table metadata"""
    import fnmatch

    config = get_config()
    src_db = db_config['source_database']
    dest_db = db_config['dest_database']
    dest_bucket = db_config['dest_bucket']
    src_prefix = db_config.get('source_s3_prefix', '')
    dest_prefix = db_config.get('dest_s3_prefix', '')
    tokens = db_config.get('table_tokens', ['*'])
    run_id = db_config['run_id']

    configure_spark_s3(spark, config)

    all_tables = [r.tableName for r in spark.sql(f"SHOW TABLES IN {src_db}").collect()]
    matched = []
    for tok in tokens:
        if tok == '*':
            matched = all_tables
            break
        matched += [t for t in all_tables if fnmatch.fnmatch(t, tok) and t not in matched]

    logger.info(f"[discover_source_hive_tables] '{src_db}': {len(matched)} table(s) matched")

    metadata = []
    for tbl in matched:
        try:
            desc_rows    = spark.sql(f"DESCRIBE FORMATTED {src_db}.{tbl}").collect()

            location     = None
            input_format = None
            table_type   = 'EXTERNAL_TABLE'

            for r in desc_rows:
                col  = (r.col_name  or '').strip().rstrip(':').lower()
                val  = (r.data_type or '').strip()
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
                        f"[discover_source_hive_tables] Unrecognised InputFormat "
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
                logger.warning(f"[discover_source_hive_tables] Could not count rows for {src_db}.{tbl}: {e}")

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
                    logger.warning(f"[discover_source_hive_tables] Could not get FS summary for {src_db}.{tbl}: {e}")

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
                f"[discover_source_hive_tables] {src_db}.{tbl} | fmt={file_format} | "
                f"parts={len(partitions)} | rows={row_count} | "
                f"size={source_total_size/(1024**2):.1f}MB | dest={dest_path}"
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
            logger.error(f"[discover_source_hive_tables] FAILED for {src_db}.{tbl}: {e}")
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

    result_dict = {
        'run_id': run_id,
        'source_database': src_db,
        'dest_database': dest_db,
        'dest_bucket': dest_bucket,
        'source_s3_prefix': src_prefix,
        'dest_s3_prefix': dest_prefix,
        'tables': metadata,
    }

    failed = [t for t in metadata if 'error' in t]
    if failed:
        context['ti'].xcom_push(key='return_value', value=result_dict)
        raise Exception(
            f"Discovery failed for {len(failed)}/{len(metadata)} table(s) in '{src_db}': "
            + ', '.join(t['source_table'] for t in failed[:3])
        )

    return result_dict


@task.pyspark(conn_id='spark_default')
def record_s3_discovered_tables(discovery: dict, spark) -> dict:
    """Persist discovered table metadata into the s3_migration_table_status tracking table."""
    if not isinstance(discovery, dict) or 'tables' not in discovery:
        logger.warning(f"[record_s3_discovered_tables] Skipping invalid input: {type(discovery)}")
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
            FROM {tracking_db}.s3_migration_table_status
            WHERE run_id = '{run_id}'
              AND source_database = '{t['source_database']}'
              AND source_table = '{t['source_table']}'
        """).collect()[0]['cnt']

        if existing > 0:
            execute_with_iceberg_retry(spark, f"""
                UPDATE {tracking_db}.s3_migration_table_status
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
            """, task_label=f"record_s3_discovered_tables:{t['source_table']}")
        else:
            execute_with_iceberg_retry(spark, f"""
                INSERT INTO {tracking_db}.s3_migration_table_status (
                    run_id, source_database, source_table, dest_database, dest_bucket,
                    source_s3_location, dest_s3_location, file_format,
                    is_partitioned, partition_columns, partition_count,
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
                    '{t['dest_database']}', '{t['dest_bucket']}',
                    '{t['source_location']}', '{t['dest_location']}', '{t['file_format']}',
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
            """, task_label=f"record_s3_discovered_tables:insert:{t['source_table']}")

    return discovery


@task.pyspark(conn_id='spark_default')
def validate_data_presence(discovery: dict, spark, **context) -> dict:
    """Check whether destination S3 paths contain data files."""
    if not isinstance(discovery, dict) or 'tables' not in discovery:
        logger.warning(f"[validate_data_presence] Skipping invalid input: {type(discovery)}")
        return {}

    config = get_config()
    configure_spark_s3(spark, config)

    results = []
    for t in discovery['tables']:
        dest_path = t.get('dest_location', '')
        tbl = t['source_table']
        src_db = t['source_database']

        if not dest_path:
            logger.warning(f"[validate_data_presence] No dest_location for {src_db}.{tbl}")
            results.append({
                'source_database': src_db,
                'source_table': tbl,
                'status': 'MISSING',
                'file_count': 0,
                'size_bytes': 0,
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
                logger.warning(f"[validate_data_presence] MISSING (path does not exist): {dest_path}")
                results.append({
                    'source_database': src_db,
                    'source_table': tbl,
                    'status': 'MISSING',
                    'file_count': 0,
                    'size_bytes': 0,
                    'error': f'Destination path does not exist: {dest_path}',
                })
                continue

            summary = fs.getContentSummary(path_obj)
            file_count = int(summary.getFileCount())
            size_bytes = int(summary.getLength())

            if file_count == 0:
                logger.warning(f"[validate_data_presence] MISSING (0 files): {dest_path}")
                results.append({
                    'source_database': src_db,
                    'source_table': tbl,
                    'status': 'MISSING',
                    'file_count': 0,
                    'size_bytes': 0,
                    'error': f'Destination path exists but contains 0 files: {dest_path}',
                })
            else:
                logger.info(
                    f"[validate_data_presence] CONFIRMED: {src_db}.{tbl} | "
                    f"files={file_count} | size={size_bytes/(1024**2):.1f}MB | path={dest_path}"
                )
                results.append({
                    'source_database': src_db,
                    'source_table': tbl,
                    'status': 'CONFIRMED',
                    'file_count': file_count,
                    'size_bytes': size_bytes,
                    'error': None,
                })

        except Exception as e:
            logger.error(f"[validate_data_presence] FAILED for {src_db}.{tbl}: {e}")
            results.append({
                'source_database': src_db,
                'source_table': tbl,
                'status': 'FAILED',
                'file_count': 0,
                'size_bytes': 0,
                'error': str(e)[:500],
            })

    missing = [r for r in results if r['status'] == 'MISSING']
    failed = [r for r in results if r['status'] == 'FAILED']
    confirmed = [r for r in results if r['status'] == 'CONFIRMED']

    logger.info(
        f"[validate_data_presence] Summary for '{discovery['source_database']}': "
        f"confirmed={len(confirmed)}, missing={len(missing)}, failed={len(failed)}"
    )

    result_dict = {**discovery, 'presence_results': results}

    if failed:
        context['ti'].xcom_push(key='return_value', value=result_dict)
        raise Exception(
            f"[validate_data_presence] Data presence check FAILED for "
            f"{len(failed)}/{len(results)} table(s) in '{discovery['source_database']}' — "
            f"check S3 credentials and endpoint config"
        )

    return result_dict


@task.pyspark(conn_id='spark_default')
def update_data_presence_status(presence_result: dict, spark) -> dict:
    """Update tracking table with data presence check results."""
    if not isinstance(presence_result, dict) or 'run_id' not in presence_result:
        logger.warning("[update_data_presence_status] Skipping invalid input")
        return {}

    config = get_config()
    tracking_db = config['tracking_database']
    run_id = presence_result['run_id']

    for r in presence_result.get('presence_results', []):
        overall = {
            'CONFIRMED': 'DATA_CONFIRMED',
            'MISSING': 'DATA_MISSING',
            'FAILED': 'FAILED',
        }.get(r['status'], 'FAILED')

        error_msg = (r.get('error') or '').replace("'", "''")[:2000]

        execute_with_iceberg_retry(spark, f"""
            UPDATE {tracking_db}.s3_migration_table_status
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
        """, task_label=f"update_data_presence_status:{r['source_table']}")

    src_db = presence_result.get('source_database', '')
    if src_db:
        execute_with_iceberg_retry(spark, f"""
            UPDATE {tracking_db}.s3_migration_table_status
            SET data_presence_status = 'FAILED',
                overall_status = 'FAILED',
                error_message = COALESCE(error_message, 'Data presence check did not process this table'),
                updated_at = current_timestamp()
            WHERE run_id = '{run_id}'
            AND source_database = '{src_db}'
            AND discovery_status = 'COMPLETED'
            AND data_presence_status IS NULL
        """, task_label="update_data_presence_status:catchall")

    return presence_result


@task.pyspark(conn_id='spark_default')
@track_duration
def create_dest_hive_tables(presence_result: dict, spark, **context) -> dict:
    """Create or repair destination Hive external tables."""
    if not isinstance(presence_result, dict) or 'tables' not in presence_result:
        logger.warning("[create_dest_hive_tables] Skipping invalid input")
        return {}

    config = get_config()
    configure_spark_s3(spark, config)
    dest_db = presence_result['dest_database']
    tables = presence_result['tables']

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {dest_db}")

    presence_map = {
        r['source_table']: r
        for r in presence_result.get('presence_results', [])
    }

    results = []
    for t in tables:
        tbl = t['source_table']
        presence = presence_map.get(tbl, {})
        p_status = presence.get('status', 'UNKNOWN')

        if p_status != 'CONFIRMED':
            logger.info(f"[create_dest_hive_tables] Skipping {dest_db}.{tbl}, data_presence={p_status}")
            results.append({
                'source_table': tbl,
                'status': 'SKIPPED',
                'existed': False,
                'error': f'Data not present at destination (status={p_status})',
            })
            continue

        dest_path = t['dest_location']
        fmt = t.get('file_format', 'PARQUET')
        schema_list = t.get('schema', [])
        part_cols_str = t.get('partition_columns', '')
        is_part = t.get('is_partitioned', False)
        full_name = f"{dest_db}.{tbl}"

        # Apply per-bucket destination credentials
        apply_bucket_credentials(
            spark, dest_path,
            config.get('_dest_endpoint', ''),
            config.get('_dest_access_key', ''),
            config.get('_dest_secret_key', ''),
        )

        logger.info(f"[create_dest_hive_tables] Processing {full_name} | fmt={fmt} | partitioned={is_part}")

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
                logger.info(f"[create_dest_hive_tables] REPAIRED (already existed): {full_name}")
                results.append({'source_table': tbl, 'status': 'COMPLETED', 'existed': True, 'error': None})
            else:
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

                logger.info(f"[create_dest_hive_tables] CREATED: {full_name} at {dest_path}")
                results.append({'source_table': tbl, 'status': 'COMPLETED', 'existed': False, 'error': None})

        except Exception as e:
            error_msg = str(e)[:2000]
            logger.error(f"[create_dest_hive_tables] FAILED for {full_name}: {error_msg}")
            results.append({'source_table': tbl, 'status': 'FAILED', 'existed': False, 'error': error_msg})

    failed = [r for r in results if r['status'] == 'FAILED']
    has_failures = len(failed) > 0

    result_dict = {
        **presence_result,
        'table_results': results,
        '_has_failures': has_failures,
    }
    context['ti'].xcom_push(key='return_value', value=result_dict)

    if has_failures:
        raise Exception(
            f"Hive table creation failed for {len(failed)}/{len(results)} table(s) in '{dest_db}'"
        )
    return result_dict


@task.pyspark(conn_id='spark_default')
def update_s3_table_create_status(table_result: dict, spark) -> dict:
    """Update tracking table with table creation results."""
    if not isinstance(table_result, dict) or 'run_id' not in table_result:
        logger.warning("[update_s3_table_create_status] Skipping invalid input")
        return {}

    config = get_config()
    tracking_db = config['tracking_database']
    run_id = table_result['run_id']
    dest_db = table_result['dest_database']
    src_db = table_result['source_database']
    table_dur = table_result.get('_task_duration', 0.0)

    for r in table_result.get('table_results', []):
        overall   = {
            'COMPLETED': 'TABLE_CREATED',
            'SKIPPED': 'DATA_MISSING',
            'FAILED': 'FAILED',
        }.get(r['status'], 'FAILED')

        error_msg = (r.get('error') or '').replace("'", "''")[:2000]

        execute_with_iceberg_retry(spark, f"""
            UPDATE {tracking_db}.s3_migration_table_status
            SET table_create_status = '{r['status']}',
                table_create_completed_at = current_timestamp(),
                table_create_duration_seconds = {table_dur},
                table_already_existed = {str(r.get('existed', False)).lower()},
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
        """, task_label=f"update_s3_table_create_status:{r['source_table']}")

    execute_with_iceberg_retry(spark, f"""
        UPDATE {tracking_db}.s3_migration_table_status
        SET table_create_status = 'FAILED',
            overall_status = 'FAILED',
            error_message = COALESCE(error_message, 'Table creation task did not process this table'),
            updated_at = current_timestamp()
        WHERE run_id = '{run_id}'
          AND source_database = '{src_db}'
          AND table_create_status IS NULL
          AND data_presence_status = 'CONFIRMED'
    """, task_label="update_s3_table_create_status:catchall")

    return table_result


@task.pyspark(conn_id='spark_default')
@track_duration
def validate_s3_destination_tables(table_result: dict, spark, **context) -> dict:
    """Validate destination Hive tables"""
    if not isinstance(table_result, dict) or 'tables' not in table_result:
        logger.warning("[validate_s3_destination_tables] Skipping invalid input")
        return {}

    config = get_config()
    configure_spark_s3(spark, config)
    tracking_db = config['tracking_database']
    run_id = table_result['run_id']
    src_db = table_result['source_database']
    dest_db = table_result['dest_database']
    tables = table_result['tables']

    validation_results = []

    for t in tables:
        tbl = t['source_table']
        dest_tbl = f"{dest_db}.{tbl}"

        upstream = spark.sql(f"""
            SELECT table_create_status, data_presence_status, overall_status, error_message
            FROM {tracking_db}.s3_migration_table_status
            WHERE run_id = '{run_id}'
              AND source_database = '{src_db}'
              AND source_table = '{tbl}'
        """).collect()

        if upstream:
            row = upstream[0]
            if row['table_create_status'] in ('FAILED', 'SKIPPED') or row['data_presence_status'] != 'CONFIRMED':
                validation_results.append({
                    'source_table': tbl,
                    'status': 'SKIPPED',
                    'error': f"Skipped validation — upstream status: {row['overall_status']}",
                })
                continue

        logger.info(f"[validate_s3_destination_tables] Validating {dest_tbl}")

        try:
            src_metrics = spark.sql(f"""
                SELECT source_row_count, source_partition_count
                FROM {tracking_db}.s3_migration_table_status
                WHERE run_id = '{run_id}'
                  AND source_database = '{src_db}'
                  AND source_table = '{tbl}'
            """).collect()

            if not src_metrics:
                validation_results.append({
                    'source_table': tbl,
                    'status': 'SKIPPED',
                    'error': 'Source metrics not found in tracking table',
                })
                continue

            src_row_count = src_metrics[0]['source_row_count'] or 0
            src_partition_count = src_metrics[0]['source_partition_count'] or t.get('partition_count', 0)

            dest_row_count = spark.sql(f"SELECT COUNT(*) as c FROM {dest_tbl}").collect()[0]['c']

            dest_partition_count = 0
            with contextlib.suppress(Exception):
                dest_partition_count = spark.sql(f"SHOW PARTITIONS {dest_tbl}").count()

            src_schema = {c['name']: c['type'] for c in t.get('schema', [])}
            dest_schema = {
                r.col_name: r.data_type
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
            partition_count_match = (src_partition_count == dest_partition_count)

            match_str = (
                f"rows={'✓' if row_count_match else '✗'} "
                f"parts={'✓' if partition_count_match else '⚠'} "
                f"schema={'✓' if schema_match else '✗'}"
            )
            logger.info(f"[validate_s3_destination_tables] {dest_tbl} | {match_str}")

            mismatch_parts = []
            if not row_count_match:
                mismatch_parts.append(f"Row count mismatch: source={src_row_count} dest={dest_row_count}")
            if not partition_count_match:
                mismatch_parts.append(f"Partition count mismatch: source={src_partition_count} dest={dest_partition_count}")
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
            logger.error(f"[validate_s3_destination_tables] FAILED for {dest_tbl}: {e}")
            validation_results.append({
                'source_table': tbl,
                'status': 'FAILED',
                'error': str(e)[:2000],
            })

    failed = [v for v in validation_results if v['status'] == 'FAILED']
    has_failures = len(failed) > 0

    result_dict = {**table_result, 'validation_results': validation_results, '_has_failures': has_failures}
    context['ti'].xcom_push(key='return_value', value=result_dict)

    if has_failures:
        raise Exception(
            f"Validation failed for {len(failed)}/{len(validation_results)} table(s)"
        )
    return result_dict


@task.pyspark(conn_id='spark_default')
def update_s3_validation_status(validation_result: dict, spark) -> dict:
    """Update tracking table with validation results. Determine final overall_status."""
    if not isinstance(validation_result, dict) or 'run_id' not in validation_result:
        logger.warning("[update_s3_validation_status] Skipping invalid input")
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
            UPDATE {tracking_db}.s3_migration_table_status
            SET validation_status = '{v['status']}',
                validation_completed_at = current_timestamp(),
                validation_duration_seconds = {val_dur},
                dest_hive_row_count = {v.get('dest_hive_row_count', 0)},
                dest_partition_count = {v.get('dest_partition_count', 0)},
                source_partition_count = {v.get('source_partition_count', 0)},
                row_count_match = {str(v.get('row_count_match', False)).lower()},
                partition_count_match  = {str(v.get('partition_count_match', False)).lower()},
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
        """, task_label=f"update_s3_validation_status:{v['source_table']}")

    for v in validation_result.get('validation_results', []):
        if v.get('status') == 'FAILED' and v.get('error'):
            per_err = str(v['error'])[:2000].replace("'", "''")
            execute_with_iceberg_retry(spark, f"""
                UPDATE {tracking_db}.s3_migration_table_status
                SET validation_status = 'FAILED',
                    overall_status = 'VALIDATION_FAILED',
                    error_message = '{per_err}',
                    updated_at = current_timestamp()
                WHERE run_id = '{run_id}'
                  AND source_database = '{src_db}'
                  AND source_table = '{v['source_table']}'
                  AND validation_status IS NULL
            """, task_label=f"update_s3_validation_status:failure_patch:{v['source_table']}")

    execute_with_iceberg_retry(spark, f"""
        UPDATE {tracking_db}.s3_migration_table_status
        SET validation_status = 'SKIPPED',
            overall_status = CASE WHEN overall_status = 'FAILED' THEN 'FAILED' ELSE 'VALIDATION_FAILED' END,
            error_message = COALESCE(error_message, 'Validation task did not process this table'),
            updated_at = current_timestamp()
        WHERE run_id  = '{run_id}'
          AND source_database = '{src_db}'
          AND data_presence_status = 'CONFIRMED'
          AND table_create_status = 'COMPLETED'
          AND validation_status IS NULL
    """, task_label="update_s3_validation_status:catchall")

    return validation_result


@task.pyspark(conn_id='spark_default')
def generate_s3_html_report(run_id: str, spark) -> dict:
    """Generate HTML report for S3-to-S3 metadata migration."""
    from datetime import datetime

    config          = get_config()
    tracking_db     = config['tracking_database']
    report_location = config['report_output_location']

    run_info = spark.sql(f"""
        SELECT * FROM {tracking_db}.s3_migration_runs WHERE run_id = '{run_id}'
    """).collect()
    run_row = run_info[0] if run_info else None

    table_status = spark.sql(f"""
        SELECT * FROM {tracking_db}.s3_migration_table_status
        WHERE run_id = '{run_id}'
        ORDER BY source_database, source_table
    """).collect()

    total_tables = len(table_status)
    successful_tables = sum(1 for t in table_status if t.overall_status == 'VALIDATED')
    failed_tables = sum(1 for t in table_status if 'FAILED' in (t.overall_status or ''))
    missing_tables = sum(1 for t in table_status if t.overall_status == 'DATA_MISSING')
    total_rows = sum(t.source_row_count or 0 for t in table_status)
    total_dest_size = sum(t.data_presence_size_bytes or 0 for t in table_status) / (1024**3)

    dag_run_id = run_row.dag_run_id if run_row else 'N/A'

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <title>S3 Metadata Migration Report — {run_id}</title>
  <style>
    body {{font-family:'Segoe UI',sans-serif;margin:0;padding:20px;background:#f5f5f5}}
    .container {{max-width:1400px;margin:0 auto;background:white;padding:30px;border-radius:8px;box-shadow:0 2px 10px rgba(0,0,0,.1)}}
    h1 {{color:#2c3e50;border-bottom:3px solid #3498db;padding-bottom:10px}}
    h2 {{color:#34495e;margin-top:30px;border-bottom:2px solid #ecf0f1;padding-bottom:8px}}
    .grid {{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:20px;margin:20px 0}}
    .card {{color:white;padding:20px;border-radius:8px;box-shadow:0 4px 6px rgba(0,0,0,.1)}}
    .c1{{background:linear-gradient(135deg,#667eea,#764ba2)}}
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
    .warn{{background:#fff3cd;color:#856404}} .skip{{background:#e2e3e5;color:#383d41}}
    .vp{{color:#27ae60;font-weight:bold}} .vf{{color:#e74c3c;font-weight:bold}}
    .vw{{color:#856404;font-weight:bold}} .metric{{font-weight:bold;color:#2980b9}}
    .ts{{color:#95a5a6;font-size:12px}} .divider{{margin:40px 0;border-top:2px dashed #ecf0f1}}
  </style>
</head>
<body><div class="container">
<h1>S3-to-S3 Metadata Migration Report</h1>
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
<h2>Data Presence Summary</h2>
<table>
  <thead><tr>
    <th>Database</th><th>Table</th><th>Dest S3 Path</th>
    <th>Presence Status</th><th>Files Found</th><th>Size (MB)</th>
  </tr></thead><tbody>
"""
    for t in table_status:
        p_status = t.data_presence_status or 'N/A'
        p_class  = 'ok' if p_status == 'CONFIRMED' else ('warn' if p_status == 'MISSING' else 'fail')
        html += f"""
  <tr>
    <td>{t.source_database}</td>
    <td><strong>{t.source_table}</strong></td>
    <td class="ts">{(t.dest_s3_location or '')[:80]}</td>
    <td><span class="badge {p_class}">{p_status}</span></td>
    <td class="metric">{(t.data_presence_file_count or 0):,}</td>
    <td class="metric">{(t.data_presence_size_bytes or 0)/(1024**2):.2f}</td>
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
        os_val   = t.overall_status or ''
        os_class = 'ok' if os_val == 'VALIDATED' else ('warn' if os_val in ('DATA_MISSING', 'TABLE_CREATED') else 'fail')
        d_dur    = f"{t.discovery_duration_seconds:.1f}s"    if t.discovery_duration_seconds    else 'N/A'
        c_dur    = f"{t.table_create_duration_seconds:.1f}s" if t.table_create_duration_seconds else 'N/A'
        v_dur    = f"{t.validation_duration_seconds:.1f}s"   if t.validation_duration_seconds   else 'N/A'
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
        rm = t.row_count_match
        pm = t.partition_count_match
        sm = t.schema_match
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

<div class="divider"></div>
<h2>Performance Metrics</h2>
<table>
  <thead><tr>
    <th>Database</th><th>Table</th><th>Source Rows</th>
    <th>Discovery</th><th>Table Create</th><th>Validation</th><th>Total</th>
  </tr></thead><tbody>
"""
    for t in table_status:
        d  = t.discovery_duration_seconds    or 0
        c  = t.table_create_duration_seconds or 0
        v  = t.validation_duration_seconds   or 0
        tot = d + c + v
        html += f"""
  <tr>
    <td>{t.source_database}</td>
    <td><strong>{t.source_table}</strong></td>
    <td class="metric">{(t.source_row_count or 0):,}</td>
    <td class="ts">{d:.1f}s</td>
    <td class="ts">{c:.1f}s</td>
    <td class="ts">{v:.1f}s</td>
    <td class="metric">{tot:.1f}s ({tot/60:.1f}m)</td>
  </tr>"""

    html += """
  </tbody></table>
<div style="margin-top:50px;padding-top:20px;border-top:1px solid #ecf0f1;color:#95a5a6;font-size:12px">
  <p>Auto-generated by the S3-to-S3 Metadata Migration DAG.</p>
</div></div></body></html>"""

    report_path = f"{report_location}/{run_id}_s3_report.html"
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jvm.java.net.URI(report_path), hadoop_conf
    )
    out = spark._jvm.org.apache.hadoop.fs.Path(report_path)
    stream = fs.create(out, True)
    stream.write(html.encode('utf-8'))
    stream.close()
    logger.info(f"[generate_s3_html_report] Report written to {report_path}")
    return {'report_path': report_path}


@task.pyspark(conn_id='spark_default')
def send_s3_report_email(report_result: dict, run_id: str, spark) -> dict:
    """Send S3 migration HTML report via email."""
    import os
    import tempfile

    from airflow.utils.email import send_email

    config = get_config()
    smtp_conn_id = config.get('smtp_conn_id', 'smtp_default')
    recipients_str = config.get('email_recipients', '')

    if not recipients_str:
        logger.warning("[send_s3_report_email] No recipients configured. Skipping.")
        return {'sent': False, 'reason': 'no_recipients'}

    recipients = [r.strip() for r in recipients_str.split(',') if r.strip()]
    report_path = report_result.get('report_path', '')

    try:
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(report_path), hadoop_conf
        )
        reader = spark._jvm.java.io.BufferedReader(
            spark._jvm.java.io.InputStreamReader(fs.open(
                spark._jvm.org.apache.hadoop.fs.Path(report_path)), "UTF-8")
        )
        lines, line = [], reader.readLine()
        while line is not None:
            lines.append(line)
            line = reader.readLine()
        reader.close()
        html_content = "\n".join(lines)

        tmp = tempfile.NamedTemporaryFile(
            mode='w', suffix='.html',
            prefix=f'{run_id}_s3_report_', delete=False
        )
        tmp.write(html_content)
        tmp.close()

        send_email(
            to=recipients,
            subject=f"S3 Metadata Migration Report — {run_id}",
            html_content=(
                f"<p>S3-to-S3 metadata migration report for run "
                f"<strong>{run_id}</strong> is attached.</p>"
            ),
            files=[tmp.name],
            conn_id=smtp_conn_id,
        )
        os.unlink(tmp.name)
        logger.info(f"[send_s3_report_email] Report sent to: {recipients}")
        return {'sent': True, 'recipients': recipients, 'report_path': report_path}
    except Exception as e:
        logger.error(f"[send_s3_report_email] Failed: {e}")
        raise Exception(f"Failed to send S3 migration report email: {e}") from e


@task.pyspark(conn_id='spark_default')
def finalize_s3_run(run_id: str, spark) -> dict:
    """Aggregate final statistics and mark run complete."""
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
            FROM {tracking_db}.s3_migration_table_status
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
        logger.error(f"[finalize_s3_run] Error querying tracking table: {e}")

    execute_with_iceberg_retry(spark, f"""
        UPDATE {tracking_db}.s3_migration_runs
        SET status = '{final_status}',
            completed_at = current_timestamp(),
            total_tables = {stats['total']},
            successful_tables = {stats['successful']},
            failed_tables = {stats['failed']},
            missing_tables = {stats['missing']}
        WHERE run_id = '{run_id}'
    """, task_label="finalize_s3_run:update")

    logger.info(
        f"[finalize_s3_run] Run '{run_id}' → {final_status} | "
        f"total={stats['total']} validated={stats['successful']} "
        f"failed={stats['failed']} missing={stats['missing']}"
    )
    return {'run_id': run_id, 'status': final_status, **stats}


# =============================================================================
# DAG 4 DEFINITION
# =============================================================================

with DAG(
    dag_id='s3_to_s3_metadata_migration',
    default_args=DEFAULT_ARGS,
    description='Metadata-only migration: recreate Hive tables pointing to dest S3',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=5,
    tags=['migration', 's3', 'hive', 'metadata-only', 'netapp'],
    params={
        'excel_file_path': Param(
            default='s3a://config-bucket/s3_metadata_migration.xlsx',
            type='string',
            description='S3 path to the Excel config file for metadata migration',
        )
    },
    render_template_as_native_obj=True,
) as dag_s3_metadata:

    t_init = init_s3_tracking_tables()
    t_run_id = create_s3_migration_run(
        excel_file_path="{{ params.excel_file_path }}",
        dag_run_id="{{ run_id }}"
    )
    t_excel = parse_s3_excel(
        excel_file_path="{{ params.excel_file_path }}",
        run_id=t_run_id
    )

    # Dynamic task mapping (per database)
    t_discover = discover_source_hive_tables.expand(db_config=t_excel)
    t_record = record_s3_discovered_tables.expand(discovery=t_discover)
    t_record.operator.trigger_rule = 'all_done'

    t_presence = validate_data_presence.expand(discovery=t_record)
    t_presence.operator.trigger_rule = 'all_done'

    t_pres_status = update_data_presence_status.expand(presence_result=t_presence)
    t_pres_status.operator.trigger_rule = 'all_done'

    t_tables = create_dest_hive_tables.expand(presence_result=t_pres_status)
    t_tables.operator.trigger_rule = 'all_done'

    t_tbl_status = update_s3_table_create_status.expand(table_result=t_tables)
    t_tbl_status.operator.trigger_rule = 'all_done'

    t_validate = validate_s3_destination_tables.expand(table_result=t_tbl_status)
    t_validate.operator.max_active_tis_per_dagrun = 3
    t_validate.operator.trigger_rule = 'all_done'

    t_val_status = update_s3_validation_status.expand(validation_result=t_validate)
    t_val_status.operator.trigger_rule = 'all_done'

    t_report = generate_s3_html_report(run_id=t_run_id)
    t_report.operator.trigger_rule = 'all_done'

    t_email = send_s3_report_email(run_id=t_run_id, report_result=t_report)
    t_email.operator.trigger_rule = 'all_done'

    t_final = finalize_s3_run(run_id=t_run_id)
    t_final.operator.trigger_rule = 'all_done'

    # Dependency chain
    t_init >> t_run_id >> t_excel >> t_discover >> t_record
    t_record >> t_presence >> t_pres_status >> t_tables >> t_tbl_status
    t_tbl_status >> t_validate >> t_val_status >> t_report >> t_email >> t_final
