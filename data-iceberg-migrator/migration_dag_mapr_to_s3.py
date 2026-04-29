"""
DAG 1: MapR/HDFS to S3 Migration

Orchestrates migration of Hive tables from MapR or HDFS to S3:
- Excel config from S3 (only DAG parameter)
- SSH operations for MapR or Kerberos authentication, discovery, DistCp (24h timeout)
- PySpark tasks for Hive table creation
- Incremental support (DistCp -update, table repair)
- Comprehensive validation (row counts, partitions, schema)

Excel columns: database | table | dest database | bucket | endpoint
"""

import json
import logging
import os
from datetime import datetime, timedelta
from pathlib import Path
import re

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.providers.ssh.hooks.ssh import SSHHook
from dotenv import load_dotenv
from utils.migrations.partition_utils import (
    partitions_to_where_clause as _partitions_to_where_clause,
)
from utils.migrations.shared import (
    SSH_COMMAND_TIMEOUT,
    _login_shell,
    build_s3_opts,
    cluster_login,
    execute_with_iceberg_retry,
    get_config,
    normalize_s3,
    track_duration,
    validate_bucket_endpoint_pairs,
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

default_args = {
    'owner': 'data-migration',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# =============================================================================
# DAG 1: SOURCE (MapR or HDP) TO S3 MIGRATION TASKS
# =============================================================================

@task
def validate_prerequisites(run_id: str) -> dict:
    """Comprehensive pre-dag validation of all required components."""
    config = get_config()
    validation_results = {
        'ssh_connectivity': False,
        'cluster_auth': False,
        'pyspark_available': False,
        'hive_available': False,
        'hadoop_fs_available': False,
        'errors': []
    }

    auth_method = config.get('auth_method', 'mapr')
    mapr_user = config.get('mapr_user', '')

    logger.info("="*60)
    logger.info("STARTING PRE-DAG VALIDATION")
    logger.info("="*60)

    try:
        ssh = SSHHook(ssh_conn_id=config['ssh_conn_id'])
        with ssh.get_conn() as client:

            # 1. SSH Connectivity
            logger.info("[1/4] Testing SSH connectivity...")
            _, stdout, stderr = client.exec_command(_login_shell('echo "SSH_TEST_OK"', config.get('cluster_type', 'MapR')), timeout=30)
            output = stdout.read().decode()
            stderr.read()
            exit_code = stdout.channel.recv_exit_status()

            if exit_code == 0 and "SSH_TEST_OK" in output:
                validation_results['ssh_connectivity'] = True
                logger.info("SSH connectivity: PASSED")
            else:
                error_msg = f"SSH command failed with exit code {exit_code}"
                validation_results['errors'].append(f"SSH: {error_msg}")
                logger.error(f"SSH connectivity: FAILED - {error_msg}")

            # 2. Cluster authentication
            # MapR:     maprlogin print — verifies a valid ticket exists
            # Kerberos: klist -s       — exits 0 if valid TGT in ccache, 1 if not
            # none:     skipped
            logger.info(f"[2/4] Testing cluster authentication (auth_method={auth_method})...")
            if auth_method == 'mapr':
                inner_cmd = f"""
if maprlogin print 2>/dev/null | grep -q "{mapr_user}"; then
    echo "CLUSTER_AUTH_OK"
else
    echo "CLUSTER_AUTH_FAIL: No valid MapR ticket found for user '{mapr_user}'. Run maprlogin on the edge node."
    exit 1
fi
"""
            elif auth_method == 'kinit':
                inner_cmd = """
if klist -s 2>/dev/null; then
    echo "CLUSTER_AUTH_OK"
    klist 2>/dev/null | head -4
else
    echo "CLUSTER_AUTH_FAIL: No valid Kerberos TGT found. Ensure ~/.profile runs kinit or a valid ticket exists."
    exit 1
fi
"""
            else:
                inner_cmd = 'echo "CLUSTER_AUTH_OK"'

            _, stdout, stderr = client.exec_command(_login_shell(inner_cmd, config.get('cluster_type', 'MapR')), timeout=30)
            output = stdout.read().decode()
            stderr.read()
            exit_code = stdout.channel.recv_exit_status()

            if exit_code == 0 and 'CLUSTER_AUTH_OK' in output:
                validation_results['cluster_auth'] = True
                logger.info(f"Cluster auth ({auth_method}): PASSED")
                if auth_method == 'kinit':
                    logger.info(f"TGT info: {output.strip()[:300]}")
            else:
                error_msg = output.replace('CLUSTER_AUTH_FAIL: ', '').strip() or f"Auth check failed (exit={exit_code})"
                validation_results['errors'].append(f"Cluster auth: {error_msg}")
                logger.error(f"Cluster auth ({auth_method}): FAILED - {error_msg}")

            # 3. PySpark + Hive metastore
            # Runs a real SparkSession with enableHiveSupport() + SHOW DATABASES.
            logger.info("[3/4] Testing PySpark with Hive metastore support (enableHiveSupport + SHOW DATABASES)...")
            pyspark_inner = """
pyspark --master local[*] << 'PYSPARK_VALIDATION_EOF'
from pyspark.sql import SparkSession
spark = SparkSession.builder.enableHiveSupport().getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.sql("SHOW DATABASES").collect()
spark.stop()
print("PYSPARK_HIVE_OK")
PYSPARK_VALIDATION_EOF
"""
            _, stdout, stderr = client.exec_command(_login_shell(pyspark_inner, config.get('cluster_type', 'MapR')), timeout=180, get_pty=True)
            output = stdout.read().decode()
            stderr.read()
            exit_code = stdout.channel.recv_exit_status()

            if exit_code == 0 and 'PYSPARK_HIVE_OK' in output:
                validation_results['pyspark_available'] = True
                validation_results['hive_available'] = True
                logger.info("PySpark + Hive metastore: PASSED")
            else:
                error_msg = f"PySpark/Hive check failed (exit={exit_code}). Output: {output[-400:]}"
                validation_results['errors'].append(f"PySpark/Hive: {error_msg}")
                logger.error(f"PySpark + Hive metastore: FAILED - {error_msg}")

            # 4. Hadoop FS access (works for both MapR-FS and HDFS sources)
            # For HDFS HA clusters set hdfs_nameservice; MapR/non-HA uses / directly.
            logger.info("[4/4] Testing Hadoop FS access...")
            hdfs_nameservice = config.get('hdfs_nameservice', '')
            fs_root = f"hdfs://{hdfs_nameservice}/" if hdfs_nameservice else "/"
            hadoop_inner = f"""
if hadoop fs -ls {fs_root} > /dev/null 2>&1; then
    echo "HADOOP_FS_OK"
else
    echo "HADOOP_FS_FAIL: hadoop fs -ls returned non-zero"
fi
"""
            _, stdout, stderr = client.exec_command(_login_shell(hadoop_inner, config.get('cluster_type', 'MapR')), timeout=60)
            output = stdout.read().decode()
            stderr.read()
            exit_code = stdout.channel.recv_exit_status()

            if 'HADOOP_FS_OK' in output:
                validation_results['hadoop_fs_available'] = True
                logger.info("Hadoop FS: PASSED")
            else:
                error_msg = output.replace('HADOOP_FS_FAIL: ', '').strip() or f"Hadoop FS access failed (exit={exit_code})"
                validation_results['errors'].append(f"Hadoop FS: {error_msg}")
                logger.error(f"Hadoop FS: FAILED - {error_msg}")

    except Exception as e:
        error_msg = f"SSH connection failed: {str(e)}"
        if not validation_results['ssh_connectivity']:
            validation_results['errors'].append(f"SSH: {error_msg}")
            logger.error(f"SSH connectivity: FAILED - {error_msg}")
        if not validation_results['cluster_auth']:
            validation_results['errors'].append("Cluster auth: Skipped due to SSH failure")
            logger.warning("Cluster auth: SKIPPED (SSH failed)")
        if not validation_results['pyspark_available']:
            validation_results['errors'].append("PySpark/Hive: Skipped due to SSH failure")
            logger.warning("PySpark/Hive: SKIPPED (SSH failed)")
        if not validation_results['hadoop_fs_available']:
            validation_results['errors'].append("Hadoop FS: Skipped due to SSH failure")
            logger.warning("Hadoop FS: SKIPPED (SSH failed)")

    # Final validation check
    logger.info("\n" + "="*60)
    logger.info("VALIDATION SUMMARY")
    logger.info("="*60)

    all_passed = all([
        validation_results['ssh_connectivity'],
        validation_results['cluster_auth'],
        validation_results['pyspark_available'],
        validation_results['hive_available'],
        validation_results['hadoop_fs_available']
    ])

    if all_passed:
        logger.info("ALL PRE-DAG CHECKS PASSED")
        logger.info("="*60)
        return validation_results
    else:
        logger.error("SOME PRE-DAG CHECKS FAILED")
        logger.warning("\nFailed checks:")
        for error in validation_results['errors']:
            logger.warning(f"  - {error}")
        logger.info("="*60)

        raise Exception(
            f"Pre-DAG validation failed. "
            f"{len(validation_results['errors'])} check(s) failed:\n" +
            "\n".join(f"  - {e}" for e in validation_results['errors'])
        )

@task.pyspark(conn_id='spark_default')
def init_tracking_tables(spark) -> dict:
    """Create Iceberg tracking tables if they don't exist."""
    config = get_config()
    tracking_db = config['tracking_database']
    tracking_loc = config['tracking_location']

    # Create database
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {tracking_db} LOCATION '{tracking_loc}'")

    # Migration runs Iceberg table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tracking_db}.migration_runs (
            run_id STRING,
            dag_run_id STRING,
            excel_file_path STRING,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            status STRING,
            total_tables INT,
            successful_tables INT,
            failed_tables INT,
            config_json STRING
        )
        USING iceberg
        LOCATION '{tracking_loc}/migration_runs'
    """)

    # Table-level tracking Iceberg table
    spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {tracking_db}.migration_table_status (
                run_id STRING,
                source_database STRING,
                source_table STRING,
                dest_database STRING,
                dest_bucket STRING,
                dest_location STRING,
                source_location STRING,
                file_format STRING,
                partition_count INT,
                is_partitioned BOOLEAN,
                schema_json STRING,
                partitions_json STRING,
                partition_columns STRING,
                table_type STRING,
                source_row_count BIGINT,
                source_total_size_bytes BIGINT,
                source_file_count BIGINT,
                s3_total_size_bytes_before BIGINT,
                s3_file_count_before BIGINT,
                s3_total_size_bytes_after BIGINT,
                s3_file_count_after BIGINT,
                s3_bytes_transferred BIGINT,
                s3_files_transferred BIGINT,
                file_size_match BOOLEAN,
                file_count_match BOOLEAN,
                partition_filter STRING,
                filtered_partition_count INT,
                full_table_row_count BIGINT,
                full_table_partition_count INT,
                discovery_status STRING,
                discovery_completed_at TIMESTAMP,
                discovery_duration_seconds DOUBLE,
                distcp_status STRING,
                distcp_started_at TIMESTAMP,
                distcp_completed_at TIMESTAMP,
                distcp_duration_seconds DOUBLE,
                distcp_is_incremental BOOLEAN,
                distcp_bytes_copied BIGINT,
                distcp_files_copied BIGINT,
                yarn_application_id STRING,
                table_create_status STRING,
                table_create_completed_at TIMESTAMP,
                table_create_duration_seconds DOUBLE,
                table_already_existed BOOLEAN,
                validation_status STRING,
                validation_completed_at TIMESTAMP,
                validation_duration_seconds DOUBLE,
                dest_hive_row_count BIGINT,
                source_partition_count INT,
                unregistered_partitions BOOLEAN,
                dest_partition_count INT,
                row_count_match BOOLEAN,
                partition_count_match BOOLEAN,
                schema_match BOOLEAN,
                schema_differences STRING,
                overall_status STRING,
                error_message STRING,
                updated_at TIMESTAMP
            )
            USING iceberg
            PARTITIONED BY (source_database)
            LOCATION '{tracking_loc}/migration_table_status'
        """)

    return {'status': 'initialized', 'database': tracking_db}


@task.pyspark(conn_id='spark_default')
def create_migration_run(excel_file_path: str, dag_run_id: str, spark) -> str:
    """Create migration run record in Iceberg tracking table."""
    import uuid
    from datetime import datetime

    config = get_config()
    tracking_db = config['tracking_database']

    run_id = f"run_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"

    spark.sql(f"""
        INSERT INTO {tracking_db}.migration_runs
        VALUES (
            '{run_id}',
            '{dag_run_id}',
            '{excel_file_path}',
            current_timestamp(),
            NULL,
            'RUNNING',
            0, 0, 0,
            '{json.dumps(config).replace("'", "''")}'
        )
    """)

    return run_id


@task.pyspark(conn_id='spark_default')
def parse_excel(excel_file_path: str, run_id: str, spark) -> list:
    """Read Excel config from S3 using pandas.read_excel."""
    import math
    from io import BytesIO

    import pandas as ps

    config = get_config()
    binary_df = spark.read.format("binaryFile").load(excel_file_path)
    row = binary_df.select("content").first()
    excel_bytes = bytes(row.content)
    df = ps.read_excel(BytesIO(excel_bytes), engine='openpyxl')

    # Normalize column names
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

    # Convert to list of dicts
    grouped = {}
    for _, row in df.iterrows():
        src_db = str(row.get('database', '') or '').strip()
        if not src_db:
            continue

        raw_pf = row.get('partition_filter', '')
        partition_filter = None
        if raw_pf is not None and not (isinstance(raw_pf, float) and math.isnan(raw_pf)):
            stripped = str(raw_pf).strip()
            if stripped and stripped.lower() not in ('', 'nan', 'none'):
                partition_filter = stripped

        raw_cell_val = row.get('table', '')
        raw_cell = '*' if (raw_cell_val is None or (isinstance(raw_cell_val, float) and __import__('math').isnan(raw_cell_val)) or str(raw_cell_val).strip().lower() in ('', 'nan')) else str(raw_cell_val).strip() or '*'
        dest_db_val = row.get('dest_database', '')
        dest_db = src_db if (dest_db_val is None or (isinstance(dest_db_val, float) and __import__('math').isnan(dest_db_val)) or str(dest_db_val).strip().lower() in ('', 'nan')) else str(dest_db_val).strip() or src_db
        raw_bucket_val = row.get('bucket', '')
        raw_bucket = '' if (raw_bucket_val is None or (isinstance(raw_bucket_val, float) and __import__('math').isnan(raw_bucket_val)) or str(raw_bucket_val).strip().lower() == 'nan') else str(raw_bucket_val).strip()
        bucket_val = normalize_s3(raw_bucket) if raw_bucket else config['default_s3_bucket']

        raw_endpoint_val = row.get('endpoint', '')
        endpoint_val = '' if (raw_endpoint_val is None or (isinstance(raw_endpoint_val, float) and __import__('math').isnan(raw_endpoint_val)) or str(raw_endpoint_val).strip().lower() in ('', 'nan')) else str(raw_endpoint_val).strip()

        if '*' in [t.strip() for t in raw_cell.split(',')] and partition_filter:
            logger.warning(
                f"[ParseExcel] Wildcard '*' combined with partition_filter='{partition_filter}' "
                f"in {src_db}. Wildcard will be expanded per-table with filter applied to each."
            )

        # Warn if comma-separated tables used with a filter
        tokens_in_row = [t.strip() for t in raw_cell.split(',') if t.strip()]
        if len(tokens_in_row) > 1 and partition_filter:
            logger.warning(
                f"[ParseExcel] Comma-separated tables {tokens_in_row} combined with "
                f"partition_filter='{partition_filter}' in {src_db}. "
                f"Each table will inherit the same filter."
            )

        # Grouping key includes endpoint so same bucket on different tenants gets separate task instances
        key = (src_db, dest_db, bucket_val, endpoint_val, partition_filter)
        if key not in grouped:
            grouped[key] = {'bucket': bucket_val, 'endpoint': endpoint_val, 'tokens': [], 'partition_filter': partition_filter}

        for tok in raw_cell.split(','):
            tok = tok.strip()
            if tok:
                grouped[key]['tokens'].append(tok)

    validate_bucket_endpoint_pairs(grouped, config)
    configs = []

    for (src_db, dest_db, bucket_val, endpoint_val, partition_filter), group in grouped.items():
        unique_tokens = list(dict.fromkeys(group['tokens']))
        if '*' in unique_tokens:
            unique_tokens = ['*']

        bucket_val = group['bucket']
        endpoint_val = group['endpoint']
        partition_filter = group['partition_filter']

        logger.info(
            f"[ParseExcel] {src_db} -> dest={dest_db} | bucket={bucket_val}"
            + (f" | endpoint={endpoint_val}" if endpoint_val else "")
            + (f" | partition_filter={partition_filter}" if partition_filter else "")
            + f" | tokens={unique_tokens[:10]}"
            + (" ..." if len(unique_tokens) > 10 else "")
        )

        configs.append({
            'source_database': src_db,
            'table_tokens': unique_tokens,
            'dest_database': dest_db,
            'dest_bucket': bucket_val,
            'dest_endpoint': endpoint_val,
            'run_id': run_id,
            'partition_filter': partition_filter,
        })

    logger.info(f"[ParseExcel] Total database configs emitted: {len(configs)}")
    return configs


@task
def cluster_login_setup(run_id: str) -> dict:
    """SSH to edge, perform cluster login (MapR or Kerberos), create temp dir."""
    return cluster_login(run_id)


@task
@track_duration
def discover_tables_via_spark_ssh(db_config: dict) -> dict:
    """Use Spark SQL via SSH on edge node to discover tables and metadata."""
    import json
    import re

    config = get_config()
    ssh = SSHHook(ssh_conn_id=config['ssh_conn_id'])

    run_id = db_config['run_id']
    src_db = db_config['source_database']
    raw_tokens = db_config.get('table_tokens') or []
    if not raw_tokens:
        pattern_str = db_config.get('table_pattern', '*')
        raw_tokens = [t.strip() for t in pattern_str.split(',') if t.strip()] or ['*']
    dest_db = db_config['dest_database']
    dest_bucket = db_config['dest_bucket']
    dest_endpoint = db_config.get('dest_endpoint', '')
    tokens_json = json.dumps(raw_tokens)
    partition_filter = db_config.get('partition_filter') or ''

    dest_bucket_slug = re.sub(r'[^a-zA-Z0-9_-]', '_', dest_bucket)
    if dest_endpoint:
        dest_endpoint_slug = re.sub(r'[^a-zA-Z0-9_-]', '_', dest_endpoint)
        dest_bucket_slug = f"{dest_bucket_slug}_{dest_endpoint_slug}"

    if partition_filter:
        pf_slug = re.sub(r"[^a-zA-Z0-9_-]", "_", partition_filter)[:40]
        dest_bucket_slug = f"{dest_bucket_slug}_{pf_slug}"

    with ssh.get_conn() as client:
        temp_dir = f"/tmp/discovery_{run_id}_{src_db}_{dest_db}_{dest_bucket_slug}"
        _, cmd_stdout, _ = client.exec_command(f"mkdir -p {temp_dir}", timeout=60)
        cmd_stdout.channel.recv_exit_status()

        pyspark_script = '''
import json
import sys
import fnmatch
from pyspark.sql import SparkSession
sys.path.insert(0, "{temp_dir}")
from partition_utils import apply_partition_filter, partitions_to_where_clause

spark = SparkSession.builder \\
    .appName("table_discovery_{run_id}_{src_db}_{dest_db}_{dest_bucket_slug}") \\
    .enableHiveSupport() \\
    .getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

src_db = "{src_db}"
dest_db = "{dest_db}"
dest_bucket = "{dest_bucket}"

tokens = json.loads('{tokens_json_escaped}')

def resolve_tokens(spark, db, tokens):
    resolved = []
    seen = set()

    for tok in tokens:
        if tok == '*':
            rows = spark.sql("SHOW TABLES IN {{0}}".format(db)).collect()
            for r in rows:
                t = r.tableName
                if t not in seen:
                    seen.add(t)
                    resolved.append(t)
        elif '*' in tok:
            rows = spark.sql(
                "SHOW TABLES IN {{0}} LIKE '{{1}}'".format(db, tok)
            ).collect()
            for r in rows:
                t = r.tableName
                if t not in seen:
                    seen.add(t)
                    resolved.append(t)
        else:
            if tok not in seen:
                seen.add(tok)
                resolved.append(tok)

    return resolved

table_list = resolve_tokens(spark, src_db, tokens)

metadata = []

for tbl in table_list:
    loc = None
    table_type = "UNKNOWN"
    input_format = None
    serde_properties = {{}}
    in_serde_section = False
    try:
        desc_df = spark.sql(
            "DESCRIBE FORMATTED {{0}}.{{1}}".format(src_db, tbl)
        )
        desc_rows = desc_df.collect()

        for row in desc_rows:
            col_name = (row.col_name or "").strip().rstrip(":").lower()
            data_type = (row.data_type or "").strip()

            if col_name == "location":
                loc = data_type
            elif col_name in ("type", "table type"):
                table_type = data_type.replace("_TABLE", "")
            elif col_name == "inputformat":
                input_format = data_type
            elif col_name in (
                "storage properties",
                "storage desc params",
                "serde library",
                "serialization lib",
                "parameters",
            ):
                raw = data_type.strip("[]")
                for pair in raw.split(", "):
                    if "=" in pair:
                        k, _, v = pair.partition("=")
                        k = k.strip()
                        v = v.strip()
                        if k in ("field.delim", "escape.delim", "null.format", "field delimiter"):
                            serde_properties[k] = v

        source_total_size = 0
        source_file_count = 0
        if loc:
            try:
                from py4j.java_gateway import java_import
                java_import(spark._jvm, "org.apache.hadoop.fs.*")

                fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                    spark._jvm.java.net.URI(loc),
                    spark._jsc.hadoopConfiguration()
                )
                path = spark._jvm.org.apache.hadoop.fs.Path(loc)

                if fs.exists(path):
                    content_summary = fs.getContentSummary(path)
                    source_total_size = int(content_summary.getLength())
                    source_file_count = int(content_summary.getFileCount())
            except Exception:
                pass

        file_format = "PARQUET"
        if input_format:
            if "parquet" in input_format.lower():
                file_format = "PARQUET"
            elif "orc" in input_format.lower():
                file_format = "ORC"
            elif "avro" in input_format.lower():
                file_format = "AVRO"
            elif "text" in input_format.lower():
                file_format = "TEXTFILE"

        row_count = 0
        try:
            row_count = spark.sql(
                "SELECT COUNT(*) as c FROM {{0}}.{{1}}".format(src_db, tbl)
            ).collect()[0].c
        except Exception:
            pass

        partition_cols_from_describe = []
        in_partition_section = False
        for row in desc_rows:
            col_name = (row.col_name or "").strip()
            if col_name == "# Partition Information":
                in_partition_section = True
                continue
            if in_partition_section and col_name == "# col_name":
                continue
            if in_partition_section and col_name.startswith("#"):
                break
            if in_partition_section and col_name:
                partition_cols_from_describe.append(col_name)

        partition_definition = len(partition_cols_from_describe) > 0
        partition_columns = ",".join(partition_cols_from_describe)

        partitions = []
        registered_partition_count = 0
        filtered_partitions = []
        partition_filter_active = False
        filtered_source_size = source_total_size
        filtered_file_count  = source_file_count
        full_row_count = row_count
        full_partition_count = 0
        try:
            parts_df = spark.sql(
                "SHOW PARTITIONS {{0}}.{{1}}".format(src_db, tbl)
            )
            partitions = [row.partition for row in parts_df.collect()]
            registered_partition_count = len(partitions)
        except Exception:
            pass

        # Apply partition filter
        filter_expr = "{filter_expr_escaped}"
        full_partition_count = len(partitions)
        full_row_count = row_count  # already computed above
        filtered_partitions = apply_partition_filter(partitions, filter_expr)
        partition_filter_active = bool(filter_expr) and len(filtered_partitions) < len(partitions)

        # Warn edge cases
        if filter_expr and not partition_definition:
            import sys as _sys
            partition_filter_active = False
            filtered_partitions = partitions

        # Filtered size and file count
        filtered_source_size = source_total_size
        filtered_file_count  = source_file_count
        if partition_filter_active and filtered_partitions and loc:
            filtered_source_size = 0
            filtered_file_count  = 0
            for part_str in filtered_partitions:
                try:
                    part_path = spark._jvm.org.apache.hadoop.fs.Path(loc + "/" + part_str)
                    if fs.exists(part_path):
                        cs = fs.getContentSummary(part_path)
                        filtered_source_size += int(cs.getLength())
                        filtered_file_count  += int(cs.getFileCount())
                except Exception:
                    pass

        # Filtered row count
        if partition_filter_active and filtered_partitions:
            try:
                where_clause = partitions_to_where_clause(filtered_partitions)
                row_count = spark.sql(
                    "SELECT COUNT(*) as c FROM {{0}}.{{1}} WHERE ".format(src_db, tbl) + where_clause
                ).collect()[0].c
            except Exception:
                pass

        is_partitioned = partition_definition
        unregistered_partitions = partition_definition and registered_partition_count == 0

        schema_df = spark.sql(
            "DESCRIBE {{0}}.{{1}}".format(src_db, tbl)
        )
        schema = []
        for row in schema_df.collect():
            col_name = row.col_name.strip() if row.col_name else ""
            data_type = row.data_type.strip() if row.data_type else ""

            if col_name.startswith("#") or col_name == "" or col_name == "col_name":
                break

            schema.append({{"name": col_name, "type": data_type}})

        s3_location = "{{0}}/{{1}}/{{2}}".format(dest_bucket, dest_db, tbl)

        metadata.append({{
            "source_database": src_db,
            "source_table": tbl,
            "dest_database": dest_db,
            "dest_bucket": dest_bucket,
            "source_location": loc or "",
            "s3_location": s3_location,
            "file_format": file_format,
            "schema": schema,
            "partitions": filtered_partitions,
            "partition_columns": partition_columns,
            "partition_count": len(partitions),
            "row_count": row_count,
            "is_partitioned": is_partitioned,
            "unregistered_partitions": unregistered_partitions,
            "table_type": table_type,
            "source_total_size_bytes": source_total_size,
            "serde_properties": serde_properties,
            "source_file_count": source_file_count,
            "partition_filter": filter_expr or None,
            "filtered_partitions": filtered_partitions,
            "partition_filter_active": partition_filter_active,
            "filtered_row_count": row_count,
            "filtered_source_size_bytes": filtered_source_size,
            "filtered_file_count": filtered_file_count,
            "full_table_row_count": full_row_count,
            "full_table_partition_count": full_partition_count,
        }})

    except Exception as e:
        metadata.append({{
            "source_database": src_db,
            "source_table": tbl,
            "dest_database": dest_db,
            "dest_bucket": dest_bucket,
            "source_location": "",
            "s3_location": dest_bucket + "/" + dest_db + "/" + tbl,
            "file_format": "PARQUET",
            "schema": [],
            "partitions": [],
            "partition_columns": "",
            "partition_count": 0,
            "row_count": 0,
            "is_partitioned": False,
            "unregistered_partitions": False,
            "table_type": "UNKNOWN",
            "source_total_size_bytes": 0,
            "source_file_count": 0,
            "serde_properties": serde_properties,
            "partition_filter": filter_expr or None,
            "filtered_partitions": filtered_partitions,
            "partition_filter_active": partition_filter_active,
            "filtered_row_count": row_count,
            "filtered_source_size_bytes": filtered_source_size,
            "filtered_file_count": filtered_file_count,
            "full_table_row_count": full_row_count,
            "full_table_partition_count": full_partition_count,
            "error": str(e)[:500]
        }})

print ("===JSON_START===")
sys.stdout.flush()
print (json.dumps(metadata))
sys.stdout.flush()
print ("===JSON_END===")
sys.stdout.flush()

spark.stop()
'''.format(
        run_id=run_id,
        src_db=src_db,
        tokens_json_escaped=tokens_json.replace("'", "\\'"),
        dest_db=dest_db,
        dest_bucket=dest_bucket,
        dest_bucket_slug=dest_bucket_slug,
        filter_expr_escaped=partition_filter.replace("'", "\\'").replace('"', '\\"'),
        temp_dir=temp_dir,
        )

        script_path = f"{temp_dir}/discover_tables.py"
        sftp = client.open_sftp()
        with sftp.file(script_path, 'w') as f:
            f.write(pyspark_script)
        local_utils = Path(__file__).resolve().parent / 'utils' / 'migrations' / 'partition_utils.py'
        sftp.put(str(local_utils), f"{temp_dir}/partition_utils.py")
        sftp.close()

        # Use pyspark < script.py instead of spark-submit
        cmd = f"""
cd {temp_dir}
pyspark --master local[*] < {script_path} 2>&1 | tee discovery_{run_id}_{src_db}_{dest_db}_{dest_bucket_slug}.log
"""
        _, stdout, stderr = client.exec_command(_login_shell(cmd, config.get('cluster_type', 'MapR')), timeout=3600, get_pty=True)
        output = stdout.read().decode()
        error_output = stderr.read().decode()
        exit_code = stdout.channel.recv_exit_status()

        logger.info("=== Spark Discovery Output ===")
        logger.info(output[-1000:])

        # client.exec_command(f"rm -rf {temp_dir}", timeout=60)

        if exit_code != 0:
            logger.error("=== Spark Discovery Errors ===")
            logger.error(error_output)
            raise Exception(
                f"Table discovery Spark job failed with exit code {exit_code}\n"
                f"Error: {error_output[:1000]}\n"
                f"Output: {output[-500:]}"
            )

        json_start = output.find("===JSON_START===")
        json_end = output.find("===JSON_END===")

        if json_start == -1 or json_end == -1:
            raise Exception(f"Could not find JSON markers in output: {output}")

        json_str = output[
            json_start + len("===JSON_START==="):json_end
        ].strip()
        metadata = json.loads(json_str)

    logger.info(f"Discovery complete for database '{src_db}': {len(metadata)} table(s) found")
    for t in metadata:
        if 'error' not in t:
            logger.info(f"  Discovered: {src_db}.{t['source_table']} | format={t['file_format']} | partitions={t['partition_count']} | rows={t['row_count']} | size={t.get('source_total_size_bytes',0)/(1024**2):.1f} MB")
        else:
            logger.error(f"  Discovery FAILED: {src_db}.{t['source_table']} | error={t.get('error','')[:200]}")
    failed_discoveries = [t for t in metadata if 'error' in t]

    if failed_discoveries:
        failed_count = len(failed_discoveries)
        total_count = len(metadata)
        failed_names = ', '.join([t['source_table'] for t in failed_discoveries[:3]])

        raise Exception(f"Discovery failed for {failed_count}/{total_count} table(s) in {src_db}: {failed_names}. ")

    return {
        'run_id': run_id,
        'source_database': src_db,
        'dest_database': dest_db,
        'dest_bucket': dest_bucket,
        'dest_endpoint': dest_endpoint,
        'tables': metadata
    }


@task.pyspark(conn_id='spark_default')
def record_discovered_tables(discovery: dict, spark) -> dict:
    """Record discovered tables in Iceberg tracking table."""

    if not isinstance(discovery, dict) or 'tables' not in discovery:
        logger.warning(f"[record_discovered_tables] Skipping invalid/failed upstream input: {type(discovery)}")
        return {}

    config = get_config()
    tracking_db = config['tracking_database']
    run_id = discovery['run_id']

    discovery_duration = discovery.get('_task_duration', 0.0)

    for t in discovery['tables']:
        parts = t.get('partitions', [])
        if isinstance(parts, str):
            parts = [p for p in parts.split(',') if p]

        schema_json = json.dumps(t.get('schema', [])).replace("'", "''")
        parts_json = json.dumps(parts).replace("'", "''")
        table_type = t.get('table_type', 'UNKNOWN')
        partition_filter_active = t.get('partition_filter_active', False)
        if partition_filter_active:
            tracking_row_count       = t.get('filtered_row_count', 0)
            tracking_partition_count = len(t.get('filtered_partitions', []))
            tracking_size_bytes      = t.get('filtered_source_size_bytes', 0)
            tracking_file_count      = t.get('filtered_file_count', 0)
        else:
            tracking_row_count       = t.get('row_count', 0)
            tracking_partition_count = t.get('partition_count', 0)
            tracking_size_bytes      = t.get('source_total_size_bytes', 0)
            tracking_file_count      = t.get('source_file_count', 0)

        full_table_row_count       = t.get('full_table_row_count', t.get('row_count', 0))
        full_table_partition_count = t.get('full_table_partition_count', t.get('partition_count', 0))
        partition_filter_val       = (t.get('partition_filter') or '').replace("'", "''")
        filtered_partition_count   = len(t.get('filtered_partitions', [])) if partition_filter_active else None
        filtered_partition_count_sql = str(filtered_partition_count) if filtered_partition_count is not None else 'NULL'

        existing = spark.sql(f"""
            SELECT COUNT(*) as cnt
            FROM {tracking_db}.migration_table_status
            WHERE run_id = '{run_id}'
              AND source_database = '{t['source_database']}'
              AND source_table = '{t['source_table']}'
        """).collect()[0]['cnt']

        if existing > 0:
            execute_with_iceberg_retry(spark, f"""
                UPDATE {tracking_db}.migration_table_status
                SET discovery_status = 'COMPLETED',
                    discovery_completed_at = current_timestamp(),
                    discovery_duration_seconds = {discovery_duration},
                    source_location = '{t['source_location']}',
                    file_format = '{t['file_format']}',
                    table_type = '{table_type}',
                    source_row_count = {tracking_row_count},
                    source_total_size_bytes = {tracking_size_bytes},
                    source_file_count = {tracking_file_count},
                    source_partition_count = {tracking_partition_count},
                    unregistered_partitions = {str(t.get('unregistered_partitions', False)).lower()},
                    partition_filter = '{partition_filter_val}',
                    filtered_partition_count = {filtered_partition_count_sql},
                    full_table_row_count = {full_table_row_count},
                    full_table_partition_count = {full_table_partition_count},
                    updated_at = current_timestamp()
                WHERE run_id = '{run_id}'
                  AND source_database = '{t['source_database']}'
                  AND source_table = '{t['source_table']}'
                  AND dest_database = '{t['dest_database']}'
            """,
            task_label=f"record_discovered_tables:{t['source_table']}")
        else:
            execute_with_iceberg_retry(spark, f"""
                INSERT INTO {tracking_db}.migration_table_status (
                    run_id, source_database, source_table, dest_database, dest_bucket,
                    dest_location, source_location, file_format,
                    partition_count, is_partitioned, schema_json, partitions_json,
                    partition_columns, table_type, source_row_count,
                    source_total_size_bytes, source_file_count,
                    source_partition_count, unregistered_partitions,
                    s3_total_size_bytes_before, s3_file_count_before,
                    s3_total_size_bytes_after, s3_file_count_after,
                    s3_bytes_transferred, s3_files_transferred,
                    file_size_match, file_count_match,
                    discovery_status, discovery_completed_at, discovery_duration_seconds,
                    distcp_status, distcp_started_at, distcp_completed_at, distcp_duration_seconds,
                    distcp_is_incremental, distcp_bytes_copied, distcp_files_copied,
                    table_create_status, table_create_completed_at, table_create_duration_seconds,
                    table_already_existed,
                    validation_status, validation_completed_at, validation_duration_seconds,
                    dest_hive_row_count, dest_partition_count,
                    row_count_match, partition_count_match, schema_match,
                    schema_differences,
                    overall_status, error_message,
                    updated_at, partition_filter, filtered_partition_count, full_table_row_count, full_table_partition_count,
                    yarn_application_id
                ) VALUES (
                    '{run_id}', '{t['source_database']}', '{t['source_table']}',
                    '{t['dest_database']}', '{t['dest_bucket']}', '{t['s3_location']}',
                    '{t['source_location']}', '{t['file_format']}',
                    {t.get('partition_count', 0)}, {str(t.get('is_partitioned', False)).lower()},
                    '{schema_json}', '{parts_json}', '{t.get('partition_columns', '')}',
                    '{table_type}', {tracking_row_count},
                    {tracking_size_bytes}, {tracking_file_count},
                    {tracking_partition_count}, {str(t.get('unregistered_partitions', False)).lower()},
                    NULL, NULL, NULL, NULL, NULL, NULL,
                    NULL, NULL,
                    'COMPLETED', current_timestamp(), {discovery_duration},
                    NULL, NULL, NULL, NULL,
                    NULL, NULL, NULL,
                    NULL, NULL, NULL,
                    NULL,
                    NULL, NULL, NULL,
                    NULL, NULL,
                    NULL, NULL, NULL,
                    NULL,
                    NULL, NULL,
                    current_timestamp(), '{partition_filter_val}', {filtered_partition_count_sql}, {full_table_row_count}, {full_table_partition_count},
                    NULL
                )
            """,
            task_label=f"record_discovered_tables:{t['source_table']}")

    return discovery


@task
@track_duration
def run_distcp_ssh(discovery: dict, cluster_setup: dict, **context) -> dict:
    """Run DistCp via SSH for all tables. Uses -update for incremental."""
    config = get_config()
    ssh = SSHHook(ssh_conn_id=config['ssh_conn_id'])

    if not isinstance(discovery, dict) or 'tables' not in discovery:
        logger.warning(f"[run_distcp_ssh] Skipping invalid/failed upstream input: {type(discovery)}")
        return {}

    tables = discovery['tables']
    temp_dir = cluster_setup['temp_dir']
    mappers = config['distcp_mappers']
    bandwidth = config['distcp_bandwidth']
    preserve_delete = config.get('distcp_preserve_delete', True)

    s3_opts = build_s3_opts(discovery['dest_bucket'], config, discovery.get('dest_endpoint', ''))

    results = []
    for t in tables:
        if t.get('error'):
            results.append({
                'source_database': t['source_database'],
                'source_table': t['source_table'],
                'dest_database': t['dest_database'],
                'status': 'SKIPPED',

            })
            continue

        if t.get('partition_filter_active') and len(t.get('filtered_partitions', [])) == 0:
            logger.warning(
                f"[DistCp] SKIPPED {t['source_database']}.{t['source_table']} — "
                f"partition_filter '{t.get('partition_filter')}' matched 0 partitions. "
                f"Full partition list: {t.get('partitions', [])[:20]}"
            )
            results.append({
                'source_database': t['source_database'],
                'source_table': t['source_table'],
                'dest_database': t['dest_database'],
                'status': 'SKIPPED',
                'error': f"partition_filter matched 0 partitions: {t.get('partition_filter')}"
            })
            continue

        src_db = t['source_database']
        tbl = t['source_table']
        dest_db_for_table = t['dest_database']
        source_loc = t['source_location']
        s3_loc = t['s3_location']

        partition_filter_active = t.get('partition_filter_active', False)
        filtered_partitions     = t.get('filtered_partitions', [])

        logger.info(f"[DistCp] Starting copy for {src_db}.{tbl}")
        logger.info(f"[DistCp]   Source : {source_loc}")
        logger.info(f"[DistCp]   Dest   : {s3_loc}")
        logger.info(f"[DistCp]   Mappers: {mappers} | Bandwidth: {bandwidth} MB/s")
        if partition_filter_active:
            logger.info(
                f"[DistCp]   Partition mode: "
                f"{'per-partition with delete preservation' if preserve_delete else 'path-list additive copy (delete disabled)'}"
            )

        if partition_filter_active and filtered_partitions:
            if preserve_delete:
                partition_copy_pairs = []
                for part_str in filtered_partitions:
                    src_part = f"{source_loc}/{part_str}"
                    dst_part = f"{s3_loc}/{part_str}"
                    partition_copy_pairs.append((src_part, dst_part))

                distcp_calls = ""
                for part_idx, (src_part, dst_part) in enumerate(partition_copy_pairs):
                    distcp_calls += f"""
echo "=== Copying partition: {src_part} -> {dst_part} ==="
run_distcp_with_retry hadoop distcp{s3_opts} -update -delete -m {mappers} -bandwidth {bandwidth} -strategy dynamic \\
    -log {temp_dir}/distcp_{tbl}_part{part_idx}.log \\
    "{src_part}" "{dst_part}"
"""

                cmd = f'''set -e

calculate_s3_metrics_hadoop() {{
    local location=$1
    if ! hadoop fs{s3_opts} -test -d "$location" 2>/dev/null; then
        echo "S3_FILE_COUNT=0"
        echo "S3_TOTAL_SIZE=0"
        return
    fi
    FILE_COUNT=$(hadoop fs{s3_opts} -ls -R "$location" 2>/dev/null | grep '^-' | wc -l)
    TOTAL_SIZE=$(hadoop fs{s3_opts} -du -s "$location" 2>/dev/null | awk '{{print $1}}')
    [ -z "$FILE_COUNT" ] && FILE_COUNT=0
    [ -z "$TOTAL_SIZE" ] && TOTAL_SIZE=0
    echo "S3_FILE_COUNT=$FILE_COUNT"
    echo "S3_TOTAL_SIZE=$TOTAL_SIZE"
}}

run_distcp_with_retry() {{
    local max_attempts=3
    local delay=30
    local attempt=1
    while [ $attempt -le $max_attempts ]; do
        echo "  [DistCp] Attempt $attempt/$max_attempts: $*"
        if "$@"; then
            return 0
        fi
        echo "  [DistCp] Attempt $attempt/$max_attempts failed"
        attempt=$((attempt + 1))
        if [ $attempt -le $max_attempts ]; then
            echo "  [DistCp] Retrying in ${{delay}}s..."
            sleep $delay
        fi
    done
    echo "  [DistCp] All $max_attempts attempts failed"
    return 1
}}

INCR=false
hadoop fs{s3_opts} -test -d {s3_loc} 2>/dev/null && INCR=true

echo "=== Calculating S3 metrics BEFORE distcp ==="
S3_BEFORE=$(calculate_s3_metrics_hadoop "{s3_loc}")
S3_FILE_COUNT_BEFORE=$(echo "$S3_BEFORE" | grep "^S3_FILE_COUNT=" | cut -d'=' -f2)
S3_TOTAL_SIZE_BEFORE=$(echo "$S3_BEFORE" | grep "^S3_TOTAL_SIZE=" | cut -d'=' -f2)
[ -z "$S3_FILE_COUNT_BEFORE" ] && S3_FILE_COUNT_BEFORE=0
[ -z "$S3_TOTAL_SIZE_BEFORE" ] && S3_TOTAL_SIZE_BEFORE=0

echo "=== Running distcp per-partition ==="
{distcp_calls}

echo "=== Calculating S3 metrics AFTER distcp ==="
S3_AFTER=$(calculate_s3_metrics_hadoop "{s3_loc}")
S3_FILE_COUNT_AFTER=$(echo "$S3_AFTER" | grep "^S3_FILE_COUNT=" | cut -d'=' -f2)
S3_TOTAL_SIZE_AFTER=$(echo "$S3_AFTER" | grep "^S3_TOTAL_SIZE=" | cut -d'=' -f2)
[ -z "$S3_FILE_COUNT_AFTER" ] && S3_FILE_COUNT_AFTER=0
[ -z "$S3_TOTAL_SIZE_AFTER" ] && S3_TOTAL_SIZE_AFTER=0

S3_FILES_TRANSFERRED=$((S3_FILE_COUNT_AFTER - S3_FILE_COUNT_BEFORE))
S3_BYTES_TRANSFERRED=$((S3_TOTAL_SIZE_AFTER - S3_TOTAL_SIZE_BEFORE))

echo "===DISTCP_METRICS_START==="
echo "INCREMENTAL=$INCR"
echo "BYTES_COPIED=0"
echo "FILES_COPIED=0"
echo "S3_FILE_COUNT_BEFORE=$S3_FILE_COUNT_BEFORE"
echo "S3_TOTAL_SIZE_BEFORE=$S3_TOTAL_SIZE_BEFORE"
echo "S3_FILE_COUNT_AFTER=$S3_FILE_COUNT_AFTER"
echo "S3_TOTAL_SIZE_AFTER=$S3_TOTAL_SIZE_AFTER"
echo "S3_FILES_TRANSFERRED=$S3_FILES_TRANSFERRED"
echo "S3_BYTES_TRANSFERRED=$S3_BYTES_TRANSFERRED"
echo "===DISTCP_METRICS_END==="
echo "PARTITIONS_REQUESTED={len(filtered_partitions)}"
exit 0
'''
            else:
                pathlist_entries = "\n".join(f"{source_loc}/{part_str}" for part_str in filtered_partitions)
                cmd = f'''set -e

calculate_s3_metrics_hadoop() {{
    local location=$1
    if ! hadoop fs{s3_opts} -test -d "$location" 2>/dev/null; then
        echo "S3_FILE_COUNT=0"
        echo "S3_TOTAL_SIZE=0"
        return
    fi
    FILE_COUNT=$(hadoop fs{s3_opts} -ls -R "$location" 2>/dev/null | grep '^-' | wc -l)
    TOTAL_SIZE=$(hadoop fs{s3_opts} -du -s "$location" 2>/dev/null | awk '{{print $1}}')
    [ -z "$FILE_COUNT" ] && FILE_COUNT=0
    [ -z "$TOTAL_SIZE" ] && TOTAL_SIZE=0
    echo "S3_FILE_COUNT=$FILE_COUNT"
    echo "S3_TOTAL_SIZE=$TOTAL_SIZE"
}}

INCR=false
hadoop fs{s3_opts} -test -d {s3_loc} 2>/dev/null && INCR=true
PATHLIST="{temp_dir}/distcp_{tbl}_sources.txt"

cat > "$PATHLIST" <<'EOF'
{pathlist_entries}
EOF

echo "=== Calculating S3 metrics BEFORE distcp ==="
S3_BEFORE=$(calculate_s3_metrics_hadoop "{s3_loc}")
S3_FILE_COUNT_BEFORE=$(echo "$S3_BEFORE" | grep "^S3_FILE_COUNT=" | cut -d'=' -f2)
S3_TOTAL_SIZE_BEFORE=$(echo "$S3_BEFORE" | grep "^S3_TOTAL_SIZE=" | cut -d'=' -f2)
[ -z "$S3_FILE_COUNT_BEFORE" ] && S3_FILE_COUNT_BEFORE=0
[ -z "$S3_TOTAL_SIZE_BEFORE" ] && S3_TOTAL_SIZE_BEFORE=0

echo "=== Running distcp using source path list (delete disabled) ==="
set +e
DISTCP_OUTPUT=$(hadoop distcp{s3_opts} -update -m {mappers} -bandwidth {bandwidth} -strategy dynamic \\
    -log {temp_dir}/distcp_{tbl}.log -f "$PATHLIST" "{s3_loc}" 2>&1)
DISTCP_EXIT=$?
set -e
echo "$DISTCP_OUTPUT"
rm -f "$PATHLIST"

BYTES_COPIED=$(echo "$DISTCP_OUTPUT" | grep -i "Bytes Copied" | awk '{{print $NF}}' | tr -d ',')
FILES_COPIED=$(echo "$DISTCP_OUTPUT" | grep -i "Number of files copied" | awk '{{print $NF}}' | tr -d ',')
[ -z "$BYTES_COPIED" ] && BYTES_COPIED=0
[ -z "$FILES_COPIED" ] && FILES_COPIED=0

echo "=== Calculating S3 metrics AFTER distcp ==="
S3_AFTER=$(calculate_s3_metrics_hadoop "{s3_loc}")
S3_FILE_COUNT_AFTER=$(echo "$S3_AFTER" | grep "^S3_FILE_COUNT=" | cut -d'=' -f2)
S3_TOTAL_SIZE_AFTER=$(echo "$S3_AFTER" | grep "^S3_TOTAL_SIZE=" | cut -d'=' -f2)
[ -z "$S3_FILE_COUNT_AFTER" ] && S3_FILE_COUNT_AFTER=0
[ -z "$S3_TOTAL_SIZE_AFTER" ] && S3_TOTAL_SIZE_AFTER=0

S3_FILES_TRANSFERRED=$((S3_FILE_COUNT_AFTER - S3_FILE_COUNT_BEFORE))
S3_BYTES_TRANSFERRED=$((S3_TOTAL_SIZE_AFTER - S3_TOTAL_SIZE_BEFORE))

echo "===DISTCP_METRICS_START==="
echo "INCREMENTAL=$INCR"
echo "BYTES_COPIED=$BYTES_COPIED"
echo "FILES_COPIED=$FILES_COPIED"
echo "S3_FILE_COUNT_BEFORE=$S3_FILE_COUNT_BEFORE"
echo "S3_TOTAL_SIZE_BEFORE=$S3_TOTAL_SIZE_BEFORE"
echo "S3_FILE_COUNT_AFTER=$S3_FILE_COUNT_AFTER"
echo "S3_TOTAL_SIZE_AFTER=$S3_TOTAL_SIZE_AFTER"
echo "S3_FILES_TRANSFERRED=$S3_FILES_TRANSFERRED"
echo "S3_BYTES_TRANSFERRED=$S3_BYTES_TRANSFERRED"
echo "===DISTCP_METRICS_END==="
echo "PARTITIONS_REQUESTED={len(filtered_partitions)}"

[ "$DISTCP_EXIT" -ne 0 ] && exit $DISTCP_EXIT
exit 0
'''
        else:
            cmd = f'''set -e

calculate_s3_metrics_hadoop() {{
    local location=$1

    if ! hadoop fs{s3_opts} -test -d "$location" 2>/dev/null; then
        echo "S3_FILE_COUNT=0"
        echo "S3_TOTAL_SIZE=0"
        return
    fi

    FILE_COUNT=$(hadoop fs{s3_opts} -ls -R "$location" 2>/dev/null | grep '^-' | wc -l)
    TOTAL_SIZE=$(hadoop fs{s3_opts} -du -s "$location" 2>/dev/null | awk '{{print $1}}')
    [ -z "$FILE_COUNT" ] && FILE_COUNT=0
    [ -z "$TOTAL_SIZE" ] && TOTAL_SIZE=0

    echo "S3_FILE_COUNT=$FILE_COUNT"
    echo "S3_TOTAL_SIZE=$TOTAL_SIZE"
}}

INCR=false
hadoop fs{s3_opts} -test -d {s3_loc} 2>/dev/null && INCR=true

echo "=== Calculating S3 metrics BEFORE distcp ==="
S3_BEFORE=$(calculate_s3_metrics_hadoop "{s3_loc}")
S3_FILE_COUNT_BEFORE=$(echo "$S3_BEFORE" | grep "^S3_FILE_COUNT=" | cut -d'=' -f2)
S3_TOTAL_SIZE_BEFORE=$(echo "$S3_BEFORE" | grep "^S3_TOTAL_SIZE=" | cut -d'=' -f2)
[ -z "$S3_FILE_COUNT_BEFORE" ] && S3_FILE_COUNT_BEFORE=0
[ -z "$S3_TOTAL_SIZE_BEFORE" ] && S3_TOTAL_SIZE_BEFORE=0

echo "=== Running distcp ==="
set +e
DISTCP_OUTPUT=$(hadoop distcp{s3_opts} -update -delete -m {mappers} -bandwidth {bandwidth} -strategy dynamic \\
    -log {temp_dir}/distcp_{tbl}.log "{source_loc}" "{s3_loc}" 2>&1)
DISTCP_EXIT=$?
set -e
echo "$DISTCP_OUTPUT"

BYTES_COPIED=$(echo "$DISTCP_OUTPUT" | grep -i "Bytes Copied" | awk '{{print $NF}}' | tr -d ',')
FILES_COPIED=$(echo "$DISTCP_OUTPUT" | grep -i "Number of files copied" | awk '{{print $NF}}' | tr -d ',')
[ -z "$BYTES_COPIED" ] && BYTES_COPIED=0
[ -z "$FILES_COPIED" ] && FILES_COPIED=0

echo "=== Calculating S3 metrics AFTER distcp ==="
S3_AFTER=$(calculate_s3_metrics_hadoop "{s3_loc}")
S3_FILE_COUNT_AFTER=$(echo "$S3_AFTER" | grep "^S3_FILE_COUNT=" | cut -d'=' -f2)
S3_TOTAL_SIZE_AFTER=$(echo "$S3_AFTER" | grep "^S3_TOTAL_SIZE=" | cut -d'=' -f2)
[ -z "$S3_FILE_COUNT_AFTER" ] && S3_FILE_COUNT_AFTER=0
[ -z "$S3_TOTAL_SIZE_AFTER" ] && S3_TOTAL_SIZE_AFTER=0

S3_FILES_TRANSFERRED=$((S3_FILE_COUNT_AFTER - S3_FILE_COUNT_BEFORE))
S3_BYTES_TRANSFERRED=$((S3_TOTAL_SIZE_AFTER - S3_TOTAL_SIZE_BEFORE))

echo "===DISTCP_METRICS_START==="
echo "INCREMENTAL=$INCR"
echo "BYTES_COPIED=$BYTES_COPIED"
echo "FILES_COPIED=$FILES_COPIED"
echo "S3_FILE_COUNT_BEFORE=$S3_FILE_COUNT_BEFORE"
echo "S3_TOTAL_SIZE_BEFORE=$S3_TOTAL_SIZE_BEFORE"
echo "S3_FILE_COUNT_AFTER=$S3_FILE_COUNT_AFTER"
echo "S3_TOTAL_SIZE_AFTER=$S3_TOTAL_SIZE_AFTER"
echo "S3_FILES_TRANSFERRED=$S3_FILES_TRANSFERRED"
echo "S3_BYTES_TRANSFERRED=$S3_BYTES_TRANSFERRED"
echo "===DISTCP_METRICS_END==="

[ "$DISTCP_EXIT" -ne 0 ] && exit $DISTCP_EXIT
exit 0
'''
        from datetime import datetime as _dt
        distcp_started_at = _dt.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        try:
            yarn_application_ids = []
            yarn_application_id = None
            with ssh.get_conn() as client:
                _, stdout, stderr = client.exec_command(_login_shell(cmd, config.get('cluster_type', 'MapR')), timeout=SSH_COMMAND_TIMEOUT, get_pty=True)
                output = stdout.read().decode()
                error_output = stderr.read().decode()
                exit_code = stdout.channel.recv_exit_status()

                logger.info(f"=== DistCp for {src_db}.{tbl} (last 1000 chars) ===")
                logger.info(output[-1000:])

                combined_output = output + "\n" + error_output

                # Extract all YARN application IDs (ordered)
                yarn_application_ids = re.findall(r'application_\d+_\d+', combined_output)

                # Deduplicate while preserving order
                yarn_application_ids = list(dict.fromkeys(yarn_application_ids))

                # Last app ID
                yarn_application_id = yarn_application_ids[-1] if yarn_application_ids else None

                if yarn_application_ids:
                    logger.info(f"[DistCp] YARN Application IDs for {src_db}.{tbl}: {yarn_application_ids}")
                    logger.info(f"[DistCp] Last YARN Application ID for {src_db}.{tbl}: {yarn_application_id}")
                else:
                    logger.warning(f"[DistCp] No YARN Application ID found for {src_db}.{tbl}")
                is_incr = False
                bytes_copied = 0
                files_copied = 0
                s3_size_before = 0
                s3_files_before = 0
                s3_size_after = 0
                s3_files_after = 0
                s3_bytes_transferred = 0
                s3_files_transferred = 0

                m_start = output.find('===DISTCP_METRICS_START===')
                m_end   = output.find('===DISTCP_METRICS_END===')
                if m_start != -1 and m_end != -1:
                    metrics = {
                        'INCREMENTAL': 'false',
                        'BYTES_COPIED': 0,
                        'FILES_COPIED': 0,
                        'S3_FILE_COUNT_BEFORE': 0,
                        'S3_TOTAL_SIZE_BEFORE': 0,
                        'S3_FILE_COUNT_AFTER': 0,
                        'S3_TOTAL_SIZE_AFTER': 0,
                        'S3_FILES_TRANSFERRED': 0,
                        'S3_BYTES_TRANSFERRED': 0,
                    }
                    metrics_block = output[m_start + len('===DISTCP_METRICS_START==='):m_end]
                    for line in metrics_block.splitlines():
                        line = line.strip()
                        if '=' not in line:
                            continue
                        key, _, val = line.partition('=')
                        key = key.strip()
                        if key not in metrics:
                            continue
                        val = val.strip()
                        if key == 'INCREMENTAL':
                            metrics[key] = val
                        else:
                            try:
                                metrics[key] = int(val or 0)
                            except ValueError:
                                logger.warning(f"[DistCp] Could not parse metric '{key}={val}' for {src_db}.{tbl}")
                    is_incr              = metrics['INCREMENTAL'].lower() == 'true'
                    bytes_copied         = metrics['BYTES_COPIED']
                    files_copied         = metrics['FILES_COPIED']
                    s3_size_before       = metrics['S3_TOTAL_SIZE_BEFORE']
                    s3_files_before      = metrics['S3_FILE_COUNT_BEFORE']
                    s3_size_after        = metrics['S3_TOTAL_SIZE_AFTER']
                    s3_files_after       = metrics['S3_FILE_COUNT_AFTER']
                    s3_bytes_transferred = metrics['S3_BYTES_TRANSFERRED']
                    s3_files_transferred = metrics['S3_FILES_TRANSFERRED']
                else:
                    logger.warning(f"[DistCp] Metrics block not found in output for {src_db}.{tbl} — all metrics will be 0")

                if exit_code != 0:
                    logger.error(f"=== DistCp Error for {src_db}.{tbl} ===")
                    logger.error(error_output[:1000])
                    raise Exception(
                        f"DistCp failed for {src_db}.{tbl} with exit code {exit_code}\n"
                        f"Error: {error_output[:1000]}"
                    )
                _end_dt = _dt.utcnow()
                distcp_completed_at = _end_dt.strftime('%Y-%m-%d %H:%M:%S')
                distcp_duration_secs = (_end_dt - _dt.strptime(distcp_started_at, '%Y-%m-%d %H:%M:%S')).total_seconds()
                logger.info(f"[DistCp] COMPLETED: {src_db}.{tbl} | incremental={is_incr} | bytes_copied={bytes_copied} | files_copied={files_copied}")

                results.append({
                    'source_database': src_db,
                    'source_table': tbl,
                    'dest_database': dest_db_for_table,
                    'status': 'COMPLETED',
                    'distcp_started_at': distcp_started_at,
                    'distcp_duration_secs': distcp_duration_secs,
                    'distcp_completed_at': distcp_completed_at,
                    'is_incremental': is_incr,
                    'bytes_copied': bytes_copied,
                    'files_copied': files_copied,
                    's3_total_size_bytes_before': s3_size_before,
                    's3_file_count_before': s3_files_before,
                    's3_total_size_bytes_after': s3_size_after,
                    's3_file_count_after': s3_files_after,
                    's3_bytes_transferred': s3_bytes_transferred,
                    's3_files_transferred': s3_files_transferred,
                    'partition_filter_active': partition_filter_active,
                    'partitions_requested': len(filtered_partitions) if partition_filter_active else None,
                    'error': None,
                    'yarn_application_id': yarn_application_id,
                    'yarn_application_ids': yarn_application_ids,
                })
        except Exception as e:
            error_msg = f"DistCp failed for {src_db}.{tbl}: {str(e)[:2000]}"
            _fail_dt = _dt.utcnow()
            results.append({
                'source_database': src_db,
                'source_table': tbl,
                'dest_database': dest_db_for_table,
                'status': 'FAILED',
                'distcp_started_at': distcp_started_at,
                'distcp_completed_at': _fail_dt.strftime('%Y-%m-%d %H:%M:%S'),
                'distcp_duration_secs': (_fail_dt - _dt.strptime(distcp_started_at, '%Y-%m-%d %H:%M:%S')).total_seconds(),
                'is_incremental': False,
                'bytes_copied': 0,
                'files_copied': 0,
                's3_total_size_bytes_before': 0,
                's3_file_count_before': 0,
                's3_total_size_bytes_after': 0,
                's3_file_count_after': 0,
                's3_bytes_transferred': 0,
                's3_files_transferred': 0,
                'partition_filter_active': partition_filter_active,
                'partitions_requested': len(filtered_partitions) if partition_filter_active else None,
                'error': str(e)[:2000],
                'yarn_application_id': yarn_application_id,
                'yarn_application_ids': yarn_application_ids
            })
            logger.error(f"ERROR: {error_msg}")

    failed_tables = [r for r in results if r['status'] == 'FAILED']
    has_failures = len(failed_tables) > 0

    result_dict = {
        **discovery,
        'distcp_results': results,
        'yarn_application_ids': list(dict.fromkeys(
            app_id
            for r in results
            for app_id in (r.get('yarn_application_ids') or [])
        )),
        '_has_failures': has_failures,
        '_failure_summary': (
            f"S3 copy failed for {len(failed_tables)}/{len(results)} table(s)"
            if has_failures else None
        )
    }

    context['ti'].xcom_push(key='return_value', value=result_dict)

    if has_failures:
        raise Exception(f"DistCp failed — {result_dict['_failure_summary']}. Per-table errors in tracking.")

    return result_dict


@task.pyspark(conn_id='spark_default')
def update_distcp_status(distcp_result: dict, spark) -> dict:
    """Update Iceberg tracking with DistCp results."""

    if not isinstance(distcp_result, dict) or 'run_id' not in distcp_result:
        logger.warning(f"[update_distcp_status] Skipping invalid input: {type(distcp_result)}")
        return {}

    config = get_config()
    tracking_db = config['tracking_database']
    run_id = distcp_result['run_id']
    src_db = distcp_result['source_database']

    for r in distcp_result.get('distcp_results', []):
        if r.get('status') == 'SKIPPED':
            continue
        overall = 'COPIED' if r['status'] == 'COMPLETED' else 'FAILED'
        error_msg = r.get('error', '').replace("'", "''") if r.get('error') else ''
        distcp_duration = r.get('distcp_duration_secs', 0.0)
        started_at = r.get('distcp_started_at', '')
        completed_at = r.get('distcp_completed_at', '')

        s3_size_before = r.get('s3_total_size_bytes_before', 0)
        s3_files_before = r.get('s3_file_count_before', 0)
        s3_size_after = r.get('s3_total_size_bytes_after', 0)
        s3_files_after = r.get('s3_file_count_after', 0)
        s3_bytes_transfer = r.get('s3_bytes_transferred', 0)
        s3_files_transfer = r.get('s3_files_transferred', 0)
        yarn_app_id = ','.join(r.get('yarn_application_ids') or ([r['yarn_application_id']] if r.get('yarn_application_id') else [])).replace("'", "''")

        execute_with_iceberg_retry(spark, f"""
            UPDATE {tracking_db}.migration_table_status
            SET distcp_status = '{r['status']}',
                distcp_started_at = CAST('{started_at}' AS TIMESTAMP),
                distcp_completed_at = CAST('{completed_at}' AS TIMESTAMP),
                distcp_duration_seconds = {distcp_duration},
                distcp_is_incremental = {str(r['is_incremental']).lower()},
                distcp_bytes_copied = {r.get('bytes_copied', 0)},
                distcp_files_copied = {r.get('files_copied', 0)},
                yarn_application_id = '{yarn_app_id}',
                s3_total_size_bytes_before = {s3_size_before},
                s3_file_count_before = {s3_files_before},
                s3_total_size_bytes_after = {s3_size_after},
                s3_file_count_after = {s3_files_after},
                s3_bytes_transferred = {s3_bytes_transfer},
                s3_files_transferred = {s3_files_transfer},
                file_count_match = CASE
                    WHEN partition_filter IS NOT NULL AND partition_filter != ''
                    THEN (source_file_count = {s3_files_transfer})
                    ELSE (source_file_count = {s3_files_after})
                END,
                file_size_match = CASE
                    WHEN partition_filter IS NOT NULL AND partition_filter != ''
                    THEN (ABS(source_total_size_bytes - {s3_bytes_transfer}) / GREATEST(source_total_size_bytes, 1) < 0.01)
                    ELSE (ABS(source_total_size_bytes - {s3_size_after}) / GREATEST(source_total_size_bytes, 1) < 0.01)
                END,
                overall_status = '{overall}',
                error_message = CASE WHEN '{r['status']}' = 'FAILED' THEN '{error_msg}' ELSE error_message END,
                updated_at = current_timestamp()
            WHERE run_id = '{run_id}'
              AND source_database = '{r['source_database']}'
              AND source_table = '{r['source_table']}'
              AND dest_database='{r['dest_database']}'
        """,
        task_label=f"update_distcp_status:{r['source_table']}")

    for r in distcp_result.get('distcp_results', []):
        if r.get('status') == 'FAILED' and r.get('error'):
            per_table_error = str(r['error'])[:2000].replace("'", "''")
            execute_with_iceberg_retry(spark, f"""
                UPDATE {tracking_db}.migration_table_status
                SET distcp_status = 'FAILED',
                    overall_status = 'FAILED',
                    error_message = '{per_table_error}',
                    updated_at = current_timestamp()
                WHERE run_id = '{run_id}'
                  AND source_database = '{r['source_database']}'
                  AND source_table = '{r['source_table']}'
                  AND distcp_status IS NULL
            """,
            task_label=f"update_distcp_status:failure_patch:{r['source_table']}")

    _distcp_processed_dbs = set(
        r.get('dest_database', distcp_result['dest_database'])
        for r in distcp_result.get('distcp_results', [])
    )
    _distcp_processed_tables = set(
        r['source_table']
        for r in distcp_result.get('distcp_results', [])
    )
    _distcp_not_in = ', '.join(f"'{t}'" for t in _distcp_processed_tables) if _distcp_processed_tables else "'__no_tables__'"

    from collections import defaultdict
    _slot_tables_by_db = defaultdict(set)
    for t in distcp_result.get('tables', []):
        _slot_tables_by_db[t.get('dest_database', distcp_result['dest_database'])].add(t['source_table'])
    if not _slot_tables_by_db:
        _slot_tables_by_db[distcp_result['dest_database']] = set()

    for _pddb, _slot_tbls in _slot_tables_by_db.items():
        _slot_in = ', '.join(f"'{t}'" for t in _slot_tbls) if _slot_tbls else "'__no_tables__'"
        execute_with_iceberg_retry(spark, f"""
            UPDATE {tracking_db}.migration_table_status
            SET distcp_status='FAILED', overall_status='FAILED',
                error_message=COALESCE(error_message,'S3 copy task did not process this table'),
                updated_at=current_timestamp()
            WHERE run_id='{run_id}' AND source_database='{src_db}'
              AND dest_database='{_pddb}'
              AND source_table IN ({_slot_in})
              AND source_table NOT IN ({_distcp_not_in})
              AND distcp_status IS NULL AND discovery_status='COMPLETED'
        """, task_label=f"update_distcp:catchall:{_pddb}")

    return distcp_result


@task.pyspark(conn_id='spark_default')
@track_duration
def create_hive_tables(distcp_result: dict, spark, **context) -> dict:
    """Create external Hive tables via Spark. Handles incremental (repairs partitions)."""

    if not isinstance(distcp_result, dict) or 'tables' not in distcp_result:
        logger.warning(f"[create_hive_tables] Skipping invalid input: {type(distcp_result)}")
        return {}

    tables = distcp_result['tables']

    results = []
    created_dbs = set()

    for t in tables:
        tbl = t['source_table']
        dest_db = t.get('dest_database') or distcp_result['dest_database']

        if dest_db not in created_dbs:
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {dest_db}")
            created_dbs.add(dest_db)

        distcp_entry = next(
            (r for r in distcp_result.get('distcp_results', [])
             if r['source_table'] == tbl and r.get('dest_database') == dest_db),
            None
        )
        if distcp_entry is None:
            distcp_entry = next(
                (r for r in distcp_result.get('distcp_results', [])
                 if r['source_table'] == tbl),
                None
            )

        distcp_status = distcp_entry['status'] if distcp_entry else 'UNKNOWN'
        if distcp_status in ('FAILED', 'SKIPPED', 'UNKNOWN'):
            results.append({
                'source_table': t['source_table'],
                'dest_database': dest_db,
                'status': 'SKIPPED',
                'error': f'DistCp status was {distcp_status}',
                'existed': False
            })
            continue

        tbl = t['source_table']
        s3_loc = t['s3_location']
        fmt = t.get('file_format', 'PARQUET')
        schema_list = t.get('schema', [])
        part_cols_str = t.get('partition_columns', '')
        is_part = t.get('is_partitioned', False)
        serde_props = t.get('serde_properties', {})
        full_name = f"{dest_db}.{tbl}"

        logger.info(f"[HiveTable] Processing {full_name} | format={fmt} | partitioned={is_part}")

        try:
            exists = False
            try:
                spark.sql(f"DESCRIBE {full_name}")
                exists = True
            except Exception:
                pass

            if exists:
                if is_part:
                    if t.get('partition_filter_active') and t.get('filtered_partitions'):
                        for part_str in t['filtered_partitions']:
                            part_kv = {}
                            for segment in part_str.split('/'):
                                if '=' in segment:
                                    k, _, v = segment.partition('=')
                                    part_kv[k.strip()] = v.strip()
                            partition_spec = ", ".join(f"{k}='{v}'" for k, v in part_kv.items())
                            part_location = f"{s3_loc}/{part_str}"
                            try:
                                spark.sql(
                                    f"ALTER TABLE {full_name} ADD IF NOT EXISTS "
                                    f"PARTITION ({partition_spec}) LOCATION '{part_location}'"
                                )
                            except Exception as add_e:
                                logger.warning(
                                    f"[HiveTable] Could not add partition {partition_spec} "
                                    f"to {full_name}: {str(add_e)[:200]}"
                                )
                    else:
                        spark.sql(f"MSCK REPAIR TABLE {full_name}")
                spark.sql(f"REFRESH TABLE {full_name}")
                logger.info(f"[HiveTable] REPAIRED (already existed): {full_name}")
                results.append({
                    'source_table': tbl,
                    'dest_database': dest_db,
                    'status': 'COMPLETED',
                    'action': 'repaired',
                    'existed': True,
                    'error': None
                })
            else:
                part_col_list = [p.strip() for p in part_cols_str.split(',') if p.strip()]

                if schema_list:
                    cols = [f"`{c['name']}` {c['type']}" for c in schema_list
                            if c.get('name') and c['name'] not in part_col_list]
                    col_def = ", ".join(cols)
                else:
                    df = spark.read.format(fmt.lower()).load(s3_loc)
                    col_def = ", ".join([
                        f"`{f.name}` {f.dataType.simpleString()}"
                        for f in df.schema.fields if f.name not in part_col_list
                    ])

                part_clause = ""
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

                row_format_clause = ""
                tbl_properties_clause = ""
                if fmt == "TEXTFILE" and serde_props:
                    field_delim = serde_props.get("field.delim") or serde_props.get("field delimiter", "")
                    escape_delim = serde_props.get("escape.delim", "")
                    null_format = serde_props.get("null.format", "")

                    if field_delim:
                        row_format_clause = f"ROW FORMAT DELIMITED FIELDS TERMINATED BY '{field_delim}'"
                        if escape_delim:
                            row_format_clause += f" ESCAPED BY '{escape_delim}'"
                    if null_format:
                        tbl_properties_clause = f"TBLPROPERTIES ('serialization.null.format'='{null_format}')"

                ddl = f"""
                    CREATE EXTERNAL TABLE IF NOT EXISTS {full_name} ({col_def})
                    {part_clause}
                    {row_format_clause}
                    STORED AS {fmt}
                    LOCATION '{s3_loc}'
                    {tbl_properties_clause}
                """
                spark.sql(ddl)

                if is_part:
                    if t.get('partition_filter_active') and t.get('filtered_partitions'):
                        for part_str in t['filtered_partitions']:
                            part_kv = {}
                            for segment in part_str.split('/'):
                                if '=' in segment:
                                    k, _, v = segment.partition('=')
                                    part_kv[k.strip()] = v.strip()
                            partition_spec = ", ".join(f"{k}='{v}'" for k, v in part_kv.items())
                            part_location = f"{s3_loc}/{part_str}"
                            try:
                                spark.sql(
                                    f"ALTER TABLE {full_name} ADD IF NOT EXISTS "
                                    f"PARTITION ({partition_spec}) LOCATION '{part_location}'"
                                )
                            except Exception as add_e:
                                logger.warning(
                                    f"[HiveTable] Could not add partition {partition_spec} "
                                    f"to {full_name}: {str(add_e)[:200]}"
                                )
                    else:
                        spark.sql(f"MSCK REPAIR TABLE {full_name}")
                spark.sql(f"REFRESH TABLE {full_name}")

                logger.info(f"[HiveTable] CREATED: {full_name} | location={s3_loc}")
                results.append({
                    'source_table': tbl,
                    'dest_database': dest_db,
                    'status': 'COMPLETED',
                    'action': 'created',
                    'existed': False,
                    'error': None
                })
        except Exception as e:
            error_msg = f"Table creation failed for {dest_db}.{tbl}: {str(e)[:2000]}"
            results.append({
                'source_table': tbl,
                'dest_database': dest_db,
                'status': 'FAILED',
                'action': 'error',
                'existed': False,
                'error': str(e)
            })
            logger.error(f"ERROR: {error_msg}")

    failed_tables = [r for r in results if r['status'] == 'FAILED']
    has_failures = len(failed_tables) > 0

    result_dict = {
        **distcp_result,
        'table_results': results,
        '_has_failures': has_failures,
        '_failure_summary': (
            f"Table creation failed for {len(failed_tables)}/{len(results)} table(s): "
            if has_failures else None
        )
    }

    context['ti'].xcom_push(key='return_value', value=result_dict)

    if has_failures:
        raise Exception(f"Hive table creation failed — {result_dict['_failure_summary']}. Per-table errors in tracking.")

    return result_dict


@task.pyspark(conn_id='spark_default')
def update_table_create_status(table_result: dict, spark) -> dict:
    """Update Iceberg tracking with table creation results."""

    if not isinstance(table_result, dict) or 'run_id' not in table_result:
        logger.warning(f"[update_table_create_status] Skipping invalid input: {type(table_result)}")
        return {}

    config = get_config()
    tracking_db = config['tracking_database']
    run_id = table_result['run_id']
    src_db = table_result['source_database']

    table_duration = table_result.get('_task_duration', 0.0)

    for r in table_result.get('table_results', []):
        per_table_dest_db = r.get('dest_database', table_result['dest_database'])
        overall = 'TABLE_CREATED' if r['status'] == 'COMPLETED' else ('FAILED' if r['status'] == 'FAILED' else 'SKIPPED')
        error_msg = (r.get('error', '') or '').replace("'", "''")[:2000]

        execute_with_iceberg_retry(spark, f"""
            UPDATE {tracking_db}.migration_table_status
            SET table_create_status = '{r['status']}',
                table_create_completed_at = current_timestamp(),
                table_create_duration_seconds = {table_duration},
                table_already_existed = {str(r.get('existed', False)).lower()},
                overall_status = CASE WHEN overall_status != 'FAILED' THEN '{overall}' ELSE overall_status END,
                error_message = CASE WHEN '{r['status']}' = 'FAILED' THEN '{error_msg}' ELSE error_message END,
                updated_at = current_timestamp()
            WHERE run_id = '{run_id}'
              AND source_database = '{src_db}'
              AND dest_database = '{per_table_dest_db}'
              AND source_table = '{r['source_table']}'
        """,
        task_label=f"update_table_create_status:{r['source_table']}")

    for r in table_result.get('table_results', []):
        if r.get('status') == 'FAILED' and r.get('error'):
            per_table_dest_db = r.get('dest_database', table_result['dest_database'])
            per_table_error = str(r['error'])[:2000].replace("'", "''")
            execute_with_iceberg_retry(spark, f"""
                UPDATE {tracking_db}.migration_table_status
                SET table_create_status = 'FAILED',
                    overall_status = 'FAILED',
                    error_message = '{per_table_error}',
                    updated_at = current_timestamp()
                WHERE run_id = '{run_id}'
                  AND source_database = '{src_db}'
                  AND dest_database = '{per_table_dest_db}'
                  AND source_table = '{r['source_table']}'
                  AND table_create_status IS NULL
            """,
            task_label=f"update_table_create_status:failure_patch:{r['source_table']}")

    _tbl_processed_tables = set(
        r['source_table']
        for r in table_result.get('table_results', [])
    )
    _tbl_not_in = ', '.join(f"'{t}'" for t in _tbl_processed_tables) if _tbl_processed_tables else "'__no_tables__'"
    from collections import defaultdict
    _tbl_slot_tables_by_db = defaultdict(set)
    for t in table_result.get('tables', []):
        _tbl_slot_tables_by_db[t.get('dest_database', table_result['dest_database'])].add(t['source_table'])
    if not _tbl_slot_tables_by_db:
        _tbl_slot_tables_by_db[table_result['dest_database']] = set()
    for _pddb, _slot_tbls in _tbl_slot_tables_by_db.items():
        _slot_in = ', '.join(f"'{t}'" for t in _slot_tbls) if _slot_tbls else "'__no_tables__'"
        execute_with_iceberg_retry(spark, f"""
            UPDATE {tracking_db}.migration_table_status
            SET table_create_status = 'FAILED',
                overall_status = 'FAILED',
                error_message = COALESCE(error_message, 'Table creation task did not process this table'),
                updated_at = current_timestamp()
            WHERE run_id = '{run_id}'
              AND source_database = '{src_db}'
              AND dest_database = '{_pddb}'
              AND source_table IN ({_slot_in})
              AND source_table NOT IN ({_tbl_not_in})
              AND table_create_status IS NULL
              AND discovery_status = 'COMPLETED'
        """,
        task_label=f"update_table_create_status:catchall:{_pddb}")

    return table_result


@task.pyspark(conn_id='spark_default')
@track_duration
def validate_destination_tables(source_validation: dict, spark, **context) -> dict:
    """Validate destination Hive tables: row counts, partition counts, schema comparison."""

    if not isinstance(source_validation, dict) or 'tables' not in source_validation:
        logger.warning(f"[validate_destination_tables] Skipping invalid input: {type(source_validation)}")
        return {}

    config = get_config()
    tracking_db = config['tracking_database']

    run_id = source_validation['run_id']
    src_db = source_validation['source_database']
    dest_db = source_validation['dest_database']
    tables = source_validation['tables']

    validation_results = []

    for t in tables:
        tbl = t['source_table']
        per_table_dest_db = t.get('dest_database', dest_db)
        dest_tbl = f"{per_table_dest_db}.{tbl}"

        upstream = spark.sql(f"""
            SELECT distcp_status, table_create_status, overall_status, error_message
            FROM {tracking_db}.migration_table_status
            WHERE run_id = '{run_id}'
              AND source_database = '{src_db}'
              AND dest_database = '{per_table_dest_db}'
              AND source_table = '{tbl}'
        """).collect()

        if upstream:
            row = upstream[0]
            if row['distcp_status'] == 'FAILED' or row['table_create_status'] in ('FAILED', 'SKIPPED') or row['overall_status'] == 'FAILED':
                validation_results.append({
                    'source_table': tbl,
                    'status': 'SKIPPED',
                    'error': f"Skipped validation — upstream failure: {row['error_message']}"
                })
                continue

        logger.info(f"[Validation] Starting validation for {per_table_dest_db}.{tbl}")

        try:
            source_metrics = spark.sql(f"""
                SELECT source_row_count, source_partition_count, partition_filter
                FROM {tracking_db}.migration_table_status
                WHERE run_id = '{run_id}'
                  AND source_database = '{src_db}'
                  AND source_table = '{tbl}'
                  AND dest_database = '{per_table_dest_db}'
            """).collect()

            if not source_metrics:
                validation_results.append({
                    'source_table': tbl,
                    'status': 'SKIPPED',
                    'error': 'Source metrics not found in tracking table'
                })
                continue

            source_row_count = source_metrics[0]['source_row_count'] or 0
            source_partition_count = source_metrics[0]['source_partition_count'] or t.get('partition_count', 0)

            partition_filter = source_metrics[0]['partition_filter'] if source_metrics else None
            if partition_filter:
                logger.info(
                    f"[Validation] {per_table_dest_db}.{tbl} — partition filter active: '{partition_filter}'. "
                    f"Comparing against filtered baseline: {source_row_count} rows, "
                    f"{source_partition_count} partitions."
                )

            # Get destination row count — scope to filtered partitions if a filter was active
            if partition_filter:
                filtered_parts = t.get('filtered_partitions', [])
                if filtered_parts:
                    where_clause = _partitions_to_where_clause(filtered_parts)
                    dest_row_count = spark.sql(
                        f"SELECT COUNT(*) as c FROM {dest_tbl} WHERE {where_clause}"
                    ).collect()[0]['c']
                else:
                    dest_row_count = spark.sql(
                        f"SELECT COUNT(*) as c FROM {dest_tbl}"
                    ).collect()[0]['c']
            else:
                dest_row_count = spark.sql(f"SELECT COUNT(*) as c FROM {dest_tbl}").collect()[0]['c']

            # Get destination partition count — scope to filtered partitions if a filter was active
            dest_partition_count = 0
            try:
                if partition_filter:
                    filtered_parts = t.get('filtered_partitions', [])
                    dest_partition_count = len(filtered_parts)
                else:
                    dest_partitions_df = spark.sql(f"SHOW PARTITIONS {dest_tbl}")
                    dest_partition_count = dest_partitions_df.count()
            except Exception:
                pass

            logger.info(f"[Validation] {per_table_dest_db}.{tbl} | source_rows={source_row_count} | dest_rows={dest_row_count} | source_parts={source_partition_count} | dest_parts={dest_partition_count}")

            # Schema comparison
            src_schema = t.get('schema', [])
            dest_schema_df = spark.sql(f"DESCRIBE {dest_tbl}")
            dest_schema = [
                {'name': row.col_name, 'type': row.data_type}
                for row in dest_schema_df.collect()
                if row.col_name and not row.col_name.startswith('#')
            ]

            # Compare schemas
            schema_match = True
            schema_diffs = []

            src_cols = {c['name']: c['type'] for c in src_schema}
            dest_cols = {c['name']: c['type'] for c in dest_schema}

            for col_name, col_type in src_cols.items():
                if col_name not in dest_cols:
                    schema_match = False
                    schema_diffs.append(f"Missing column: {col_name}")
                elif dest_cols[col_name] != col_type:
                    schema_match = False
                    schema_diffs.append(f"Type mismatch for {col_name}: {col_type} vs {dest_cols[col_name]}")

            for col_name in dest_cols:
                if col_name not in src_cols:
                    schema_match = False
                    schema_diffs.append(f"Extra column in dest: {col_name}")

            # Validations
            row_count_match = (source_row_count == dest_row_count)
            partition_count_match = (source_partition_count == dest_partition_count)

            match_summary = f"rows={'✓' if row_count_match else '✗'} partitions={'✓' if partition_count_match else '✗'} schema={'✓' if schema_match else '✗'}"
            logger.info(f"[Validation] DONE: {per_table_dest_db}.{tbl} | {match_summary}")
            if schema_diffs:
                logger.warning(f"[Validation] Schema diffs for {per_table_dest_db}.{tbl}: {'; '.join(schema_diffs[:5])}")

            mismatch_parts = []
            if not row_count_match:
                mismatch_parts.append(
                    f"Row count mismatch: source={source_row_count}, dest={dest_row_count}"
                )
            if not partition_count_match:
                mismatch_parts.append(
                    f"Partition count mismatch: source={source_partition_count}, dest={dest_partition_count}"
                )
            if not schema_match and schema_diffs:
                mismatch_parts.append(f"Schema differences: {'; '.join(schema_diffs[:3])}")

            mismatch_error = '; '.join(mismatch_parts) if mismatch_parts else None

            validation_results.append({
                'source_table': tbl,
                'status': 'COMPLETED',
                'source_row_count': source_row_count,
                'dest_hive_row_count': dest_row_count,
                'source_partition_count': source_partition_count,
                'dest_partition_count': dest_partition_count,
                'row_count_match': row_count_match,
                'partition_count_match': partition_count_match,
                'schema_match': schema_match,
                'schema_differences': '; '.join(schema_diffs) if schema_diffs else '',
                'error': mismatch_error
            })

        except Exception as e:
            error_msg = f"Validation failed for {dest_db}.{tbl}: {str(e)[:2000]}"
            validation_results.append({
                'source_table': tbl,
                'status': 'FAILED',
                'error': str(e)[:2000]
            })
            logger.error(f"ERROR: {error_msg}")

    failed_validations = [v for v in validation_results if v['status'] == 'FAILED']
    warned_count_checks = [
        v for v in validation_results
        if v.get('status') == 'COMPLETED' and (
            not v.get('row_count_match', True) or
            not v.get('partition_count_match', True)
        )
    ]

    if warned_count_checks:
        for v in warned_count_checks:
            warn_parts = []
            if not v.get('row_count_match', True):
                warn_parts.append(f"row count mismatch (source={v.get('source_row_count')}, dest={v.get('dest_hive_row_count')})")
            if not v.get('partition_count_match', True):
                warn_parts.append(f"partition count mismatch (source={v.get('source_partition_count')}, dest={v.get('dest_partition_count')}) — Stale partitions on source, Run MSCK")
            logger.warning(f"[Validation] WARNING for {v['source_table']}: {'; '.join(warn_parts)}")

    has_failures = len(failed_validations) > 0

    result_dict = {
        **source_validation,
        'validation_results': validation_results,
        '_has_failures': has_failures,
        '_failure_summary': (
            f"Validation failed for {len(failed_validations)}/{len(validation_results)} table(s)"
            if has_failures else None
        )
    }

    context['ti'].xcom_push(key='return_value', value=result_dict)

    if has_failures:
        raise Exception(f"Destination validation failed — {result_dict['_failure_summary']}. Per-table errors in tracking.")

    return result_dict


@task.pyspark(conn_id='spark_default')
def update_validation_status(validation_result: dict, spark) -> dict:
    """Update Iceberg tracking with validation results."""

    if not isinstance(validation_result, dict) or 'run_id' not in validation_result:
        logger.warning(f"[update_validation_status] Skipping invalid input: {type(validation_result)}")
        return {}

    config = get_config()
    tracking_db = config['tracking_database']

    run_id = validation_result['run_id']
    dest_db = validation_result['dest_database']
    src_db = validation_result.get('source_database', '')

    validation_duration = validation_result.get('_task_duration', 0.0)

    for v in validation_result.get('validation_results', []):
        if v['status'] != 'COMPLETED':
            continue

        error_msg = (v.get('error', '') or '').replace("'", "''")[:2000]
        schema_diffs = (v.get('schema_differences', '') or '').replace("'", "''")[:2000]

        is_validated = (
            v.get('row_count_match', False) and
            v.get('partition_count_match', False) and
            v.get('schema_match', False)
        )
        has_mismatch_only = (
            not is_validated and
            v['status'] == 'COMPLETED' and
            (not v.get('row_count_match', True) or not v.get('partition_count_match', True))
        )
        final_overall_status = 'VALIDATED' if is_validated else ('VALIDATED_WITH_WARNINGS' if has_mismatch_only else 'VALIDATION_FAILED')

        if v['status'] == 'FAILED':
            error_message_sql = f"'{error_msg}'"
        elif not is_validated and v.get('error'):
            mismatch_msg = str(v['error']).replace("'", "''")[:2000]
            error_message_sql = f"'{mismatch_msg}'"
        elif is_validated:
            error_message_sql = "NULL"
        else:
            error_message_sql = "error_message"

        execute_with_iceberg_retry(spark, f"""
            UPDATE {tracking_db}.migration_table_status
            SET validation_status = '{v['status']}',
                validation_completed_at = current_timestamp(),
                validation_duration_seconds = {validation_duration},
                source_row_count = {v.get('source_row_count', 0)},
                dest_hive_row_count = {v.get('dest_hive_row_count', 0)},
                source_partition_count = {v.get('source_partition_count', 0)},
                dest_partition_count = {v.get('dest_partition_count', 0)},
                row_count_match = {str(v.get('row_count_match', False)).lower()},
                partition_count_match = {str(v.get('partition_count_match', False)).lower()},
                schema_match = {str(v.get('schema_match', False)).lower()},
                schema_differences = '{schema_diffs}',
                overall_status = CASE
                    WHEN overall_status = 'FAILED' THEN overall_status
                    ELSE '{final_overall_status}'
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
        """,
        task_label=f"update_validation_status:{v['source_table']}")

    for v in validation_result.get('validation_results', []):
        if v.get('status') == 'FAILED' and v.get('error'):
            per_table_error = str(v['error'])[:2000].replace("'", "''")
            execute_with_iceberg_retry(spark, f"""
                UPDATE {tracking_db}.migration_table_status
                SET validation_status = 'FAILED',
                    overall_status = 'VALIDATION_FAILED',
                    error_message = '{per_table_error}',
                    updated_at = current_timestamp()
                WHERE run_id = '{run_id}'
                  AND source_database = '{src_db}'
                  AND dest_database = '{dest_db}'
                  AND source_table = '{v['source_table']}'
                  AND validation_status IS NULL
            """,
            task_label=f"update_validation_status:failure_patch:{v['source_table']}")

    _val_processed_tables = set(
        v['source_table']
        for v in validation_result.get('validation_results', [])
    )
    _val_not_in = ', '.join(f"'{t}'" for t in _val_processed_tables) if _val_processed_tables else "'__no_tables__'"

    from collections import defaultdict
    _slot_tables_by_db = defaultdict(set)
    for t in validation_result.get('tables', []):
        _slot_tables_by_db[t.get('dest_database', dest_db)].add(t['source_table'])
    if not _slot_tables_by_db:
        _slot_tables_by_db[dest_db] = set()

    for _vpddb, _slot_tbls in _slot_tables_by_db.items():
        _slot_in = ', '.join(f"'{t}'" for t in _slot_tbls) if _slot_tbls else "'__no_tables__'"
        execute_with_iceberg_retry(spark, f"""
            UPDATE {tracking_db}.migration_table_status
            SET validation_status = 'SKIPPED',
                overall_status = CASE WHEN overall_status = 'FAILED' THEN 'FAILED' ELSE 'VALIDATION_FAILED' END,
                error_message = COALESCE(error_message, 'Validation task did not process this table'),
                updated_at = current_timestamp()
            WHERE run_id = '{run_id}'
              AND source_database = '{src_db}'
              AND dest_database = '{_vpddb}'
              AND source_table IN ({_slot_in})
              AND source_table NOT IN ({_val_not_in})
              AND table_create_status = 'COMPLETED'
              AND validation_status IS NULL
        """,
        task_label=f"update_validation_status:catchall:{_vpddb}")

    return validation_result


@task.pyspark(conn_id='spark_default')
def generate_html_report(run_id: str, spark) -> str:
    """Generate comprehensive HTML migration report."""
    from datetime import datetime

    config = get_config()
    tracking_db = config['tracking_database']
    report_location = config['report_output_location']

    # Get migration run info
    run_info = spark.sql(f"""
        SELECT * FROM {tracking_db}.migration_runs
        WHERE run_id = '{run_id}'
    """).collect()[0]

    # Get table status
    table_status = spark.sql(f"""
        SELECT * FROM {tracking_db}.migration_table_status
        WHERE run_id = '{run_id}'
        ORDER BY source_database, source_table
    """).collect()

    # Calculate summary stats
    total_tables = len(table_status)
    successful_tables = sum(1 for t in table_status if t.overall_status in ['VALIDATED', 'VALIDATED_WITH_WARNINGS', 'TABLE_CREATED'])
    failed_tables = sum(1 for t in table_status if 'FAILED' in (t.overall_status or ''))
    total_data_gb = sum(t.s3_total_size_bytes_after or 0 for t in table_status) / (1024**3)
    total_files = sum(t.s3_file_count_after or 0 for t in table_status)
    total_rows = sum(t.source_row_count or 0 for t in table_status)
    incremental_runs = sum(1 for t in table_status if t.distcp_is_incremental)

    cluster_type = config.get('cluster_type', 'MapR')

    # Generate HTML
    html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{cluster_type} to S3 Migration Report - {run_id}</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background-color: white;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #2c3e50;
            border-bottom: 3px solid #3498db;
            padding-bottom: 10px;
        }}
        h2 {{
            color: #34495e;
            margin-top: 30px;
            border-bottom: 2px solid #ecf0f1;
            padding-bottom: 8px;
        }}
        .summary-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }}
        .summary-card {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }}
        .summary-card.success {{
            background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
        }}
        .summary-card.warning {{
            background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
        }}
        .summary-card.info {{
            background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
        }}
        .summary-card h3 {{
            margin: 0 0 10px 0;
            font-size: 14px;
            opacity: 0.9;
        }}
        .summary-card .value {{
            font-size: 32px;
            font-weight: bold;
            margin: 0;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
            font-size: 14px;
        }}
        th {{
            background-color: #34495e;
            color: white;
            padding: 12px;
            text-align: left;
            position: sticky;
            top: 0;
        }}
        td {{
            padding: 10px 12px;
            border-bottom: 1px solid #ecf0f1;
        }}
        tr:hover {{
            background-color: #f8f9fa;
        }}
        .status-badge {{
            padding: 4px 12px;
            border-radius: 12px;
            font-size: 12px;
            font-weight: bold;
            display: inline-block;
        }}
        .status-completed {{
            background-color: #d4edda;
            color: #155724;
        }}
        .status-failed {{
            background-color: #f8d7da;
            color: #721c24;
        }}
        .status-skipped {{
            background-color: #fff3cd;
            color: #856404;
        }}
        .status-warning {{
            background-color: #fff3cd;
            color: #856404;
        }}
        .metric {{
            font-weight: bold;
            color: #2980b9;
        }}
        .duration {{
            color: #7f8c8d;
            font-size: 12px;
        }}
        .validation-pass {{
            color: #27ae60;
            font-weight: bold;
        }}
        .validation-fail {{
            color: #e74c3c;
            font-weight: bold;
        }}
        .validation-warn {{
            color: #856404;
            background-color: #fff3cd;
            font-weight: bold;
            padding: 2px 6px;
            border-radius: 4px;
        }}
        .timestamp {{
            color: #95a5a6;
            font-size: 12px;
        }}
        .section-divider {{
            margin: 40px 0;
            border-top: 2px dashed #ecf0f1;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>{cluster_type} to S3 Migration Report</h1>

        <div class="timestamp">
            Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC<br>
            Run ID: <strong>{run_id}</strong><br>
            DAG Run: <strong>{run_info.dag_run_id}</strong>
"""

    html += f"""
        </div>

        <h2>Migration Summary</h2>
        <div class="summary-grid">
            <div class="summary-card">
                <h3>TOTAL TABLES</h3>
                <p class="value">{total_tables}</p>
            </div>
            <div class="summary-card success">
                <h3>SUCCESSFUL</h3>
                <p class="value">{successful_tables}</p>
            </div>
            <div class="summary-card warning">
                <h3>FAILED</h3>
                <p class="value">{failed_tables}</p>
            </div>
            <div class="summary-card info">
                <h3>TOTAL DATA</h3>
                <p class="value">{total_data_gb:.6f} GB</p>
            </div>
            <div class="summary-card info">
                <h3>TOTAL FILES</h3>
                <p class="value">{total_files:,}</p>
            </div>
            <div class="summary-card info">
                <h3>TOTAL ROWS</h3>
                <p class="value">{total_rows:,}</p>
            </div>
            <div class="summary-card">
                <h3>INCREMENTAL RUNS</h3>
                <p class="value">{incremental_runs}</p>
            </div>
        </div>

        <div class="section-divider"></div>

        <h2>Validation Summary</h2>
"""
    validation_summary_data = spark.sql(f"""
        SELECT
            COUNT(*) as total_tables_validated,
            SUM(CASE WHEN row_count_match = true AND partition_count_match = true AND schema_match = true THEN 1 ELSE 0 END) as tables_passed_validation,
            SUM(CASE WHEN row_count_match = false OR partition_count_match = false OR schema_match = false THEN 1 ELSE 0 END) as tables_failed_validation,
            SUM(CASE WHEN row_count_match = false THEN 1 ELSE 0 END) as total_row_count_mismatches,
            SUM(CASE WHEN partition_count_match = false THEN 1 ELSE 0 END) as total_partition_count_mismatches,
            SUM(CASE WHEN schema_match = false THEN 1 ELSE 0 END) as total_schema_mismatches
        FROM {tracking_db}.migration_table_status
        WHERE run_id = '{run_id}'
          AND validation_status = 'COMPLETED'
    """).collect()

    file_metrics_data = spark.sql(f"""
        SELECT
            SUM(CASE WHEN file_size_match = true THEN 1 ELSE 0 END)  as tables_size_match,
            SUM(CASE WHEN file_size_match = false THEN 1 ELSE 0 END) as tables_size_mismatch,
            SUM(CASE WHEN file_count_match = true THEN 1 ELSE 0 END)  as tables_file_count_match,
            SUM(CASE WHEN file_count_match = false THEN 1 ELSE 0 END) as tables_file_count_mismatch,
            SUM(source_total_size_bytes) as total_source_bytes,
            SUM(s3_total_size_bytes_after) as total_dest_bytes
        FROM {tracking_db}.migration_table_status
        WHERE run_id = '{run_id}'
          AND distcp_status = 'COMPLETED'
    """).collect()

    fm = file_metrics_data[0] if file_metrics_data else None

    if validation_summary_data and validation_summary_data[0]['total_tables_validated']:
        vs = validation_summary_data[0]

        size_match_count      = (fm.tables_size_match      or 0) if fm else 0
        size_mismatch_count   = (fm.tables_size_mismatch   or 0) if fm else 0
        fcount_match_count    = (fm.tables_file_count_match   or 0) if fm else 0
        fcount_mismatch_count = (fm.tables_file_count_mismatch or 0) if fm else 0
        total_src_gb  = (fm.total_source_bytes or 0) / (1024**3) if fm else 0.0
        total_dest_gb = (fm.total_dest_bytes   or 0) / (1024**3) if fm else 0.0
        size_diff_pct = (
            abs(total_src_gb - total_dest_gb) / total_src_gb * 100
            if total_src_gb > 0 else 0.0
        )

        html += f"""
        <div class="summary-grid">
            <div class="summary-card info">
                <h3>TABLES VALIDATED</h3>
                <p class="value">{vs.total_tables_validated}</p>
            </div>
            <div class="summary-card success">
                <h3>PASSED VALIDATION</h3>
                <p class="value">{vs.tables_passed_validation}</p>
            </div>
            <div class="summary-card warning">
                <h3>FAILED VALIDATION</h3>
                <p class="value">{vs.tables_failed_validation}</p>
            </div>
            <div class="summary-card warning">
                <h3>ROW COUNT MISMATCHES</h3>
                <p class="value">{vs.total_row_count_mismatches}</p>
            </div>
            <div class="summary-card warning">
                <h3>PARTITION MISMATCHES</h3>
                <p class="value">{vs.total_partition_count_mismatches}</p>
            </div>
            <div class="summary-card warning">
                <h3>SCHEMA MISMATCHES</h3>
                <p class="value">{vs.total_schema_mismatches}</p>
            </div>
            <div class="summary-card {'success' if size_mismatch_count == 0 else 'warning'}">
                <h3>SIZE MATCH</h3>
                <p class="value">{size_match_count} / {size_match_count + size_mismatch_count}</p>
            </div>
            <div class="summary-card {'success' if fcount_mismatch_count == 0 else 'warning'}">
                <h3>FILE COUNT MATCH</h3>
                <p class="value">{fcount_match_count} / {fcount_match_count + fcount_mismatch_count}</p>
            </div>
            <div class="summary-card info">
                <h3>SOURCE SIZE</h3>
                <p class="value">{total_src_gb:.6f} GB</p>
            </div>
            <div class="summary-card info">
                <h3>DEST SIZE</h3>
                <p class="value">{total_dest_gb:.6f} GB</p>
            </div>
            <div class="summary-card {'success' if size_diff_pct < 1.0 else 'warning'}">
                <h3>SIZE DELTA</h3>
                <p class="value">{size_diff_pct:.2f}%</p>
            </div>
        </div>
"""
    else:
        html += """
        <p style="color: #95a5a6; font-style: italic;">No validation summary available for this run.</p>
"""

    html += """
        <div class="section-divider"></div>

        <h2>Table Migration Details</h2>
        <table>
            <thead>
                <tr>
                    <th>Database</th>
                    <th>Table</th>
                    <th>Status</th>
                    <th>Partition Filter</th>
                    <th>Discovery</th>
                    <th>DistCp</th>
                    <th>Table Create</th>
                    <th>Validation</th>
                    <th>Format</th>
                    <th>Total Duration</th>
                </tr>
            </thead>
            <tbody>
"""

    for t in table_status:
        status = t.overall_status or ''
        if 'VALIDATED_WITH_WARNINGS' in status:
            status_class = 'status-warning'
        elif 'VALIDATED' in status or 'TABLE_CREATED' in status:
            status_class = 'status-completed'
        else:
            status_class = 'status-failed'

        discovery_dur = f"{t.discovery_duration_seconds:.1f}s" if t.discovery_duration_seconds else "N/A"
        distcp_dur = f"{t.distcp_duration_seconds:.1f}s" if t.distcp_duration_seconds else "N/A"
        distcp_detail = f"<br><small>{t.distcp_bytes_copied/(1024**2):.1f} MB, {t.distcp_files_copied:,} files</small>" if t.distcp_bytes_copied else ""
        if t.distcp_is_incremental:
            distcp_dur += " <span style='background-color: #fff3cd; padding: 2px 6px; border-radius: 4px; font-size: 10px;'>INCREMENTAL</span>"
        yarn_app_id_val = getattr(t, 'yarn_application_id', None) or ''
        _yarn_ids = [x for x in yarn_app_id_val.split(',') if x]
        if len(_yarn_ids) > 1:
            _yarn_list = ''.join(f"<div style='font-family:monospace;color:#666;font-size:11px;'>{i}</div>" for i in _yarn_ids)
            yarn_app_detail = f"<br><details style='font-size:11px;color:#666;'><summary style='cursor:pointer;color:#2980b9;'>{len(_yarn_ids)} YARN app IDs</summary>{_yarn_list}</details>"
        elif _yarn_ids:
            yarn_app_detail = f"<br><small style='font-family:monospace;color:#666;'>{_yarn_ids[0]}</small>"
        else:
            yarn_app_detail = ""
        table_dur = f"{t.table_create_duration_seconds:.1f}s" if t.table_create_duration_seconds else "N/A"
        val_dur = f"{t.validation_duration_seconds:.1f}s" if t.validation_duration_seconds else "N/A"

        total_dur = (t.discovery_duration_seconds or 0) + (t.distcp_duration_seconds or 0) + \
                    (t.table_create_duration_seconds or 0) + (t.validation_duration_seconds or 0)

        pf_display = t.partition_filter or ''
        if pf_display:
            fpc = t.filtered_partition_count
            pf_display = f"<span style='background-color:#e8f4fd;color:#1a6fa3;padding:2px 6px;border-radius:4px;font-size:11px;font-weight:bold;'>{pf_display}</span>"
            if fpc is not None:
                pf_display += f"<br><small style='color:#7f8c8d;'>({fpc} partition{'s' if fpc != 1 else ''})</small>"
        else:
            pf_display = "<span style='color:#bdc3c7;font-size:11px;'>—</span>"

        html += f"""
                <tr>
                    <td>{t.source_database}</td>
                    <td><strong>{t.source_table}</strong></td>
                    <td><span class="status-badge {status_class}">{t.overall_status}</span></td>
                    <td>{pf_display}</td>
                    <td class="duration">{discovery_dur}</td>
                    <td class="duration">{distcp_dur}{distcp_detail}{yarn_app_detail}</td>
                    <td class="duration">{table_dur}</td>
                    <td class="duration">{val_dur}</td>
                    <td>{t.file_format or 'N/A'}</td>
                    <td class="metric">{total_dur:.1f}s</td>
                </tr>
"""

    html += """
            </tbody>
        </table>

        <div class="section-divider"></div>

        <h2>Metadata Validation Results</h2>
        <table>
            <thead>
                <tr>
                    <th>Database</th>
                    <th>Table</th>
                    <th>Source Rows</th>
                    <th>Dest Hive Rows</th>
                    <th>Row Count Match</th>
                    <th>Source Partitions</th>
                    <th>Dest Partitions</th>
                    <th>Partition Match</th>
                    <th>Schema Match</th>
                </tr>
            </thead>
            <tbody>
"""

    for t in table_status:
        if not t.validation_status:
            continue

        row_match_class = 'validation-pass' if t.row_count_match else 'validation-fail'
        row_match_icon = '✓ PASS' if t.row_count_match else '✗ FAIL'

        part_match_class = 'validation-pass' if t.partition_count_match else 'validation-warn'
        part_match_icon = '✓ PASS' if t.partition_count_match else '⚠ WARN: Stale partitions on source, Run MSCK'

        schema_match_class = 'validation-pass' if t.schema_match else 'validation-fail'
        schema_match_icon = '✓ PASS' if t.schema_match else '✗ FAIL'

        html += f"""
                <tr>
                    <td>{t.source_database}</td>
                    <td><strong>{t.source_table}</strong></td>
                    <td class="metric">{(t.source_row_count or 0):,}</td>
                    <td class="metric">{(t.dest_hive_row_count or 0):,}</td>
                    <td class="{row_match_class}">{row_match_icon}</td>
                    <td class="metric">{t.source_partition_count or 0}</td>
                    <td class="metric">{t.dest_partition_count or 0}</td>
                    <td class="{part_match_class}">{part_match_icon}</td>
                    <td class="{schema_match_class}">{schema_match_icon}</td>
                </tr>
"""
    html += f"""
            </tbody>
        </table>

        <div class="section-divider"></div>

        <h2>Data Validation Results</h2>
        <table>
            <thead>
                <tr>
                    <th>Database</th>
                    <th>Table</th>
                    <th>{cluster_type} Size (GB)</th>
                    <th>S3 Size Before (GB)</th>
                    <th>S3 Size After (GB)</th>
                    <th>S3 Size - Transferred (GB)</th>
                    <th>Size Match</th>
                    <th>{cluster_type} Files</th>
                    <th>S3 Files Before</th>
                    <th>S3 Files After</th>
                    <th>S3 Files - Transferred</th>
                    <th>File Count Match</th>
                </tr>
            </thead>
            <tbody>
"""

    for t in table_status:
        if not t.distcp_status:
            continue

        source_total_size_gb = (t.source_total_size_bytes or 0) / (1024**3)
        s3_size_before_gb = (t.s3_total_size_bytes_before or 0) / (1024**3)
        s3_size_after_gb = (t.s3_total_size_bytes_after or 0) / (1024**3)
        s3_transferred_gb = (t.s3_bytes_transferred or 0) / (1024**3)

        size_match_class = 'validation-pass' if t.file_size_match else 'validation-fail'
        size_match_icon = '✓ PASS' if t.file_size_match else '✗ FAIL'

        count_match_class = 'validation-pass' if t.file_count_match else 'validation-fail'
        count_match_icon = '✓ PASS' if t.file_count_match else '✗ FAIL'

        html += f"""
                <tr>
                    <td>{t.source_database}</td>
                    <td><strong>{t.source_table}</strong></td>
                    <td class="metric">{source_total_size_gb:.6f}</td>
                    <td class="metric">{s3_size_before_gb:.6f}</td>
                    <td class="metric">{s3_size_after_gb:.6f}</td>
                    <td class="metric">{s3_transferred_gb:.6f}</td>
                    <td class="{size_match_class}">{size_match_icon}</td>
                    <td class="metric">{(t.source_file_count or 0):,}</td>
                    <td class="metric">{(t.s3_file_count_before or 0):,}</td>
                    <td class="metric">{(t.s3_file_count_after or 0):,}</td>
                    <td class="metric">{(t.s3_files_transferred or 0):,}</td>
                    <td class="{count_match_class}">{count_match_icon}</td>
                </tr>
"""

    html += """
            </tbody>
        </table>

        <div class="section-divider"></div>

        <h2>Performance Metrics</h2>
        <table>
            <thead>
                <tr>
                    <th>Database</th>
                    <th>Table</th>
                    <th>Data Volume</th>
                    <th>DistCp Speed</th>
                    <th>Rows/Second</th>
                    <th>End-to-End Duration</th>
                </tr>
            </thead>
            <tbody>
"""

    for t in table_status:
        data_gb = (t.s3_total_size_bytes_after or 0) / (1024**3)
        distcp_speed = (t.s3_total_size_bytes_after or 0) / (1024**2) / (t.distcp_duration_seconds or 1)

        total_dur = (t.discovery_duration_seconds or 0) + (t.distcp_duration_seconds or 0) + \
                    (t.table_create_duration_seconds or 0) + (t.validation_duration_seconds or 0)

        rows_per_sec = (t.source_row_count or 0) / (total_dur or 1)

        html += f"""
                <tr>
                    <td>{t.source_database}</td>
                    <td><strong>{t.source_table}</strong></td>
                    <td class="metric">{data_gb:.6f} GB</td>
                    <td class="metric">{distcp_speed:.6f} MB/s</td>
                    <td class="metric">{rows_per_sec:,.0f}</td>
                    <td class="metric">{total_dur:.1f}s ({total_dur/60:.1f}m)</td>
                </tr>
"""

    html += f"""
            </tbody>
        </table>

        <div style="margin-top: 50px; padding-top: 20px; border-top: 1px solid #ecf0f1; color: #95a5a6; font-size: 12px;">
            <p>This report was automatically generated by the {cluster_type} to S3 Migration DAG.</p>
        </div>
    </div>
</body>
</html>
"""

    # Write HTML to S3
    report_filename = f"{run_id}_report.html"
    report_path = f"{report_location}/{report_filename}"

    # Use Spark to write HTML
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jvm.java.net.URI(report_path),
        hadoop_conf
    )

    output_path = spark._jvm.org.apache.hadoop.fs.Path(report_path)
    output_stream = fs.create(output_path, True)
    output_stream.write(html.encode('utf-8'))
    output_stream.close()

    return {'report_path': report_path}


@task.pyspark(conn_id='spark_default')
def finalize_run(run_id: str, spark) -> dict:
    """Finalize migration run - update stats in Iceberg tracking."""
    config = get_config()
    tracking_db = config['tracking_database']

    stats = {'total': 0, 'successful': 0, 'failed': 0}
    final_status = 'FAILED'

    try:
        stats_result = spark.sql(f"""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN overall_status IN ('VALIDATED', 'VALIDATED_WITH_WARNINGS', 'TABLE_CREATED') THEN 1 ELSE 0 END) as successful,
                SUM(CASE WHEN overall_status IN ('FAILED', 'VALIDATION_FAILED') THEN 1 ELSE 0 END) as failed
            FROM {tracking_db}.migration_table_status
            WHERE run_id = '{run_id}'
        """).collect()

        if not stats_result or stats_result[0]['total'] == 0:
            logger.warning(f"[finalize_run] No table records found for run_id '{run_id}'. "
                           f"Upstream tasks (discover/distcp) likely failed before writing any records.")
            final_status = 'FAILED'
        else:
            stats = {
                'total': stats_result[0]['total'] or 0,
                'successful': stats_result[0]['successful'] or 0,
                'failed': stats_result[0]['failed'] or 0,
            }
            final_status = 'COMPLETED' if stats['failed'] == 0 else 'COMPLETED_WITH_FAILURES'

    except Exception as e:
        logger.error(f"[finalize_run] Failed to query migration_table_status: {str(e)}")
        final_status = 'FAILED'

    try:
        execute_with_iceberg_retry(spark, f"""
            UPDATE {tracking_db}.migration_runs
            SET status = '{final_status}',
                completed_at = current_timestamp(),
                total_tables = {stats['total']},
                successful_tables = {stats['successful']},
                failed_tables = {stats['failed']}
            WHERE run_id = '{run_id}'
        """, task_label="finalize_run:update_migration_runs")

        logger.info(f"[finalize_run] Run '{run_id}' finalized with status '{final_status}'. "
                    f"total={stats['total']}, successful={stats['successful']}, failed={stats['failed']}")

    except Exception as e:
        logger.error(f"[finalize_run] Failed to update migration_runs for run_id '{run_id}': {str(e)}")
        raise

    return {
        'run_id': run_id,
        'status': final_status,
        'total': stats['total'],
        'successful': stats['successful'],
        'failed': stats['failed']
    }


# Disabled for now as logs must remain in temp_dir for validation checks.
# Can be revisited as a future enhancement.
'''
@task
def cleanup_edge(cluster_setup: dict, run_id: str) -> dict:
    """Clean up temp files on edge node."""
    config = get_config()
    ssh = SSHHook(ssh_conn_id=config['ssh_conn_id'])
    temp_dir = cluster_setup.get('temp_dir', '')

    if temp_dir:
        try:
            with ssh.get_conn() as client:
                _, stdout, _ = client.exec_command(f"rm -rf {temp_dir}", timeout=60)
                stdout.channel.recv_exit_status()
        except Exception:
            pass

    return {'cleaned': temp_dir}
'''


@task.pyspark(conn_id='spark_default')
def send_migration_report_email(report_result: dict, run_id: str, spark) -> dict:
    """Send HTML migration report via email using SMTP."""

    config = get_config()
    smtp_conn_id = config.get('smtp_conn_id', 'smtp_default')
    recipients_str = config.get('email_recipients', '')

    if not recipients_str:
        logger.warning("[Email] No recipients configured in 'migration_email_recipients' variable. Skipping email.")
        return {'sent': False, 'reason': 'no_recipients'}

    recipients = [r.strip() for r in recipients_str.split(',') if r.strip()]
    report_path = report_result.get('report_path', '')

    try:
        import os
        import tempfile

        from airflow.utils.email import send_email

        logger.info(f"[Email] Reading HTML report from S3: {report_path}")
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(report_path),
            hadoop_conf
        )
        s3_path_obj = spark._jvm.org.apache.hadoop.fs.Path(report_path)
        reader = spark._jvm.java.io.BufferedReader(
            spark._jvm.java.io.InputStreamReader(fs.open(s3_path_obj), "UTF-8")
        )
        lines = []
        line = reader.readLine()
        while line is not None:
            lines.append(line)
            line = reader.readLine()
        reader.close()
        html_content = "\n".join(lines)

        tmp = tempfile.NamedTemporaryFile(
            mode='w', suffix='.html',
            prefix=f'{run_id}_report_',
            delete=False
        )
        tmp.write(html_content)
        tmp.close()

        send_email(
            to=recipients,
            subject=f"Migration Report - {run_id}",
            html_content=f"<p>Please find the migration report for run <strong>{run_id}</strong> attached.</p>",
            files=[tmp.name],
            conn_id=smtp_conn_id,
        )
        os.unlink(tmp.name)
        logger.info(f"[Email] Report sent successfully to: {recipients}")
        return {'sent': True, 'recipients': recipients, 'report_path': report_path}
    except Exception as e:
        logger.error(f"[Email] Failed to send report: {str(e)}")
        raise Exception(f"Failed to send migration report email: {str(e)}") from e


# =============================================================================
# DAG 1 DEFINITION: MAPR TO S3 MIGRATION
# =============================================================================

with DAG(
    dag_id='source_to_s3_migration',
    default_args=default_args,
    description='Migrate Hive tables from source cluster (MapR/HDP) to S3',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=5,
    tags=['migration', 'source-cluster', 's3', 'hive'],
    params={
        'excel_file_path': Param(
            default='s3a://config-bucket/migration.xlsx',
            type='string',
            description='S3 path to Excel config file'
        )
    },
    render_template_as_native_obj=True,
) as dag_mapr_to_s3:

    # Initialize
    t_validate = validate_prerequisites(run_id="{{ run_id }}")
    t_init = init_tracking_tables()
    t_run_id = create_migration_run(
        excel_file_path="{{ params.excel_file_path }}",
        dag_run_id="{{ run_id }}"
    )
    t_excel = parse_excel(
        excel_file_path="{{ params.excel_file_path }}",
        run_id=t_run_id
    )
    t_cluster = cluster_login_setup(run_id=t_run_id)

    # Per-database processing (dynamic task mapping)
    t_discover = discover_tables_via_spark_ssh.expand(db_config=t_excel)
    t_record = record_discovered_tables.expand(discovery=t_discover)
    t_record.operator.trigger_rule = 'all_done'
    t_distcp = run_distcp_ssh.partial(cluster_setup=t_cluster).expand(discovery=t_record)
    t_distcp.operator.trigger_rule = 'all_done'
    t_distcp_status = update_distcp_status.expand(distcp_result=t_distcp)
    t_distcp_status.operator.trigger_rule = 'all_done'
    t_tables = create_hive_tables.expand(distcp_result=t_distcp_status)
    t_tables.operator.trigger_rule = 'all_done'
    t_tbl_status = update_table_create_status.expand(table_result=t_tables)
    t_tbl_status.operator.trigger_rule = 'all_done'

    # Validation tasks
    t_dest_validation = validate_destination_tables.expand(source_validation=t_tbl_status)
    t_dest_validation.operator.max_active_tis_per_dagrun = 3
    t_dest_validation.operator.trigger_rule = 'all_done'
    t_val_status = update_validation_status.expand(validation_result=t_dest_validation)
    t_val_status.operator.trigger_rule = 'all_done'

    # Report generation
    t_report = generate_html_report(run_id=t_run_id)
    t_report.operator.trigger_rule = 'all_done'

    # Email report
    t_email = send_migration_report_email(run_id=t_run_id, report_result=t_report)
    t_email.operator.trigger_rule = 'all_done'

    # Finalize
    t_final = finalize_run(run_id=t_run_id)
    t_final.operator.trigger_rule = 'all_done'
    # t_cleanup = cleanup_edge(cluster_setup=t_cluster, run_id=t_run_id)
    # t_cleanup.operator.trigger_rule = 'all_done'

    # Dependencies
    t_validate >> t_init >> t_run_id >> t_excel >> t_cluster >> t_discover >> t_record
    t_record >> t_distcp >> t_distcp_status >> t_tables >> t_tbl_status
    t_tbl_status >> t_dest_validation >> t_val_status
    t_val_status >> t_report >> t_email >> t_final #>> t_cleanup
