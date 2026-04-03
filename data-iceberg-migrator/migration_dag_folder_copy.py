"""
DAG 3: Folder-Only Data Copy

Copies folders from MapR/HDFS to S3 via DistCp — no Hive metadata.
For migrating raw data directories that don't have associated Hive tables.

Excel columns: source_path | target_bucket | dest_folder | endpoint
"""

import logging
import os
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.providers.ssh.hooks.ssh import SSHHook
from dotenv import load_dotenv

from utils.shared import (
    DEFAULT_ARGS,
    SSH_COMMAND_TIMEOUT,
    build_s3_opts,
    cluster_login,
    execute_with_iceberg_retry,
    get_config,
    normalize_s3,
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

@task
def cluster_login_setup(run_id: str) -> dict:
    """SSH to edge, perform cluster login (MapR or Kerberos), create temp dir."""
    return cluster_login(run_id)

# =============================================================================
# DAG 3: FOLDER-ONLY DATA COPY TASKS
# =============================================================================

@task
def validate_prerequisites_folder_copy() -> dict:
    """Validate SSH connectivity and Hadoop DistCp availability before starting the folder copy."""
    config = get_config()
    ssh = SSHHook(ssh_conn_id=config['ssh_conn_id'])

    checks = {
        'ssh_connectivity': False,
        'hadoop_distcp_available': False,
        'hadoop_fs_available': False,
    }
    errors = []

    logger.info("=" * 60)
    logger.info("[FolderCopy] STARTING PRE-DAG VALIDATION")
    logger.info("=" * 60)

    try:
        with ssh.get_conn() as client:

            # 1. SSH connectivity
            logger.info("[1/3] Testing SSH connectivity...")
            _, stdout, stderr = client.exec_command('echo "SSH_TEST_OK"', timeout=30)
            exit_code = stdout.channel.recv_exit_status()
            output = stdout.read().decode()
            stderr.read()
            if exit_code == 0 and "SSH_TEST_OK" in output:
                checks['ssh_connectivity'] = True
                logger.info("SSH connectivity: PASSED")
            else:
                msg = f"SSH command failed with exit code {exit_code}"
                errors.append(f"SSH: {msg}")
                logger.error(f"SSH connectivity: FAILED - {msg}")

            # 2. Hadoop DistCp
            logger.info("[2/3] Testing hadoop distcp availability...")
            test_cmd = "source ~/.profile 2>/dev/null || true\nhadoop distcp --help > /dev/null 2>&1 && echo DISTCP_OK || echo DISTCP_FAIL"
            _, stdout, stderr = client.exec_command(test_cmd, timeout=60)
            exit_code = stdout.channel.recv_exit_status()
            output = stdout.read().decode()
            stderr.read()
            if "DISTCP_OK" in output:
                checks['hadoop_distcp_available'] = True
                logger.info("Hadoop DistCp: PASSED")
            else:
                msg = "hadoop distcp not found or not executable"
                errors.append(f"DistCp: {msg}")
                logger.error(f"Hadoop DistCp: FAILED - {msg}")

            # 3. Hadoop FS
            logger.info("[3/3] Testing Hadoop FS commands...")
            test_cmd = "source ~/.profile 2>/dev/null || true\nhadoop fs -ls / > /dev/null 2>&1 && echo HADOOP_FS_OK || echo HADOOP_FS_FAIL"
            _, stdout, stderr = client.exec_command(test_cmd, timeout=60)
            exit_code = stdout.channel.recv_exit_status()
            output = stdout.read().decode()
            stderr.read()
            if "HADOOP_FS_OK" in output:
                checks['hadoop_fs_available'] = True
                logger.info("Hadoop FS: PASSED")
            else:
                msg = "hadoop fs -ls / failed"
                errors.append(f"Hadoop FS: {msg}")
                logger.error(f"Hadoop FS: FAILED - {msg}")

    except Exception as e:
        msg = f"SSH connection failed: {str(e)}"
        errors.append(f"SSH: {msg}")
        errors.append("DistCp: Skipped due to SSH failure")
        errors.append("Hadoop FS: Skipped due to SSH failure")
        logger.error(f"SSH connectivity: FAILED - {msg}")

    logger.info("=" * 60)
    logger.info("[FolderCopy] VALIDATION SUMMARY")
    logger.info("=" * 60)

    if errors:
        logger.error("SOME PRE-DAG CHECKS FAILED:")
        for e in errors:
            logger.warning(f"  - {e}")
        raise Exception(
            f"Pre-DAG validation failed — {len(errors)} check(s) failed:\n"
            + "\n".join(f"  - {e}" for e in errors)
        )

    logger.info("ALL PRE-DAG CHECKS PASSED")
    return checks


@task.pyspark(conn_id='spark_default')
def init_folder_copy_tracking_tables(spark) -> dict:
    """Create tracking tables for folder-only data copy if they don't exist."""
    config = get_config()
    tracking_db = config['tracking_database']
    tracking_loc = config['tracking_location']
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {tracking_db} LOCATION '{tracking_loc}'")
    # Run-level table: one row per DAG run
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tracking_db}.data_copy_runs (
            run_id          STRING,
            excel_file_path STRING,
            started_at      TIMESTAMP,
            completed_at    TIMESTAMP,
            status          STRING,
            total_folders   INT,
            successful_folders INT,
            failed_folders  INT,
            error_message   STRING,
            created_at      TIMESTAMP
        )
        USING iceberg
        LOCATION '{tracking_loc}/data_copy_runs'
    """)
    # Folder-level table: one row per source/dest folder pair
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tracking_db}.data_copy_status (
            run_id            STRING,
            source_path       STRING,
            dest_bucket       STRING,
            dest_path         STRING,
            status            STRING,
            started_at        TIMESTAMP,
            completed_at      TIMESTAMP,
            source_file_count BIGINT,
            source_size_bytes BIGINT,
            dest_file_count   BIGINT,
            dest_size_bytes   BIGINT,
            files_copied      BIGINT,
            bytes_copied      BIGINT,
            is_incremental    BOOLEAN,
            file_count_match  BOOLEAN,
            size_match        BOOLEAN,
            error_message     STRING,
            updated_at        TIMESTAMP
        )
        USING iceberg
        LOCATION '{tracking_loc}/data_copy_status'
    """)
    logger.info("[FolderCopy] Tracking tables initialized: data_copy_runs, data_copy_status")
    return {'status': 'initialized', 'database': tracking_db}


@task.pyspark(conn_id='spark_default')
def create_data_copy_run(excel_file_path: str, spark) -> str:
    """Create a run record in data_copy_runs at the start of a folder-only copy DAG run."""
    import uuid
    from datetime import datetime
    config = get_config()
    tracking_db = config['tracking_database']
    run_id = f"folder_run_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    spark.sql(f"""
        INSERT INTO {tracking_db}.data_copy_runs (
            run_id, excel_file_path, started_at, completed_at,
            status, total_folders, successful_folders, failed_folders,
            error_message, created_at
        ) VALUES (
            '{run_id}',
            '{excel_file_path}',
            current_timestamp(),
            NULL,
            'RUNNING',
            NULL, NULL, NULL, NULL,
            current_timestamp()
        )
    """)
    logger.info(f"[FolderCopy] Created run record: {run_id}")
    return run_id


@task.pyspark(conn_id='spark_default')
def parse_folder_copy_excel(excel_file_path: str, run_id: str, spark) -> list:
    """Read folder copy Excel config from S3.

    Expected columns:
      - source_path   (required) : Full MapR/HDFS source path
      - target_bucket (required) : S3 bucket, normalised to s3a://
      - dest_folder   (optional) : Destination folder inside the bucket;
                                   defaults to the basename of source_path
      - endpoint      (optional) : S3 endpoint URL for non-default tenants;
                                   credentials looked up via <hostname>_access_key/secret_key Variables
    Returns a list of dicts, one per valid row.
    """
    import os
    from io import BytesIO

    import pandas as ps

    binary_df = spark.read.format("binaryFile").load(excel_file_path)
    row = binary_df.select("content").first()
    excel_bytes = bytes(row.content)
    df = ps.read_excel(BytesIO(excel_bytes), engine='openpyxl')

    # Normalise column names
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

    configs = []
    skipped = 0
    for _, row in df.iterrows():
        source_path = str(row.get('source_path', '') or '').strip()
        if not source_path:
            skipped += 1
            continue

        raw_bucket = str(row.get('target_bucket', '') or '').strip()
        if not raw_bucket or raw_bucket.lower() in ('nan', 'none'):
            logger.warning(f"[FolderCopy] Skipping row — missing target_bucket for source_path={source_path!r}")
            skipped += 1
            continue

        dest_bucket = normalize_s3(raw_bucket)

        raw_dest_folder = str(row.get('dest_folder', '') or '').strip()
        # Default dest_folder to the basename of source_path when not specified
        dest_folder = raw_dest_folder if raw_dest_folder else os.path.basename(source_path.rstrip('/'))

        raw_endpoint = row.get('endpoint', '')
        dest_endpoint = '' if (raw_endpoint is None or str(raw_endpoint).strip().lower() in ('', 'nan', 'none')) else str(raw_endpoint).strip()

        config_entry = {
            'run_id': run_id,
            'source_path': source_path,
            'dest_bucket': dest_bucket,
            'dest_folder': dest_folder,
            'dest_endpoint': dest_endpoint,
        }
        logger.info(
            f"[FolderCopy] Parsed: {source_path} -> {dest_bucket}/{dest_folder}"
        )
        configs.append(config_entry)

    logger.info(
        f"[FolderCopy] parse_folder_copy_excel: {len(configs)} folders to copy, {skipped} rows skipped"
    )
    if not configs:
        raise ValueError("[FolderCopy] No valid rows found in Excel — check source_path and target_bucket columns")

    return configs


@task
def run_folder_distcp_ssh(folder_config: dict, **context) -> dict:
    """Copy a single source folder to S3 via SSH DistCp with -update for incremental runs."""
    from datetime import datetime as _dt

    config = get_config()
    ssh = SSHHook(ssh_conn_id=config['ssh_conn_id'])

    run_id = folder_config['run_id']
    source_path = folder_config['source_path']
    dest_bucket = folder_config['dest_bucket']
    dest_folder = folder_config['dest_folder']
    dest_endpoint = folder_config.get('dest_endpoint', '')
    s3_dest = f"{dest_bucket}/{dest_folder}"

    mappers = config['distcp_mappers']
    bandwidth = config['distcp_bandwidth']

    s3_opts = build_s3_opts(dest_bucket, config, dest_endpoint)

    source_profile = "source ~/.profile 2>/dev/null || true\n"

    cmd = f'''{source_profile}
set -e

calculate_s3_metrics() {{
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
hadoop fs{s3_opts} -test -d "{s3_dest}" 2>/dev/null && INCR=true
echo "INCREMENTAL=$INCR"

echo "=== S3 metrics BEFORE distcp ==="
S3_BEFORE=$(calculate_s3_metrics "{s3_dest}")
S3_FILE_COUNT_BEFORE=$(echo "$S3_BEFORE" | grep "S3_FILE_COUNT=" | cut -d'=' -f2)
S3_TOTAL_SIZE_BEFORE=$(echo "$S3_BEFORE" | grep "S3_TOTAL_SIZE=" | cut -d'=' -f2)
echo "S3_FILE_COUNT_BEFORE=$S3_FILE_COUNT_BEFORE"
echo "S3_TOTAL_SIZE_BEFORE=$S3_TOTAL_SIZE_BEFORE"

echo "=== Source metrics ==="
SRC_FILE_COUNT=$(hadoop fs -ls -R "{source_path}" 2>/dev/null | grep '^-' | wc -l)
SRC_TOTAL_SIZE=$(hadoop fs -du -s "{source_path}" 2>/dev/null | awk '{{print $1}}')
[ -z "$SRC_FILE_COUNT" ] && SRC_FILE_COUNT=0
[ -z "$SRC_TOTAL_SIZE" ] && SRC_TOTAL_SIZE=0
echo "SRC_FILE_COUNT=$SRC_FILE_COUNT"
echo "SRC_TOTAL_SIZE=$SRC_TOTAL_SIZE"

echo "=== Running distcp ==="
hadoop distcp{s3_opts} -update -m {mappers} -bandwidth {bandwidth} -strategy dynamic \\
    "{source_path}" "{s3_dest}"
DISTCP_EXIT=$?
echo "DISTCP_EXIT_CODE=$DISTCP_EXIT"

echo "=== S3 metrics AFTER distcp ==="
S3_AFTER=$(calculate_s3_metrics "{s3_dest}")
S3_FILE_COUNT_AFTER=$(echo "$S3_AFTER" | grep "S3_FILE_COUNT=" | cut -d'=' -f2)
S3_TOTAL_SIZE_AFTER=$(echo "$S3_AFTER" | grep "S3_TOTAL_SIZE=" | cut -d'=' -f2)
echo "S3_FILE_COUNT_AFTER=$S3_FILE_COUNT_AFTER"
echo "S3_TOTAL_SIZE_AFTER=$S3_TOTAL_SIZE_AFTER"

[ "$DISTCP_EXIT" -ne 0 ] && exit $DISTCP_EXIT
exit 0
'''

    started_at = _dt.utcnow()
    started_at_str = started_at.strftime('%Y-%m-%d %H:%M:%S')

    try:
        with ssh.get_conn() as client:
            _, stdout, stderr = client.exec_command(cmd, timeout=SSH_COMMAND_TIMEOUT)
            exit_code = stdout.channel.recv_exit_status()
            output = stdout.read().decode()
            error_output = stderr.read().decode()

            logger.info(f"[FolderDistCp] {source_path} -> {s3_dest} (last 1000 chars):")
            logger.info(output[-1000:])

            is_incr = "INCREMENTAL=true" in output

            src_file_count = 0
            src_size_bytes = 0
            s3_files_before = 0
            s3_size_before = 0
            s3_files_after = 0
            s3_size_after = 0

            try:
                for line in output.split('\n'):
                    line = line.strip()
                    if line.startswith('SRC_FILE_COUNT='):
                        src_file_count = int(line.split('=', 1)[1] or 0)
                    elif line.startswith('SRC_TOTAL_SIZE='):
                        src_size_bytes = int(line.split('=', 1)[1] or 0)
                    elif line.startswith('S3_FILE_COUNT_BEFORE='):
                        s3_files_before = int(line.split('=', 1)[1] or 0)
                    elif line.startswith('S3_TOTAL_SIZE_BEFORE='):
                        s3_size_before = int(line.split('=', 1)[1] or 0)
                    elif line.startswith('S3_FILE_COUNT_AFTER='):
                        s3_files_after = int(line.split('=', 1)[1] or 0)
                    elif line.startswith('S3_TOTAL_SIZE_AFTER='):
                        s3_size_after = int(line.split('=', 1)[1] or 0)
            except Exception:
                pass

            files_copied = max(s3_files_after - s3_files_before, 0)
            bytes_copied = max(s3_size_after - s3_size_before, 0)
            file_count_match = src_file_count == s3_files_after
            size_match = abs(src_size_bytes - s3_size_after) <= max(1, int(src_size_bytes * 0.01))

            if exit_code != 0:
                logger.error(f"[FolderDistCp] FAILED: {source_path} -> {s3_dest}")
                logger.error(error_output[:1000])
                raise Exception(
                    f"DistCp failed for {source_path} -> {s3_dest} "
                    f"(exit {exit_code}): {error_output[:1000]}"
                )

            completed_at = _dt.utcnow()
            logger.info(
                f"[FolderDistCp] COMPLETED: {source_path} -> {s3_dest} | "
                f"incremental={is_incr} | files_copied={files_copied} | bytes_copied={bytes_copied}"
            )
            return {
                'run_id': run_id,
                'source_path': source_path,
                'dest_bucket': dest_bucket,
                'dest_path': dest_folder,
                'dest_endpoint': dest_endpoint,
                'status': 'COMPLETED',
                'started_at': started_at_str,
                'completed_at': completed_at.strftime('%Y-%m-%d %H:%M:%S'),
                'source_file_count': src_file_count,
                'source_size_bytes': src_size_bytes,
                'dest_file_count': s3_files_after,
                'dest_size_bytes': s3_size_after,
                'files_copied': files_copied,
                'bytes_copied': bytes_copied,
                'is_incremental': is_incr,
                'file_count_match': file_count_match,
                'size_match': size_match,
                'error': None,
            }

    except Exception as e:
        completed_at = _dt.utcnow()
        error_msg = str(e)[:2000]
        logger.error(f"[FolderDistCp] ERROR: {source_path} -> {s3_dest}: {error_msg}")
        result = {
            'run_id': run_id,
            'source_path': source_path,
            'dest_bucket': dest_bucket,
            'dest_path': dest_folder,
            'dest_endpoint': dest_endpoint,
            'status': 'FAILED',
            'started_at': started_at_str,
            'completed_at': completed_at.strftime('%Y-%m-%d %H:%M:%S'),
            'source_file_count': 0,
            'source_size_bytes': 0,
            'dest_file_count': 0,
            'dest_size_bytes': 0,
            'files_copied': 0,
            'bytes_copied': 0,
            'is_incremental': False,
            'file_count_match': False,
            'size_match': False,
            'error': error_msg,
        }
        context['ti'].xcom_push(key='return_value', value=result)
        raise Exception(f"DistCp failed for {source_path} -> {s3_dest}: {error_msg}") from e


@task.pyspark(conn_id='spark_default')
def record_data_copy_status(distcp_result: dict, spark) -> dict:
    """Insert a row into data_copy_status for the completed/failed folder copy."""
    config = get_config()
    tracking_db = config['tracking_database']

    run_id        = distcp_result['run_id']
    source_path   = distcp_result['source_path'].replace("'", "''")
    dest_bucket   = distcp_result['dest_bucket'].replace("'", "''")
    dest_path     = distcp_result['dest_path'].replace("'", "''")
    status        = distcp_result['status']
    started_at    = distcp_result.get('started_at', '')
    completed_at  = distcp_result.get('completed_at', '')
    error_msg     = (distcp_result.get('error') or '').replace("'", "''")[:2000]

    src_file_count  = distcp_result.get('source_file_count', 0) or 0
    src_size_bytes  = distcp_result.get('source_size_bytes', 0) or 0
    dest_file_count = distcp_result.get('dest_file_count', 0) or 0
    dest_size_bytes = distcp_result.get('dest_size_bytes', 0) or 0
    files_copied    = distcp_result.get('files_copied', 0) or 0
    bytes_copied    = distcp_result.get('bytes_copied', 0) or 0
    is_incremental  = str(distcp_result.get('is_incremental', False)).lower()
    file_count_match = str(distcp_result.get('file_count_match', False)).lower()
    size_match       = str(distcp_result.get('size_match', False)).lower()

    execute_with_iceberg_retry(spark, f"""
        INSERT INTO {tracking_db}.data_copy_status (
            run_id, source_path, dest_bucket, dest_path,
            status, started_at, completed_at,
            source_file_count, source_size_bytes,
            dest_file_count, dest_size_bytes,
            files_copied, bytes_copied,
            is_incremental, file_count_match, size_match,
            error_message, updated_at
        ) VALUES (
            '{run_id}',
            '{source_path}',
            '{dest_bucket}',
            '{dest_path}',
            '{status}',
            CAST('{started_at}' AS TIMESTAMP),
            CAST('{completed_at}' AS TIMESTAMP),
            {src_file_count},
            {src_size_bytes},
            {dest_file_count},
            {dest_size_bytes},
            {files_copied},
            {bytes_copied},
            {is_incremental},
            {file_count_match},
            {size_match},
            '{error_msg}',
            current_timestamp()
        )
    """, task_label=f"record_data_copy_status:{source_path}")

    logger.info(
        f"[FolderCopy] Recorded status={status} for {source_path} -> {dest_bucket}/{dest_path}"
    )
    return distcp_result


@task
def validate_data_copy(copy_status: dict, **context) -> dict:
    """Re-verify the S3 destination after copy: recount files/bytes and update data_copy_status."""

    config = get_config()
    ssh = SSHHook(ssh_conn_id=config['ssh_conn_id'])

    source_path = copy_status['source_path']
    dest_bucket = copy_status['dest_bucket']
    dest_path   = copy_status['dest_path']
    dest_endpoint = copy_status.get('dest_endpoint', '')
    s3_dest     = f"{dest_bucket}/{dest_path}"

    s3_opts = build_s3_opts(dest_bucket, config, dest_endpoint)

    # If the copy itself failed, skip SSH validation and mark as VALIDATION_SKIPPED
    if copy_status.get('status') == 'FAILED':
        logger.warning(f"[FolderValidate] Skipping validation — copy FAILED for {source_path}")
        result = {**copy_status, 'validation_status': 'VALIDATION_SKIPPED'}
        context['ti'].xcom_push(key='return_value', value=result)
        raise Exception(f"Validation skipped — upstream copy FAILED for {source_path}")

    cmd = f"""source ~/.profile 2>/dev/null || true

if ! hadoop fs{s3_opts} -test -d "{s3_dest}" 2>/dev/null; then
    echo "DEST_EXISTS=false"
    echo "DEST_FILE_COUNT=0"
    echo "DEST_TOTAL_SIZE=0"
else
    echo "DEST_EXISTS=true"
    DEST_FILE_COUNT=$(hadoop fs{s3_opts} -ls -R "{s3_dest}" 2>/dev/null | grep '^-' | wc -l)
    DEST_TOTAL_SIZE=$(hadoop fs{s3_opts} -du -s "{s3_dest}" 2>/dev/null | awk '{{print $1}}')
    [ -z "$DEST_FILE_COUNT" ] && DEST_FILE_COUNT=0
    [ -z "$DEST_TOTAL_SIZE" ] && DEST_TOTAL_SIZE=0
    echo "DEST_FILE_COUNT=$DEST_FILE_COUNT"
    echo "DEST_TOTAL_SIZE=$DEST_TOTAL_SIZE"
fi
"""

    dest_exists = False
    dest_file_count = 0
    dest_size_bytes = 0
    validation_error = None

    try:
        with ssh.get_conn() as client:
            _, stdout, stderr = client.exec_command(cmd, timeout=SSH_COMMAND_TIMEOUT)
            stdout.channel.recv_exit_status()
            output = stdout.read().decode()
            stderr.read()

            for line in output.split('\n'):
                line = line.strip()
                if line == 'DEST_EXISTS=true':
                    dest_exists = True
                elif line.startswith('DEST_FILE_COUNT='):
                    dest_file_count = int(line.split('=', 1)[1] or 0)
                elif line.startswith('DEST_TOTAL_SIZE='):
                    dest_size_bytes = int(line.split('=', 1)[1] or 0)

            if not dest_exists:
                validation_error = f"S3 destination does not exist after copy: {s3_dest}"
                logger.error(f"[FolderValidate] {validation_error}")

    except Exception as e:
        validation_error = f"Validation SSH error: {str(e)[:1000]}"
        logger.error(f"[FolderValidate] {validation_error}")

    src_file_count = copy_status.get('source_file_count', 0) or 0
    src_size_bytes = copy_status.get('source_size_bytes', 0) or 0

    file_count_match = dest_file_count == src_file_count
    size_match = abs(src_size_bytes - dest_size_bytes) <= max(1, int(src_size_bytes * 0.01))

    validation_status = 'VALIDATED' if (dest_exists and file_count_match and size_match and not validation_error) else 'VALIDATION_FAILED'

    logger.info(
        f"[FolderValidate] {source_path} -> {s3_dest} | "
        f"dest_exists={dest_exists} | file_count_match={file_count_match} | "
        f"size_match={size_match} | status={validation_status}"
    )

    result = {
        **copy_status,
        'dest_file_count': dest_file_count,
        'dest_size_bytes': dest_size_bytes,
        'file_count_match': file_count_match,
        'size_match': size_match,
        'validation_status': validation_status,
        'validation_error': validation_error,
    }
    if validation_status != 'VALIDATED':
        context['ti'].xcom_push(key='return_value', value=result)
        raise Exception(
            f"Validation {validation_status} for {source_path} -> {s3_dest}: "
            f"{validation_error or 'file count or size mismatch'}"
        )
    return result


@task.pyspark(conn_id='spark_default')
def update_data_copy_validation(validation_result: dict, spark) -> dict:
    """Update data_copy_status with final validation metrics and status."""
    config = get_config()
    tracking_db = config['tracking_database']

    run_id      = validation_result['run_id']
    source_path = validation_result['source_path'].replace("'", "''")
    dest_bucket = validation_result['dest_bucket'].replace("'", "''")
    dest_path   = validation_result['dest_path'].replace("'", "''")

    validation_status = validation_result.get('validation_status', 'VALIDATION_FAILED')
    dest_file_count   = validation_result.get('dest_file_count', 0) or 0
    dest_size_bytes   = validation_result.get('dest_size_bytes', 0) or 0
    file_count_match  = str(validation_result.get('file_count_match', False)).lower()
    size_match        = str(validation_result.get('size_match', False)).lower()
    val_error         = (validation_result.get('validation_error') or '').replace("'", "''")[:2000]

    execute_with_iceberg_retry(spark, f"""
        UPDATE {tracking_db}.data_copy_status
        SET status           = '{validation_status}',
            dest_file_count  = {dest_file_count},
            dest_size_bytes  = {dest_size_bytes},
            file_count_match = {file_count_match},
            size_match       = {size_match},
            error_message    = CASE
                                 WHEN '{val_error}' != '' THEN '{val_error}'
                                 ELSE error_message
                               END,
            updated_at       = current_timestamp()
        WHERE run_id     = '{run_id}'
          AND source_path = '{source_path}'
          AND dest_bucket = '{dest_bucket}'
          AND dest_path   = '{dest_path}'
    """, task_label=f"update_data_copy_validation:{source_path}")

    logger.info(f"[FolderValidate] Updated tracking: {source_path} -> {validation_status}")
    return validation_result


@task.pyspark(conn_id='spark_default')
def finalize_data_copy_run(run_id: str, spark) -> dict:
    """Aggregate folder-level counts and mark the data_copy_runs record as COMPLETED."""
    config = get_config()
    tracking_db = config['tracking_database']

    stats = spark.sql(f"""
        SELECT
            COUNT(*)                                                              AS total_folders,
            SUM(CASE WHEN status IN ('VALIDATED')                                      THEN 1 ELSE 0 END) AS successful_folders,
            SUM(CASE WHEN status IN ('FAILED', 'VALIDATION_FAILED', 'VALIDATION_SKIPPED') THEN 1 ELSE 0 END) AS failed_folders
        FROM {tracking_db}.data_copy_status
        WHERE run_id = '{run_id}'
    """).collect()

    if stats:
        total      = int(stats[0]['total_folders']      or 0)
        successful = int(stats[0]['successful_folders'] or 0)
        failed     = int(stats[0]['failed_folders']     or 0)
    else:
        total = successful = failed = 0

    overall_status = 'COMPLETED' if failed == 0 else 'COMPLETED_WITH_ERRORS'

    execute_with_iceberg_retry(spark, f"""
        UPDATE {tracking_db}.data_copy_runs
        SET status             = '{overall_status}',
            completed_at       = current_timestamp(),
            total_folders      = {total},
            successful_folders = {successful},
            failed_folders     = {failed}
        WHERE run_id = '{run_id}'
    """, task_label="finalize_data_copy_run")

    logger.info(
        f"[FolderCopy] Run {run_id} finalized: status={overall_status} | "
        f"total={total} | successful={successful} | failed={failed}"
    )
    return {
        'run_id': run_id,
        'status': overall_status,
        'total_folders': total,
        'successful_folders': successful,
        'failed_folders': failed,
    }


@task.pyspark(conn_id='spark_default')
def generate_data_copy_html_report(finalize_result: dict, run_id: str, spark) -> dict:
    """Generate HTML report for folder-only data copy run and write to S3."""
    from datetime import datetime

    config = get_config()
    tracking_db = config['tracking_database']
    report_location = config['report_output_location']

    run_info = spark.sql(f"""
        SELECT * FROM {tracking_db}.data_copy_runs
        WHERE run_id = '{run_id}'
    """).collect()
    run_row = run_info[0] if run_info else None

    folders = spark.sql(f"""
        SELECT * FROM {tracking_db}.data_copy_status
        WHERE run_id = '{run_id}'
        ORDER BY source_path
    """).collect()

    total_folders     = len(folders)
    validated         = sum(1 for f in folders if f.status == 'VALIDATED')
    failed            = sum(1 for f in folders if (f.status or '') in ('FAILED', 'VALIDATION_FAILED', 'VALIDATION_SKIPPED'))
    incremental       = sum(1 for f in folders if f.is_incremental)
    total_bytes       = sum(f.dest_size_bytes or 0 for f in folders)
    total_files       = sum(f.dest_file_count or 0 for f in folders)
    total_bytes_copied = sum(f.bytes_copied or 0 for f in folders)
    total_gb          = total_bytes / (1024 ** 3)

    run_status = (run_row.status if run_row else 'UNKNOWN')
    excel_path = (run_row.excel_file_path if run_row else '')
    started_at = str(run_row.started_at if run_row else '')
    completed_at = str(run_row.completed_at if run_row else '')

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Folder Data Copy Report - {run_id}</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0; padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1400px; margin: 0 auto;
            background-color: white; padding: 30px;
            border-radius: 8px; box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        h1 {{ color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px; }}
        h2 {{ color: #34495e; margin-top: 30px; border-bottom: 2px solid #ecf0f1; padding-bottom: 8px; }}
        .summary-grid {{
            display: grid; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr));
            gap: 20px; margin: 20px 0;
        }}
        .summary-card {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white; padding: 20px; border-radius: 8px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }}
        .summary-card.success {{ background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%); }}
        .summary-card.warning {{ background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); }}
        .summary-card.info    {{ background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%); }}
        .summary-card h3 {{ margin: 0 0 10px 0; font-size: 14px; opacity: 0.9; }}
        .summary-card .value {{ font-size: 32px; font-weight: bold; margin: 0; }}
        table {{ width: 100%; border-collapse: collapse; margin: 20px 0; font-size: 13px; }}
        th {{ background-color: #34495e; color: white; padding: 10px 12px; text-align: left; position: sticky; top: 0; }}
        td {{ padding: 9px 12px; border-bottom: 1px solid #ecf0f1; }}
        tr:hover {{ background-color: #f8f9fa; }}
        .status-badge {{ padding: 3px 10px; border-radius: 10px; font-size: 11px; font-weight: bold; display: inline-block; }}
        .status-completed  {{ background-color: #d4edda; color: #155724; }}
        .status-failed     {{ background-color: #f8d7da; color: #721c24; }}
        .status-skipped    {{ background-color: #fff3cd; color: #856404; }}
        .metric {{ font-weight: bold; color: #2980b9; }}
        .pass {{ color: #27ae60; font-weight: bold; }}
        .fail {{ color: #e74c3c; font-weight: bold; }}
        .timestamp {{ color: #95a5a6; font-size: 12px; }}
        .section-divider {{ margin: 40px 0; border-top: 2px dashed #ecf0f1; }}
        .error-cell {{ color: #721c24; font-size: 11px; word-break: break-word; max-width: 300px; }}
    </style>
</head>
<body>
<div class="container">
    <h1>Folder Data Copy Report</h1>
    <div class="timestamp">
        Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC<br>
        Run ID: <strong>{run_id}</strong><br>
        Excel: <strong>{excel_path}</strong><br>
        Started: <strong>{started_at}</strong> &nbsp; Completed: <strong>{completed_at}</strong>
    </div>

    <h2>Summary</h2>
    <div class="summary-grid">
        <div class="summary-card">
            <h3>RUN STATUS</h3>
            <p class="value" style="font-size:20px;">{run_status}</p>
        </div>
        <div class="summary-card">
            <h3>TOTAL FOLDERS</h3>
            <p class="value">{total_folders}</p>
        </div>
        <div class="summary-card success">
            <h3>VALIDATED</h3>
            <p class="value">{validated}</p>
        </div>
        <div class="summary-card warning">
            <h3>FAILED</h3>
            <p class="value">{failed}</p>
        </div>
        <div class="summary-card info">
            <h3>INCREMENTAL</h3>
            <p class="value">{incremental}</p>
        </div>
        <div class="summary-card info">
            <h3>TOTAL DATA (S3)</h3>
            <p class="value">{total_gb:.3f} GB</p>
        </div>
        <div class="summary-card info">
            <h3>TOTAL FILES (S3)</h3>
            <p class="value">{total_files:,}</p>
        </div>
        <div class="summary-card info">
            <h3>BYTES COPIED</h3>
            <p class="value">{total_bytes_copied / (1024**3):.3f} GB</p>
        </div>
    </div>

    <div class="section-divider"></div>

    <h2>Copy Details</h2>
    <table>
        <thead>
            <tr>
                <th>Source Path</th>
                <th>Destination</th>
                <th>Status</th>
                <th>Incremental</th>
                <th>Src Files</th>
                <th>Dest Files</th>
                <th>File Match</th>
                <th>Src Size (GB)</th>
                <th>Dest Size (GB)</th>
                <th>Size Match</th>
                <th>Bytes Copied (GB)</th>
                <th>Files Copied</th>
                <th>Error</th>
            </tr>
        </thead>
        <tbody>
"""

    for f in folders:
        status = f.status or ''
        if 'VALIDATED' in status:
            badge_cls = 'status-completed'
        elif 'FAILED' in status or 'SKIPPED' in status:
            badge_cls = 'status-failed'
        else:
            badge_cls = 'status-skipped'

        src_gb  = (f.source_size_bytes or 0) / (1024 ** 3)
        dest_gb = (f.dest_size_bytes or 0) / (1024 ** 3)
        copied_gb = (f.bytes_copied or 0) / (1024 ** 3)
        fc_class = 'pass' if f.file_count_match else 'fail'
        fc_icon  = '✓' if f.file_count_match else '✗'
        sm_class = 'pass' if f.size_match else 'fail'
        sm_icon  = '✓' if f.size_match else '✗'
        incr_lbl = '✓' if f.is_incremental else ''
        error_cell = f'<span class="error-cell">{(f.error_message or "")[:200]}</span>' if f.error_message else ''

        html += f"""
            <tr>
                <td>{f.source_path}</td>
                <td>{f.dest_bucket}/{f.dest_path}</td>
                <td><span class="status-badge {badge_cls}">{status}</span></td>
                <td style="text-align:center">{incr_lbl}</td>
                <td class="metric">{(f.source_file_count or 0):,}</td>
                <td class="metric">{(f.dest_file_count or 0):,}</td>
                <td class="{fc_class}">{fc_icon}</td>
                <td class="metric">{src_gb:.4f}</td>
                <td class="metric">{dest_gb:.4f}</td>
                <td class="{sm_class}">{sm_icon}</td>
                <td class="metric">{copied_gb:.4f}</td>
                <td class="metric">{(f.files_copied or 0):,}</td>
                <td>{error_cell}</td>
            </tr>
"""

    html += """
        </tbody>
    </table>

    <div style="margin-top:50px; padding-top:20px; border-top:1px solid #ecf0f1; color:#95a5a6; font-size:12px;">
        <p>This report was automatically generated by the Folder Data Copy DAG.</p>
    </div>
</div>
</body>
</html>
"""

    report_path = f"{report_location}/{run_id}_data_copy_report.html"
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jvm.java.net.URI(report_path),
        hadoop_conf
    )
    out = spark._jvm.org.apache.hadoop.fs.Path(report_path)
    stream = fs.create(out, True)
    stream.write(html.encode('utf-8'))
    stream.close()

    logger.info(f"[FolderCopy] HTML report written to {report_path}")
    return {'report_path': report_path}


@task.pyspark(conn_id='spark_default')
def send_data_copy_report_email(report_result: dict, run_id: str, spark) -> dict:
    """Send the folder data copy HTML report via SMTP."""
    import os
    import tempfile

    from airflow.utils.email import send_email

    config = get_config()
    smtp_conn_id   = config.get('smtp_conn_id', 'smtp_default')
    recipients_raw = config.get('email_recipients', '')

    if not recipients_raw or not recipients_raw.strip():
        logger.warning("[FolderCopy] No recipients configured in 'migration_email_recipients' variable. Skipping email.")
        return {'sent': False, 'reason': 'no_recipients'}

    recipients  = [r.strip() for r in recipients_raw.split(',') if r.strip()]
    report_path = report_result.get('report_path', '')

    try:
        logger.info(f"[FolderCopy] Reading HTML report from S3: {report_path}")
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
            prefix=f'{run_id}_data_copy_report_',
            delete=False
        )
        tmp.write(html_content)
        tmp.close()

        send_email(
            to=recipients,
            subject=f"Folder Data Copy Report - {run_id}",
            html_content=f"<p>Please find the Folder Data Copy report for run <strong>{run_id}</strong> attached.</p>",
            files=[tmp.name],
            conn_id=smtp_conn_id,
        )
        os.unlink(tmp.name)
        logger.info(f"[FolderCopy] Report email sent to: {recipients}")
        return {'sent': True, 'recipients': recipients, 'report_path': report_path}
    except Exception as e:
        logger.error(f"[FolderCopy] Failed to send report email: {str(e)}")
        raise Exception(f"Failed to send Folder Data Copy report email: {str(e)}") from e


# =============================================================================
# DAG 3 DEFINITION: FOLDER-ONLY DATA COPY
# =============================================================================

with DAG(
    dag_id='folder_only_data_copy',
    default_args=DEFAULT_ARGS,
    description='Copy folders from MapR/HDFS to S3 via DistCp — no Hive metadata',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=3,
    tags=['migration', 'mapr', 's3', 'folder-copy'],
    params={
        'excel_file_path': Param(
            default='s3a://config-bucket/folder_copy.xlsx',
            type='string',
            description='S3 path to Excel config file (columns: source_path, target_bucket, dest_folder)'
        )
    },
    render_template_as_native_obj=True,
) as dag_folder_copy:

    # Pre-flight checks
    t_fc_prereq  = validate_prerequisites_folder_copy()

    # Initialize tracking tables and create a run record
    t_fc_init   = init_folder_copy_tracking_tables()
    t_fc_run_id = create_data_copy_run(
        excel_file_path="{{ params.excel_file_path }}"
    )

    # Cluster authentication — receives tracking run_id
    t_fc_cluster = cluster_login_setup(run_id=t_fc_run_id)

    # Parse Excel — one dict per folder row
    t_fc_excel = parse_folder_copy_excel(
        excel_file_path="{{ params.excel_file_path }}",
        run_id=t_fc_run_id
    )

    # Per-folder: copy → record → validate → update validation (dynamically mapped)
    # max_active_tis_per_dagrun=3 caps concurrent DistCp YARN jobs
    t_fc_distcp = run_folder_distcp_ssh.override(
        trigger_rule='all_done', max_active_tis_per_dagrun=3
    ).expand(folder_config=t_fc_excel)

    t_fc_record = record_data_copy_status.override(
        trigger_rule='all_done'
    ).expand(distcp_result=t_fc_distcp)

    t_fc_copy_validate = validate_data_copy.override(
        trigger_rule='all_done'
    ).expand(copy_status=t_fc_record)

    t_fc_val_status = update_data_copy_validation.override(
        trigger_rule='all_done'
    ).expand(validation_result=t_fc_copy_validate)

    # Finalize run — waits for all per-folder validation to finish (via dependency chain)
    t_fc_final = finalize_data_copy_run.override(trigger_rule='all_done')(
        run_id=t_fc_run_id
    )

    # Report and email
    t_fc_report = generate_data_copy_html_report.override(trigger_rule='all_done')(
        run_id=t_fc_run_id,
        finalize_result=t_fc_final
    )

    t_fc_email = send_data_copy_report_email.override(trigger_rule='all_done')(
        report_result=t_fc_report,
        run_id=t_fc_run_id
    )

    # Dependency chain — prereqs → init → run_id → excel → cluster → work
    # Per-folder chain is linear: distcp → record_status → validate → update_validation
    t_fc_prereq >> t_fc_init >> t_fc_run_id >> t_fc_excel >> t_fc_cluster
    t_fc_cluster >> t_fc_distcp >> t_fc_record >> t_fc_copy_validate >> t_fc_val_status
    t_fc_val_status >> t_fc_final >> t_fc_report >> t_fc_email
