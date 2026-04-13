"""
Shared utilities for all migration DAGs.

Contains configuration, S3 helpers, retry logic, and constants
used across mapr_to_s3, iceberg, folder_copy, and s3_metadata DAGs.
"""

import logging
import math
import os
import random
import time
from functools import wraps

from airflow.models import Variable

logger = logging.getLogger(__name__)

__all__ = [
    "SSH_COMMAND_TIMEOUT",
    "_login_shell",
    "apply_bucket_credentials",
    "build_s3_opts",
    "cell_str",
    "cluster_login",
    "compute_dest_path",
    "configure_spark_s3",
    "execute_with_iceberg_retry",
    "get_config",
    "normalize_s3",
    "track_duration",
    "validate_bucket_endpoint_pairs",
]

# =============================================================================
# Duration tracking decorator using XCom
# =============================================================================
def track_duration(func):
    """Decorator to automatically track task duration via result dict."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        from datetime import datetime as dt
        start_time = dt.utcnow()
        result = func(*args, **kwargs)
        end_time = dt.utcnow()
        duration = (end_time - start_time).total_seconds()

        if isinstance(result, dict):
            result['_task_duration'] = duration

        return result

    return wrapper

def execute_with_iceberg_retry(spark, sql: str, max_retries: int = 6, task_label: str = ""):
    """Execute Spark SQL with retry logic for Iceberg commit conflicts."""
    status = False
    counter = 0
    last_exception = None

    while not status and counter < max_retries:
        try:
            spark.sql(sql)
            status = True
        except Exception as e:
            last_exception = e
            counter += 1
            if counter < max_retries:
                sleep_secs = random.choice([10, 20, 30, 40, 50])
                logger.warning(
                    f"[IcebergRetry] {task_label} hit commit conflict "
                    f"(attempt {counter}/{max_retries}). Retrying in {sleep_secs}s... Error: {str(e)[:200]}"
                )
                time.sleep(sleep_secs)
            else:
                logger.error(f"[IcebergRetry] {task_label} failed after {max_retries} attempts.")

    if not status:
        raise last_exception

# =============================================================================
# HELPER: configure dual-S3 credentials on a Spark session
# =============================================================================

def configure_spark_s3(spark, config: dict):
    """ Configure Spark with per-bucket S3A credentials for source and destination. """
    src_endpoint   = config.get('s3_source_endpoint')   or config.get('s3_endpoint', '')
    src_access_key = config.get('s3_source_access_key') or config.get('s3_access_key', '')
    src_secret_key = config.get('s3_source_secret_key') or config.get('s3_secret_key', '')

    dest_endpoint   = config.get('s3_dest_endpoint')   or config.get('s3_endpoint', '')
    dest_access_key = config.get('s3_dest_access_key') or config.get('s3_access_key', '')
    dest_secret_key = config.get('s3_dest_secret_key') or config.get('s3_secret_key', '')

    if src_endpoint:
        spark.conf.set("fs.s3a.endpoint", src_endpoint)
    if src_access_key:
        spark.conf.set("fs.s3a.access.key", src_access_key)
    if src_secret_key:
        spark.conf.set("fs.s3a.secret.key", src_secret_key)

    config['_src_endpoint']    = src_endpoint
    config['_src_access_key']  = src_access_key
    config['_src_secret_key']  = src_secret_key
    config['_dest_endpoint']   = dest_endpoint
    config['_dest_access_key'] = dest_access_key
    config['_dest_secret_key'] = dest_secret_key


def apply_bucket_credentials(spark, bucket_url: str, endpoint: str, access_key: str, secret_key: str):
    """Apply per-bucket S3A credentials given an s3a://bucket-name/... URL."""
    if not bucket_url.startswith('s3a://') or not (access_key or endpoint):
        return
    bucket_name = bucket_url.split('/')[2]
    if endpoint:
        spark.conf.set(f"fs.s3a.bucket.{bucket_name}.endpoint", endpoint)
    if access_key:
        spark.conf.set(f"fs.s3a.bucket.{bucket_name}.access.key", access_key)
    if secret_key:
        spark.conf.set(f"fs.s3a.bucket.{bucket_name}.secret.key", secret_key)


def compute_dest_path(source_location: str, dest_database: str, table_name: str,
                      dest_bucket: str, source_s3_prefix: str, dest_s3_prefix: str) -> str:
    """ Compute the destination S3 path for a table. """
    if source_s3_prefix and dest_s3_prefix and source_location.startswith(source_s3_prefix):
        relative = source_location[len(source_s3_prefix):].lstrip('/')
        return f"{dest_s3_prefix.rstrip('/')}/{relative}"
    return f"{dest_bucket.rstrip('/')}/{dest_database}/{table_name}"


def cell_str(val, default=''):
    """Safely convert a pandas cell value to a stripped string, handling NaN/None."""
    if val is None or (isinstance(val, float) and math.isnan(val)):
        return default
    return str(val).strip() or default


def normalize_s3(path: str) -> str:
    """Normalize S3 path prefixes to s3a://."""
    if not path:
        return path
    if path.startswith('s3n://'):
        return 's3a://' + path[6:]
    if path.startswith('s3://'):
        return 's3a://' + path[5:]
    if not path.startswith('s3a://'):
        return 's3a://' + path
    return path

# =============================================================================
# SHARED CONFIGURATION
# =============================================================================

def get_config() -> dict:
    """Shared configuration for all migration DAGs."""
    return {
        # SSH Configuration (for MapR migration)
        'ssh_conn_id': Variable.get('cluster_ssh_conn_id', default_var=os.getenv('CLUSTER_SSH_CONN_ID', 'cluster_edge_ssh')),
        'edge_temp_path': Variable.get('cluster_edge_temp_path', default_var=os.getenv('CLUSTER_EDGE_TEMP_PATH', '/tmp/migration')),

        # S3 Configuration
        'default_s3_bucket': Variable.get('migration_default_s3_bucket', default_var=os.getenv('MIGRATION_DEFAULT_S3_BUCKET', 's3a://data-lake')),
        's3_endpoint': Variable.get('s3_endpoint', default_var=os.getenv('S3_ENDPOINT', '')),
        's3_access_key': Variable.get('s3_access_key', default_var=os.getenv('S3_ACCESS_KEY', '')),
        's3_secret_key': Variable.get('s3_secret_key', default_var=os.getenv('S3_SECRET_KEY', '')),

        # DistCp Configuration
        'distcp_mappers': Variable.get('migration_distcp_mappers', default_var=os.getenv('MIGRATION_DISTCP_MAPPERS', '50')),
        'distcp_bandwidth': Variable.get('migration_distcp_bandwidth', default_var=os.getenv('MIGRATION_DISTCP_BANDWIDTH', '100')),

        # Spark Configuration
        'spark_conn_id': Variable.get('migration_spark_conn_id', default_var=os.getenv('MIGRATION_SPARK_CONN_ID', 'spark_default')),

        # Tracking Configuration
        'tracking_database': Variable.get('migration_tracking_database', default_var=os.getenv('MIGRATION_TRACKING_DATABASE', 'migration_tracking')),
        'tracking_location': Variable.get('migration_tracking_location', default_var=os.getenv('MIGRATION_TRACKING_LOCATION', 's3a://data-lake/migration_tracking')),
        'report_output_location': Variable.get('migration_report_location', default_var=os.getenv('MIGRATION_REPORT_LOCATION', 's3a://data-lake/migration_reports')),

        # Cluster type for display/reporting purposes ('MapR' or 'HDP')
        'cluster_type': Variable.get('cluster_type', default_var=os.getenv('CLUSTER_TYPE', 'MapR')),
        # Cluster Authentication ('mapr', 'kinit', or 'none')
        'auth_method': Variable.get('auth_method', default_var=os.getenv('AUTH_METHOD', 'mapr')),  # 'mapr' or 'kinit'
        'mapr_user': Variable.get('mapr_user', default_var=os.getenv('MAPR_USER', '')),
        'mapr_ticketfile_location': Variable.get('mapr_ticketfile_location', default_var=os.getenv('MAPR_TICKETFILE_LOCATION', '/tmp/maprticket_${USER}')),
        # HDFS nameservice (required for HDFS HA clusters; leave empty for MapR)
        'hdfs_nameservice': Variable.get('hdfs_nameservice', default_var=os.getenv('HDFS_NAMESERVICE', '')),

        # Listing tool
        's3_listing_tool': Variable.get('s3_listing_tool', default_var=os.getenv('S3_LISTING_TOOL', 'hadoop')),

        # S3 source credentials
        's3_source_endpoint': Variable.get('s3_source_endpoint', default_var=os.getenv('S3_SOURCE_ENDPOINT', '')),
        's3_source_access_key': Variable.get('s3_source_access_key', default_var=os.getenv('S3_SOURCE_ACCESS_KEY', '')),
        's3_source_secret_key': Variable.get('s3_source_secret_key', default_var=os.getenv('S3_SOURCE_SECRET_KEY', '')),

        # S3 destination credentials
        's3_dest_endpoint': Variable.get('s3_dest_endpoint', default_var=os.getenv('S3_DEST_ENDPOINT', '')),
        's3_dest_access_key': Variable.get('s3_dest_access_key', default_var=os.getenv('S3_DEST_ACCESS_KEY', '')),
        's3_dest_secret_key': Variable.get('s3_dest_secret_key', default_var=os.getenv('S3_DEST_SECRET_KEY', '')),

        # Email / SMTP Configuration
        'smtp_conn_id': Variable.get('migration_smtp_conn_id', default_var=os.getenv('MIGRATION_SMTP_CONN_ID', 'smtp_default')),
        'email_recipients': Variable.get('migration_email_recipients', default_var=os.getenv('MIGRATION_EMAIL_RECIPIENTS', '')),
    }

# SSH timeout: 24 hours
SSH_COMMAND_TIMEOUT = 86400


def _login_shell(cmd: str, cluster_type: str = 'MapR') -> str:
    """Wrap a shell command for execution over SSH.

    - MapR: sources ``~/.profile`` directly (customer-tested approach).
    - HDP (and any non-MapR cluster): uses ``bash -l`` (login shell) so
      /etc/profile.d/*.sh is sourced, ensuring JAVA_HOME, SPARK_HOME,
      HADOOP_HOME, HADOOP_CONF_DIR and PATH are set.
    """
    if cluster_type.upper() == 'MAPR':
        return f"source ~/.profile 2>/dev/null || true\n{cmd}"
    return f"bash -l <<'__LOGIN_SHELL_EOF__'\n{cmd}\n__LOGIN_SHELL_EOF__\n"


def cluster_login(run_id: str) -> dict:
    """SSH to edge node, perform cluster login (MapR or Kerberos), create temp dir.

    This is the core logic — DAG files wrap it with @task to make it an Airflow task.
    """
    from airflow.providers.ssh.hooks.ssh import SSHHook

    config = get_config()
    ssh = SSHHook(ssh_conn_id=config['ssh_conn_id'])
    temp_dir = f"{config['edge_temp_path']}/{run_id}"

    auth_method = config.get('auth_method', 'mapr')
    mapr_user = config.get('mapr_user', '')
    mapr_ticketfile = config.get('mapr_ticketfile_location', '/tmp/maprticket_${USER}')

    auth_script_parts = []

    auth_script_parts.append(f"""
echo "=== Cluster Authentication ({auth_method}) ==="

if [ "{auth_method}" = "mapr" ]; then
    MAPR_TICKETFILE_LOCATION="{mapr_ticketfile}"
    export MAPR_TICKETFILE_LOCATION

    if maprlogin print 2>/dev/null | grep -q "{mapr_user}"; then
        echo "Using existing valid MapR ticket"
    else
        echo "ERROR: No valid MapR ticket found"
        echo "Please ensure a valid MapR ticket exists before running this DAG"
        exit 1
    fi

elif [ "{auth_method}" = "kinit" ]; then
    echo "Kerberos authentication handled via login shell"

elif [ "{auth_method}" = "none" ]; then
    echo "No authentication required (auth_method=none)"

else
    echo "ERROR: Unknown auth_method: {auth_method}"
    exit 1
fi

echo "Authentication successful"
""")

    auth_script_parts.append(f"""
echo "=== Creating temp directory ==="
mkdir -p {temp_dir}
chmod 755 {temp_dir}

echo "CLUSTER_LOGIN_SUCCESS"
echo "TEMP_DIR={temp_dir}"
""")
    full_script = "set -e\n" + "\n".join(auth_script_parts)
    with ssh.get_conn() as client:
        _, stdout, stderr = client.exec_command(_login_shell(full_script), timeout=300)
        output = stdout.read().decode()
        error = stderr.read().decode()
        exit_code = stdout.channel.recv_exit_status()

        logger.info("=== Cluster Login Output ===")
        logger.info(output)

        if exit_code != 0:
            logger.error("=== Cluster Login Errors ===")
            logger.error(error)
            raise Exception(
                f"Cluster login setup failed with exit code {exit_code}\n"
                f"Error: {error}\n"
                f"Output: {output[-500:]}"
            )

        if "CLUSTER_LOGIN_SUCCESS" not in output:
            raise Exception(
                f"Cluster login setup incomplete - success marker not found\n"
                f"Output: {output[-500:]}"
            )

    return {'temp_dir': temp_dir, 'run_id': run_id}


def build_s3_opts(dest_bucket_url: str, config: dict, dest_endpoint: str = '') -> str:
    """Build per-bucket Hadoop S3A JVM options scoped to the destination bucket name.

    Resolution order:
      Case 1 — dest_endpoint is provided (from the Excel 'endpoint' column):
        - Endpoint  : used directly from dest_endpoint.
        - Credentials: looked up via Airflow Variable '<endpoint-hostname>_access_key/secret_key'
                       or env var '<ENDPOINT_HOSTNAME>_ACCESS_KEY/SECRET_KEY'.
        - Emitted as fs.s3a.bucket.<name>.* so multi-tenant rows in one DistCp command
          carry isolated credentials per bucket.
        The endpoint hostname is used as the credential slug so that two buckets with the
        same name on different tenant managers are always disambiguated by their endpoint.

      Case 2 — no dest_endpoint (original single-tenant behaviour, unchanged):
        - Uses the global config keys s3_endpoint / s3_access_key / s3_secret_key.
        - Emitted as unscoped fs.s3a.* properties, exactly as before this feature.
        - If those are also empty, Hadoop uses its own credential chain (e.g. IAM role).

    Credential Variable naming (Case 1):
      Slug = hostname of dest_endpoint, lowercased, e.g. "s3.tenant-a.example.com"
      Hyphens and dots are kept in the Airflow Variable name; env var uses underscores.

      Airflow Variable                          Environment variable
      ----------------------------------------- -------------------------------------------
      s3.tenant-a.example.com_access_key        S3_TENANT_A_EXAMPLE_COM_ACCESS_KEY  (masked)
      s3.tenant-a.example.com_secret_key        S3_TENANT_A_EXAMPLE_COM_SECRET_KEY  (masked)
    """
    from urllib.parse import urlparse

    raw = (dest_bucket_url or '').strip()
    for prefix in ('s3a://', 's3n://', 's3://'):
        if raw.startswith(prefix):
            raw = raw[len(prefix):]
            break
    bucket_name = urlparse(f's3a://{raw}').netloc.lower().strip()

    if not bucket_name:
        logger.warning(f"[build_s3_opts] Could not extract bucket name from '{dest_bucket_url}' — falling back to global credentials")

    endpoint = (dest_endpoint or '').strip()

    if endpoint and bucket_name:
        ep_hostname = urlparse(endpoint).hostname or urlparse(endpoint).netloc or endpoint
        ep_hostname = ep_hostname.lower().strip()
        env_slug = ep_hostname.upper().replace('.', '_').replace('-', '_')

        access_key = (Variable.get(f'{ep_hostname}_access_key',
                                   default_var=os.getenv(f'{env_slug}_ACCESS_KEY', ''))
                      or config.get('s3_access_key') or '')
        secret_key = (Variable.get(f'{ep_hostname}_secret_key',
                                   default_var=os.getenv(f'{env_slug}_SECRET_KEY', ''))
                      or config.get('s3_secret_key') or '')

        s3_opts = f" -Dfs.s3a.bucket.{bucket_name}.endpoint={endpoint}"
        if access_key:
            s3_opts += f" -Dfs.s3a.bucket.{bucket_name}.access.key={access_key}"
        if secret_key:
            s3_opts += f" -Dfs.s3a.bucket.{bucket_name}.secret.key={secret_key}"
        return s3_opts

    global_endpoint   = config.get('s3_endpoint')   or ''
    global_access_key = config.get('s3_access_key') or ''
    global_secret_key = config.get('s3_secret_key') or ''

    s3_opts = ""
    if global_endpoint:
        s3_opts += f" -Dfs.s3a.endpoint={global_endpoint}"
    if global_access_key:
        s3_opts += f" -Dfs.s3a.access.key={global_access_key}"
    if global_secret_key:
        s3_opts += f" -Dfs.s3a.secret.key={global_secret_key}"
    return s3_opts


def validate_bucket_endpoint_pairs(grouped: dict, config: dict) -> None:
    """Pre-flight check: verify each (bucket, endpoint) pair in the Excel config is reachable"""
    try:
        from urllib.parse import urlparse as _urlparse

        import boto3
        from botocore.exceptions import ClientError
    except ImportError:
        logger.warning(
            "[ValidateBucketEndpoint] boto3 not available — skipping pre-flight validation"
        )
        return

    errors = []
    checked: set = set()

    for (src_db, _dest_db, bucket_val, endpoint_val, _partition_filter), _group in grouped.items():
        if not endpoint_val:
            continue

        pair = (bucket_val, endpoint_val)
        if pair in checked:
            continue
        checked.add(pair)

        raw = bucket_val.strip()
        for prefix in ('s3a://', 's3n://', 's3://'):
            if raw.startswith(prefix):
                raw = raw[len(prefix):]
                break
        bucket_name = _urlparse(f's3a://{raw}').netloc.lower().strip()

        if not bucket_name:
            errors.append(
                f"  - src_db={src_db}: could not extract bucket name from '{bucket_val}'"
            )
            continue

        ep_hostname = (
            _urlparse(endpoint_val).hostname
            or _urlparse(endpoint_val).netloc
            or endpoint_val
        )
        ep_hostname = ep_hostname.lower().strip()
        env_slug = ep_hostname.upper().replace('.', '_').replace('-', '_')

        access_key = (
            Variable.get(f'{ep_hostname}_access_key',
                         default_var=os.getenv(f'{env_slug}_ACCESS_KEY', ''))
            or config.get('s3_access_key') or ''
        )
        secret_key = (
            Variable.get(f'{ep_hostname}_secret_key',
                         default_var=os.getenv(f'{env_slug}_SECRET_KEY', ''))
            or config.get('s3_secret_key') or ''
        )

        try:
            s3 = boto3.client(
                's3',
                endpoint_url=endpoint_val,
                aws_access_key_id=access_key or None,
                aws_secret_access_key=secret_key or None,
            )
            s3.head_bucket(Bucket=bucket_name)
            logger.info(
                f"[ValidateBucketEndpoint] ✓ bucket='{bucket_name}' "
                f"reachable at endpoint='{endpoint_val}'"
            )
        except ClientError as exc:
            code = exc.response.get('Error', {}).get('Code', '')
            if code in ('403', 'AccessDenied'):
                logger.warning(
                    f"[ValidateBucketEndpoint] bucket='{bucket_name}' at "
                    f"endpoint='{endpoint_val}' returned 403 — bucket exists but "
                    f"credentials may lack full access. Proceeding."
                )
            elif code in ('404', 'NoSuchBucket'):
                errors.append(
                    f"  - src_db={src_db}: bucket='{bucket_name}' does NOT exist at "
                    f"endpoint='{endpoint_val}' (HTTP 404) — "
                    f"likely a bucket/endpoint mismatch in the Excel config"
                )
            else:
                errors.append(
                    f"  - src_db={src_db}: bucket='{bucket_name}' at "
                    f"endpoint='{endpoint_val}' returned unexpected S3 error "
                    f"{code}: {exc}"
                )
        except Exception as exc:
            errors.append(
                f"  - src_db={src_db}: could not reach endpoint='{endpoint_val}' "
                f"for bucket='{bucket_name}': {exc}"
            )

    if errors:
        raise Exception(
            f"[ParseExcel] Bucket/endpoint validation failed for {len(errors)} "
            f"pair(s). Fix the Excel config and re-trigger the DAG:\n"
            + "\n".join(errors)
        )
