"""
Combined Migration DAGs

This file contains two independent DAGs:
1. mapr_to_s3_migration: Migrates data and Hive tables (metadata) from MapR or HDFS to S3
2. iceberg_migration: Converts existing Hive tables in S3 to Apache Iceberg format

Both DAGs can be run independently. The iceberg_migration DAG is typically run after mapr_to_s3_migration is complete, but they are not automatically chained.


1. MapR/HDFS to S3 Migration DAG

Orchestrates migration of Hive tables from MapR or HDFS to S3:
- Excel config from S3 (only DAG parameter)
- SSH operations for MapR or Kerberos authentication, beeline discovery, distcp (24h timeout)
- PySpark tasks for Hive table creation
- Incremental support (distcp -update, table repair)
- Comprehensive validation (row counts, partitions, schema)

Excel columns: database | table | dest database | bucket

2. Iceberg Migration DAG

Converts existing Hive tables in S3 to Apache Iceberg format.
This DAG runs independently after the main MapR-to-S3 migration is complete.

Two migration strategies supported:
1. In-place migration: Convert existing Hive table to Iceberg (overwrites metadata)
2. Snapshot migration: Create separate Iceberg table alongside Hive table

Excel columns: database | table | inplace_migration | destination_iceberg_database
"""

from datetime import datetime, timedelta
import json
from functools import wraps

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.models.param import Param
from airflow.providers.ssh.hooks.ssh import SSHHook
import logging

logger = logging.getLogger(__name__)

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
        
        # Add duration to result if it's a dict
        if isinstance(result, dict):
            result['_task_duration'] = duration
        
        return result
    
    return wrapper

def execute_with_iceberg_retry(spark, sql: str, max_retries: int = 6, task_label: str = ""):
    """Execute Spark SQL with retry logic for Iceberg commit conflicts."""
    status = False
    counter = 0
    last_exception = None

    while status == False and counter < max_retries:
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

    if status == False:
        raise last_exception

# =============================================================================
# SHARED CONFIGURATION
# =============================================================================

def get_config() -> dict:
    """Shared configuration for both DAGs"""
    return {
        # SSH Configuration (for MapR migration)
        'ssh_conn_id': Variable.get('cluster_ssh_conn_id', default_var='cluster_edge_ssh'),
        'edge_temp_path': Variable.get('cluster_edge_temp_path', default_var='/tmp/migration'),
        
        # S3 Configuration
        'default_s3_bucket': Variable.get('migration_default_s3_bucket', default_var='s3a://data-lake'),
        's3_endpoint': Variable.get('s3_endpoint', default_var=''),
        's3_access_key': Variable.get('s3_access_key', default_var=''),
        's3_secret_key': Variable.get('s3_secret_key', default_var=''),
        
        # DistCp Configuration
        'distcp_mappers': Variable.get('migration_distcp_mappers', default_var='50'),
        'distcp_bandwidth': Variable.get('migration_distcp_bandwidth', default_var='100'),
        
        # Spark Configuration
        'spark_conn_id': Variable.get('migration_spark_conn_id', default_var='spark_default'),
        
        # Tracking Configuration
        'tracking_database': Variable.get('migration_tracking_database', default_var='migration_tracking'),
        'tracking_location': Variable.get('migration_tracking_location', default_var='s3a://data-lake/migration_tracking'),
        'report_output_location': Variable.get('migration_report_location', default_var='s3a://data-lake/migration_reports'),

        # Cluster Authentication (MapR or Kerberos)
        'auth_method': Variable.get('auth_method', default_var='mapr'),  # 'mapr' or 'kinit'
        'mapr_user': Variable.get('mapr_user', default_var=''),
        'mapr_ticketfile_location': Variable.get('mapr_ticketfile_location', default_var='/tmp/maprticket_${USER}'),
        'kinit_principal': Variable.get('kinit_principal', default_var=''),
        'kinit_keytab': Variable.get('kinit_keytab', default_var=''),
        'kinit_password': Variable.get('kinit_password', default_var=''),

        # Listing tool
        's3_listing_tool': Variable.get('s3_listing_tool', default_var='hadoop'),

        # Email / SMTP Configuration
        'smtp_conn_id': Variable.get('migration_smtp_conn_id', default_var='smtp_default'),
        'email_recipients': Variable.get('migration_email_recipients', default_var=''),
    }

# SSH timeout: 24 hours
SSH_COMMAND_TIMEOUT = 86400

DEFAULT_ARGS = {
    'owner': 'data-migration',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# =============================================================================
# DAG 1: MAPR TO S3 MIGRATION TASKS
# =============================================================================

@task
def validate_prerequisites(run_id: str) -> dict:
    """Comprehensive pre-dag validation of all required components."""
    config = get_config()
    validation_results = {
        'ssh_connectivity': False,
        'pyspark_available': False,
        'hive_available': False,
        'hadoop_fs_available': False,
        'errors': []
    }

    logger.info("="*60)
    logger.info("STARTING PRE-DAG VALIDATION")
    logger.info("="*60)

    try:
        ssh = SSHHook(ssh_conn_id=config['ssh_conn_id'])
        with ssh.get_conn() as client:

            # 1. SSH Connectivity
            logger.info("[1/4] Testing SSH connectivity...")
            _, stdout, stderr = client.exec_command('echo "SSH_TEST_OK"', timeout=30)
            exit_code = stdout.channel.recv_exit_status()
            output = stdout.read().decode()
            stderr.read()

            if exit_code == 0 and "SSH_TEST_OK" in output:
                validation_results['ssh_connectivity'] = True
                logger.info("SSH connectivity: PASSED")
            else:
                error_msg = f"SSH command failed with exit code {exit_code}"
                validation_results['errors'].append(f"SSH: {error_msg}")
                logger.error(f"SSH connectivity: FAILED - {error_msg}")

            # 2. PySpark
            logger.info("[2/4] Testing PySpark availability...")
            test_cmd = """
    source ~/.profile 2>/dev/null || true
    which pyspark && pyspark --version 2>&1 | head -5
    """
            _, stdout, stderr = client.exec_command(test_cmd, timeout=60)
            exit_code = stdout.channel.recv_exit_status()
            output = stdout.read().decode()
            stderr.read()

            if exit_code == 0 and ('pyspark' in output.lower() or 'spark' in output.lower()):
                validation_results['pyspark_available'] = True
                logger.info(f"PySpark: PASSED")
                logger.info(f"Version info: {output.strip()[:200]}")
            else:
                error_msg = f"PySpark not found or failed. Output: {output[:200]}"
                validation_results['errors'].append(f"PySpark: {error_msg}")
                logger.error(f"PySpark: FAILED - {error_msg}")

            # Hive
            logger.info("[3/4] Testing Hive availability...")
            test_cmd = """
    source ~/.profile 2>/dev/null || true
    hive --version 2>&1 | head -3
    """
            _, stdout, stderr = client.exec_command(test_cmd, timeout=60)
            exit_code = stdout.channel.recv_exit_status()
            output = stdout.read().decode()
            stderr.read()

            if exit_code == 0 and 'hive' in output.lower():
                validation_results['hive_available'] = True
                logger.info(f"Hive: PASSED")
                logger.info(f"Version info: {output.strip()[:200]}")
            else:
                error_msg = f"Hive not found or failed. Output: {output[:200]}"
                validation_results['errors'].append(f"Hive: {error_msg}")
                logger.error(f"Hive: FAILED - {error_msg}")

            # Hadoop FS
            logger.info("[4/4] Testing Hadoop FS commands...")
            test_cmd = """
    source ~/.profile 2>/dev/null || true
    hadoop version 2>&1 | head -3
    hadoop fs -ls / > /dev/null 2>&1 && echo "HADOOP_FS_OK"
    """
            _, stdout, stderr = client.exec_command(test_cmd, timeout=60)
            exit_code = stdout.channel.recv_exit_status()
            output = stdout.read().decode()
            stderr.read()

            if exit_code == 0 and 'HADOOP_FS_OK' in output:
                validation_results['hadoop_fs_available'] = True
                logger.info(f"Hadoop FS: PASSED")
                version_line = [l for l in output.split('\n') if 'hadoop' in l.lower()]
                if version_line:
                    logger.info(f"Version info: {version_line[0].strip()}")
            else:
                error_msg = f"Hadoop FS commands failed. Output: {output[:200]}"
                validation_results['errors'].append(f"Hadoop FS: {error_msg}")
                logger.error(f"Hadoop FS: FAILED - {error_msg}")

    except Exception as e:
        error_msg = f"SSH connection failed: {str(e)}"
        if not validation_results['ssh_connectivity']:
            validation_results['errors'].append(f"SSH: {error_msg}")
            logger.error(f"SSH connectivity: FAILED - {error_msg}")
        if not validation_results['pyspark_available']:
            validation_results['errors'].append("PySpark: Skipped due to SSH failure")
            logger.warning("PySpark: SKIPPED (SSH failed)")
        if not validation_results['hive_available']:
            validation_results['errors'].append("Hive: Skipped due to SSH failure")
            logger.warning("Hive: SKIPPED (SSH failed)")
        if not validation_results['hadoop_fs_available']:
            validation_results['errors'].append("Hadoop FS: Skipped due to SSH failure")
            logger.warning("Hadoop FS: SKIPPED (SSH failed)")

    # Final validation check
    logger.info("\n" + "="*60)
    logger.info("VALIDATION SUMMARY")
    logger.info("="*60)

    all_passed = all([
        validation_results['ssh_connectivity'],
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
                table_create_status STRING,
                table_create_completed_at TIMESTAMP,
                table_create_duration_seconds DOUBLE,
                table_already_existed BOOLEAN,
                validation_status STRING,
                validation_completed_at TIMESTAMP,
                validation_duration_seconds DOUBLE,
                dest_hive_row_count BIGINT,
                source_partition_count INT,
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

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tracking_db}.validation_results (
            run_id STRING,
            validation_run_timestamp TIMESTAMP,
            total_tables_validated INT,
            tables_passed_validation INT,
            tables_failed_validation INT,
            total_row_count_mismatches INT,
            total_partition_count_mismatches INT,
            total_schema_mismatches INT,
            total_file_size_mismatches INT,
            total_file_count_mismatches INT,
            validation_summary_json STRING,
            created_at TIMESTAMP
        )
        USING iceberg
        LOCATION '{tracking_loc}/validation_results'
    """)
    
    return {'status': 'initialized', 'database': tracking_db}


@task.pyspark(conn_id='spark_default')
def create_migration_run(excel_file_path: str, dag_run_id: str, spark) -> str:
    """Create migration run record in Iceberg tracking table."""
    from datetime import datetime
    import uuid
    
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
    import pandas as ps
    from io import BytesIO

    config = get_config()
    binary_df = spark.read.format("binaryFile").load(excel_file_path)
    row = binary_df.select("content").first()
    excel_bytes = bytes(row.content)
    df = ps.read_excel(BytesIO(excel_bytes), engine='openpyxl')
    
    # Normalize column names
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    
    # Convert to list of dicts
    configs = []
    for _, row in df.iterrows():
        src_db = str(row.get('database', '')).strip() if row.get('database') is not None else ''
        if not src_db:
            continue
        
        tbl_pattern = str(row.get('table', '*')).strip() if row.get('table') is not None else '*'
        tbl_pattern = tbl_pattern or '*'
        
        dest_db = str(row.get('dest_database', '')).strip() if row.get('dest_database') is not None else ''
        dest_db = dest_db or src_db
        
        bucket_val = str(row.get('bucket', '')).strip() if row.get('bucket') is not None else ''
        bucket_val = bucket_val or config['default_s3_bucket']
        if bucket_val.startswith('s3n://'):
            bucket_val = 's3a://' + bucket_val[6:]
        elif bucket_val.startswith('s3://'):
            bucket_val = 's3a://' + bucket_val[5:]
        elif not bucket_val.startswith('s3a://'):
            bucket_val = f"s3a://{bucket_val}"
        logger.info(f"[ParseExcel] Normalised bucket to: {bucket_val}")
        
        configs.append({
            'source_database': src_db,
            'table_pattern': tbl_pattern,
            'dest_database': dest_db,
            'dest_bucket': bucket_val,
            'run_id': run_id,
        })
    
    return configs


@task
def cluster_login_setup(run_id: str) -> dict:
    """SSH to edge, perform cluster login (MapR or Kerberos), create temp dir."""
    config = get_config()
    ssh = SSHHook(ssh_conn_id=config['ssh_conn_id'])
    temp_dir = f"{config['edge_temp_path']}/{run_id}"

    auth_script_parts = []

    auth_script_parts.append("""
echo "=== Sourcing User Profile ==="
if [ -f ~/.profile ]; then
    source ~/.profile
    echo "Profile sourced: ~/.profile"
else
    echo "WARNING: Profile not found at ~/.profile"
fi
""")
    
    auth_method = config.get('auth_method', 'mapr')
    mapr_user = config.get('mapr_user', '')
    mapr_ticketfile = config.get('mapr_ticketfile_location', '') 
    kinit_principal = config.get('kinit_principal', '')
    kinit_keytab = config.get('kinit_keytab', '')
    kinit_password = config.get('kinit_password', '')

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
    if [ -n "{kinit_keytab}" ] && [ -n "{kinit_principal}" ]; then
        kinit -kt "{kinit_keytab}" "{kinit_principal}"
    elif [ -n "{kinit_principal}" ] && [ -n "{kinit_password}" ]; then
        echo "{kinit_password}" | kinit "{kinit_principal}"
    else
        echo "ERROR: kinit requires principal and keytab or password"
        exit 1
    fi
    
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
        _, stdout, stderr = client.exec_command(full_script, timeout=300)
        exit_code = stdout.channel.recv_exit_status()
        output = stdout.read().decode()
        error = stderr.read().decode()

        logger.info(f"=== Cluster Login Output ===")
        logger.info(output)

        if exit_code != 0:
            logger.error(f"=== Cluster Login Errors ===")
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


@task
@track_duration 
def discover_tables_via_spark_ssh(db_config: dict) -> dict:
    """Use Spark SQL via SSH on edge node to discover tables and metadata."""
    import json
    
    config = get_config()
    ssh = SSHHook(ssh_conn_id=config['ssh_conn_id'])
    
    run_id = db_config['run_id']
    src_db = db_config['source_database']
    pattern = db_config['table_pattern']
    dest_db = db_config['dest_database']
    dest_bucket = db_config['dest_bucket']
    
    pyspark_script = '''
import json
import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder \\
    .appName("table_discovery_{run_id}_{src_db}") \\
    .enableHiveSupport() \\
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

src_db = "{src_db}"
pattern = "{pattern}"
dest_db = "{dest_db}"
dest_bucket = "{dest_bucket}"

if pattern == '*':
    tables_df = spark.sql("SHOW TABLES IN {{0}}".format(src_db))
else:
    like_pattern = pattern.replace('*', '%')
    tables_df = spark.sql(
        "SHOW TABLES IN {{0}} LIKE '{{1}}'".format(src_db, like_pattern)
    )

table_list = [row.tableName for row in tables_df.collect()]

metadata = []

for tbl in table_list:
    try:
        desc_df = spark.sql(
            "DESCRIBE FORMATTED {{0}}.{{1}}".format(src_db, tbl)
        )
        desc_rows = desc_df.collect()
        
        loc = None
        table_type = "UNKNOWN"
        input_format = None
        
        for row in desc_rows:
            col_name = (row.col_name or "").strip().rstrip(":").lower()
            data_type = (row.data_type or "").strip()
            
            if col_name == "location":
                loc = data_type
            elif col_name in ("type", "table type"):
                table_type = data_type.replace("_TABLE", "")
            elif col_name == "InputFormat:":
                input_format = data_type
        
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
            except:
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
        except:
            pass
        
        partitions = []
        partition_columns = ""
        is_partitioned = False
        try:
            parts_df = spark.sql(
                "SHOW PARTITIONS {{0}}.{{1}}".format(src_db, tbl)
            )
            partitions = [row.partition for row in parts_df.collect()]
            is_partitioned = len(partitions) > 0
            
            if partitions:
                first_part = partitions[0]
                partition_columns = ",".join(
                    [p.split("=")[0] for p in first_part.split("/")]
                )
        except:
            pass
        
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
            "partitions": partitions,
            "partition_columns": partition_columns,
            "partition_count": len(partitions),
            "row_count": row_count,
            "is_partitioned": is_partitioned,
            "table_type": table_type,
            "source_total_size_bytes": source_total_size,
            "source_file_count": source_file_count
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
            "table_type": "UNKNOWN",
            "source_total_size_bytes": 0,
            "source_file_count": 0,
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
        pattern=pattern,
        dest_db=dest_db,
        dest_bucket=dest_bucket
    )
        
    with ssh.get_conn() as client:
        temp_dir = f"/tmp/discovery_{run_id}_{src_db}"
        _, cmd_stdout, _ = client.exec_command(f"mkdir -p {temp_dir}", timeout=60)
        cmd_stdout.channel.recv_exit_status() 
        
        script_path = f"{temp_dir}/discover_tables.py"
        sftp = client.open_sftp()
        with sftp.file(script_path, 'w') as f:
            f.write(pyspark_script)
        sftp.close()

        source_profile = "source ~/.profile 2>/dev/null || true\n"
        
        # Use pyspark < script.py instead of spark-submit
        cmd = f"""
{source_profile} cd {temp_dir}
pyspark < {script_path} 2>&1 | tee discovery_{run_id}_{src_db}.log
"""
        
        _, stdout, stderr = client.exec_command(cmd, timeout=3600)
        exit_code = stdout.channel.recv_exit_status()
        output = stdout.read().decode()
        error_output = stderr.read().decode()
        
        logger.info(f"=== Spark Discovery Output ===")
        logger.info(output[-1000:])

        # client.exec_command(f"rm -rf {temp_dir}", timeout=60)

        if exit_code != 0:
            logger.error(f"=== Spark Discovery Errors ===")
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
        row_count = t.get('row_count', 0)
        table_type = t.get('table_type', 'UNKNOWN')
        source_total_size = t.get('source_total_size_bytes', 0)
        source_file_count = t.get('source_file_count', 0)
        
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
                    source_row_count = {row_count},
                    source_total_size_bytes = {source_total_size},
                    source_file_count = {source_file_count},
                    source_partition_count = {t.get('partition_count', 0)}, 
                    updated_at = current_timestamp()
                WHERE run_id = '{run_id}'
                  AND source_database = '{t['source_database']}'
                  AND source_table = '{t['source_table']}'
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
                    source_partition_count,
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
                    updated_at
                ) VALUES (
                    '{run_id}', '{t['source_database']}', '{t['source_table']}',
                    '{t['dest_database']}', '{t['dest_bucket']}', '{t['s3_location']}',
                    '{t['source_location']}', '{t['file_format']}',
                    {t.get('partition_count', 0)}, {str(t.get('is_partitioned', False)).lower()},
                    '{schema_json}', '{parts_json}', '{t.get('partition_columns', '')}',
                    '{table_type}', {row_count},
                    {source_total_size}, {source_file_count},
                    {t.get('partition_count', 0)}, 
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
                    current_timestamp()
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
    
    run_id = discovery['run_id']
    tables = discovery['tables']
    temp_dir = cluster_setup['temp_dir']
    mappers = config['distcp_mappers']
    bandwidth = config['distcp_bandwidth']
    
    s3_endpoint = config['s3_endpoint']
    s3_access_key = config['s3_access_key']
    s3_secret_key = config['s3_secret_key']
    
    s3_opts = ""
    if s3_endpoint:
        s3_opts += f" -Dfs.s3a.endpoint={s3_endpoint}"
    if s3_access_key:
        s3_opts += f" -Dfs.s3a.access.key={s3_access_key}"
    if s3_secret_key:
        s3_opts += f" -Dfs.s3a.secret.key={s3_secret_key}"

    source_profile = "source ~/.profile 2>/dev/null || true\n"
    
    results = []
    for t in tables:
        if t.get('error'): 
            results.append({
                'source_database': t['source_database'],
                'source_table': t['source_table'],
                'status': 'SKIPPED',

            })
            continue

        src_db = t['source_database']
        tbl = t['source_table']
        source_loc = t['source_location']
        s3_loc = t['s3_location']

        logger.info(f"[DistCp] Starting copy for {src_db}.{tbl}")
        logger.info(f"[DistCp]   Source : {source_loc}")
        logger.info(f"[DistCp]   Dest   : {s3_loc}")
        logger.info(f"[DistCp]   Mappers: {mappers} | Bandwidth: {bandwidth} MB/s")

        cmd = f'''{source_profile}
set -e

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
echo "INCREMENTAL=$INCR"

echo "=== Calculating S3 metrics BEFORE distcp ==="
S3_BEFORE=$(calculate_s3_metrics_hadoop "{s3_loc}")

S3_FILE_COUNT_BEFORE=$(echo "$S3_BEFORE" | grep "S3_FILE_COUNT=" | cut -d'=' -f2)
S3_TOTAL_SIZE_BEFORE=$(echo "$S3_BEFORE" | grep "S3_TOTAL_SIZE=" | cut -d'=' -f2)

echo "S3_FILE_COUNT_BEFORE=$S3_FILE_COUNT_BEFORE"
echo "S3_TOTAL_SIZE_BEFORE=$S3_TOTAL_SIZE_BEFORE"

echo "=== Running distcp ==="
DISTCP_OUTPUT=$(hadoop distcp{s3_opts} -update -m {mappers} -bandwidth {bandwidth} -strategy dynamic \\
    -log {temp_dir}/distcp_{tbl}.log "{source_loc}" "{s3_loc}" 2>&1)
DISTCP_EXIT=$?
echo "DISTCP_EXIT_CODE=$DISTCP_EXIT"

BYTES_COPIED=$(echo "$DISTCP_OUTPUT" | grep -i "Bytes Copied" | awk '{{print $NF}}' | tr -d ',')
FILES_COPIED=$(echo "$DISTCP_OUTPUT" | grep -i "Number of files copied" | awk '{{print $NF}}' | tr -d ',')

[ -z "$BYTES_COPIED" ] && BYTES_COPIED=0
[ -z "$FILES_COPIED" ] && FILES_COPIED=0

echo "BYTES_COPIED=$BYTES_COPIED"
echo "FILES_COPIED=$FILES_COPIED"

echo "=== Calculating S3 metrics AFTER distcp ==="
S3_AFTER=$(calculate_s3_metrics_hadoop "{s3_loc}")

S3_FILE_COUNT_AFTER=$(echo "$S3_AFTER" | grep "S3_FILE_COUNT=" | cut -d'=' -f2)
S3_TOTAL_SIZE_AFTER=$(echo "$S3_AFTER" | grep "S3_TOTAL_SIZE=" | cut -d'=' -f2)

echo "S3_FILE_COUNT_AFTER=$S3_FILE_COUNT_AFTER"
echo "S3_TOTAL_SIZE_AFTER=$S3_TOTAL_SIZE_AFTER"

S3_FILES_TRANSFERRED=$((S3_FILE_COUNT_AFTER - S3_FILE_COUNT_BEFORE))
S3_BYTES_TRANSFERRED=$((S3_TOTAL_SIZE_AFTER - S3_TOTAL_SIZE_BEFORE))

echo "S3_FILES_TRANSFERRED=$S3_FILES_TRANSFERRED"
echo "S3_BYTES_TRANSFERRED=$S3_BYTES_TRANSFERRED"

[ "$DISTCP_EXIT" -ne 0 ] && exit $DISTCP_EXIT
exit 0
'''
        from datetime import datetime as _dt
        distcp_started_at = _dt.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        try:
            with ssh.get_conn() as client:
                _, stdout, stderr = client.exec_command(cmd, timeout=SSH_COMMAND_TIMEOUT)
                exit_code = stdout.channel.recv_exit_status()
                output = stdout.read().decode()
                error_output = stderr.read().decode()

                logger.info(f"=== DistCp for {src_db}.{tbl} (last 1000 chars) ===")
                logger.info(output[-1000:])

                is_incr = "INCREMENTAL=true" in output

                bytes_copied = 0
                files_copied = 0
                s3_size_before = 0
                s3_files_before = 0
                s3_size_after = 0
                s3_files_after = 0
                s3_bytes_transferred = 0
                s3_files_transferred = 0

                try:
                    for line in output.split('\n'):
                        line = line.strip()
                        if 'BYTES_COPIED=' in line:
                            bytes_copied = int(line.split('=')[1].strip() or 0)
                        elif 'FILES_COPIED=' in line:
                            files_copied = int(line.split('=')[1].strip() or 0)
                        elif 'S3_TOTAL_SIZE_BEFORE=' in line:
                            s3_size_before = int(line.split('=')[1].strip() or 0)
                        elif 'S3_FILE_COUNT_BEFORE=' in line:
                            s3_files_before = int(line.split('=')[1].strip() or 0)
                        elif 'S3_TOTAL_SIZE_AFTER=' in line:
                            s3_size_after = int(line.split('=')[1].strip() or 0)
                        elif 'S3_FILE_COUNT_AFTER=' in line:
                            s3_files_after = int(line.split('=')[1].strip() or 0)
                        elif 'S3_BYTES_TRANSFERRED=' in line:
                            s3_bytes_transferred = int(line.split('=')[1].strip() or 0)
                        elif 'S3_FILES_TRANSFERRED=' in line:
                            s3_files_transferred = int(line.split('=')[1].strip() or 0)
                except:
                    pass
                
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
                    'error': None
                })
        except Exception as e:
            error_msg = f"DistCp failed for {src_db}.{tbl}: {str(e)[:2000]}"
            _fail_dt = _dt.utcnow()
            results.append({
                'source_database': src_db,
                'source_table': tbl,
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
                'error': str(e)[:2000]
            })
            logger.error(f"ERROR: {error_msg}")

    failed_tables = [r for r in results if r['status'] == 'FAILED']
    has_failures = len(failed_tables) > 0

    result_dict = {
        **discovery,
        'distcp_results': results,
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
        completed_at = r.get('distcp_completed_at', '') 

        s3_size_before = r.get('s3_total_size_bytes_before', 0)
        s3_files_before = r.get('s3_file_count_before', 0)
        s3_size_after = r.get('s3_total_size_bytes_after', 0)
        s3_files_after = r.get('s3_file_count_after', 0)
        s3_bytes_transfer = r.get('s3_bytes_transferred', 0)
        s3_files_transfer = r.get('s3_files_transferred', 0)
        started_at = r.get('distcp_started_at', '')
        
        execute_with_iceberg_retry(spark, f"""
            UPDATE {tracking_db}.migration_table_status
            SET distcp_status = '{r['status']}',
                distcp_started_at = CAST('{started_at}' AS TIMESTAMP),
                distcp_completed_at = CAST('{completed_at}' AS TIMESTAMP),
                distcp_duration_seconds = {distcp_duration},
                distcp_is_incremental = {str(r['is_incremental']).lower()},
                distcp_bytes_copied = {r.get('bytes_copied', 0)},
                distcp_files_copied = {r.get('files_copied', 0)},
                s3_total_size_bytes_before = {s3_size_before},
                s3_file_count_before = {s3_files_before},
                s3_total_size_bytes_after = {s3_size_after},
                s3_file_count_after = {s3_files_after},
                s3_bytes_transferred = {s3_bytes_transfer},
                s3_files_transferred = {s3_files_transfer},                                     
                file_count_match = (source_file_count = {s3_files_after}),               
                file_size_match = (ABS(source_total_size_bytes - {s3_size_after}) / GREATEST(source_total_size_bytes, 1) < 0.01),  
                overall_status = '{overall}',
                error_message = CASE WHEN '{r['status']}' = 'FAILED' THEN '{error_msg}' ELSE error_message END,
                updated_at = current_timestamp()
            WHERE run_id = '{run_id}'
              AND source_database = '{r['source_database']}'
              AND source_table = '{r['source_table']}'
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

    execute_with_iceberg_retry(spark, f"""
        UPDATE {tracking_db}.migration_table_status
        SET distcp_status = 'FAILED',
            overall_status = 'FAILED',
            error_message = COALESCE(error_message, 'S3 copy task did not process this table'),
            updated_at = current_timestamp()
        WHERE run_id = '{run_id}'
          AND source_database = '{src_db}'
          AND distcp_status IS NULL
          AND discovery_status = 'COMPLETED'
    """,
    task_label="update_distcp_status:catchall")
    
    return distcp_result


@task.pyspark(conn_id='spark_default')
@track_duration
def create_hive_tables(distcp_result: dict, spark, **context) -> dict:
    """Create external Hive tables via Spark. Handles incremental (repairs partitions)."""

    if not isinstance(distcp_result, dict) or 'tables' not in distcp_result:
        logger.warning(f"[create_hive_tables] Skipping invalid input: {type(distcp_result)}")
        return {}
    
    dest_db = distcp_result['dest_database']
    tables = distcp_result['tables']
    
    results = []
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {dest_db}")
    
    for t in tables:
        distcp_status = next(
            (r['status'] for r in distcp_result.get('distcp_results', [])
             if r['source_table'] == t['source_table']),
            'UNKNOWN'
        )
        if distcp_status in ('FAILED', 'SKIPPED', 'UNKNOWN'):
            results.append({
                'source_table': t['source_table'],
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
        full_name = f"{dest_db}.{tbl}"

        logger.info(f"[HiveTable] Processing {full_name} | format={fmt} | partitioned={is_part}")
        
        try:
            exists = False
            try:
                spark.sql(f"DESCRIBE {full_name}")
                exists = True
            except:
                pass
            
            if exists:
                if is_part:
                    spark.sql(f"MSCK REPAIR TABLE {full_name}")
                logger.info(f"[HiveTable] REPAIRED (already existed): {full_name}")
                results.append({
                    'source_table': tbl,
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
                
                ddl = f"""
                    CREATE EXTERNAL TABLE IF NOT EXISTS {full_name} ({col_def})
                    {part_clause}
                    STORED AS {fmt}
                    LOCATION '{s3_loc}'
                """
                spark.sql(ddl)
                
                if is_part:
                    spark.sql(f"MSCK REPAIR TABLE {full_name}")
                
                logger.info(f"[HiveTable] CREATED: {full_name} | location={s3_loc}")
                results.append({
                    'source_table': tbl,
                    'status': 'COMPLETED',
                    'action': 'created',
                    'existed': False,
                    'error': None
                })
        except Exception as e:
            error_msg = f"Table creation failed for {dest_db}.{tbl}: {str(e)[:2000]}"
            results.append({
                'source_table': tbl,
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
    dest_db = table_result['dest_database']
    src_db = table_result['source_database']

    table_duration = table_result.get('_task_duration', 0.0)
    
    for r in table_result.get('table_results', []):
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
              AND dest_database = '{dest_db}'
              AND source_table = '{r['source_table']}'
        """,
        task_label=f"update_table_create_status:{r['source_table']}")

    for r in table_result.get('table_results', []):
        if r.get('status') == 'FAILED' and r.get('error'):
            per_table_error = str(r['error'])[:2000].replace("'", "''")
            execute_with_iceberg_retry(spark, f"""
                UPDATE {tracking_db}.migration_table_status
                SET table_create_status = 'FAILED',
                    overall_status = 'FAILED',
                    error_message = '{per_table_error}',
                    updated_at = current_timestamp()
                WHERE run_id = '{run_id}'
                  AND dest_database = '{dest_db}'
                  AND source_table = '{r['source_table']}'
                  AND table_create_status IS NULL
            """,
            task_label=f"update_table_create_status:failure_patch:{r['source_table']}")

    execute_with_iceberg_retry(spark, f"""
        UPDATE {tracking_db}.migration_table_status
        SET table_create_status = 'FAILED',
            overall_status = 'FAILED',
            error_message = COALESCE(error_message, 'Table creation task did not process this table'),
            updated_at = current_timestamp()
        WHERE run_id = '{run_id}'
          AND source_database = '{src_db}'
          AND table_create_status IS NULL
          AND discovery_status = 'COMPLETED'
    """,
    task_label="update_table_create_status:catchall")
    
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
        dest_tbl = f"{dest_db}.{tbl}"

        upstream = spark.sql(f"""
            SELECT distcp_status, table_create_status, overall_status, error_message
            FROM {tracking_db}.migration_table_status
            WHERE run_id = '{run_id}'
              AND source_database = '{src_db}'
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

        logger.info(f"[Validation] Starting validation for {dest_db}.{tbl}")
        
        try:
            source_metrics = spark.sql(f"""
                SELECT source_row_count, source_partition_count
                FROM {tracking_db}.migration_table_status
                WHERE run_id = '{run_id}'
                  AND source_database = '{src_db}'
                  AND source_table = '{tbl}'
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
            
            # Get destination row count
            dest_row_count = spark.sql(f"SELECT COUNT(*) as c FROM {dest_tbl}").collect()[0]['c']
            
            # Get destination partition count
            dest_partition_count = 0
            try:
                dest_partitions_df = spark.sql(f"SHOW PARTITIONS {dest_tbl}")
                dest_partition_count = dest_partitions_df.count()
            except:
                pass  
            
            logger.info(f"[Validation] {dest_db}.{tbl} | source_rows={source_row_count} | dest_rows={dest_row_count} | source_parts={source_partition_count} | dest_parts={dest_partition_count}")

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
            logger.info(f"[Validation] DONE: {dest_db}.{tbl} | {match_summary}")
            if schema_diffs:
                logger.warning(f"[Validation] Schema diffs for {dest_db}.{tbl}: {'; '.join(schema_diffs[:5])}")

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

    total_validated = 0
    passed_validation = 0
    failed_validation = 0
    row_mismatches = 0
    partition_mismatches = 0
    schema_mismatches = 0
    
    for v in validation_result.get('validation_results', []):
        if v['status'] != 'COMPLETED':
            continue

        total_validated += 1
        
        error_msg = (v.get('error', '') or '').replace("'", "''")[:2000]
        schema_diffs = (v.get('schema_differences', '') or '').replace("'", "''")[:2000]

        if not v.get('row_count_match', False):
            row_mismatches += 1
        if not v.get('partition_count_match', True):
            partition_mismatches += 1
        if not v.get('schema_match', False):
            schema_mismatches += 1

        if (v.get('row_count_match', True) and 
            v.get('partition_count_match', True) and 
            v.get('schema_match', True)):
            passed_validation += 1
        else:
            failed_validation += 1

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
                    WHEN overall_status = 'FAILED' THEN overall_status  -- only preserve genuine failures
                    ELSE '{final_overall_status}'
                END,
                error_message = CASE 
                    WHEN overall_status = 'FAILED' THEN error_message   -- only preserve genuine failure errors
                    ELSE {error_message_sql}
                END,
                updated_at = current_timestamp()
            WHERE run_id = '{run_id}'
              AND source_database = '{src_db}'
              AND dest_database = '{dest_db}'
              AND source_table = '{v['source_table']}'
        """,
        task_label=f"update_validation_status:{v['source_table']}")

    if total_validated > 0:
        file_metrics = spark.sql(f"""
            SELECT 
                SUM(CASE WHEN file_size_match = false THEN 1 ELSE 0 END) as size_mismatches,
                SUM(CASE WHEN file_count_match = false THEN 1 ELSE 0 END) as count_mismatches
            FROM {tracking_db}.migration_table_status
            WHERE run_id = '{run_id}'
        """).collect()[0]
        
        size_mismatches = file_metrics['size_mismatches'] or 0
        count_mismatches = file_metrics['count_mismatches'] or 0
        
        validation_summary = {
            'run_id': run_id,
            'total_validated': total_validated,
            'passed': passed_validation,
            'failed': failed_validation,
            'row_mismatches': row_mismatches,
            'partition_mismatches': partition_mismatches,
            'schema_mismatches': schema_mismatches,
            'file_size_mismatches': size_mismatches,
            'file_count_mismatches': count_mismatches
        }

        summary_json = json.dumps(validation_summary).replace("'", "''")
        execute_with_iceberg_retry(spark, f"DELETE FROM {tracking_db}.validation_results WHERE run_id = '{run_id}'", task_label="update_validation_status:delete_summary")
        
        execute_with_iceberg_retry(spark, f"""
            INSERT INTO {tracking_db}.validation_results
            VALUES (
                '{run_id}',
                current_timestamp(),
                {total_validated},
                {passed_validation},
                {failed_validation},
                {row_mismatches},
                {partition_mismatches},
                {schema_mismatches},
                {size_mismatches},
                {count_mismatches},
                '{summary_json}',
                current_timestamp()
            )
        """,
        task_label="update_validation_status:insert_summary")

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

    execute_with_iceberg_retry(spark, f"""
        UPDATE {tracking_db}.migration_table_status
        SET validation_status = 'SKIPPED',
            overall_status = CASE WHEN overall_status = 'FAILED' THEN 'FAILED' ELSE 'VALIDATION_FAILED' END,
            error_message = COALESCE(error_message, 'Validation task did not process this table'),
            updated_at = current_timestamp()
        WHERE run_id = '{run_id}'
          AND source_database = '{src_db}'
          AND table_create_status = 'COMPLETED'
          AND validation_status IS NULL
    """,
    task_label="update_validation_status:catchall")
    
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
    successful_tables = sum(1 for t in table_status if t.overall_status in ['VALIDATED', 'TABLE_CREATED'])
    failed_tables = sum(1 for t in table_status if 'FAILED' in (t.overall_status or ''))
    total_data_gb = sum(t.s3_total_size_bytes_after or 0 for t in table_status) / (1024**3)
    total_files = sum(t.s3_file_count_after or 0 for t in table_status)
    total_rows = sum(t.source_row_count or 0 for t in table_status)
    incremental_runs = sum(1 for t in table_status if t.distcp_is_incremental)
    
    # Generate HTML
    html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MapR to S3 Migration Report - {run_id}</title>
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
        <h1>MapR to S3 Migration Report</h1>
        
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
                <p class="value">{total_data_gb:.5f} GB</p>
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
            
    if validation_summary_data and validation_summary_data[0]['total_tables_validated']:
        vs = validation_summary_data[0]
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
        </div>
"""
    else:
        html += f"""
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
                    <th>Discovery</th>
                    <th>DistCp</th>
                    <th>Table Create</th>
                    <th>Validation</th>
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
        table_name = f"<strong>{t.source_table}</strong>"
        table_dur = f"{t.table_create_duration_seconds:.1f}s" if t.table_create_duration_seconds else "N/A"
        val_dur = f"{t.validation_duration_seconds:.1f}s" if t.validation_duration_seconds else "N/A"
        
        total_dur = (t.discovery_duration_seconds or 0) + (t.distcp_duration_seconds or 0) + \
                    (t.table_create_duration_seconds or 0) + (t.validation_duration_seconds or 0)
        
        html += f"""
                <tr>
                    <td>{t.source_database}</td>
                    <td>{table_name}</td>
                    <td><span class="status-badge {status_class}">{t.overall_status}</span></td>
                    <td class="duration">{discovery_dur}</td>
                    <td class="duration">{distcp_dur}{distcp_detail}</td>
                    <td class="duration">{table_dur}</td>
                    <td class="duration">{val_dur}</td>
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
    html += """
            </tbody>
        </table>

        <div class="section-divider"></div>
        
        <h2>Data Validation Results</h2>
        <table>
            <thead>
                <tr>
                    <th>Database</th>
                    <th>Table</th>
                    <th>MapR Size (GB)</th>
                    <th>S3 Size Before (GB)</th>
                    <th>S3 Size After (GB)</th>
                    <th>S3 Size - Transferred (GB)</th>
                    <th>Size Match</th>
                    <th>MapR Files</th>
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
                    <td class="metric">{source_total_size_gb:.5f}</td>
                    <td class="metric">{s3_size_before_gb:.5f}</td>
                    <td class="metric">{s3_size_after_gb:.5f}</td>
                    <td class="metric">{s3_transferred_gb:.5f}</td>
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
                    <td class="metric">{data_gb:.5f} GB</td>
                    <td class="metric">{distcp_speed:.5f} MB/s</td>
                    <td class="metric">{rows_per_sec:,.0f}</td>
                    <td class="metric">{total_dur:.1f}s ({total_dur/60:.1f}m)</td>
                </tr>
"""
    
    html += """
            </tbody>
        </table>
        
        <div style="margin-top: 50px; padding-top: 20px; border-top: 1px solid #ecf0f1; color: #95a5a6; font-size: 12px;">
            <p>This report was automatically generated by the MapR to S3 Migration DAG.</p>
        </div>
    </div>
</body>
</html>
"""
    
    # Write HTML to S3
    report_filename = f"{run_id}_report.html"
    report_path = f"{report_location}/{report_filename}"
    
    # Use Spark to write HTML
    from pyspark.sql import Row
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jvm.java.net.URI(report_path),
        hadoop_conf
    )
    
    output_path = spark._jvm.org.apache.hadoop.fs.Path(report_path)
    output_stream = fs.create(output_path, True)  
    output_stream.write(html.encode('utf-8'))
    output_stream.close()
    
    return {
        'report_path': report_path,
        'html_content': html
    }


@task.pyspark(conn_id='spark_default')
def finalize_run(run_id: str, spark) -> dict:
    """Finalize migration run - update stats in Iceberg tracking."""
    config = get_config()
    tracking_db = config['tracking_database']
    
    stats_result = spark.sql(f"""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN overall_status = 'VALIDATED' THEN 1 ELSE 0 END) as successful,
            SUM(CASE WHEN overall_status IN ('FAILED', 'VALIDATION_FAILED') THEN 1 ELSE 0 END) as failed

        FROM {tracking_db}.migration_table_status
        WHERE run_id = '{run_id}'
    """).collect()
    if not stats_result:
        logger.error(f"No stats found for run_id: {run_id}")
        stats = {'total': 0, 'successful': 0, 'failed': 0}
    else:
        stats = stats_result[0]
    
    spark.sql(f"""
        UPDATE {tracking_db}.migration_runs
        SET status = 'COMPLETED',
            completed_at = current_timestamp(),
            total_tables = {stats['total']},
            successful_tables = {stats['successful']},
            failed_tables = {stats['failed']}
        WHERE run_id = '{run_id}'
    """)
    
    return {
        'run_id': run_id,
        'status': 'COMPLETED',
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
        except:
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

    html_content = report_result.get('html_content', '')
    report_path = report_result.get('report_path', '')

    try:
        import tempfile
        import os
        from airflow.utils.email import send_email

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
# DAG 2: ICEBERG MIGRATION TASKS
# =============================================================================
@task.pyspark(conn_id='spark_default')
def init_iceberg_tracking_tables(spark) -> dict:
    """Create Iceberg tracking tables for Iceberg migration if they don't exist."""
    config = get_config()
    tracking_db = config['tracking_database']
    tracking_loc = config['tracking_location']
    spark.sql(f"""
        CREATE DATABASE IF NOT EXISTS {tracking_db} LOCATION '{tracking_loc}'
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tracking_db}.iceberg_migration_runs (
            run_id STRING,
            dag_run_id STRING,
            excel_file_path STRING,
            migration_type STRING,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            status STRING,
            total_tables INT,
            successful_tables INT,
            failed_tables INT,
            config_json STRING
        )
        USING iceberg
        LOCATION '{tracking_loc}/iceberg_migration_runs'
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tracking_db}.iceberg_migration_table_status (
            run_id STRING,
            dag_run_id STRING,
            source_database STRING,
            source_table STRING,
            migration_type STRING,
            destination_database STRING,
            destination_table STRING,
            table_location STRING,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            migration_duration_seconds DOUBLE,
            status STRING,
            source_hive_row_count BIGINT,
            destination_iceberg_row_count BIGINT,
            row_count_match BOOLEAN,
            source_hive_partition_count INT,
            dest_iceberg_partition_count INT,
            partition_count_match BOOLEAN,
            schema_match BOOLEAN,
            schema_differences STRING,
            validation_status STRING,
            validation_completed_at TIMESTAMP,
            validation_duration_seconds DOUBLE,
            error_message STRING,
            updated_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (source_database)
        LOCATION '{tracking_loc}/iceberg_migration_table_status'
    """)
    return {'status': 'initialized', 'database': tracking_db}

@task.pyspark(conn_id='spark_default')
def create_iceberg_migration_run(excel_file_path: str, dag_run_id: str, spark) -> str:
    """Create migration run record."""
    from datetime import datetime
    import uuid

    config = get_config()
    tracking_db = config['tracking_database']
    
    run_id = f"iceberg_run_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    
    spark.sql(f"""
        INSERT INTO {tracking_db}.iceberg_migration_runs
        VALUES (
            '{run_id}',
            '{dag_run_id}',
            '{excel_file_path}',
            NULL,
            current_timestamp(),
            NULL,
            'RUNNING',
            0, 0, 0,
            '{json.dumps(config).replace("'", "''")}'
        )
    """)
    
    return run_id


@task.pyspark(conn_id='spark_default')
def parse_iceberg_excel(excel_file_path: str, run_id: str, spark) -> list:
    """Read Excel config for Iceberg migration from S3."""
    import pandas as ps
    from io import BytesIO

    binary_df = spark.read.format("binaryFile").load(excel_file_path)
    row = binary_df.select("content").first()
    excel_bytes = bytes(row.content)
    df = ps.read_excel(BytesIO(excel_bytes), engine='openpyxl')
    
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    
    configs = []
    for _, row in df.iterrows():
        src_db = str(row.get('database', '')).strip() if row.get('database') is not None else ''
        if not src_db:
            continue
        
        tbl_pattern = str(row.get('table', '*')).strip() if row.get('table') is not None else '*'
        tbl_pattern = tbl_pattern or '*'
        
        inplace_val = row.get('inplace_migration', 'F')
        if inplace_val is None or (hasattr(inplace_val, '__len__') and len(str(inplace_val).strip()) == 0):
            inplace_migration = False
        else:
            inplace_migration = str(inplace_val).strip().upper() in ('T', 'TRUE', 'YES', '1')
        
        dest_ice_db = str(row.get('destination_iceberg_database', '')).strip() if row.get('destination_iceberg_database') is not None else ''
        if not dest_ice_db or dest_ice_db.lower() == 'nan':
            dest_ice_db = src_db if inplace_migration else f"{src_db}_iceberg"
        
        configs.append({
            'source_database': src_db,
            'table_pattern': tbl_pattern,
            'inplace_migration': inplace_migration,
            'destination_iceberg_database': dest_ice_db,
            'run_id': run_id, 
        })
    
    return configs


@task.pyspark(conn_id='spark_default')
@track_duration
def discover_hive_tables(db_config: dict, spark) -> dict:
    """Discover Hive tables matching the pattern in the source database."""
    src_db = db_config['source_database']
    tbl_pattern = db_config['table_pattern']
    run_id = db_config['run_id']
    
    all_tables = [row.tableName for row in spark.sql(f"SHOW TABLES IN {src_db}").collect()]
    
    if tbl_pattern == '*':
        matched_tables = all_tables
    else:
        import fnmatch
        matched_tables = [t for t in all_tables if fnmatch.fnmatch(t, tbl_pattern)]

    logger.info(f"[IcebergDiscover] Database '{src_db}': {len(all_tables)} total tables, {len(matched_tables)} matched pattern '{tbl_pattern}'")
    
    tables_metadata = []
    for tbl in matched_tables:
        logger.info(f"[IcebergDiscover] Getting location for {src_db}.{tbl}")
        try:
            desc_df = spark.sql(f"DESCRIBE FORMATTED {src_db}.{tbl}")
            location = None
            for row in desc_df.collect():
                if row.col_name and row.col_name.strip() == "Location":
                    location = row.data_type.strip() if row.data_type else None
                    break
            
            tables_metadata.append({
                'table': tbl,
                'location': location
            })
        except Exception as e:
            logger.error(f"[IcebergDiscover] Failed to get location for {src_db}.{tbl}: {str(e)[:300]}")
            tables_metadata.append({
                'table': tbl,
                'location': None,
                'discovery_error': str(e)
            })

    logger.info(f"[IcebergDiscover] Completed discovery for '{src_db}': {len(tables_metadata)} table(s) ready for migration")
    
    return {
        **db_config,
        'discovered_tables': tables_metadata
    }


@task.pyspark(conn_id='spark_default')
@track_duration
def migrate_tables_to_iceberg(discovery: dict, dag_run_id: str, spark, **context) -> dict:
    """Migrate discovered Hive tables to Iceberg format."""
    config = get_config()
    tracking_db = config['tracking_database']
    
    src_db = discovery['source_database']
    dest_db = discovery['destination_iceberg_database']
    inplace = discovery['inplace_migration']
    run_id = discovery['run_id']
    
    if not inplace:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {dest_db}")
    
    results = []
    
    for tbl_meta in discovery.get('discovered_tables', []):
        tbl = tbl_meta['table']
        location = tbl_meta.get('location')

        logger.info(f"[IcebergMigrate] Starting migration for {src_db}.{tbl} | strategy={'INPLACE' if inplace else 'SNAPSHOT'} | dest={dest_db}.{tbl}")
        from datetime import datetime as _dt
        tbl_migrate_start = _dt.utcnow()
        
        try:
            hive_count = spark.sql(f"SELECT COUNT(*) as c FROM {src_db}.{tbl}").collect()[0]['c']
            src_hive_partition_count = 0
            try:
                src_partitions_df = spark.sql(f"SHOW PARTITIONS {src_db}.{tbl}")
                all_partitions = src_partitions_df.collect()

                if not inplace and all_partitions:
                    non_empty_count = 0
                    table_location = tbl_meta.get('location') or ''
                    for part_row in all_partitions:
                        part_spec = part_row[0]   # e.g. "dt=2024-01-01/country=US"
                        part_path = f"{table_location}/{part_spec.replace('=', '=').rstrip('/')}"
                        try:
                            from py4j.java_gateway import java_import
                            java_import(spark._jvm, "org.apache.hadoop.fs.*")
                            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                                spark._jvm.java.net.URI(part_path),
                                spark._jsc.hadoopConfiguration()
                            )
                            path_obj = spark._jvm.org.apache.hadoop.fs.Path(part_path)
                            if fs.exists(path_obj):
                                summary = fs.getContentSummary(path_obj)
                                if int(summary.getLength()) > 0:
                                    non_empty_count += 1
                        except Exception:
                            # If we can't check, count it conservatively
                            non_empty_count += 1
                    src_hive_partition_count = non_empty_count
                    logger.info(
                        f"[IcebergMigrate] {src_db}.{tbl} | "
                        f"total_hive_partitions={len(all_partitions)} | "
                        f"non_empty_partitions={non_empty_count} "
                        f"(snapshot skips 0-byte partitions)"
                    )
                else:
                    src_hive_partition_count = len(all_partitions)
            except:
                pass 

            if inplace:
                migration_type = "INPLACE"
                dest_table = f"{src_db}.{tbl}"
                spark.sql(f"CALL spark_catalog.system.migrate('{src_db}.{tbl}')")
            else:
                migration_type = "SNAPSHOT"
                dest_table = f"{dest_db}.{tbl}"
                try:
                    spark.sql(f"DESCRIBE {dest_table}")
                    logger.info(f"[IcebergMigrate] Destination {dest_table} already exists (prior attempt). Dropping before re-snapshot.")
                    spark.sql(f"DROP TABLE IF EXISTS {dest_table}")
                except Exception:
                    pass  
                spark.sql(f"CALL spark_catalog.system.snapshot('{src_db}.{tbl}', '{dest_db}.{tbl}')")
            
            iceberg_count = spark.sql(f"SELECT COUNT(*) as c FROM {dest_table}").collect()[0]['c']
            dest_iceberg_partition_count = 0
            try:
                spark.catalog.refreshTable(dest_table) 
                dest_iceberg_partition_count = spark.sql(f"""SELECT COUNT(*) as cnt FROM {dest_table}.partitions""").collect()[0]['cnt']
            except:
                pass

            counts_match = (hive_count == iceberg_count)
            partition_match = (src_hive_partition_count == dest_iceberg_partition_count)

            logger.info(f"[IcebergMigrate] COMPLETED: {src_db}.{tbl} | hive_rows={hive_count} | iceberg_rows={iceberg_count} | rows_match={counts_match} | partitions_match={partition_match}")
            
            desc_df = spark.sql(f"DESCRIBE FORMATTED {dest_table}")
            new_location = None
            for row in desc_df.collect():
                if row.col_name and row.col_name.strip() == "Location":
                    new_location = row.data_type.strip() if row.data_type else None
                    break
            
            results.append({
                'source_table': f"{src_db}.{tbl}",
                'destination_table': dest_table,
                'migration_type': migration_type,
                'status': 'COMPLETED',
                'hive_count': hive_count,
                'iceberg_count': iceberg_count,
                'counts_match': counts_match,
                'hive_partition_count': src_hive_partition_count,
                'iceberg_partition_count': dest_iceberg_partition_count,
                'partition_match': partition_match,
                'error': None
            })

            spark.sql(f"""
                DELETE FROM {tracking_db}.iceberg_migration_table_status
                WHERE run_id = '{run_id}'
                  AND source_database = '{src_db}'
                  AND source_table = '{tbl}'
            """)
            
            tbl_migrate_duration = (_dt.utcnow() - tbl_migrate_start).total_seconds()
            spark.sql(f"""
                INSERT INTO {tracking_db}.iceberg_migration_table_status
                VALUES (
                    '{run_id}',
                    '{dag_run_id}',
                    '{src_db}',
                    '{tbl}',
                    '{migration_type}',
                    '{dest_db}',
                    '{tbl}',
                    '{new_location or location or ""}',
                    current_timestamp(),
                    current_timestamp(),
                    {tbl_migrate_duration},
                    'COMPLETED',
                    {hive_count},
                    {iceberg_count},
                    {str(counts_match).lower()},
                    {src_hive_partition_count},
                    {dest_iceberg_partition_count},
                    {str(partition_match).lower()},
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    current_timestamp()
                )
            """)
            
        except Exception as e:
            error_msg = f"Migration to Iceberg failed for {dest_db}.{tbl}: {str(e)[:2000]}".replace("'", "''")
            tbl_fail_duration = (_dt.utcnow() - tbl_migrate_start).total_seconds() 
            
            results.append({
                'source_table': f"{src_db}.{tbl}",
                'destination_table': f"{dest_db}.{tbl}" if not inplace else f"{src_db}.{tbl}",
                'migration_type': "INPLACE" if inplace else "SNAPSHOT",
                'status': 'FAILED',
                'error': str(e)
            })

            spark.sql(f"""
                DELETE FROM {tracking_db}.iceberg_migration_table_status
                WHERE run_id = '{run_id}'
                  AND source_database = '{src_db}'
                  AND source_table = '{tbl}'
            """)
            
            spark.sql(f"""
                INSERT INTO {tracking_db}.iceberg_migration_table_status
                VALUES (
                    '{run_id}',
                    '{dag_run_id}',
                    '{src_db}',
                    '{tbl}',
                    '{"INPLACE" if inplace else "SNAPSHOT"}',
                    '{dest_db}',
                    '{tbl}',
                    '{location or ""}',
                    current_timestamp(),
                    current_timestamp(),
                    {tbl_fail_duration},
                    'FAILED',
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    '{error_msg}',
                    current_timestamp()
                )
            """)
            logger.error(f"ERROR: {error_msg}")

    failed_migrations = [r for r in results if r['status'] == 'FAILED']
    has_failures = len(failed_migrations) > 0

    result_dict = {
        'run_id': run_id,
        'source_database': src_db,
        'destination_database': dest_db,
        'migration_type': 'INPLACE' if inplace else 'SNAPSHOT',
        'results': results,
        '_has_failures': has_failures,
        '_failure_summary': (
            f"Iceberg migration failed for {len(failed_migrations)}/{len(results)} table(s): "
            if has_failures else None
        )
    }

    context['ti'].xcom_push(key='return_value', value=result_dict)

    if has_failures:
        raise Exception(f"Iceberg migration failed — {result_dict['_failure_summary']}. Per-table errors in tracking.")

    return result_dict


@task.pyspark(conn_id='spark_default')
def update_migration_durations(migration_result: dict, spark) -> dict:
    """Update tracking table with migration durations from XCom."""

    if not isinstance(migration_result, dict) or 'run_id' not in migration_result:
        logger.warning(f"[update_migration_durations] Skipping invalid input: {type(migration_result)}")
        return {}
    
    config = get_config()
    tracking_db = config['tracking_database']
    
    run_id = migration_result['run_id']
    src_db = migration_result['source_database']
    
    # Extract duration from XCom result
    migration_duration = migration_result.get('_task_duration', 0.0)
    
    # Update all records for this run
    execute_with_iceberg_retry(spark, f"""
        UPDATE {tracking_db}.iceberg_migration_table_status
        SET migration_duration_seconds = {migration_duration},
            updated_at = current_timestamp()
        WHERE run_id = '{run_id}'
          AND migration_duration_seconds = 0.0
    """,
    task_label="update_migration_durations:duration_bulk")

    for r in migration_result.get('results', []):
        if r.get('status') == 'FAILED' and r.get('error'):
            per_table_error = str(r['error'])[:2000].replace("'", "''")
            tbl_name = r['source_table'].split('.')[-1]
            src_db_name = r['source_table'].split('.')[0]
            execute_with_iceberg_retry(spark, f"""
                UPDATE {tracking_db}.iceberg_migration_table_status
                SET status = 'FAILED',
                    error_message = '{per_table_error}',
                    updated_at = current_timestamp()
                WHERE run_id = '{run_id}'
                  AND source_database = '{src_db_name}'
                  AND source_table = '{tbl_name}'
                  AND status IS NULL
            """,
            task_label=f"update_migration_durations:failure_patch:{tbl_name}")

    execute_with_iceberg_retry(spark, f"""
        UPDATE {tracking_db}.iceberg_migration_table_status
        SET status = 'FAILED',
            error_message = COALESCE(error_message, 'Iceberg migration task did not process this table'),
            updated_at = current_timestamp()
        WHERE run_id = '{run_id}'
          AND source_database = '{src_db}'
          AND status IS NULL
    """,
    task_label="update_migration_durations:catchall")
    
    return migration_result


@task.pyspark(conn_id='spark_default')
@track_duration
def validate_iceberg_tables(migration_result: dict, spark, **context) -> dict:
    """Validate Iceberg tables: row counts, partition counts, schema comparison between source Hive and destination Iceberg."""

    if not isinstance(migration_result, dict) or 'run_id' not in migration_result:
        logger.warning(f"[validate_iceberg_tables] Skipping invalid input: {type(migration_result)}")
        return {}
    
    config = get_config()
    tracking_db = config['tracking_database']
    
    run_id = migration_result['run_id']
    src_db = migration_result['source_database']
    dest_db = migration_result['destination_database']
    
    validation_results = []
    
    for r in migration_result.get('results', []):
        if r['status'] != 'COMPLETED':
            continue
        
        # Extract table name from fully qualified name
        src_tbl_full = r['source_table']
        tbl = src_tbl_full.split('.')[-1]
        dest_tbl = r['destination_table']

        logger.info(f"[IcebergValidation] Validating {src_db}.{tbl} vs {dest_tbl}")
        from datetime import datetime as _dt
        tbl_val_start = _dt.utcnow()
        
        try:
            # Schema comparison between source Hive and destination Iceberg
            src_hive_schema_df = spark.sql(f"DESCRIBE {src_db}.{tbl}")
            src_hive_schema = [
                {'name': row.col_name, 'type': row.data_type}
                for row in src_hive_schema_df.collect()
                if row.col_name and not row.col_name.startswith('#')
            ]
            
            dest_iceberg_schema_df = spark.sql(f"DESCRIBE {dest_tbl}")
            dest_iceberg_schema = [
                {'name': row.col_name, 'type': row.data_type}
                for row in dest_iceberg_schema_df.collect()
                if row.col_name and not row.col_name.startswith('#')
            ]
            
            # Compare schemas
            schema_match = True
            schema_diffs = []
            
            src_cols = {c['name']: c['type'] for c in src_hive_schema}
            dest_cols = {c['name']: c['type'] for c in dest_iceberg_schema}
            
            for col_name, col_type in src_cols.items():
                if col_name not in dest_cols:
                    schema_match = False
                    schema_diffs.append(f"Missing column in Iceberg: {col_name}")
                elif dest_cols[col_name] != col_type:
                    schema_match = False
                    schema_diffs.append(f"Type mismatch for {col_name}: Hive {col_type} vs Iceberg {dest_cols[col_name]}")
            
            for col_name in dest_cols:
                if col_name not in src_cols:
                    schema_match = False
                    schema_diffs.append(f"Extra column in Iceberg: {col_name}")

            row_ok = r.get('counts_match', False)
            part_ok = r.get('partition_match', False)
            logger.info(f"[IcebergValidation] DONE: {src_db}.{tbl} | rows={'✓' if row_ok else '✗'} partitions={'✓' if part_ok else '✗'} schema={'✓' if schema_match else '✗'}")
            if schema_diffs:
                logger.warning(f"[IcebergValidation] Schema diffs for {src_db}.{tbl}: {'; '.join(schema_diffs[:5])}")
            
            validation_results.append({
                'source_table': tbl,
                'destination_table': dest_tbl,
                'status': 'COMPLETED',
                'source_hive_row_count': r.get('hive_count', 0),
                'dest_iceberg_row_count': r.get('iceberg_count', 0),
                'row_count_match': r.get('counts_match', False),
                'source_hive_partition_count': r.get('hive_partition_count', 0),
                'dest_iceberg_partition_count': r.get('iceberg_partition_count', 0),
                'partition_count_match': r.get('partition_match', False),
                'schema_match': schema_match,
                'schema_differences': '; '.join(schema_diffs) if schema_diffs else '',
                'per_table_validation_duration': (_dt.utcnow() - tbl_val_start).total_seconds(),
                'error': None
            })
            
        except Exception as e:
            error_msg = f"Validation failed for {dest_db}.{tbl}: {str(e)[:2000]}"
            validation_results.append({
                'source_table': tbl,
                'destination_table': dest_tbl,
                'status': 'FAILED',
                'per_table_validation_duration': (_dt.utcnow() - tbl_val_start).total_seconds(),
                'error': str(e)[:2000]
            })
            logger.error(f"ERROR: {error_msg}")

    failed_validations = [v for v in validation_results if v['status'] == 'FAILED']
    mismatched = [
        v for v in validation_results
        if v.get('status') == 'COMPLETED' and (
            not v.get('row_count_match', True) or
            not v.get('partition_count_match', True)
        )
    ]
    total_failures = len(failed_validations) + len(mismatched)
    has_failures = total_failures > 0
    
    result_dict = {
        **migration_result,
        'validation_results': validation_results,
        '_has_failures': has_failures,
        '_failure_summary': (
            f"Iceberg validation failed for {total_failures}/{len(validation_results)} table(s)"
            if has_failures else None
        )
    }

    context['ti'].xcom_push(key='return_value', value=result_dict)

    if has_failures:
        raise Exception(f"Iceberg validation failed — {result_dict['_failure_summary']}. Per-table errors in tracking.")

    return result_dict


@task.pyspark(conn_id='spark_default')
def update_iceberg_validation_status(validation_result: dict, spark) -> dict:
    """Update Iceberg tracking with validation results."""

    if not isinstance(validation_result, dict) or 'run_id' not in validation_result:
        logger.warning(f"[update_iceberg_validation_status] Skipping invalid input: {type(validation_result)}")
        return {}
    
    config = get_config()
    tracking_db = config['tracking_database']
    
    run_id = validation_result['run_id']
    src_db = validation_result['source_database']
    
    # Extract duration from XCom result
    task_level_validation_duration = validation_result.get('_task_duration', 0.0)
    
    for v in validation_result.get('validation_results', []):
        if v['status'] != 'COMPLETED':
            continue
        
        error_msg = (v.get('error', '') or '').replace("'", "''")[:2000]
        schema_diffs = (v.get('schema_differences', '') or '').replace("'", "''")[:2000]
        
        overall_status = 'VALIDATED' if (
            v.get('row_count_match', False) and 
            v.get('partition_count_match', True) and 
            v.get('schema_match', False)
        ) else 'VALIDATION_FAILED'

        is_validated = (
            v.get('row_count_match', False) and
            v.get('partition_count_match', True) and
            v.get('schema_match', False)
        )
        
        if not is_validated and v.get('error'):
            mismatch_msg = str(v['error']).replace("'", "''")[:2000]
            error_message_sql = f"'{mismatch_msg}'"
        elif is_validated:
            error_message_sql = "NULL"
        else:
            error_message_sql = "error_message" 
        
        execute_with_iceberg_retry(spark, f"""
            UPDATE {tracking_db}.iceberg_migration_table_status
            SET validation_status = '{v['status']}',
                validation_completed_at = current_timestamp(),
                validation_duration_seconds = {v.get('per_table_validation_duration', task_level_validation_duration)},
                source_hive_row_count = {v.get('source_hive_row_count', 0)},
                destination_iceberg_row_count = {v.get('dest_iceberg_row_count', 0)},
                row_count_match = {str(v.get('row_count_match', False)).lower()},
                source_hive_partition_count = {v.get('source_hive_partition_count', 0)},
                dest_iceberg_partition_count = {v.get('dest_iceberg_partition_count', 0)},
                partition_count_match = {str(v.get('partition_count_match', False)).lower()},
                schema_match = {str(v.get('schema_match', False)).lower()},
                schema_differences = '{schema_diffs}',
                status = CASE
                    WHEN status = 'FAILED' THEN status  -- preserve original migration failure
                    ELSE '{overall_status}'
                END,
                error_message = CASE
                    WHEN status = 'FAILED' THEN error_message  -- preserve original error
                    ELSE {error_message_sql}
                END,
                updated_at = current_timestamp()
            WHERE run_id = '{run_id}'
              AND source_database = '{src_db}'
              AND source_table = '{v['source_table']}'
        """,
        task_label=f"update_iceberg_validation_status:{v['source_table']}")

    for v in validation_result.get('validation_results', []):
        if v.get('status') == 'FAILED' and v.get('error'):
            per_table_error = str(v['error'])[:2000].replace("'", "''")
            execute_with_iceberg_retry(spark, f"""
                UPDATE {tracking_db}.iceberg_migration_table_status
                SET validation_status = 'FAILED',
                    status = 'VALIDATION_FAILED',
                    error_message = '{per_table_error}',
                    updated_at = current_timestamp()
                WHERE run_id = '{run_id}'
                  AND source_database = '{src_db}'
                  AND source_table = '{v['source_table']}'
                  AND validation_status IS NULL
            """,
            task_label=f"update_iceberg_validation_status:failure_patch:{v['source_table']}")

    execute_with_iceberg_retry(spark, f"""
        UPDATE {tracking_db}.iceberg_migration_table_status
        SET validation_status = 'SKIPPED',
            status = CASE WHEN status = 'FAILED' THEN 'FAILED' ELSE 'VALIDATION_FAILED' END,
            error_message = COALESCE(error_message, 'Iceberg validation task did not process this table'),
            updated_at = current_timestamp()
        WHERE run_id = '{run_id}'
          AND source_database = '{src_db}'
          AND status = 'COMPLETED'
          AND validation_status IS NULL
    """,
    task_label="update_iceberg_validation_status:catchall")
    
    return validation_result


@task.pyspark(conn_id='spark_default')
def generate_iceberg_html_report(run_id: str, spark) -> str:
    """Generate comprehensive HTML Iceberg migration report."""
    from datetime import datetime
    
    config = get_config()
    tracking_db = config['tracking_database']
    report_location = config['report_output_location']
    
    # Get migration status
    migration_status = spark.sql(f"""
        SELECT * FROM {tracking_db}.iceberg_migration_table_status
        WHERE run_id = '{run_id}'
        ORDER BY source_database, source_table
    """).collect()
    
    # Calculate summary stats
    total_tables = len(migration_status)
    successful_tables = sum(1 for t in migration_status if t.status in ['VALIDATED', 'COMPLETED'])
    failed_tables = sum(1 for t in migration_status if 'FAILED' in (t.status or ''))
    total_rows = sum(t.source_hive_row_count or 0 for t in migration_status)
    count_mismatches = sum(1 for t in migration_status if not t.row_count_match and t.row_count_match is not None)

    # Validation summary query 
    iceberg_validation_summary = spark.sql(f"""
        SELECT
            COUNT(*) as total_tables_validated,
            SUM(CASE WHEN row_count_match = true AND partition_count_match = true AND schema_match = true THEN 1 ELSE 0 END) as tables_passed_validation,
            SUM(CASE WHEN row_count_match = false OR partition_count_match = false OR schema_match = false THEN 1 ELSE 0 END) as tables_failed_validation,
            SUM(CASE WHEN row_count_match = false THEN 1 ELSE 0 END) as total_row_count_mismatches,
            SUM(CASE WHEN partition_count_match = false THEN 1 ELSE 0 END) as total_partition_count_mismatches,
            SUM(CASE WHEN schema_match = false THEN 1 ELSE 0 END) as total_schema_mismatches
        FROM {tracking_db}.iceberg_migration_table_status
        WHERE run_id = '{run_id}'
          AND validation_status = 'COMPLETED'
    """).collect()
    
    # Generate HTML
    html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Iceberg Migration Report - {run_id}</title>
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
        .status-validated {{
            background-color: #c3e6cb;
            color: #155724;
        }}
        .status-failed {{
            background-color: #f8d7da;
            color: #721c24;
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
        <h1>Iceberg Migration Report</h1>
        
        <div class="timestamp">
            Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC<br>
            Run ID: <strong>{run_id}</strong>
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
                <h3>TOTAL ROWS</h3>
                <p class="value">{total_rows:,}</p>
            </div>
            <div class="summary-card warning">
                <h3>COUNT MISMATCHES</h3>
                <p class="value">{count_mismatches}</p>
            </div>
        </div>
        
        <div class="section-divider"></div>

        <h2>Validation Summary</h2>

"""

    if iceberg_validation_summary and iceberg_validation_summary[0]['total_tables_validated']:
        ivs = iceberg_validation_summary[0]
        html += f"""
        <div class="summary-grid">
            <div class="summary-card info">
                <h3>TABLES VALIDATED</h3>
                <p class="value">{ivs.total_tables_validated}</p>
            </div>
            <div class="summary-card success">
                <h3>PASSED VALIDATION</h3>
                <p class="value">{ivs.tables_passed_validation}</p>
            </div>
            <div class="summary-card warning">
                <h3>FAILED VALIDATION</h3>
                <p class="value">{ivs.tables_failed_validation}</p>
            </div>
            <div class="summary-card warning">
                <h3>ROW COUNT MISMATCHES</h3>
                <p class="value">{ivs.total_row_count_mismatches}</p>
            </div>
            <div class="summary-card warning">
                <h3>PARTITION MISMATCHES</h3>
                <p class="value">{ivs.total_partition_count_mismatches}</p>
            </div>
            <div class="summary-card warning">
                <h3>SCHEMA MISMATCHES</h3>
                <p class="value">{ivs.total_schema_mismatches}</p>
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
                    <th>Source Database</th>
                    <th>Table</th>
                    <th>Migration Type</th>
                    <th>Destination</th>
                    <th>Status</th>
                    <th>Migration Duration</th>
                    <th>Validation Duration</th>
                </tr>
            </thead>
            <tbody>
"""
    
    for t in migration_status:
        if t.status == 'VALIDATED':
            status_class = 'status-validated'
        elif t.status == 'COMPLETED':
            status_class = 'status-completed'
        else:
            status_class = 'status-failed'
        
        migration_dur = f"{t.migration_duration_seconds:.1f}s" if t.migration_duration_seconds else "N/A"
        validation_dur = f"{t.validation_duration_seconds:.1f}s" if t.validation_duration_seconds else "N/A"
        
        html += f"""
                <tr>
                    <td>{t.source_database}</td>
                    <td><strong>{t.source_table}</strong></td>
                    <td>{t.migration_type}</td>
                    <td>{t.destination_table}</td>
                    <td><span class="status-badge {status_class}">{t.status}</span></td>
                    <td class="duration">{migration_dur}</td>
                    <td class="duration">{validation_dur}</td>
                </tr>
"""
    
    html += f"""
            </tbody>
        </table>
        
        <div class="section-divider"></div>
        
        <h2>Validation Results (Hive vs Iceberg)</h2>
        <table>
            <thead>
                <tr>
                    <th>Database</th>
                    <th>Table</th>
                    <th>Source Hive Rows</th>
                    <th>Dest Iceberg Rows</th>
                    <th>Row Count Match</th>
                    <th>Source Partitions</th>
                    <th>Dest Partitions</th>
                    <th>Partition Match</th>
                    <th>Schema Match</th>
                </tr>
            </thead>
            <tbody>
"""
    
    for t in migration_status:
        if t.validation_status == 'COMPLETED':
            row_match_class = 'validation-pass' if t.row_count_match else 'validation-fail'
            row_match_icon = '✓ PASS' if t.row_count_match else '✗ FAIL'
            part_match_class = 'validation-pass' if t.partition_count_match else 'validation-fail'
            part_match_icon = '✓ PASS' if t.partition_count_match else '✗ FAIL'
            schema_match_class = 'validation-pass' if t.schema_match else 'validation-fail'
            schema_match_icon = '✓ PASS' if t.schema_match else '✗ FAIL'
        else:
            row_match_class = part_match_class = schema_match_class = 'duration'
            row_match_icon = part_match_icon = schema_match_icon = 'N/A'
        
        html += f"""
                <tr>
                    <td>{t.source_database}</td>
                    <td><strong>{t.source_table}</strong></td>
                    <td class="metric">{(t.source_hive_row_count or 0):,}</td>
                    <td class="metric">{(t.destination_iceberg_row_count or 0):,}</td>
                    <td class="{row_match_class}">{row_match_icon}</td>
                    <td class="metric">{t.source_hive_partition_count or 0}</td>
                    <td class="metric">{t.dest_iceberg_partition_count or 0}</td>
                    <td class="{part_match_class}">{part_match_icon}</td>
                    <td class="{schema_match_class}">{schema_match_icon}</td>
                </tr>
"""
    
    html += f"""
            </tbody>
        </table>
        
        <div class="section-divider"></div>
        
        <h2>Performance Metrics</h2>
        <table>
            <thead>
                <tr>
                    <th>Database</th>
                    <th>Table</th>
                    <th>Migration Duration</th>
                    <th>Validation Duration</th>
                    <th>Total Duration</th>
                    <th>Rows Migrated</th>
                    <th>Rows/Second</th>
                </tr>
            </thead>
            <tbody>
"""
    
    for t in migration_status:
        migration_dur = t.migration_duration_seconds or 0
        validation_dur = t.validation_duration_seconds or 0
        total_dur = migration_dur + validation_dur
        
        rows_per_sec = (t.source_hive_row_count or 0) / (total_dur or 1)
        
        html += f"""
                <tr>
                    <td>{t.source_database}</td>
                    <td><strong>{t.source_table}</strong></td>
                    <td class="metric">{migration_dur:.1f}s</td>
                    <td class="metric">{validation_dur:.1f}s</td>
                    <td class="metric">{total_dur:.1f}s ({total_dur/60:.1f}m)</td>
                    <td class="metric">{(t.source_hive_row_count or 0):,}</td>
                    <td class="metric">{rows_per_sec:,.0f}</td>
                </tr>
"""
    
    html += """
            </tbody>
        </table>
        
        <div style="margin-top: 50px; padding-top: 20px; border-top: 1px solid #ecf0f1; color: #95a5a6; font-size: 12px;">
            <p>This report was automatically generated by the Iceberg Migration DAG.</p>
        </div>
    </div>
</body>
</html>
"""
    
    # Write HTML to S3
    report_filename = f"{run_id}_iceberg_report.html"
    report_path = f"{report_location}/{report_filename}"
    
    # Use Spark to write HTML
    from pyspark.sql import Row
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jvm.java.net.URI(report_path),
        hadoop_conf
    )
    
    output_path = spark._jvm.org.apache.hadoop.fs.Path(report_path)
    output_stream = fs.create(output_path, True)  
    output_stream.write(html.encode('utf-8'))
    output_stream.close()
    
    return {
        'report_path': report_path,
        'html_content': html
    }


@task.pyspark(conn_id='spark_default')
def finalize_iceberg_run(run_id: str, spark) -> dict:
    """Finalize Iceberg migration run - aggregate statistics."""
    config = get_config()
    tracking_db = config['tracking_database']
    
    stats_result = spark.sql(f"""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN status IN ('VALIDATED', 'COMPLETED') THEN 1 ELSE 0 END) as successful,
            SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed,
            SUM(CASE WHEN status = 'SKIPPED' THEN 1 ELSE 0 END) as skipped,
            SUM(CASE WHEN row_count_match = false THEN 1 ELSE 0 END) as count_mismatches
        FROM {tracking_db}.iceberg_migration_table_status
        WHERE run_id = '{run_id}'
    """).collect()
    if not stats_result:
        logger.error(f"No stats found for run_id: {run_id}")
        stats = {'total': 0, 'successful': 0, 'failed': 0, 'skipped': 0, 'count_mismatches': 0}
    else:
        stats = stats_result[0]

    migration_type_result = spark.sql(f"""
        SELECT migration_type, COUNT(*) as cnt
        FROM {tracking_db}.iceberg_migration_table_status
        WHERE run_id = '{run_id}'
        GROUP BY migration_type
        ORDER BY cnt DESC
        LIMIT 1
    """).collect()
    
    overall_migration_type = migration_type_result[0]['migration_type'] if migration_type_result else 'UNKNOWN'

    spark.sql(f"""
        UPDATE {tracking_db}.iceberg_migration_runs
        SET status = 'COMPLETED',
            completed_at = current_timestamp(),
            migration_type = '{overall_migration_type}',
            total_tables = {stats['total']},
            successful_tables = {stats['successful']},
            failed_tables = {stats['failed']}
        WHERE run_id = '{run_id}'
    """)
    
    return {
        'run_id': run_id,
        'status': 'COMPLETED',
        'total': stats['total'],
        'successful': stats['successful'],
        'failed': stats['failed'],
        'skipped': stats['skipped'],
        'count_mismatches': stats['count_mismatches']
    }



@task.pyspark(conn_id='spark_default')
def send_iceberg_report_email(report_result: dict, run_id: str, spark) -> dict:
    """Send HTML Iceberg migration report via email using SMTP."""
    import tempfile, os
    from airflow.utils.email import send_email

    config = get_config()
    smtp_conn_id = config.get('smtp_conn_id', 'smtp_default')
    recipients_str = config.get('email_recipients', '')

    if not recipients_str:
        logger.warning("[Email] No recipients configured. Skipping email.")
        return {'sent': False, 'reason': 'no_recipients'}

    recipients = [r.strip() for r in recipients_str.split(',') if r.strip()]

    html_content = report_result.get('html_content', '')
    report_path = report_result.get('report_path', '')

    try:
        tmp = tempfile.NamedTemporaryFile(
            mode='w', suffix='.html',
            prefix=f'{run_id}_iceberg_report_',
            delete=False
        )
        tmp.write(html_content)
        tmp.close()
    
        send_email(
            to=recipients,
            subject=f"Iceberg Migration Report - {run_id}",
            html_content=f"<p>Please find the Iceberg migration report for run <strong>{run_id}</strong> attached.</p>",
            files=[tmp.name],
            conn_id=smtp_conn_id,
        )
        os.unlink(tmp.name)
        logger.info(f"[Email] Iceberg report sent to: {recipients}")
        return {'sent': True, 'recipients': recipients, 'report_path': report_path}
    except Exception as e:
        logger.error(f"[Email] Failed to send Iceberg report: {str(e)}")
        raise Exception(f"Failed to send Iceberg report email: {str(e)}") from e

# =============================================================================
# DAG 1 DEFINITION: MAPR TO S3 MIGRATION
# =============================================================================

with DAG(
    dag_id='mapr_to_s3_migration',
    default_args=DEFAULT_ARGS,
    description='Migrate Hive tables from MapR-FS to S3',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=5,
    tags=['migration', 'mapr', 's3', 'hive'],
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

# =============================================================================
# DAG 2 DEFINITION: ICEBERG MIGRATION
# =============================================================================

with DAG(
    dag_id='iceberg_migration',
    default_args=DEFAULT_ARGS,
    description='Migrate existing Hive tables in S3 to Iceberg format',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=5,
    tags=['migration', 'iceberg', 'hive'],
    params={
        'excel_file_path': Param(
            default='s3a://config-bucket/iceberg_migration.xlsx',
            type='string',
            description='S3 path to Excel config file for Iceberg migration'
        )
    },
    render_template_as_native_obj=True,
) as dag_iceberg:

    # Initialize
    t_ice_init = init_iceberg_tracking_tables()
    t_ice_run_id = create_iceberg_migration_run(
        excel_file_path="{{ params.excel_file_path }}",
        dag_run_id="{{ run_id }}"
    )
    t_ice_excel = parse_iceberg_excel(
        excel_file_path="{{ params.excel_file_path }}",
        run_id=t_ice_run_id
    )
    
    # Per-database processing
    t_ice_discover = discover_hive_tables.expand(db_config=t_ice_excel)
    t_ice_migrate = migrate_tables_to_iceberg.partial(dag_run_id="{{ run_id }}").expand(discovery=t_ice_discover)
    t_ice_migrate.operator.trigger_rule = 'all_done'

    # Duration update
    t_ice_durations = update_migration_durations.expand(migration_result=t_ice_migrate)
    t_ice_durations.operator.trigger_rule = 'all_done'
    
    # Validation 
    t_ice_validate = validate_iceberg_tables.expand(migration_result=t_ice_durations)
    t_ice_validate.operator.max_active_tis_per_dagrun = 3
    t_ice_validate.operator.trigger_rule = 'all_done'
    t_ice_val_status = update_iceberg_validation_status.expand(validation_result=t_ice_validate)
    t_ice_val_status.operator.trigger_rule = 'all_done'
    
    # Report generation 
    t_ice_report = generate_iceberg_html_report(run_id=t_ice_run_id)
    t_ice_report.operator.trigger_rule = 'all_done'

    # Email report
    t_ice_email = send_iceberg_report_email(run_id=t_ice_run_id, report_result=t_ice_report)
    t_ice_email.operator.trigger_rule = 'all_done'
    
    # Finalize
    t_ice_final = finalize_iceberg_run(run_id=t_ice_run_id)
    t_ice_final.operator.trigger_rule = 'all_done'
    
    # Dependencies
    t_ice_init >> t_ice_run_id >> t_ice_excel >> t_ice_discover >> t_ice_migrate >> t_ice_durations
    t_ice_durations >> t_ice_validate >> t_ice_val_status >> t_ice_report >> t_ice_email >> t_ice_final
