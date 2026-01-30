"""
Combined Migration DAGs

This file contains two independent DAGs:
1. mapr_to_s3_migration: Migrates data and Hive tables (metadata) from MapR-FS to S3
2. mapr_to_s3_migration_retry: Retries failed tables from previous MapR-to-S3 migration
3. iceberg_migration: Converts existing Hive tables in S3 to Apache Iceberg format
4. iceberg_migration_retry: Retries failed Iceberg migrations from previous run

Both DAGs can be run independently. The iceberg_migration DAG is typically run after mapr_to_s3_migration is complete, but they are not automatically chained.
The retry DAGs enable automatic recovery from transient failures without re-processing successful migrations.

1. MapR to S3 Migration DAG

Orchestrates migration of Hive tables from MapR-FS to S3:
- Excel config from S3 (only DAG parameter)
- SSH operations for MapR token, beeline discovery, distcp (24h timeout)
- PySpark tasks for Hive table creation
- Incremental support (distcp -update, table repair)
- Comprehensive validation (row counts, partitions, schema)

Excel columns: database | table | dest database | bucket 

2. MapR to S3 Migration Retry

Key Features:
- Selective re-processing: Only retries failed/validation-failed tables
- Incremental data copy: DistCp -update flag copies only missing/changed files
- Idempotent operations: Safe to run multiple times
- Tracking continuity: Updates original run_id records

Parameters:
- parent_run_id (REQUIRED): Run ID from initial migration to retry

3. Iceberg Migration DAG

Converts existing Hive tables in S3 to Apache Iceberg format.
This DAG runs independently after the main MapR-to-S3 migration is complete.

Two migration strategies supported:
1. In-place migration: Convert existing Hive table to Iceberg (overwrites metadata)
2. Snapshot migration: Create separate Iceberg table alongside Hive table

Excel columns: database | table | inplace_migration | destination_iceberg_database

4. Iceberg Migration Retry

Retries failed Iceberg migrations from a previous run.

Key Features:
- Selective re-processing: Only retries failed migrations
- Migration re-execution: Re-runs migrate/snapshot procedures
- Comprehensive validation: Row counts, partitions, schema comparison
- Tracking continuity: Updates original run_id records

Parameters:
- parent_run_id (REQUIRED): Run ID from initial Iceberg migration to retry
"""

from datetime import datetime, timedelta
import json
from functools import wraps

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.models.param import Param
from airflow.providers.ssh.hooks.ssh import SSHHook

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

# =============================================================================
# SHARED CONFIGURATION
# =============================================================================

def get_config() -> dict:
    """Shared configuration for both DAGs"""
    return {
        # SSH Configuration (for MapR migration)
        'ssh_conn_id': Variable.get('mapr_ssh_conn_id', default_var='mapr_edge_ssh'),
        'edge_temp_path': Variable.get('mapr_edge_temp_path', default_var='/tmp/migration'),
        
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
        
        # MapR Authentication
        'mapr_user': Variable.get('mapr_user', default_var=''),
        'mapr_password': Variable.get('mapr_password', default_var=''),
        'mapr_cluster': Variable.get('mapr_cluster', default_var=''),
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

@task.pyspark(conn_id='spark_default')
def init_tracking_tables(spark, sc) -> dict:
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
            mapr_location STRING,
            file_format STRING,
            partition_count INT,
            is_partitioned BOOLEAN,
            schema_json STRING,
            partitions_json STRING,
            partition_columns STRING,
            table_type STRING,                              
            source_row_count BIGINT,                        
            mapr_total_size_bytes BIGINT,                   
            mapr_file_count BIGINT,                         
            s3_total_size_bytes BIGINT,                    
            s3_file_count BIGINT,                           
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
def create_migration_run(excel_file_path: str, dag_run_id: str, spark, sc) -> str:
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
def parse_excel(excel_file_path: str, run_id: str, spark, sc) -> list:
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
        if not bucket_val.startswith('s3'):
            bucket_val = f"s3a://{bucket_val}"
        
        configs.append({
            'source_database': src_db,
            'table_pattern': tbl_pattern,
            'dest_database': dest_db,
            'dest_bucket': bucket_val,
            'run_id': run_id,
        })
    
    return configs


@task
def mapr_token_setup(run_id: str) -> dict:
    """SSH to edge, generate MapR ticket using maprlogin, create temp dir."""
    config = get_config()
    ssh = SSHHook(ssh_conn_id=config['ssh_conn_id'])
    temp_dir = f"{config['edge_temp_path']}/{run_id}"
    
    mapr_user = config.get('mapr_user', '')
    mapr_password = config.get('mapr_password', '')
    mapr_cluster = config.get('mapr_cluster', '')
    
    cmd = f"""
set -e

echo "=== MapR Authentication ==="

if maprlogin print 2>/dev/null | grep -q "Valid"; then
    echo "Using existing valid MapR ticket"
else
    echo "Generating new MapR ticket..."
    
    if [ -n "{mapr_user}" ] && [ -n "{mapr_password}" ]; then
        echo "{mapr_password}" | maprlogin password -user {mapr_user}
    elif klist -s 2>/dev/null; then
        maprlogin kerberos
    else
        echo "Attempting maprlogin with current user..."
        maprlogin password || maprlogin kerberos || {{ echo "ERROR: MapR authentication failed"; exit 1; }}
    fi
fi

echo "=== Verifying MapR Ticket ==="
maprlogin print
if ! maprlogin print 2>/dev/null | grep -q "Valid"; then
    echo "ERROR: No valid MapR ticket"
    exit 1
fi

echo "=== Creating temp directory ==="
mkdir -p {temp_dir}
chmod 755 {temp_dir}

echo "MAPR_SETUP_SUCCESS"
echo "TEMP_DIR={temp_dir}"
"""
    with ssh.get_conn() as client:
        _, stdout, stderr = client.exec_command(cmd, timeout=300)
        exit_code = stdout.channel.recv_exit_status()
        output = stdout.read().decode()
        error = stderr.read().decode()
        
        if exit_code != 0 or "MAPR_SETUP_SUCCESS" not in output:
            raise Exception(f"MapR setup failed: {error}\n{output}")
    
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
    
    pyspark_script = f'''
import json
import sys
from pyspark.sql import SparkSession

spark = SparkSession.builder \\
    .appName("table_discovery_{run_id}") \\
    .enableHiveSupport() \\
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

src_db = "{src_db}"
pattern = "{pattern}"
dest_db = "{dest_db}"
dest_bucket = "{dest_bucket}"

if pattern == '*':
    tables_df = spark.sql(f"SHOW TABLES IN {{src_db}}")
else:
    like_pattern = pattern.replace('*', '%')
    tables_df = spark.sql(f"SHOW TABLES IN {{src_db}} LIKE '{{like_pattern}}'")

table_list = [row.tableName for row in tables_df.collect()]

metadata = []

for tbl in table_list:
    try:
        desc_df = spark.sql(f"DESCRIBE FORMATTED {{src_db}}.{{tbl}}")
        desc_rows = desc_df.collect()
        
        loc = None
        table_type = "UNKNOWN"
        input_format = None
        
        for row in desc_rows:
            col_name = row.col_name.strip() if row.col_name else ""
            data_type = row.data_type.strip() if row.data_type else ""
            
            if col_name == "Location:":
                loc = data_type
            elif col_name == "Table Type:":
                table_type = data_type.replace("_TABLE", "")
            elif col_name == "InputFormat:":
                input_format = data_type
        
        mapr_total_size = 0
        mapr_file_count = 0
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
                    mapr_total_size = int(content_summary.getLength())
                    mapr_file_count = int(content_summary.getFileCount())
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
            row_count = spark.sql(f"SELECT COUNT(*) as c FROM {{src_db}}.{{tbl}}").collect()[0].c
        except:
            pass
        
        partitions = []
        partition_columns = ""
        is_partitioned = False
        try:
            parts_df = spark.sql(f"SHOW PARTITIONS {{src_db}}.{{tbl}}")
            partitions = [row.partition for row in parts_df.collect()]
            is_partitioned = len(partitions) > 0
            
            if partitions:
                first_part = partitions[0]
                partition_columns = ",".join([p.split("=")[0] for p in first_part.split("/")])
        except:
            pass
        
        schema_df = spark.sql(f"DESCRIBE {{src_db}}.{{tbl}}")
        schema = []
        for row in schema_df.collect():
            col_name = row.col_name.strip() if row.col_name else ""
            data_type = row.data_type.strip() if row.data_type else ""
            
            if col_name.startswith("#") or col_name == "" or col_name == "col_name":
                break
            
            schema.append({{"name": col_name, "type": data_type}})
        
        s3_location = f"{{dest_bucket}}/{{dest_db}}/{{tbl}}"
        
        metadata.append({{
            "source_database": src_db,
            "source_table": tbl,
            "dest_database": dest_db,
            "dest_bucket": dest_bucket,
            "mapr_location": loc or "",
            "s3_location": s3_location,
            "file_format": file_format,
            "schema": schema,
            "partitions": partitions,
            "partition_columns": partition_columns,
            "partition_count": len(partitions),
            "row_count": row_count,
            "is_partitioned": is_partitioned,
            "table_type": table_type,
            "mapr_total_size_bytes": mapr_total_size,
            "mapr_file_count": mapr_file_count
        }})
        
    except Exception as e:
        metadata.append({{
            "source_database": src_db,
            "source_table": tbl,
            "dest_database": dest_db,
            "dest_bucket": dest_bucket,
            "mapr_location": "",
            "s3_location": f"{{dest_bucket}}/{{dest_db}}/{{tbl}}",
            "file_format": "PARQUET",
            "schema": [],
            "partitions": [],
            "partition_columns": "",
            "partition_count": 0,
            "row_count": 0,
            "is_partitioned": False,
            "table_type": "UNKNOWN",
            "mapr_total_size_bytes": 0,
            "mapr_file_count": 0,
            "error": str(e)[:500]
        }})

print("===JSON_START===", file=sys.stdout, flush=True)
print(json.dumps(metadata), file=sys.stdout, flush=True)
print("===JSON_END===", file=sys.stdout, flush=True)

spark.stop()
'''

    with ssh.get_conn() as client:
        temp_dir = f"/tmp/discovery_{run_id}"
        client.exec_command(f"mkdir -p {temp_dir}", timeout=60)
        
        script_path = f"{temp_dir}/discover_tables.py"
        sftp = client.open_sftp()
        with sftp.file(script_path, 'w') as f:
            f.write(pyspark_script)
        sftp.close()
        
        cmd = f"""
cd {temp_dir}
spark-submit {script_path} 2>/dev/null
"""
        
        _, stdout, stderr = client.exec_command(cmd, timeout=3600)
        exit_code = stdout.channel.recv_exit_status()
        output = stdout.read().decode()
        
        client.exec_command(f"rm -rf {temp_dir}", timeout=60)
        
        if exit_code != 0:
            raise Exception(f"Spark job failed: {stderr.read().decode()}\\n{output}")
        
        json_start = output.find("===JSON_START===")
        json_end = output.find("===JSON_END===")
        
        if json_start == -1 or json_end == -1:
            raise Exception(f"Could not find JSON markers in output: {output}")
        
        json_str = output[json_start + len("===JSON_START==="):json_end].strip()
        metadata = json.loads(json_str)
    
    return {
        'run_id': run_id,
        'source_database': src_db,
        'dest_database': dest_db,
        'dest_bucket': dest_bucket,
        'tables': metadata
    }


@task.pyspark(conn_id='spark_default')
def record_discovered_tables(discovery: dict, spark, sc) -> dict:
    """Record discovered tables in Iceberg tracking table."""
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
        mapr_size = t.get('mapr_total_size_bytes', 0)
        mapr_files = t.get('mapr_file_count', 0)
        
        spark.sql(f"""
            MERGE INTO {tracking_db}.migration_table_status t
            USING (SELECT 
                '{run_id}' as run_id,
                '{t['source_database']}' as source_database,
                '{t['source_table']}' as source_table
            ) s
            ON t.run_id = s.run_id 
               AND t.source_database = s.source_database 
               AND t.source_table = s.source_table
            WHEN MATCHED THEN UPDATE SET
                discovery_status = 'COMPLETED',
                discovery_completed_at = current_timestamp(),
                discovery_duration_seconds = {discovery_duration},
                mapr_location = '{t['mapr_location']}',
                file_format = '{t['file_format']}',
                table_type = '{table_type}',                    
                source_row_count = {row_count},                
                mapr_total_size_bytes = {mapr_size},           
                mapr_file_count = {mapr_files},     
                updated_at = current_timestamp()
            WHEN NOT MATCHED THEN INSERT (
                run_id, source_database, source_table, dest_database, dest_bucket,
                dest_location, mapr_location, file_format,
                partition_count, is_partitioned, schema_json, partitions_json,
                partition_columns, table_type, source_row_count,         
                mapr_total_size_bytes, mapr_file_count,                   
                discovery_status, discovery_completed_at,
                discovery_duration_seconds, overall_status, updated_at
            ) VALUES (
                '{run_id}', '{t['source_database']}', '{t['source_table']}',
                '{t['dest_database']}', '{t['dest_bucket']}', '{t['s3_location']}',
                '{t['mapr_location']}', '{t['file_format']}',
                {t.get('partition_count', 0)}, {str(t.get('is_partitioned', False)).lower()},
                '{schema_json}', '{parts_json}', '{t.get('partition_columns', '')}',
                '{table_type}', {row_count},                               
                {mapr_size}, {mapr_files},                               
                'COMPLETED', current_timestamp(), {discovery_duration}, 'DISCOVERED', current_timestamp()
            )
        """)
    
    return discovery


@task
@track_duration
def run_distcp_ssh(discovery: dict, mapr_setup: dict) -> dict:
    """Run DistCp via SSH for all tables. Uses -update for incremental."""
    config = get_config()
    ssh = SSHHook(ssh_conn_id=config['ssh_conn_id'])
    
    run_id = discovery['run_id']
    tables = discovery['tables']
    temp_dir = mapr_setup['temp_dir']
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
    
    results = []
    for t in tables:
        src_db = t['source_database']
        tbl = t['source_table']
        mapr_loc = t['mapr_location']
        s3_loc = t['s3_location']
        
        cmd = f'''
set -e
INCR=false
hadoop fs{s3_opts} -test -d {s3_loc} 2>/dev/null && INCR=true
echo "INCREMENTAL=$INCR"
DISTCP_OUTPUT=$(hadoop distcp{s3_opts} -update -m {mappers} -bandwidth {bandwidth} -strategy dynamic \\
    -log {temp_dir}/distcp_{tbl}.log "{mapr_loc}" "{s3_loc}" 2>&1)
DISTCP_EXIT=$?
echo "DISTCP_EXIT_CODE=$DISTCP_EXIT"

BYTES_COPIED=$(echo "$DISTCP_OUTPUT" | grep -i "Bytes Copied" | awk '{{print $NF}}' | tr -d ',')
FILES_COPIED=$(echo "$DISTCP_OUTPUT" | grep -i "Number of files copied" | awk '{{print $NF}}' | tr -d ',')
S3_PATH="${{s3_loc#s3a://}}"
S3_OBJECTS=$(s3Cli ls "$S3_PATH" 2>/dev/null | tail -n +3 | grep '^f')
S3_FILE_COUNT=$(echo "$S3_OBJECTS" | grep -v '^$' | wc -l)
S3_TOTAL_SIZE=$(echo "$S3_OBJECTS" | awk '{{
    size = $2
    unit = $3
    
    # Convert to bytes
    if (unit == "B") bytes = size
    else if (unit == "KB") bytes = size * 1024
    else if (unit == "MB") bytes = size * 1024 * 1024
    else if (unit == "GB") bytes = size * 1024 * 1024 * 1024
    else if (unit == "TB") bytes = size * 1024 * 1024 * 1024 * 1024
    else if (unit == "PB") bytes = size * 1024 * 1024 * 1024 * 1024 * 1024
    else bytes = 0
    
    sum += bytes
}} END {{print sum}}')

[ -z "$S3_TOTAL_SIZE" ] && S3_TOTAL_SIZE=0
[ -z "$S3_FILE_COUNT" ] && S3_FILE_COUNT=0

echo "DISTCP_EXIT_CODE=$DISTCP_EXIT"
echo "BYTES_COPIED=$BYTES_COPIED"
echo "FILES_COPIED=$FILES_COPIED"
echo "S3_TOTAL_SIZE=$S3_TOTAL_SIZE"
echo "S3_FILE_COUNT=$S3_FILE_COUNT"
'''
        try:
            with ssh.get_conn() as client:
                _, stdout, stderr = client.exec_command(cmd, timeout=SSH_COMMAND_TIMEOUT)
                exit_code = stdout.channel.recv_exit_status()
                output = stdout.read().decode()
                is_incr = "INCREMENTAL=true" in output

                bytes_copied = 0
                files_copied = 0
                s3_size = 0
                s3_files = 0

                try:
                    for line in output.split('\n'):
                        if 'BYTES_COPIED=' in line:
                            bytes_copied = int(line.split('=')[1].strip() or 0)
                        if 'FILES_COPIED=' in line:
                            files_copied = int(line.split('=')[1].strip() or 0)
                        if 'S3_TOTAL_SIZE=' in line:
                            s3_size = int(line.split('=')[1].strip() or 0)
                        if 'S3_FILE_COUNT=' in line:
                            s3_files = int(line.split('=')[1].strip() or 0)
                except:
                    pass
                
                if exit_code != 0:
                    raise Exception(stderr.read().decode())
                
                results.append({
                    'source_database': src_db,
                    'source_table': tbl,
                    'status': 'COMPLETED',
                    'is_incremental': is_incr,
                    'bytes_copied': bytes_copied,
                    'files_copied': files_copied,
                    's3_total_size_bytes': s3_size,      
                    's3_file_count': s3_files, 
                    'error': None
                })
        except Exception as e:
            results.append({
                'source_database': src_db,
                'source_table': tbl,
                'status': 'FAILED',
                'is_incremental': False,
                'bytes_copied': 0,
                'files_copied': 0,
                's3_total_size_bytes': 0,               
                's3_file_count': 0,  
                'error': str(e)[:2000]
            })
    
    return {**discovery, 'distcp_results': results}


@task.pyspark(conn_id='spark_default')
def update_distcp_status(distcp_result: dict, spark, sc) -> dict:
    """Update Iceberg tracking with DistCp results."""
    config = get_config()
    tracking_db = config['tracking_database']
    run_id = distcp_result['run_id']

    distcp_duration = distcp_result.get('_task_duration', 0.0)
    
    for r in distcp_result.get('distcp_results', []):
        overall = 'COPIED' if r['status'] == 'COMPLETED' else 'FAILED'
        error_msg = r.get('error', '').replace("'", "''") if r.get('error') else ''

        s3_size = r.get('s3_total_size_bytes', 0)
        s3_files = r.get('s3_file_count', 0)
        
        spark.sql(f"""
            UPDATE {tracking_db}.migration_table_status
            SET distcp_status = '{r['status']}',
                distcp_completed_at = current_timestamp(),
                distcp_duration_seconds = {distcp_duration},
                distcp_is_incremental = {str(r['is_incremental']).lower()},
                distcp_bytes_copied = {r.get('bytes_copied', 0)},
                distcp_files_copied = {r.get('files_copied', 0)},
                s3_total_size_bytes = {s3_size},                                   
                s3_file_count = {s3_files},                                        
                file_count_match = (mapr_file_count = {s3_files}),               
                file_size_match = (ABS(mapr_total_size_bytes - {s3_size}) / GREATEST(mapr_total_size_bytes, 1) < 0.01),  
                overall_status = '{overall}',
                error_message = CASE WHEN '{r['status']}' = 'FAILED' THEN '{error_msg}' ELSE error_message END,
                updated_at = current_timestamp()
            WHERE run_id = '{run_id}'
              AND source_database = '{r['source_database']}'
              AND source_table = '{r['source_table']}'
        """)
    
    return distcp_result


@task.pyspark(conn_id='spark_default')
@track_duration
def create_hive_tables(distcp_result: dict, spark, sc) -> dict:
    """Create external Hive tables via Spark. Handles incremental (repairs partitions)."""
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
        if distcp_status == 'FAILED':
            results.append({
                'source_table': t['source_table'],
                'status': 'SKIPPED',
                'error': 'DistCp failed',
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
                
                results.append({
                    'source_table': tbl,
                    'status': 'COMPLETED',
                    'action': 'created',
                    'existed': False,
                    'error': None
                })
        except Exception as e:
            results.append({
                'source_table': tbl,
                'status': 'FAILED',
                'action': 'error',
                'existed': False,
                'error': str(e)
            })
    
    return {**distcp_result, 'table_results': results}


@task.pyspark(conn_id='spark_default')
def update_table_create_status(table_result: dict, spark, sc) -> dict:
    """Update Iceberg tracking with table creation results."""
    config = get_config()
    tracking_db = config['tracking_database']
    run_id = table_result['run_id']
    dest_db = table_result['dest_database']

    table_duration = table_result.get('_task_duration', 0.0)
    
    for r in table_result.get('table_results', []):
        overall = 'TABLE_CREATED' if r['status'] == 'COMPLETED' else ('FAILED' if r['status'] == 'FAILED' else 'SKIPPED')
        error_msg = (r.get('error', '') or '').replace("'", "''")[:2000]
        
        spark.sql(f"""
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
        """)
    
    return table_result


@task.pyspark(conn_id='spark_default')
@track_duration
def validate_destination_tables(source_validation: dict, spark, sc) -> dict:
    """Validate destination Hive tables: row counts, partition counts, schema comparison."""
    
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
            source_partition_count = t.get('partition_count', 0)
            
            # Get destination row count
            dest_row_count = spark.sql(f"SELECT COUNT(*) as c FROM {dest_tbl}").collect()[0]['c']
            
            # Get destination partition count
            dest_partition_count = 0
            try:
                dest_partitions_df = spark.sql(f"SHOW PARTITIONS {dest_tbl}")
                dest_partition_count = dest_partitions_df.count()
            except:
                pass  
            
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
                'error': None
            })
            
        except Exception as e:
            validation_results.append({
                'source_table': tbl,
                'status': 'FAILED',
                'error': str(e)[:2000]
            })
    
    return {**source_validation, 'validation_results': validation_results}


@task.pyspark(conn_id='spark_default')
def update_validation_status(validation_result: dict, spark, sc) -> dict:
    """Update Iceberg tracking with validation results."""
    
    config = get_config()
    tracking_db = config['tracking_database']
    
    run_id = validation_result['run_id']
    dest_db = validation_result['dest_database']
    
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
        
        spark.sql(f"""
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
                    WHEN {str(v.get('row_count_match', False)).lower()} 
                         AND {str(v.get('partition_count_match', False)).lower()} 
                         AND {str(v.get('schema_match', False)).lower()} 
                    THEN 'VALIDATED' 
                    ELSE 'VALIDATION_FAILED' 
                END,
                error_message = CASE WHEN '{v['status']}' = 'FAILED' THEN '{error_msg}' ELSE error_message END,
                updated_at = current_timestamp()
            WHERE run_id = '{run_id}'
              AND dest_database = '{dest_db}'
              AND source_table = '{v['source_table']}'
        """)

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
        
        spark.sql(f"""
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
        """)
    
    return validation_result


@task.pyspark(conn_id='spark_default')
def generate_html_report(run_id: str, spark, sc) -> str:
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
    failed_tables = sum(1 for t in table_status if 'FAILED' in t.overall_status)
    total_data_gb = sum(t.distcp_bytes_copied or 0 for t in table_status) / (1024**3)
    total_files = sum(t.distcp_files_copied or 0 for t in table_status)
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
        <h1>MapR to S3 Migration Report</h1>
        
        <div class="timestamp">
            Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC<br>
            Run ID: <strong>{run_id}</strong><br>
            DAG Run: <strong>{run_info.dag_run_id}</strong>
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
                <p class="value">{total_data_gb:.2f} GB</p>
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
        SELECT * FROM {tracking_db}.validation_results
        WHERE run_id = '{run_id}'
        ORDER BY validation_run_timestamp DESC
        LIMIT 1
    """).collect()
            
    if validation_summary_data:
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
        status_class = 'status-completed' if 'VALIDATED' in t.overall_status or 'TABLE_CREATED' in t.overall_status else 'status-failed'
        
        discovery_dur = f"{t.discovery_duration_seconds:.1f}s" if t.discovery_duration_seconds else "N/A"
        distcp_dur = f"{t.distcp_duration_seconds:.1f}s" if t.distcp_duration_seconds else "N/A"
        distcp_detail = f"<br><small>{t.distcp_bytes_copied/(1024**2):.1f} MB, {t.distcp_files_copied:,} files</small>" if t.distcp_bytes_copied else ""
        if t.distcp_is_incremental:
            distcp_dur += " <span style='background-color: #fff3cd; padding: 2px 6px; border-radius: 4px; font-size: 10px;'>INCREMENTAL</span>"
        table_dur = f"{t.table_create_duration_seconds:.1f}s" if t.table_create_duration_seconds else "N/A"
        val_dur = f"{t.validation_duration_seconds:.1f}s" if t.validation_duration_seconds else "N/A"
        
        total_dur = (t.discovery_duration_seconds or 0) + (t.distcp_duration_seconds or 0) + \
                    (t.table_create_duration_seconds or 0) + (t.validation_duration_seconds or 0)
        
        html += f"""
                <tr>
                    <td>{t.source_database}</td>
                    <td><strong>{t.source_table}</strong></td>
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
        
        part_match_class = 'validation-pass' if t.partition_count_match else 'validation-fail'
        part_match_icon = '✓ PASS' if t.partition_count_match else '✗ FAIL'
        
        schema_match_class = 'validation-pass' if t.schema_match else 'validation-fail'
        schema_match_icon = '✓ PASS' if t.schema_match else '✗ FAIL'
        
        html += f"""
                <tr>
                    <td>{t.source_database}</td>
                    <td><strong>{t.source_table}</strong></td>
                    <td class="metric">{t.source_row_count:,}</td>
                    <td class="metric">{t.dest_hive_row_count:,}</td>
                    <td class="{row_match_class}">{row_match_icon}</td>
                    <td class="metric">{t.source_partition_count}</td>
                    <td class="metric">{t.dest_partition_count}</td>
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
                    <th>S3 Size (GB)</th>
                    <th>Size Match</th>
                    <th>MapR Files</th>
                    <th>S3 Files</th>
                    <th>File Count Match</th>
                </tr>
            </thead>
            <tbody>
"""
    
    for t in table_status:
        if not t.distcp_status:
            continue
        
        mapr_size_gb = (t.mapr_total_size_bytes or 0) / (1024**3)
        s3_size_gb = (t.s3_total_size_bytes or 0) / (1024**3)
        
        size_match_class = 'validation-pass' if t.file_size_match else 'validation-fail'
        size_match_icon = '✓ PASS' if t.file_size_match else '✗ FAIL'
        
        count_match_class = 'validation-pass' if t.file_count_match else 'validation-fail'
        count_match_icon = '✓ PASS' if t.file_count_match else '✗ FAIL'
        
        html += f"""
                <tr>
                    <td>{t.source_database}</td>
                    <td><strong>{t.source_table}</strong></td>
                    <td class="metric">{mapr_size_gb:.2f}</td>
                    <td class="metric">{s3_size_gb:.2f}</td>
                    <td class="{size_match_class}">{size_match_icon}</td>
                    <td class="metric">{t.mapr_file_count:,}</td>
                    <td class="metric">{t.s3_file_count:,}</td>
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
        data_gb = (t.distcp_bytes_copied or 0) / (1024**3)
        distcp_speed = (t.distcp_bytes_copied or 0) / (1024**2) / (t.distcp_duration_seconds or 1)
        
        total_dur = (t.discovery_duration_seconds or 0) + (t.distcp_duration_seconds or 0) + \
                    (t.table_create_duration_seconds or 0) + (t.validation_duration_seconds or 0)
        
        rows_per_sec = (t.source_row_count or 0) / (total_dur or 1)
        
        html += f"""
                <tr>
                    <td>{t.source_database}</td>
                    <td><strong>{t.source_table}</strong></td>
                    <td class="metric">{data_gb:.2f} GB</td>
                    <td class="metric">{distcp_speed:.2f} MB/s</td>
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
    report_path = f"{report_location}/{run_id}_report.html"
    
    # Use Spark to write HTML
    from pyspark.sql import Row
    report_df = spark.createDataFrame([Row(content=html)])
    report_df.coalesce(1).write.mode('overwrite').text(report_path)
    
    return report_path


@task.pyspark(conn_id='spark_default')
def finalize_run(run_id: str, spark, sc) -> dict:
    """Finalize migration run - update stats in Iceberg tracking."""
    config = get_config()
    tracking_db = config['tracking_database']
    
    stats = spark.sql(f"""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN overall_status NOT IN ('FAILED', 'PENDING') THEN 1 ELSE 0 END) as successful,
            SUM(CASE WHEN overall_status IN ('FAILED') THEN 1 ELSE 0 END) as failed
        FROM {tracking_db}.migration_table_status
        WHERE run_id = '{run_id}'
    """).collect()[0]
    
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


@task
def cleanup_edge(mapr_setup: dict, run_id: str) -> dict:
    """Clean up temp files on edge node."""
    config = get_config()
    ssh = SSHHook(ssh_conn_id=config['ssh_conn_id'])
    temp_dir = mapr_setup.get('temp_dir', '')
    
    if temp_dir:
        try:
            with ssh.get_conn() as client:
                client.exec_command(f"rm -rf {temp_dir}", timeout=60)
        except:
            pass
    
    return {'cleaned': temp_dir}


# =============================================================================
#  DAG 2: MAPR TO S3 MIGRATION RETRY TASKS
# =============================================================================

@task.pyspark(conn_id='spark_default')
def get_failed_tables(parent_run_id: str, spark, sc) -> list:
    """ Query tracking table to identify tables that need retry. """
    config = get_config()
    tracking_db = config['tracking_database']

    # Query for tables needing retry
    failed_tables_df = spark.sql(f"""
        SELECT
            source_database,
            source_table,
            dest_database,
            dest_bucket,
            mapr_location,
            dest_location,
            file_format,
            schema_json,
            partitions_json,
            partition_columns,
            partition_count,
            is_partitioned,
            table_type,
            source_row_count,
            mapr_total_size_bytes,
            mapr_file_count,
            overall_status,
            discovery_status,
            distcp_status,
            table_create_status,
            validation_status,
            error_message
        FROM {tracking_db}.migration_table_status
        WHERE run_id = '{parent_run_id}'
          AND (
              overall_status IN ('FAILED', 'VALIDATION_FAILED', 'SKIPPED')
              OR file_count_match = false
          )
        ORDER BY source_database, source_table
    """)

    failed_tables = []
    for row in failed_tables_df.collect():
        import json as json_lib
        schema = json_lib.loads(row.schema_json) if row.schema_json else []
        partitions = json_lib.loads(row.partitions_json) if row.partitions_json else []

        failed_tables.append({
            'source_database': row.source_database,
            'source_table': row.source_table,
            'dest_database': row.dest_database,
            'dest_bucket': row.dest_bucket,
            'mapr_location': row.mapr_location,
            's3_location': row.dest_location,
            'file_format': row.file_format,
            'schema': schema,
            'partitions': partitions,
            'partition_columns': row.partition_columns,
            'partition_count': row.partition_count,
            'row_count': row.source_row_count,
            'is_partitioned': row.is_partitioned,
            'table_type': row.table_type,
            'mapr_total_size_bytes': row.mapr_total_size_bytes,
            'mapr_file_count': row.mapr_file_count,
            'run_id': parent_run_id,
            'previous_status': row.overall_status,
            'discovery_status': row.discovery_status,
            'distcp_status': row.distcp_status,
            'table_create_status': row.table_create_status,
            'validation_status': row.validation_status,
            'error_message': row.error_message
        })

    return failed_tables


@task.pyspark(conn_id='spark_default')
def group_failed_tables_by_database(failed_tables: list, spark, sc) -> list:
    """ Group failed tables by database for efficient batch processing. """
    from collections import defaultdict

    db_groups = defaultdict(list)
    for table in failed_tables:
        key = (table['source_database'], table['dest_database'], table['dest_bucket'])
        db_groups[key].append(table)

    grouped_configs = []
    for (src_db, dest_db, dest_bucket), tables in db_groups.items():
        grouped_configs.append({
            'run_id': tables[0]['run_id'], 
            'source_database': src_db,
            'dest_database': dest_db,
            'dest_bucket': dest_bucket,
            'tables': tables
        })

    return grouped_configs


# =============================================================================
# DAG 3: ICEBERG MIGRATION TASKS
# =============================================================================

@task.pyspark(conn_id='spark_default')
def init_iceberg_tracking_tables(spark, sc) -> dict:
    """Create Iceberg migration tracking table if it doesn't exist."""
    config = get_config()
    tracking_db = config['tracking_database']
    tracking_loc = config['tracking_location']
    
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {tracking_db} LOCATION '{tracking_loc}'")

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tracking_db}.iceberg_migration_runs (
            run_id STRING,
            dag_run_id STRING,
            excel_file_path STRING,
            parent_migration_run_id STRING,
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
            updated_at TIMESTAMP,
            parent_migration_run_id STRING
        )
        USING iceberg
        LOCATION '{tracking_loc}/iceberg_migration_table_status'
    """)
    
    return {'status': 'initialized', 'database': tracking_db}


@task.pyspark(conn_id='spark_default')
def create_iceberg_migration_run(excel_file_path: str, dag_run_id: str, spark, sc) -> str:
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
            'PENDING',
            current_timestamp(),
            NULL,
            'RUNNING',
            0, 0, 0,
            '{json.dumps(config).replace("'", "''")}'
        )
    """)
    
    return run_id


@task.pyspark(conn_id='spark_default')
def parse_iceberg_excel(excel_file_path: str, run_id: str, spark, sc) -> list:
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
        if not dest_ice_db:
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
def lookup_parent_migration_run(excel_configs: list, spark, sc) -> dict:
    """Query migration_table_status to find the parent migration run_id."""
    config = get_config()
    tracking_db = config['tracking_database']
    
    tables_to_lookup = []
    for cfg in excel_configs:
        src_db = cfg['source_database']
        tbl_pattern = cfg['table_pattern']
        
        all_tables = [row.tableName for row in spark.sql(f"SHOW TABLES IN {src_db}").collect()]
        
        if tbl_pattern == '*':
            matched_tables = all_tables
        else:
            import fnmatch
            matched_tables = [t for t in all_tables if fnmatch.fnmatch(t, tbl_pattern)]
        
        for tbl in matched_tables:
            tables_to_lookup.append((src_db, tbl))
    
    parent_mapping = {}
    
    for src_db, src_tbl in tables_to_lookup:
        try:
            result = spark.sql(f"""
                SELECT run_id
                FROM {tracking_db}.migration_table_status
                WHERE source_database = '{src_db}'
                  AND source_table = '{src_tbl}'
                  AND overall_status IN ('TABLE_CREATED', 'COPIED')
                ORDER BY updated_at DESC
                LIMIT 1
            """).collect()
            
            if result:
                parent_mapping[f"{src_db}.{src_tbl}"] = result[0]['run_id']
            else:
                parent_mapping[f"{src_db}.{src_tbl}"] = None 
        except Exception as e:
            parent_mapping[f"{src_db}.{src_tbl}"] = None
    
    from collections import Counter
    valid_parents = [v for v in parent_mapping.values() if v is not None]
    
    if valid_parents:
        most_common_parent = Counter(valid_parents).most_common(1)[0][0]
    else:
        most_common_parent = None 
    
    return {
        'parent_migration_run_id': most_common_parent,
        'table_parent_mapping': parent_mapping
    }


@task.pyspark(conn_id='spark_default')
def update_parent_run_id(parent_lookup: dict, run_id: str, spark, sc) -> dict:
    """Update the iceberg_migration_runs table with parent_migration_run_id after lookup."""
    config = get_config()
    tracking_db = config['tracking_database']
    
    parent_run_id = parent_lookup.get('parent_migration_run_id')
    
    if parent_run_id:
        spark.sql(f"""
            UPDATE {tracking_db}.iceberg_migration_runs
            SET parent_migration_run_id = '{parent_run_id}'
            WHERE run_id = '{run_id}'
        """)
    
    return {
        'run_id': run_id,
        'parent_updated': parent_run_id is not None
    }


@task.pyspark(conn_id='spark_default')
@track_duration
def discover_hive_tables(db_config: dict, spark, sc) -> dict:
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
    
    tables_metadata = []
    for tbl in matched_tables:
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
            tables_metadata.append({
                'table': tbl,
                'location': None,
                'discovery_error': str(e)
            })
    
    return {
        **db_config,
        'discovered_tables': tables_metadata
    }


@task.pyspark(conn_id='spark_default')
@track_duration
def migrate_tables_to_iceberg(discovery: dict, parent_lookup: dict, dag_run_id: str, spark, sc) -> dict:
    """Migrate discovered Hive tables to Iceberg format."""
    config = get_config()
    tracking_db = config['tracking_database']
    
    src_db = discovery['source_database']
    dest_db = discovery['destination_iceberg_database']
    inplace = discovery['inplace_migration']
    run_id = discovery['run_id']

    table_parent_mapping = parent_lookup.get('table_parent_mapping', {})
    
    if not inplace:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {dest_db}")
    
    results = []
    
    for tbl_meta in discovery.get('discovered_tables', []):
        tbl = tbl_meta['table']
        location = tbl_meta.get('location')
        table_key = f"{src_db}.{tbl}"
        parent_run_id = table_parent_mapping.get(table_key)
        parent_run_id_sql = f"'{parent_run_id}'" if parent_run_id else 'NULL'
        
        try:
            hive_count = spark.sql(f"SELECT COUNT(*) as c FROM {src_db}.{tbl}").collect()[0]['c']
            src_hive_partition_count = 0
            try:
                src_partitions_df = spark.sql(f"SHOW PARTITIONS {src_db}.{tbl}")
                src_hive_partition_count = src_partitions_df.count()
            except:
                pass 

            if inplace:
                migration_type = "INPLACE"
                dest_table = f"{src_db}.{tbl}"
                spark.sql(f"CALL spark_catalog.system.migrate('{src_db}.{tbl}')")
            else:
                migration_type = "SNAPSHOT"
                dest_table = f"{dest_db}.{tbl}"
                spark.sql(f"CALL spark_catalog.system.snapshot('{src_db}.{tbl}', '{dest_db}.{tbl}')")
            
            iceberg_count = spark.sql(f"SELECT COUNT(*) as c FROM {dest_table}").collect()[0]['c']
            dest_iceberg_partition_count = 0
            try:
                dest_partitions_df = spark.sql(f"SHOW PARTITIONS {dest_table}")
                dest_iceberg_partition_count = dest_partitions_df.count()
            except:
                pass

            counts_match = (hive_count == iceberg_count)
            partition_match = (src_hive_partition_count == dest_iceberg_partition_count)
            
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
                    0.0,
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
                    current_timestamp(),
                    {parent_run_id_sql}
                )
            """)
            
        except Exception as e:
            error_msg = str(e).replace("'", "''")[:2000]
            
            results.append({
                'source_table': f"{src_db}.{tbl}",
                'destination_table': f"{dest_db}.{tbl}" if not inplace else f"{src_db}.{tbl}",
                'migration_type': "INPLACE" if inplace else "SNAPSHOT",
                'status': 'FAILED',
                'error': str(e)
            })
            
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
                    0.0,
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
                    current_timestamp(),
                    {parent_run_id_sql}
                )
            """)
    
    return {
        'run_id': run_id,
        'source_database': src_db,
        'destination_database': dest_db,
        'migration_type': 'INPLACE' if inplace else 'SNAPSHOT',
        'results': results
    }


@task.pyspark(conn_id='spark_default')
def update_migration_durations(migration_result: dict, spark, sc) -> dict:
    """Update tracking table with migration durations from XCom."""
    
    config = get_config()
    tracking_db = config['tracking_database']
    
    run_id = migration_result['run_id']
    
    # Extract duration from XCom result
    migration_duration = migration_result.get('_task_duration', 0.0)
    
    # Update all records for this run
    spark.sql(f"""
        UPDATE {tracking_db}.iceberg_migration_table_status
        SET migration_duration_seconds = {migration_duration},
            updated_at = current_timestamp()
        WHERE run_id = '{run_id}'
          AND migration_duration_seconds = 0.0
    """)
    
    return migration_result


@task.pyspark(conn_id='spark_default')
@track_duration
def validate_iceberg_tables(migration_result: dict, spark, sc) -> dict:
    """Validate Iceberg tables: row counts, partition counts, schema comparison between source Hive and destination Iceberg."""
    
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
                'error': None
            })
            
        except Exception as e:
            validation_results.append({
                'source_table': tbl,
                'destination_table': dest_tbl,
                'status': 'FAILED',
                'error': str(e)[:2000]
            })
    
    return {**migration_result, 'validation_results': validation_results}


@task.pyspark(conn_id='spark_default')
def update_iceberg_validation_status(validation_result: dict, spark, sc) -> dict:
    """Update Iceberg tracking with validation results."""
    
    config = get_config()
    tracking_db = config['tracking_database']
    
    run_id = validation_result['run_id']
    src_db = validation_result['source_database']
    
    # Extract duration from XCom result
    validation_duration = validation_result.get('_task_duration', 0.0)
    
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
        
        spark.sql(f"""
            UPDATE {tracking_db}.iceberg_migration_table_status
            SET validation_status = '{v['status']}',
                validation_completed_at = current_timestamp(),
                validation_duration_seconds = {validation_duration},
                source_hive_row_count = {v.get('source_hive_row_count', 0)},
                destination_iceberg_row_count = {v.get('dest_iceberg_row_count', 0)},
                row_count_match = {str(v.get('row_count_match', False)).lower()},
                source_hive_partition_count = {v.get('source_hive_partition_count', 0)},
                dest_iceberg_partition_count = {v.get('dest_iceberg_partition_count', 0)},
                partition_count_match = {str(v.get('partition_count_match', False)).lower()},
                schema_match = {str(v.get('schema_match', False)).lower()},
                schema_differences = '{schema_diffs}',
                status = '{overall_status}',
                error_message = CASE WHEN '{v['status']}' = 'FAILED' THEN '{error_msg}' ELSE error_message END,
                updated_at = current_timestamp()
            WHERE run_id = '{run_id}'
              AND source_database = '{src_db}'
              AND source_table = '{v['source_table']}'
        """)
    
    return validation_result


@task.pyspark(conn_id='spark_default')
def generate_iceberg_html_report(run_id: str, spark, sc) -> str:
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
    failed_tables = sum(1 for t in migration_status if 'FAILED' in t.status)
    total_rows = sum(t.source_hive_row_count or 0 for t in migration_status)
    count_mismatches = sum(1 for t in migration_status if not t.row_count_match and t.row_count_match is not None)
    
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
    
    html += """
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
        if t.validation_status != 'COMPLETED':
            continue
        
        row_match_class = 'validation-pass' if t.row_count_match else 'validation-fail'
        row_match_icon = '✓ PASS' if t.row_count_match else '✗ FAIL'
        
        part_match_class = 'validation-pass' if t.partition_count_match else 'validation-fail'
        part_match_icon = '✓ PASS' if t.partition_count_match else '✗ FAIL'
        
        schema_match_class = 'validation-pass' if t.schema_match else 'validation-fail'
        schema_match_icon = '✓ PASS' if t.schema_match else '✗ FAIL'
        
        html += f"""
                <tr>
                    <td>{t.source_database}</td>
                    <td><strong>{t.source_table}</strong></td>
                    <td class="metric">{t.source_hive_row_count:,}</td>
                    <td class="metric">{t.destination_iceberg_row_count:,}</td>
                    <td class="{row_match_class}">{row_match_icon}</td>
                    <td class="metric">{t.source_hive_partition_count or 0}</td>
                    <td class="metric">{t.dest_iceberg_partition_count or 0}</td>
                    <td class="{part_match_class}">{part_match_icon}</td>
                    <td class="{schema_match_class}">{schema_match_icon}</td>
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
        if t.status not in ['COMPLETED', 'VALIDATED']:
            continue
        
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
                    <td class="metric">{t.source_hive_row_count:,}</td>
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
    report_path = f"{report_location}/{run_id}_iceberg_report.html"
    
    # Use Spark to write HTML
    from pyspark.sql import Row
    report_df = spark.createDataFrame([Row(content=html)])
    report_df.coalesce(1).write.mode('overwrite').text(report_path)
    
    return report_path


@task.pyspark(conn_id='spark_default')
def finalize_iceberg_run(run_id: str, spark, sc) -> dict:
    """Finalize Iceberg migration run - aggregate statistics."""
    config = get_config()
    tracking_db = config['tracking_database']
    
    stats = spark.sql(f"""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN status = 'COMPLETED' THEN 1 ELSE 0 END) as successful,
            SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed,
            SUM(CASE WHEN status = 'SKIPPED' THEN 1 ELSE 0 END) as skipped,
            SUM(CASE WHEN row_count_match = false THEN 1 ELSE 0 END) as count_mismatches
        FROM {tracking_db}.iceberg_migration_table_status
        WHERE run_id = '{run_id}'
    """).collect()[0]

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


# =============================================================================
#  DAG 4: ICEBERG MIGRATION RETRY TASKS
# =============================================================================

@task.pyspark(conn_id='spark_default')
def get_failed_iceberg_migrations(parent_run_id: str, spark, sc) -> list:
    """ Query Iceberg tracking table to identify failed Iceberg migrations. """
    config = get_config()
    tracking_db = config['tracking_database']

    failed_df = spark.sql(f"""
        SELECT
            source_database,
            source_table,
            migration_type,
            destination_database,
            destination_table,
            status,
            error_message
        FROM {tracking_db}.iceberg_migration_table_status
        WHERE run_id = '{parent_run_id}'
          AND (
              status = 'FAILED'
              OR row_count_match = false
              OR partition_count_match = false
          )
        ORDER BY source_database, source_table
    """)

    failed_tables = []
    for row in failed_df.collect():
        failed_tables.append({
            'source_database': row.source_database,
            'table': row.source_table,
            'location': None,  
            'run_id': parent_run_id  
        })

    return failed_tables


@task.pyspark(conn_id='spark_default')
def group_iceberg_failures(failed_tables: list, spark, sc) -> list:
    """ Group failed Iceberg migrations by database and migration type. """
    from collections import defaultdict

    config = get_config()
    tracking_db = config['tracking_database']

    # Group by source database
    db_groups = defaultdict(list)
    for table in failed_tables:
        db_groups[table['source_database']].append(table)

    grouped_configs = []
    for src_db, tables in db_groups.items():
        sample_table = tables[0]['table']
        config_row = spark.sql(f"""
            SELECT migration_type, destination_database
            FROM {tracking_db}.iceberg_migration_table_status
            WHERE source_database = '{src_db}'
              AND source_table = '{sample_table}'
            LIMIT 1
        """).collect()[0]

        inplace = (config_row.migration_type == 'INPLACE')
        dest_db = config_row.destination_database

        grouped_configs.append({
            'source_database': src_db,
            'table_pattern': '*',
            'inplace_migration': inplace,
            'destination_iceberg_database': dest_db,
            'run_id': tables[0]['run_id'],
            'discovered_tables': tables  
        })

    return grouped_configs


# =============================================================================
# DAG 1 DEFINITION: MAPR TO S3 MIGRATION
# =============================================================================

with DAG(
    dag_id='mapr_to_s3_migration',
    default_args=DEFAULT_ARGS,
    description='Migrate Hive tables from MapR-FS to S3',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
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
    t_init = init_tracking_tables()
    t_run_id = create_migration_run(
        excel_file_path="{{ params.excel_file_path }}",
        dag_run_id="{{ run_id }}"
    )
    t_excel = parse_excel(
        excel_file_path="{{ params.excel_file_path }}",
        run_id=t_run_id
    )
    t_mapr = mapr_token_setup(run_id=t_run_id)
    
    # Per-database processing (dynamic task mapping)
    t_discover = discover_tables_via_spark_ssh.expand(db_config=t_excel)
    t_record = record_discovered_tables.expand(discovery=t_discover)
    t_distcp = run_distcp_ssh.partial(mapr_setup=t_mapr).expand(discovery=t_record)
    t_distcp_status = update_distcp_status.expand(distcp_result=t_distcp)
    t_tables = create_hive_tables.expand(distcp_result=t_distcp_status)
    t_tbl_status = update_table_create_status.expand(table_result=t_tables)

    # Validation tasks
    t_dest_validation = validate_destination_tables.expand(source_validation=t_tbl_status)
    t_val_status = update_validation_status.expand(validation_result=t_dest_validation)
    
    # Report generation 
    t_report = generate_html_report(run_id=t_run_id)
    
    # Finalize
    t_final = finalize_run(run_id=t_run_id)
    t_cleanup = cleanup_edge(mapr_setup=t_mapr, run_id=t_run_id)
    
    # Dependencies
    t_init >> t_run_id >> t_excel >> t_mapr >> t_discover >> t_record
    t_record >> t_distcp >> t_distcp_status >> t_tables >> t_tbl_status
    t_tbl_status >> t_dest_validation >> t_val_status 
    t_val_status >> t_report >> t_final >> t_cleanup


# =============================================================================
# DAG 2 DEFINITION: MAPR TO S3 MIGRATION RETRY
# =============================================================================

with DAG(
    dag_id='mapr_to_s3_migration_retry', 
    default_args=DEFAULT_ARGS,
    description='Retry failed tables from previous MapR to S3 migration run',
    schedule_interval=None, 
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['migration', 'mapr', 's3', 'retry'], 
    params={
        'parent_run_id': Param(
            default='',  
            type='string',
            description='Run ID from initial migration to retry failed tables'
        )
    },
    render_template_as_native_obj=True,
) as dag_mapr_to_s3_retry:

    # Identify failed tables from parent run 
    t_retry_failed = get_failed_tables(parent_run_id="{{ params.parent_run_id }}")
    t_retry_grouped = group_failed_tables_by_database(failed_tables=t_retry_failed)
    t_retry_run_id = create_migration_run(
        excel_file_path="retry_run_for_{{ params.parent_run_id }}",
        dag_run_id="{{ run_id }}"
    )
    t_retry_mapr = mapr_token_setup(run_id=t_retry_run_id)

    # Per-database processing (dynamic task mapping)
    t_retry_distcp = run_distcp_ssh.partial(mapr_setup=t_retry_mapr).expand(
        discovery=t_retry_grouped  
    )
    t_retry_distcp_status = update_distcp_status.expand(distcp_result=t_retry_distcp)
    t_retry_tables = create_hive_tables.expand(distcp_result=t_retry_distcp_status)
    t_retry_tbl_status = update_table_create_status.expand(table_result=t_retry_tables)

    # Validation taks
    t_retry_validation = validate_destination_tables.expand(source_validation=t_retry_tbl_status)
    t_retry_val_status = update_validation_status.expand(validation_result=t_retry_validation)

    # Report generation 
    t_retry_report = generate_html_report(run_id="{{ params.parent_run_id }}")

    # Finalize
    t_retry_final = finalize_run(run_id="{{ params.parent_run_id }}")
    t_retry_cleanup = cleanup_edge(mapr_setup=t_retry_mapr, run_id=t_retry_run_id)

    # Dependencies
    t_retry_failed >> t_retry_grouped
    [t_retry_run_id, t_retry_grouped] >> t_retry_mapr
    t_retry_mapr >> t_retry_distcp >> t_retry_distcp_status
    t_retry_distcp_status >> t_retry_tables >> t_retry_tbl_status
    t_retry_tbl_status >> t_retry_validation >> t_retry_val_status
    t_retry_val_status >> t_retry_report >> t_retry_final >> t_retry_cleanup


# =============================================================================
# DAG 3 DEFINITION: ICEBERG MIGRATION
# =============================================================================

with DAG(
    dag_id='iceberg_migration',
    default_args=DEFAULT_ARGS,
    description='Migrate existing Hive tables in S3 to Iceberg format',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
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
    t_ice_parent_lookup = lookup_parent_migration_run(
        excel_configs=t_ice_excel
    )
    t_ice_update_parent = update_parent_run_id(
        run_id=t_ice_run_id,
        parent_lookup=t_ice_parent_lookup
    )
    
    # Per-database processing
    t_ice_discover = discover_hive_tables.expand(db_config=t_ice_excel)
    t_ice_migrate = migrate_tables_to_iceberg.partial(dag_run_id="{{ run_id }}", parent_lookup=t_ice_parent_lookup).expand(discovery=t_ice_discover)

    # Duration update
    t_ice_durations = update_migration_durations.expand(migration_result=t_ice_migrate)
    
    # Validation 
    t_ice_validate = validate_iceberg_tables.expand(migration_result=t_ice_durations)
    t_ice_val_status = update_iceberg_validation_status.expand(validation_result=t_ice_validate)
    
    # Report generation 
    t_ice_report = generate_iceberg_html_report(run_id=t_ice_run_id)
    
    # Finalize
    t_ice_final = finalize_iceberg_run(run_id=t_ice_run_id)
    
    # Dependencies
    t_ice_init >> t_ice_run_id >> t_ice_excel >> t_ice_parent_lookup >> t_ice_update_parent
    t_ice_excel >> t_ice_discover
    [t_ice_discover, t_ice_parent_lookup] >> t_ice_migrate >> t_ice_durations
    t_ice_durations >> t_ice_validate >> t_ice_val_status >> t_ice_report >> t_ice_final


# =============================================================================
#  DAG 4 DEFINITION:  ICEBERG MIGRATION RETRY
# =============================================================================

with DAG(
    dag_id='iceberg_migration_retry', 
    default_args=DEFAULT_ARGS,
    description='Retry failed Iceberg migrations from previous run',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['migration', 'iceberg', 'retry'],
    params={
        'parent_run_id': Param(
            default='',
            type='string',
            description='Run ID from initial Iceberg migration to retry'
        )
    },
    render_template_as_native_obj=True,
) as dag_iceberg_retry:

    # Identify failed Iceberg migrations
    t_ice_retry_failed = get_failed_iceberg_migrations(
        parent_run_id="{{ params.parent_run_id }}"
    )
    t_ice_retry_grouped = group_iceberg_failures(failed_tables=t_ice_retry_failed)
    t_ice_retry_run_id = create_iceberg_migration_run(
        excel_file_path="iceberg_retry_for_{{ params.parent_run_id }}",
        dag_run_id="{{ run_id }}"
    )
    t_ice_retry_parent = lookup_parent_migration_run(excel_configs=t_ice_retry_grouped)

    # Per-database processing
    t_ice_retry_migrate = migrate_tables_to_iceberg.partial(
        dag_run_id="{{ run_id }}",
        parent_lookup=t_ice_retry_parent
    ).expand(discovery=t_ice_retry_grouped)
    t_ice_retry_durations = update_migration_durations.expand(
        migration_result=t_ice_retry_migrate
    )

    # Validation
    t_ice_retry_validate = validate_iceberg_tables.expand(
        migration_result=t_ice_retry_durations
    )
    t_ice_retry_val_status = update_iceberg_validation_status.expand(
        validation_result=t_ice_retry_validate
    )

    # Report generation
    t_ice_retry_report = generate_iceberg_html_report(
        run_id="{{ params.parent_run_id }}"
    )

    # Finalise
    t_ice_retry_final = finalize_iceberg_run(run_id="{{ params.parent_run_id }}")

    # Dependencies
    t_ice_retry_failed >> t_ice_retry_grouped
    [t_ice_retry_run_id, t_ice_retry_grouped] >> t_ice_retry_parent
    [t_ice_retry_grouped, t_ice_retry_parent] >> t_ice_retry_migrate
    t_ice_retry_migrate >> t_ice_retry_durations >> t_ice_retry_validate
    t_ice_retry_validate >> t_ice_retry_val_status >> t_ice_retry_report
    t_ice_retry_report >> t_ice_retry_final