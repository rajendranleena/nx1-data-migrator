"""
MapR to S3 Migration DAG

Orchestrates migration of Hive tables from MapR-FS to S3:
- Excel config from S3 (only DAG parameter)
- SSH operations for MapR token, beeline discovery, distcp (24h timeout)
- PySpark tasks for Hive table creation and Iceberg migration
- Tracking/metadata stored in Iceberg tables
- Incremental support (distcp -update, table repair)

Excel columns: database | table | dest database | bucket | convert to iceberg
"""

from datetime import datetime, timedelta
import json

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.models.param import Param
from airflow.providers.ssh.hooks.ssh import SSHHook

# =============================================================================
# CONFIGURATION FROM AIRFLOW VARIABLES
# =============================================================================

def get_config() -> dict:
    return {
        'ssh_conn_id': Variable.get('mapr_ssh_conn_id', default_var='mapr_edge_ssh'),
        'edge_temp_path': Variable.get('mapr_edge_temp_path', default_var='/tmp/migration'),
        'hive_server_host': Variable.get('mapr_hive_server_host', default_var='hiveserver2.mapr.local'),
        'hive_server_port': Variable.get('mapr_hive_server_port', default_var='10000'),
        'hive_auth': Variable.get('mapr_hive_auth', default_var='maprsasl'),
        'default_s3_bucket': Variable.get('migration_default_s3_bucket', default_var='s3a://data-lake'),
        'aws_conn_id': Variable.get('migration_aws_conn_id', default_var='aws_default'),
        'distcp_mappers': Variable.get('migration_distcp_mappers', default_var='50'),
        'distcp_bandwidth': Variable.get('migration_distcp_bandwidth', default_var='100'),
        'spark_conn_id': Variable.get('migration_spark_conn_id', default_var='spark_default'),
        'tracking_database': Variable.get('migration_tracking_database', default_var='migration_tracking'),
        'tracking_location': Variable.get('migration_tracking_location', default_var='s3a://data-lake/migration_tracking'),
        # MapR authentication
        'mapr_user': Variable.get('mapr_user', default_var=''),
        'mapr_password': Variable.get('mapr_password', default_var=''),
        'mapr_cluster': Variable.get('mapr_cluster', default_var=''),
        # S3 credentials for DistCp
        's3_endpoint': Variable.get('s3_endpoint', default_var=''),
        's3_access_key': Variable.get('s3_access_key', default_var=''),
        's3_secret_key': Variable.get('s3_secret_key', default_var=''),
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
# TASKS
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
            convert_to_iceberg BOOLEAN,
            mapr_location STRING,
            file_format STRING,
            partition_count INT,
            is_partitioned BOOLEAN,
            schema_json STRING,
            partitions_json STRING,
            partition_columns STRING,
            discovery_status STRING,
            discovery_completed_at TIMESTAMP,
            distcp_status STRING,
            distcp_started_at TIMESTAMP,
            distcp_completed_at TIMESTAMP,
            distcp_is_incremental BOOLEAN,
            table_create_status STRING,
            table_create_completed_at TIMESTAMP,
            table_already_existed BOOLEAN,
            iceberg_status STRING,
            iceberg_completed_at TIMESTAMP,
            iceberg_table_name STRING,
            overall_status STRING,
            error_message STRING,
            updated_at TIMESTAMP
        )
        USING iceberg
        LOCATION '{tracking_loc}/migration_table_status'
    """)
    
    return {'status': 'initialized', 'database': tracking_db}


@task.pyspark(conn_id='spark_default')
def create_migration_run(spark, sc, excel_file_path: str, dag_run_id: str) -> str:
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
def parse_excel(spark, sc, excel_file_path: str, run_id: str) -> list:
    """Read Excel config from S3 using pyspark.pandas.read_excel."""
    import pyspark.pandas as ps
    
    config = get_config()
    
    # Read Excel directly from S3 using pyspark.pandas
    df = ps.read_excel(excel_file_path)
    
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
        
        iceberg_val = row.get('convert_to_iceberg', 'F')
        if iceberg_val is None or (hasattr(iceberg_val, '__len__') and len(str(iceberg_val).strip()) == 0):
            convert_iceberg = False
        else:
            convert_iceberg = str(iceberg_val).strip().upper() in ('T', 'TRUE', 'YES', '1')
        
        configs.append({
            'source_database': src_db,
            'table_pattern': tbl_pattern,
            'dest_database': dest_db,
            'dest_bucket': bucket_val,
            'convert_to_iceberg': convert_iceberg,
            'run_id': run_id,
        })
    
    return configs


@task
def mapr_token_setup(run_id: str) -> dict:
    """SSH to edge, generate MapR ticket using maprlogin, create temp dir."""
    config = get_config()
    ssh = SSHHook(ssh_conn_id=config['ssh_conn_id'])
    temp_dir = f"{config['edge_temp_path']}/{run_id}"
    
    # Get MapR credentials from Airflow variables
    mapr_user = config.get('mapr_user', '')
    mapr_password = config.get('mapr_password', '')
    mapr_cluster = config.get('mapr_cluster', '')
    
    # Build maprlogin command
    # Option 1: password-based auth (from variable)
    # Option 2: kerberos if available
    # Option 3: use existing ticket
    cmd = f"""
set -e

echo "=== MapR Authentication ==="

# Check for existing valid ticket first
if maprlogin print 2>/dev/null | grep -q "Valid"; then
    echo "Using existing valid MapR ticket"
else
    echo "Generating new MapR ticket..."
    
    # Try password auth if credentials provided
    if [ -n "{mapr_user}" ] && [ -n "{mapr_password}" ]; then
        echo "{mapr_password}" | maprlogin password -user {mapr_user}
    # Try kerberos
    elif klist -s 2>/dev/null; then
        maprlogin kerberos
    # Try password prompt (will use current user)
    else
        echo "Attempting maprlogin with current user..."
        maprlogin password || maprlogin kerberos || {{ echo "ERROR: MapR authentication failed"; exit 1; }}
    fi
fi

# Verify ticket
echo "=== Verifying MapR Ticket ==="
maprlogin print
if ! maprlogin print 2>/dev/null | grep -q "Valid"; then
    echo "ERROR: No valid MapR ticket"
    exit 1
fi

# Create temp directory
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
def discover_tables_ssh(db_config: dict, mapr_setup: dict) -> dict:
    """Use beeline via SSH to discover tables and metadata."""
    config = get_config()
    ssh = SSHHook(ssh_conn_id=config['ssh_conn_id'])
    
    run_id = db_config['run_id']
    src_db = db_config['source_database']
    pattern = db_config['table_pattern']
    dest_db = db_config['dest_database']
    dest_bucket = db_config['dest_bucket']
    convert_iceberg = db_config['convert_to_iceberg']
    temp_dir = mapr_setup['temp_dir']
    
    hive_host = config['hive_server_host']
    hive_port = config['hive_server_port']
    hive_auth = config['hive_auth']
    output_file = f"{temp_dir}/{src_db}_tables.json"

    script = f'''#!/bin/bash
set -e
JDBC="jdbc:hive2://{hive_host}:{hive_port}/{src_db};auth={hive_auth}"

if [ "{pattern}" = "*" ]; then
    TABLES=$(beeline -u "$JDBC" --silent=true --outputformat=csv2 -e "SHOW TABLES IN {src_db};" 2>/dev/null | tail -n +2 | tr -d '"')
else
    LIKE=$(echo "{pattern}" | sed 's/\\*/%/g')
    TABLES=$(beeline -u "$JDBC" --silent=true --outputformat=csv2 -e "SHOW TABLES IN {src_db} LIKE '$LIKE';" 2>/dev/null | tail -n +2 | tr -d '"')
fi

echo "[" > {output_file}
FIRST=true
for TBL in $TABLES; do
    [ -z "$TBL" ] && continue
    
    DESC=$(beeline -u "$JDBC" --silent=true -e "DESCRIBE FORMATTED {src_db}.$TBL;" 2>/dev/null)
    LOC=$(echo "$DESC" | grep "Location:" | head -1 | sed 's/.*Location:[[:space:]]*//' | sed 's/[[:space:]]*|.*//' | tr -d ' \\t')
    
    INFMT=$(echo "$DESC" | grep "InputFormat:" | head -1)
    if echo "$INFMT" | grep -qi parquet; then FMT="PARQUET"
    elif echo "$INFMT" | grep -qi orc; then FMT="ORC"
    elif echo "$INFMT" | grep -qi avro; then FMT="AVRO"
    else FMT="PARQUET"; fi
    
    COLS=$(beeline -u "$JDBC" --silent=true --outputformat=csv2 -e "DESCRIBE {src_db}.$TBL;" 2>/dev/null | tail -n +2 | grep -v "^#" | while IFS=',' read -r cn ct rest; do
        cn=$(echo "$cn" | tr -d '" ')
        ct=$(echo "$ct" | tr -d '" ')
        [ -z "$cn" ] && continue
        echo "{{\\"name\\":\\"$cn\\",\\"type\\":\\"$ct\\"}}"
    done | paste -sd "," -)
    
    PARTS=$(beeline -u "$JDBC" --silent=true --outputformat=csv2 -e "SHOW PARTITIONS {src_db}.$TBL;" 2>/dev/null | tail -n +2 | tr -d '"' || echo "")
    IS_PART=false; PCNT=0; PJSON=""
    if [ -n "$PARTS" ]; then
        IS_PART=true
        PCNT=$(echo "$PARTS" | grep -c . || echo 0)
        PJSON=$(echo "$PARTS" | while read p; do [ -z "$p" ] && continue; echo "\\"$p\\""; done | paste -sd "," -)
    fi
    
    PCOLS=$(echo "$DESC" | awk '/# Partition Information/,/^[[:space:]]*$/' | grep -v "^#" | grep -v col_name | awk -F'|' '{{gsub(/^ +| +$/,"",$2); if($2!="") print $2}}' | paste -sd "," -)
    
    S3LOC="{dest_bucket}/{dest_db}/$TBL"
    
    [ "$FIRST" = true ] && FIRST=false || echo "," >> {output_file}
    cat >> {output_file} << EOF
{{"source_database":"{src_db}","source_table":"$TBL","dest_database":"{dest_db}","dest_bucket":"{dest_bucket}","mapr_location":"$LOC","s3_location":"$S3LOC","file_format":"$FMT","schema":[$COLS],"partitions":[$PJSON],"partition_columns":"$PCOLS","partition_count":$PCNT,"is_partitioned":$IS_PART,"convert_to_iceberg":{str(convert_iceberg).lower()}}}
EOF
done
echo "]" >> {output_file}
cat {output_file}
'''

    with ssh.get_conn() as client:
        _, stdout, stderr = client.exec_command(
            f"bash -s << 'EOFSCRIPT'\n{script}\nEOFSCRIPT",
            timeout=SSH_COMMAND_TIMEOUT
        )
        exit_code = stdout.channel.recv_exit_status()
        output = stdout.read().decode()
        if exit_code != 0:
            raise Exception(f"Discovery failed: {stderr.read().decode()}")
    
    # Parse JSON
    try:
        idx = output.rfind('[')
        tables = json.loads(output[idx:output.rfind(']')+1]) if idx >= 0 else []
    except json.JSONDecodeError as e:
        raise Exception(f"JSON parse error: {e}")
    
    return {
        'run_id': run_id,
        'source_database': src_db,
        'dest_database': dest_db,
        'dest_bucket': dest_bucket,
        'tables': tables,
        'convert_to_iceberg': convert_iceberg
    }


@task.pyspark(conn_id='spark_default')
def record_discovered_tables(spark, sc, discovery: dict) -> dict:
    """Record discovered tables in Iceberg tracking table."""
    
    config = get_config()
    
    
    tracking_db = config['tracking_database']
    run_id = discovery['run_id']
    
    for t in discovery['tables']:
        parts = t.get('partitions', [])
        if isinstance(parts, str):
            parts = [p for p in parts.split(',') if p]
        
        # Escape single quotes for SQL
        schema_json = json.dumps(t.get('schema', [])).replace("'", "''")
        parts_json = json.dumps(parts).replace("'", "''")
        
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
                mapr_location = '{t['mapr_location']}',
                file_format = '{t['file_format']}',
                updated_at = current_timestamp()
            WHEN NOT MATCHED THEN INSERT (
                run_id, source_database, source_table, dest_database, dest_bucket,
                dest_location, convert_to_iceberg, mapr_location, file_format,
                partition_count, is_partitioned, schema_json, partitions_json,
                partition_columns, discovery_status, discovery_completed_at,
                overall_status, updated_at
            ) VALUES (
                '{run_id}', '{t['source_database']}', '{t['source_table']}',
                '{t['dest_database']}', '{t['dest_bucket']}', '{t['s3_location']}',
                {str(t.get('convert_to_iceberg', False)).lower()},
                '{t['mapr_location']}', '{t['file_format']}',
                {t.get('partition_count', 0)}, {str(t.get('is_partitioned', False)).lower()},
                '{schema_json}', '{parts_json}', '{t.get('partition_columns', '')}',
                'COMPLETED', current_timestamp(), 'DISCOVERED', current_timestamp()
            )
        """)
    
    return discovery


@task
def run_distcp_ssh(discovery: dict, mapr_setup: dict) -> dict:
    """Run DistCp via SSH for all tables. Uses -update for incremental."""
    
    config = get_config()
    ssh = SSHHook(ssh_conn_id=config['ssh_conn_id'])
    
    run_id = discovery['run_id']
    tables = discovery['tables']
    temp_dir = mapr_setup['temp_dir']
    mappers = config['distcp_mappers']
    bandwidth = config['distcp_bandwidth']
    
    # S3 credentials
    s3_endpoint = config['s3_endpoint']
    s3_access_key = config['s3_access_key']
    s3_secret_key = config['s3_secret_key']
    
    # Build S3 config options for distcp
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
hadoop distcp{s3_opts} -update -m {mappers} -bandwidth {bandwidth} -strategy dynamic \\
    -log {temp_dir}/distcp_{tbl}.log "{mapr_loc}" "{s3_loc}" 2>&1
echo "DISTCP_EXIT_CODE=$?"
'''
        try:
            with ssh.get_conn() as client:
                _, stdout, stderr = client.exec_command(cmd, timeout=SSH_COMMAND_TIMEOUT)
                exit_code = stdout.channel.recv_exit_status()
                output = stdout.read().decode()
                is_incr = "INCREMENTAL=true" in output
                
                if exit_code != 0:
                    raise Exception(stderr.read().decode())
                
                results.append({
                    'source_database': src_db,
                    'source_table': tbl,
                    'status': 'COMPLETED',
                    'is_incremental': is_incr,
                    'error': None
                })
        except Exception as e:
            results.append({
                'source_database': src_db,
                'source_table': tbl,
                'status': 'FAILED',
                'is_incremental': False,
                'error': str(e)[:2000]
            })
    
    return {**discovery, 'distcp_results': results}


@task.pyspark(conn_id='spark_default')
def update_distcp_status(spark, sc, distcp_result: dict) -> dict:
    """Update Iceberg tracking with DistCp results."""
    
    config = get_config()
    
    
    tracking_db = config['tracking_database']
    run_id = distcp_result['run_id']
    
    for r in distcp_result.get('distcp_results', []):
        overall = 'COPIED' if r['status'] == 'COMPLETED' else 'FAILED'
        error_msg = r.get('error', '').replace("'", "''") if r.get('error') else ''
        
        spark.sql(f"""
            UPDATE {tracking_db}.migration_table_status
            SET distcp_status = '{r['status']}',
                distcp_completed_at = current_timestamp(),
                distcp_is_incremental = {str(r['is_incremental']).lower()},
                overall_status = '{overall}',
                error_message = CASE WHEN '{r['status']}' = 'FAILED' THEN '{error_msg}' ELSE error_message END,
                updated_at = current_timestamp()
            WHERE run_id = '{run_id}'
              AND source_database = '{r['source_database']}'
              AND source_table = '{r['source_table']}'
        """)
    
    return distcp_result


@task.pyspark(conn_id='spark_default')
def create_hive_tables(spark, sc, distcp_result: dict) -> dict:
    """Create external Hive tables via Spark. Handles incremental (repairs partitions)."""
    
    dest_db = distcp_result['dest_database']
    tables = distcp_result['tables']
    
    results = []
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {dest_db}")
    
    for t in tables:
        # Skip if distcp failed
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
            # Check if exists (incremental case)
            exists = False
            try:
                spark.sql(f"DESCRIBE {full_name}")
                exists = True
            except:
                pass
            
            if exists:
                # Table exists - just repair partitions
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
                # Create new table
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
def update_table_create_status(spark, sc, table_result: dict) -> dict:
    """Update Iceberg tracking with table creation results."""
    
    config = get_config()
    
    
    tracking_db = config['tracking_database']
    run_id = table_result['run_id']
    dest_db = table_result['dest_database']
    
    for r in table_result.get('table_results', []):
        overall = 'TABLE_CREATED' if r['status'] == 'COMPLETED' else ('FAILED' if r['status'] == 'FAILED' else 'SKIPPED')
        error_msg = (r.get('error', '') or '').replace("'", "''")[:2000]
        
        spark.sql(f"""
            UPDATE {tracking_db}.migration_table_status
            SET table_create_status = '{r['status']}',
                table_create_completed_at = current_timestamp(),
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
def migrate_to_iceberg(spark, sc, table_result: dict) -> dict:
    """Convert tables to Iceberg format via CTAS. Only if convert_to_iceberg=True."""
    
    if not table_result.get('convert_to_iceberg', False):
        return {**table_result, 'iceberg_results': [], 'iceberg_skipped': True}
    
    config = get_config()
    
    dest_db = table_result['dest_database']
    tables = table_result['tables']
    
    results = []
    for t in tables:
        if not t.get('convert_to_iceberg', False):
            continue
        
        # Skip if table creation failed
        tbl_status = next(
            (r['status'] for r in table_result.get('table_results', [])
             if r['source_table'] == t['source_table']),
            'UNKNOWN'
        )
        if tbl_status != 'COMPLETED':
            results.append({
                'source_table': t['source_table'],
                'status': 'SKIPPED',
                'error': 'Table creation not completed'
            })
            continue
        
        tbl = t['source_table']
        s3_loc = t['s3_location']
        part_cols = t.get('partition_columns', '')
        is_part = t.get('is_partitioned', False)
        
        src_tbl = f"{dest_db}.{tbl}"
        ice_tbl = f"{dest_db}.{tbl}_iceberg"
        ice_loc = f"{s3_loc}_iceberg"
        
        try:
            # Check if iceberg table already exists
            ice_exists = False
            try:
                spark.sql(f"DESCRIBE {ice_tbl}")
                ice_exists = True
            except:
                pass
            
            if ice_exists:
                # Already migrated - skip
                results.append({
                    'source_table': tbl,
                    'iceberg_table': ice_tbl,
                    'status': 'COMPLETED',
                    'action': 'already_exists',
                    'error': None
                })
                continue
            
            part_clause = ""
            if is_part and part_cols:
                pcols = [p.strip() for p in part_cols.split(',') if p.strip()]
                if pcols:
                    part_clause = f"PARTITIONED BY ({', '.join(pcols)})"
            
            ddl = f"""
                CREATE TABLE IF NOT EXISTS {ice_tbl}
                USING iceberg
                {part_clause}
                LOCATION '{ice_loc}'
                AS SELECT * FROM {src_tbl}
            """
            spark.sql(ddl)
            
            # Verify counts
            src_cnt = spark.sql(f"SELECT COUNT(*) as c FROM {src_tbl}").collect()[0]['c']
            ice_cnt = spark.sql(f"SELECT COUNT(*) as c FROM {ice_tbl}").collect()[0]['c']
            
            results.append({
                'source_table': tbl,
                'iceberg_table': ice_tbl,
                'status': 'COMPLETED',
                'action': 'created',
                'src_count': src_cnt,
                'ice_count': ice_cnt,
                'counts_match': src_cnt == ice_cnt,
                'error': None
            })
        except Exception as e:
            results.append({
                'source_table': tbl,
                'iceberg_table': ice_tbl,
                'status': 'FAILED',
                'error': str(e)
            })
    
    return {**table_result, 'iceberg_results': results}


@task.pyspark(conn_id='spark_default')
def update_iceberg_status(spark, sc, iceberg_result: dict) -> dict:
    """Update Iceberg tracking with Iceberg migration results."""
    
    config = get_config()
    
    
    tracking_db = config['tracking_database']
    run_id = iceberg_result['run_id']
    dest_db = iceberg_result['dest_database']
    
    for r in iceberg_result.get('iceberg_results', []):
        if r['status'] == 'COMPLETED':
            overall = 'ICEBERG_MIGRATED'
        elif r['status'] == 'FAILED':
            overall = 'ICEBERG_FAILED'
        else:
            overall = 'TABLE_CREATED'  # skipped iceberg
        
        error_msg = (r.get('error', '') or '').replace("'", "''")[:2000]
        ice_tbl = r.get('iceberg_table', '').replace("'", "''")
        
        spark.sql(f"""
            UPDATE {tracking_db}.migration_table_status
            SET iceberg_status = '{r['status']}',
                iceberg_completed_at = current_timestamp(),
                iceberg_table_name = '{ice_tbl}',
                overall_status = CASE WHEN overall_status NOT IN ('FAILED', 'ICEBERG_FAILED') THEN '{overall}' ELSE overall_status END,
                error_message = CASE WHEN '{r['status']}' = 'FAILED' THEN '{error_msg}' ELSE error_message END,
                updated_at = current_timestamp()
            WHERE run_id = '{run_id}'
              AND dest_database = '{dest_db}'
              AND source_table = '{r['source_table']}'
        """)
    
    return iceberg_result


@task.pyspark(conn_id='spark_default')
def finalize_run(spark, sc, run_id: str) -> dict:
    """Finalize migration run - update stats in Iceberg tracking."""
    
    config = get_config()
    
    
    tracking_db = config['tracking_database']
    
    # Get counts
    stats = spark.sql(f"""
        SELECT
            COUNT(*) as total,
            SUM(CASE WHEN overall_status NOT IN ('FAILED', 'PENDING', 'ICEBERG_FAILED') THEN 1 ELSE 0 END) as successful,
            SUM(CASE WHEN overall_status IN ('FAILED', 'ICEBERG_FAILED') THEN 1 ELSE 0 END) as failed
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
def cleanup_edge(run_id: str, mapr_setup: dict) -> dict:
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
# DAG DEFINITION
# =============================================================================

with DAG(
    dag_id='mapr_to_s3_migration',
    default_args=DEFAULT_ARGS,
    description='Migrate Hive tables from MapR-FS to S3 with optional Iceberg conversion',
    schedule_interval=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['migration', 'mapr', 's3', 'hive', 'iceberg'],
    params={
        'excel_file_path': Param(
            default='s3://config-bucket/migration.xlsx',
            type='string',
            description='S3 path to Excel config file'
        )
    },
    render_template_as_native_obj=True,
) as dag:

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
    t_discover = discover_tables_ssh.partial(mapr_setup=t_mapr).expand(db_config=t_excel)
    t_record = record_discovered_tables.expand(discovery=t_discover)
    t_distcp = run_distcp_ssh.partial(mapr_setup=t_mapr).expand(discovery=t_record)
    t_distcp_status = update_distcp_status.expand(distcp_result=t_distcp)
    t_tables = create_hive_tables.expand(distcp_result=t_distcp_status)
    t_tbl_status = update_table_create_status.expand(table_result=t_tables)
    t_iceberg = migrate_to_iceberg.expand(table_result=t_tbl_status)
    t_ice_status = update_iceberg_status.expand(iceberg_result=t_iceberg)
    
    # Finalize
    t_final = finalize_run(run_id=t_run_id)
    t_cleanup = cleanup_edge(run_id=t_run_id, mapr_setup=t_mapr)
    
    # Dependencies
    t_init >> t_run_id >> t_excel >> t_mapr >> t_discover >> t_record
    t_record >> t_distcp >> t_distcp_status >> t_tables >> t_tbl_status
    t_tbl_status >> t_iceberg >> t_ice_status >> t_final >> t_cleanup