"""
Combined Migration DAGs

This file contains two independent DAGs:
1. mapr_to_s3_migration: Migrates Hive tables from MapR-FS to S3
2. iceberg_migration: Converts existing Hive tables in S3 to Apache Iceberg format

Both DAGs can be run independently. The iceberg_migration DAG is typically run
after mapr_to_s3_migration is complete, but they are not automatically chained.

1. MapR to S3 Migration DAG

Orchestrates migration of Hive tables from MapR-FS to S3:
- Excel config from S3 (only DAG parameter)
- SSH operations for MapR token, beeline discovery, distcp (24h timeout)
- PySpark tasks for Hive table creation
- Incremental support (distcp -update, table repair)

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

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.models.param import Param
from airflow.providers.ssh.hooks.ssh import SSHHook

# =============================================================================
# SHARED CONFIGURATION
# =============================================================================

def get_config() -> dict:
    """Shared configuration for both DAGs"""
    return {
        # SSH Configuration (for MapR migration)
        'ssh_conn_id': Variable.get('mapr_ssh_conn_id', default_var='mapr_edge_ssh'),
        'edge_temp_path': Variable.get('mapr_edge_temp_path', default_var='/tmp/migration'),
        'hive_server_host': Variable.get('mapr_hive_server_host', default_var='hiveserver2.mapr.local'),
        'hive_server_port': Variable.get('mapr_hive_server_port', default_var='10000'),
        'hive_auth': Variable.get('mapr_hive_auth', default_var='maprsasl'),
        
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
            discovery_status STRING,
            discovery_completed_at TIMESTAMP,
            distcp_status STRING,
            distcp_started_at TIMESTAMP,
            distcp_completed_at TIMESTAMP,
            distcp_is_incremental BOOLEAN,
            table_create_status STRING,
            table_create_completed_at TIMESTAMP,
            table_already_existed BOOLEAN,
            overall_status STRING,
            error_message STRING,
            updated_at TIMESTAMP
        )
        USING iceberg
        LOCATION '{tracking_loc}/migration_table_status'
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
def discover_tables_ssh(mapr_setup: dict, db_config: dict) -> dict:
    """Use beeline via SSH to discover tables and metadata."""
    config = get_config()
    ssh = SSHHook(ssh_conn_id=config['ssh_conn_id'])
    
    run_id = db_config['run_id']
    src_db = db_config['source_database']
    pattern = db_config['table_pattern']
    dest_db = db_config['dest_database']
    dest_bucket = db_config['dest_bucket']
    temp_dir = mapr_setup['temp_dir']
    
    hive_host = config['hive_server_host']
    hive_port = config['hive_server_port']
    hive_auth = config['hive_auth']
    output_file = f"{temp_dir}/{src_db}_tables.json"
    
    script = f'''#!/bin/bash
set -e

JDBC="jdbc:hive2://{hive_host}:{hive_port}/{src_db};auth={hive_auth}"

if [ "{pattern}" = "*" ]; then
    TABLES=$(beeline -u "$JDBC" --silent=true --outputformat=csv2 \
        -e "SHOW TABLES IN {src_db};" 2>/dev/null \
        | tail -n +2 | tr -d '"')
else
    LIKE=$(echo "{pattern}" | sed 's/\\*/%/g')
    TABLES=$(beeline -u "$JDBC" --silent=true --outputformat=csv2 \
        -e "SHOW TABLES IN {src_db} LIKE '$LIKE';" 2>/dev/null \
        | tail -n +2 | tr -d '"')
fi

echo "[" > {output_file}
FIRST=true

for TBL in $TABLES; do
    [ -z "$TBL" ] && continue

    DESC=$(beeline -u "$JDBC" --silent=true \
        -e "DESCRIBE FORMATTED {src_db}.$TBL;" 2>/dev/null)

    LOC=$(echo "$DESC" | awk -F'|' '/Location:/ {{gsub(/^ +| +$/,"",$3); print $3; exit}}')

    INFMT=$(echo "$DESC" | grep "InputFormat:" | head -1)
    if echo "$INFMT" | grep -qi parquet; then
        FMT="PARQUET"
    elif echo "$INFMT" | grep -qi orc; then
        FMT="ORC"
    elif echo "$INFMT" | grep -qi avro; then
        FMT="AVRO"
    else
        FMT="PARQUET"
    fi

    PARTS=$(beeline -u "$JDBC" --silent=true --outputformat=csv2 \
        -e "SHOW PARTITIONS {src_db}.$TBL" 2>/dev/null \
        | tail -n +2)

    PART_COUNT=$(echo "$PARTS" | wc -l | tr -d ' ')

    PCOLS=$(echo "$PARTS" \
        | awk -F'/' '{{for(i=1;i<=NF;i++){{split($i,a,"="); print a[1]}}}}' \
        | sort -u \
        | tr '\\n' ',' | sed 's/,$//')

    COLS=$(beeline -u "$JDBC" --silent=true --outputformat=csv2 \
        -e "DESCRIBE {src_db}.$TBL;" 2>/dev/null \
        | tail -n +2 \
        | grep -v "^#" \
        | while IFS=',' read -r cn ct rest; do
            cn=$(echo "$cn" | tr -d '" ')
            ct=$(echo "$ct" | tr -d '" ')
            [ -z "$cn" ] && continue
            if [ -n "$PCOLS" ] && echo "$PCOLS" | tr ',' '\\n' | grep -qx "$cn"; then
                continue
            fi
            echo "{{\\"name\\":\\"$cn\\",\\"type\\":\\"$ct\\"}}"
        done | paste -sd "," -)

    IS_PART=false
    PJSON=""
    if [ "$PART_COUNT" -gt 0 ]; then
        IS_PART=true
        PJSON=$(echo "$PARTS" | while read p; do echo "\\"$p\\""; done | paste -sd "," -)
    fi

    S3LOC="{dest_bucket}/{dest_db}/$TBL"

    [ "$FIRST" = true ] && FIRST=false || echo "," >> {output_file}

    cat >> {output_file} << EOF
{{"source_database":"{src_db}","source_table":"$TBL","dest_database":"{dest_db}","dest_bucket":"{dest_bucket}","mapr_location":"$LOC","s3_location":"$S3LOC","file_format":"$FMT","schema":[$COLS],"partitions":[$PJSON],"partition_columns":"$PCOLS","partition_count":$PART_COUNT,"is_partitioned":$IS_PART}}
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
        'tables': tables
    }


@task.pyspark(conn_id='spark_default')
def record_discovered_tables(discovery: dict, spark, sc) -> dict:
    """Record discovered tables in Iceberg tracking table."""
    config = get_config()
    tracking_db = config['tracking_database']
    run_id = discovery['run_id']
    
    for t in discovery['tables']:
        parts = t.get('partitions', [])
        if isinstance(parts, str):
            parts = [p for p in parts.split(',') if p]
        
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
                dest_location, mapr_location, file_format,
                partition_count, is_partitioned, schema_json, partitions_json,
                partition_columns, discovery_status, discovery_completed_at,
                overall_status, updated_at
            ) VALUES (
                '{run_id}', '{t['source_database']}', '{t['source_table']}',
                '{t['dest_database']}', '{t['dest_bucket']}', '{t['s3_location']}',
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
def update_distcp_status(distcp_result: dict, spark, sc) -> dict:
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
# DAG 2: ICEBERG MIGRATION TASKS
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
        CREATE TABLE IF NOT EXISTS {tracking_db}.iceberg_migration_status (
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
            status STRING,
            source_row_count BIGINT,
            destination_row_count BIGINT,
            counts_match BOOLEAN,
            error_message STRING,
            updated_at TIMESTAMP,
            parent_migration_run_id STRING
        )
        USING iceberg
        LOCATION '{tracking_loc}/iceberg_migration_status'
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
            
            if inplace:
                migration_type = "INPLACE"
                dest_table = f"{src_db}.{tbl}"
                spark.sql(f"CALL spark_catalog.system.migrate('{src_db}.{tbl}')")
            else:
                migration_type = "SNAPSHOT"
                dest_table = f"{dest_db}.{tbl}"
                spark.sql(f"CALL spark_catalog.system.snapshot('{src_db}.{tbl}', '{dest_db}.{tbl}')")
            
            iceberg_count = spark.sql(f"SELECT COUNT(*) as c FROM {dest_table}").collect()[0]['c']
            counts_match = (hive_count == iceberg_count)
            
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
                'error': None
            })
            
            spark.sql(f"""
                INSERT INTO {tracking_db}.iceberg_migration_status
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
                    'COMPLETED',
                    {hive_count},
                    {iceberg_count},
                    {str(counts_match).lower()},
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
                INSERT INTO {tracking_db}.iceberg_migration_status
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
                    'FAILED',
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
            SUM(CASE WHEN counts_match = false THEN 1 ELSE 0 END) as count_mismatches
        FROM {tracking_db}.iceberg_migration_status
        WHERE run_id = '{run_id}'
    """).collect()[0]

    migration_type_result = spark.sql(f"""
        SELECT migration_type, COUNT(*) as cnt
        FROM {tracking_db}.iceberg_migration_status
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
    t_discover = discover_tables_ssh.partial(mapr_setup=t_mapr).expand(db_config=t_excel)
    t_record = record_discovered_tables.expand(discovery=t_discover)
    t_distcp = run_distcp_ssh.partial(mapr_setup=t_mapr).expand(discovery=t_record)
    t_distcp_status = update_distcp_status.expand(distcp_result=t_distcp)
    t_tables = create_hive_tables.expand(distcp_result=t_distcp_status)
    t_tbl_status = update_table_create_status.expand(table_result=t_tables)
    
    # Finalize
    t_final = finalize_run(run_id=t_run_id)
    t_cleanup = cleanup_edge(mapr_setup=t_mapr, run_id=t_run_id)
    
    # Dependencies
    t_init >> t_run_id >> t_excel >> t_mapr >> t_discover >> t_record
    t_record >> t_distcp >> t_distcp_status >> t_tables >> t_tbl_status
    t_tbl_status >> t_final >> t_cleanup


# =============================================================================
# DAG 2 DEFINITION: ICEBERG MIGRATION
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
    
    # Finalize
    t_ice_final = finalize_iceberg_run(run_id=t_ice_run_id)
    
    # Dependencies
    t_ice_init >> t_ice_run_id >> t_ice_excel >> t_ice_parent_lookup >> t_ice_update_parent
    t_ice_excel >> t_ice_discover
    [t_ice_discover, t_ice_parent_lookup] >> t_ice_migrate >> t_ice_final