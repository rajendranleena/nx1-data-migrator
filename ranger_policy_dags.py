from airflow import DAG
from airflow.decorators import task
from airflow.models.param import Param
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
from typing import List, Dict
import uuid
from apache_ranger.client.ranger_client import RangerClient
from apache_ranger.model.ranger_policy import RangerPolicy, RangerPolicyItem, RangerPolicyItemAccess, RangerPolicyResource
from apache_ranger.exceptions import RangerServiceException

# =====================================================
# Config
# =====================================================
def get_config() -> dict:
    return {
        'spark_conn_id': Variable.get('policy_spark_conn_id', default_var='spark_default'),
        'tracking_database': Variable.get('policy_tracking_database', default_var='policy_tracking'),
        'tracking_location': Variable.get('policy_tracking_location', default_var='s3a://policy-tracking'),
        'ranger_endpoint': Variable.get('ranger_endpoint', default_var='http://ranger:6080'),
        'ranger_user': Variable.get('ranger_user', default_var='admin'),
        'ranger_password': Variable.get('ranger_password', default_var='admin'),
        'service_name': Variable.get('ranger_service_name', default_var='hive_prod')
    }

# DB/Table access mapping
DB_ACCESS_MAP = {
    "read": "select",
    "write": "update",
    "create": "create",
    "delete": "drop",
    "all": "all"
}

logger = logging.getLogger(__name__)

# =====================================================
# Shared Tasks
# =====================================================
@task.pyspark(conn_id='spark_default')
def init_tracking_tables(spark, sc) -> dict:
    """Create Iceberg tracking tables if they don't exist."""
    config = get_config()
    tracking_db = config['tracking_database']
    tracking_loc = config['tracking_location']

    spark.sql(f"CREATE DATABASE IF NOT EXISTS {tracking_db} LOCATION '{tracking_loc}'")

    # Runs table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tracking_db}.ranger_policy_runs (
            run_id STRING,
            dag_run_id STRING,
            excel_file_path STRING,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            total_policies INT,
            successful_policies INT,
            failed_policies INT,
            status STRING,
            config_json STRING
        )
        USING iceberg
        LOCATION '{tracking_loc}/ranger_policy_runs'
    """)

    # Policy status table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tracking_db}.ranger_policy_status (
            run_id STRING,
            role STRING,
            database STRING,
            table STRING,
            column STRING,
            path STRING,
            groups STRING,
            access_level STRING,
            status STRING,
            created_at TIMESTAMP,
            message STRING
        )
        USING iceberg
        LOCATION '{tracking_loc}/ranger_policy_status'
    """)

    logger.info(f"Initialized tracking tables in {tracking_loc}")
    return {"status": "initialized", "database": tracking_db}

@task
def create_policy_run() -> str:
    run_id = f"policy_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    logger.info(f"Starting policy run: {run_id}")
    return run_id

@task.pyspark(conn_id='spark_default')
def parse_policy_excel(excel_file_path: str, run_id: str, spark, sc) -> List[Dict]:
    import pandas as pd
    from io import BytesIO

    logger.info(f"Reading Excel from {excel_file_path}")
    binary_df = spark.read.format("binaryFile").load(excel_file_path)
    row = binary_df.select("content").first()
    if not row:
        raise ValueError(f"Excel file {excel_file_path} not found")

    df = pd.read_excel(BytesIO(bytes(row.content)), engine='openpyxl')
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

    required_columns = ['role', 'database', 'table', 'column', 'path/url', 'groups', 'access_level']
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        raise ValueError(f"Excel is missing required columns: {missing}")

    policies = []
    for _, row in df.iterrows():
        role = str(row.get('role', '')).strip()
        if not role:
            continue

        def parse_cell(cell):
            if cell is None or str(cell).strip() == '':
                return ['*']
            return [x.strip() for x in str(cell).split(',') if x.strip()]

        raw_access_levels = parse_cell(row.get('access_level'))

        policies.append({
            'role': role,
            'databases': parse_cell(row.get('database')),
            'tables': parse_cell(row.get('table')),
            'columns': parse_cell(row.get('column')),
            'paths': parse_cell(row.get('path/url')),
            'groups': parse_cell(row.get('groups')),
            'access_levels': raw_access_levels,
            'run_id': run_id
        })

    logger.info(f"Parsed {len(policies)} policies from Excel")
    return policies

@task
def apply_policy(policy_config: dict) -> dict:
    try:
        config = get_config()
        service_name = config['service_name']

        role = policy_config.get("role", "unknown_policy")
        databases = policy_config.get("databases") or ["*"]
        tables = policy_config.get("tables") or ["*"]
        columns = policy_config.get("columns") or ["*"]
        paths = policy_config.get("paths") or ["*"]
        groups = policy_config.get("groups") or []
        raw_access_levels = policy_config.get("access_levels") or ["read"]

        # Map access levels
        if paths != ["*"]:  # URL/path → use as is
            access_levels = raw_access_levels
        else:  # DB/Table → map
            access_levels = [DB_ACCESS_MAP.get(a.lower().strip(), a) for a in raw_access_levels]

        client = RangerClient(
            service_name,
            username=config['ranger_user'],
            password=config['ranger_password'],
            endpoint=config['ranger_endpoint']
        )

        policy_name = role

        # Check existing policy
        try:
            existing_policy = client.get_policy(service_name, policy_name)
            is_update = True
            logger.info(f"Policy '{policy_name}' exists, updating")
        except RangerServiceException:
            existing_policy = None
            is_update = False
            logger.info(f"Policy '{policy_name}' does not exist, creating")

        # Resources
        resources = {
            "database": {"values": databases},
            "table": {"values": tables},
            "column": {"values": columns},
            "path": {"values": paths}
        }

        # Policy items
        policy_items = []
        if groups:
            for group in groups:
                accesses = [RangerPolicyItemAccess({"type": a, "isAllowed": True}) for a in access_levels]
                policy_items.append(RangerPolicyItem({
                    "groups": [group],
                    "users": [],
                    "roles": [],
                    "accesses": accesses,
                    "delegateAdmin": False,
                    "conditions": []
                }))
        else:
            accesses = [RangerPolicyItemAccess({"type": a, "isAllowed": True}) for a in access_levels]
            policy_items.append(RangerPolicyItem({
                "users": ["{USER}"],
                "groups": [],
                "roles": [],
                "accesses": accesses,
                "delegateAdmin": False,
                "conditions": []
            }))

        # Create/update
        if is_update:
            existing_policy["resources"] = resources
            existing_policy["policyItems"] = policy_items
            client.update_policy(service_name, existing_policy["id"], existing_policy)
            policy_config["status"] = "SUCCESS"
            policy_config["message"] = f"Updated policy '{policy_name}'"
        else:
            policy = RangerPolicy()
            policy.service = service_name
            policy.name = policy_name
            policy.description = f"DAG run policy for {policy_name}"
            policy.resources = {k: RangerPolicyResource(v) for k, v in resources.items()}
            policy.policyItems = policy_items
            client.create_policy(policy)
            policy_config["status"] = "SUCCESS"
            policy_config["message"] = f"Created policy '{policy_name}'"

    except Exception as e:
        logger.error(f"Failed to apply policy '{policy_config.get('role')}': {e}")
        policy_config["status"] = "FAILED"
        policy_config["message"] = str(e)

    return policy_config

@task.pyspark(conn_id='spark_default')
def write_policy_status_iceberg(policy_results: List[Dict], spark, sc):
    if not policy_results:
        return
    config = get_config()
    tracking_loc = config['tracking_location']

    records = [(r['run_id'], r['role'],
                ','.join(r.get('databases',['*'])),
                ','.join(r.get('tables',['*'])),
                ','.join(r.get('columns',['*'])),
                ','.join(r.get('paths',['*'])),
                ','.join(r.get('groups',[])),
                ','.join(r.get('access_levels',[])),
                r['status'],
                datetime.utcnow(),
                r.get('message','')) for r in policy_results]

    df = spark.createDataFrame(records,
        schema=['run_id','role','database','table','column','path','groups','access_level','status','created_at','message'])

    df.write.format("iceberg").mode("append").save(f"{tracking_loc}/ranger_policy_status")
    logger.info(f"Wrote {len(policy_results)} policy statuses to Iceberg at {tracking_loc}/ranger_policy_status")

@task.pyspark(conn_id='spark_default')
def finalize_policy_run(run_id: str, dag_run_id: str, excel_file_path: str, spark, sc) -> dict:
    """Aggregate policy run results and update ranger_policy_runs table."""
    config = get_config()
    tracking_db = config['tracking_database']
    tracking_loc = config['tracking_location']

    stats = spark.sql(f"""
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN status = 'SUCCESS' THEN 1 ELSE 0 END) AS successful,
            SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) AS failed
        FROM {tracking_db}.ranger_policy_status
        WHERE run_id = '{run_id}'
    """).collect()[0]

    spark.sql(f"""
        INSERT INTO {tracking_db}.ranger_policy_runs
        VALUES (
            '{run_id}',
            '{dag_run_id}',
            '{excel_file_path}',
            current_timestamp(),
            current_timestamp(),
            {stats['total']},
            {stats['successful']},
            {stats['failed']},
            CASE WHEN {stats['failed']} > 0 THEN 'PARTIAL_FAILURE' ELSE 'COMPLETED' END,
            NULL
        )
    """)

    return {
        "run_id": run_id,
        "status": "COMPLETED" if stats['failed'] == 0 else "PARTIAL_FAILURE",
        "total": stats['total'],
        "successful": stats['successful'],
        "failed": stats['failed']
    }

@task.pyspark(conn_id='spark_default')
def fetch_failed_policies(run_id: str, spark, sc) -> List[Dict]:
    config = get_config()
    tracking_db = config['tracking_database']

    df_failed = spark.sql(
        f"SELECT * FROM {tracking_db}.ranger_policy_status WHERE run_id='{run_id}' AND status='FAILED'"
    )
    return [row.asDict() for row in df_failed.collect()]

# =====================================================
# DAG 1: Policy Creation
# =====================================================
with DAG(
    dag_id='ranger_policy_creation',
    default_args={'retries':1,'retry_delay':timedelta(minutes=5)},
    schedule_interval=None,
    start_date=datetime(2025,1,1),
    catchup=False,
    max_active_runs=1,
    params={'excel_file_path': Param(default='s3a://policy-configs/policies.xlsx', type='string')}
) as dag_creation:

    init_tables = init_tracking_tables()
    run_id = create_policy_run()
    parsed_policies = parse_policy_excel(excel_file_path="{{ params.excel_file_path }}", run_id=run_id)
    applied_policies = apply_policy.expand(policy_config=parsed_policies)
    write_status = write_policy_status_iceberg(policy_results=applied_policies)
    finalize_run = finalize_policy_run(
        run_id=run_id,
        dag_run_id="{{ run_id }}",
        excel_file_path="{{ params.excel_file_path }}"
    )

    # DAG flow
    init_tables >> run_id >> parsed_policies >> applied_policies >> write_status >> finalize_run

# =====================================================
# DAG 2: Retry Failed Policies
# =====================================================
with DAG(
    dag_id='ranger_policy_retry',
    default_args={'retries':1,'retry_delay':timedelta(minutes=5)},
    schedule_interval=None,
    start_date=datetime(2025,1,1),
    catchup=False,
    max_active_runs=1,
    params={
        'run_id': Param(default='', type='string'),
        'excel_file_path': Param(default='s3a://policy-configs/policies.xlsx', type='string')
    }
) as dag_retry:

    failed_policies = fetch_failed_policies(run_id="{{ params.run_id }}")
    applied_policies_retry = apply_policy.expand(policy_config=failed_policies)
    write_status_retry = write_policy_status_iceberg(policy_results=applied_policies_retry)
    finalize_run_retry = finalize_policy_run(
        run_id="{{ params.run_id }}",
        dag_run_id="{{ run_id }}",
        excel_file_path="{{ params.excel_file_path }}"
    )

    failed_policies >> applied_policies_retry >> write_status_retry >> finalize_run_retry

