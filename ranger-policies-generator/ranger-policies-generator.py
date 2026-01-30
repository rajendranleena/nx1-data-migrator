"""
Airflow DAG for automated Ranger policy creation from Excel sheet.
Clean version using the ranger_utils module.

This DAG:
1. Parses an Excel sheet with policy definitions
2. Creates Ranger groups (from roles in the Excel)
3. Creates Ranger policies based on parsed data
4. Creates Keycloak realm roles and assigns groups to them
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from typing import Dict, List, Any
import logging
import sys

sys.path.append('/opt/airflow/utils/migrations') 

logger = logging.getLogger(__name__)

def get_config() -> dict:
    return {
        'ranger_url': Variable.get('ranger_url'),
        'ranger_username': Variable.get('ranger_username'),
        'ranger_password': Variable.get('ranger_password', deserialize_json=False),
        'service_name': Variable.get('nx1_repo_name', default_var='nx1-unifiedsql'),
        'keycloak_url': Variable.get('keycloak_url'),
        'keycloak_realm': Variable.get('keycloak_realm'),
        'keycloak_client_id': Variable.get('keycloak_admin_client_id'),
        'keycloak_client_secret': Variable.get('keycloak_admin_client_secret', deserialize_json=False),
        'spark_conn_id': Variable.get('spark_conn_id', default_var='spark_default'),
        'tracking_database': Variable.get('policy_tracking_database', default_var='policy_tracking'),
        'tracking_location': Variable.get('policy_tracking_location', default_var='s3a://data-lake/policy_tracking'),
        'report_output_location': Variable.get('policy_report_location', default_var='s3a://data-lake/policy_reports')
    }

default_args = {
    'owner': 'trino-admin',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# -----------------------------
# DAG
# -----------------------------
with DAG(
    dag_id='ranger_policy_automation_v3',
    default_args=default_args,
    description='Automate Ranger & Keycloak policy creation from Excel configuration',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ranger', 'keycloak', 'security', 'policy-automation'],
    params={
        'excel_file_path': 's3a://your-bucket/configs/ranger_policies.xlsx',
        'dry_run': False,
    }
) as dag:

    # -----------------------------
    # Spark tasks
    # -----------------------------
    @task.pyspark(conn_id="spark_default")
    def init_policy_tracking_tables(spark, sc) -> Dict:
        config = get_config()
        db = config["tracking_database"]
        loc = config["tracking_location"]

        spark.sql(f"""
            CREATE DATABASE IF NOT EXISTS {db} LOCATION '{loc}'
        """)

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {db}.ranger_policy_runs (
                run_id STRING,
                dag_run_id STRING,
                excel_file_path STRING,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                status STRING,
                total_objects INT,
                successful_objects INT,
                failed_objects INT,
                error_message STRING,
                created_at TIMESTAMP DEFAULT current_timestamp(),
                updated_at TIMESTAMP DEFAULT current_timestamp()
            )
            USING iceberg
            LOCATION '{loc}/ranger_policy_runs'
        """)

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {db}.ranger_policy_object_status (
                run_id STRING,
                object_type STRING,
                object_name STRING,
                object_key STRING,
                status STRING,
                error_message STRING,
                attempt INT DEFAULT 1,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                created_at TIMESTAMP DEFAULT current_timestamp(),
                updated_at TIMESTAMP DEFAULT current_timestamp()
            )
            USING iceberg
            LOCATION '{loc}/ranger_policy_object_status'
        """)

        return {"status": "initialized", "tracking_db": db}

    @task.pyspark(conn_id="spark_default")
    def create_policy_run(excel_file_path: str, dag_run_id: str, spark, sc) -> str:
        import uuid
        run_id = f"run_{datetime.utcnow():%Y%m%d_%H%M%S}_{uuid.uuid4().hex[:8]}"
        db = get_config()["tracking_database"]

        spark.sql(f"""
            INSERT INTO {db}.ranger_policy_runs
            VALUES (
                '{run_id}', '{dag_run_id}', '{excel_file_path}',
                current_timestamp(), NULL, 'RUNNING',
                0, 0, 0, NULL,
                current_timestamp(), current_timestamp()
            )
        """)

        return run_id

    @task.pyspark(conn_id='spark_default')
    def parse_excel_to_dicts(excel_file_path: str, run_id: str, spark, sc) -> Dict[str, Any]:
        import pandas as pd
        from io import BytesIO
        # Read Excel from S3
        binary_df = spark.read.format("binaryFile").load(excel_file_path)
        row = binary_df.select("content").first()
        excel_bytes = bytes(row.content)
        df = pd.read_excel(BytesIO(excel_bytes), engine='openpyxl')
        df.columns = df.columns.str.lower().str.strip()

        policies = {}
        role_groups = {}

        for _, row in df.iterrows():
            role = str(row.get('role', '')).strip()
            databases = str(row.get('database', '')).strip()
            tables = str(row.get('tables', '*')).strip() or '*'
            columns = str(row.get('columns', '*')).strip() or '*'
            url = str(row.get('url', '')).strip()
            permissions = str(row.get('permissions', 'read')).strip()
            groups = str(row.get('groups', '')).strip()

            if not role or role.lower() == 'nan':
                continue

            # Role-groups
            group_list = parse_csv_field(groups)
            if group_list:
                role_groups.setdefault(role, [])
                for g in group_list:
                    if g not in role_groups[role]:
                        role_groups[role].append(g)

            perm_list = parse_permission_string(permissions)

            if url and url.lower() != 'nan':
                policy_name = url
                if policy_name not in policies:
                    policies[policy_name] = {'type': 'url', 'url': url, 'roles': []}
                existing_roles = [r['role'] for r in policies[policy_name]['roles']]
                if role not in existing_roles:
                    policies[policy_name]['roles'].append({'role': role, 'permissions': perm_list})
                else:
                    for r in policies[policy_name]['roles']:
                        if r['role'] == role:
                            for p in perm_list:
                                if p not in r['permissions']:
                                    r['permissions'].append(p)
            else:
                db_list = parse_csv_field(databases)
                table_list = parse_csv_field(tables) or ['*']
                column_list = parse_csv_field(columns) or ['*']
                for database in db_list:
                    if not database or database.lower() == 'nan':
                        continue
                    for table in table_list:
                        for column in column_list:
                            policy_name = build_policy_name('iceberg', database, table, column)
                            if policy_name not in policies:
                                policies[policy_name] = {
                                    'type': 'table',
                                    'catalog': 'iceberg',
                                    'schema': database,
                                    'table': table or '*',
                                    'column': column or '*',
                                    'roles': []
                                }
                            existing_roles = [r['role'] for r in policies[policy_name]['roles']]
                            if role not in existing_roles:
                                policies[policy_name]['roles'].append({'role': role, 'permissions': perm_list})
                            else:
                                for r in policies[policy_name]['roles']:
                                    if r['role'] == role:
                                        for p in perm_list:
                                            if p not in r['permissions']:
                                                r['permissions'].append(p)

        logger.info(f"Parsed {len(policies)} policies and {len(role_groups)} role-group mappings")
        return {'policies': policies, 'role_groups': role_groups}

    def parse_permission_string(permissions: str) -> List[str]:
        """Parse permission string into list."""
        if not permissions or str(permissions).lower() == 'nan':
            return ['read']
        return [p.strip().lower() for p in str(permissions).split(',') if p.strip()]


    def parse_csv_field(value: str) -> List[str]:
        """Parse comma-separated field into list."""
        if not value or str(value).lower() == 'nan':
            return []
        return [v.strip() for v in str(value).split(',') if v.strip()]


    def build_policy_name(catalog: str, database: str, table: str, column: str) -> str:
        """
        Build policy name following the convention:
        iceberg.${database}.${table if not *}.${column if not *}
        """
        parts = [catalog, database]
        
        if table and table != '*':
            parts.append(table)
            if column and column != '*':
                parts.append(column)
        
        return '.'.join(parts)

    @task.pyspark(conn_id="spark_default")
    def write_policy_object_status(object_status: dict, spark, sc) -> dict:
        from datetime import datetime
        cfg = get_config()
        db = cfg["tracking_database"]

        run_id = object_status["run_id"]
        obj_type = object_status["object_type"]
        obj_name = object_status["object_name"]
        obj_key = object_status.get("object_key", "")
        status = object_status.get("status", "RUNNING")
        error_msg = object_status.get("error_message", "")
        started_at = object_status.get("started_at", datetime.utcnow())
        completed_at = object_status.get("completed_at", None)
        attempt = object_status.get("attempt", 1)
        created_at = datetime.utcnow()

        error_msg_sql = error_msg.replace("'", "''")
        completed_at_sql = f"'{completed_at}'" if completed_at else "NULL"

        spark.sql(f"""
            MERGE INTO {db}.ranger_policy_object_status t
            USING (SELECT '{run_id}' AS run_id, '{obj_type}' AS object_type, '{obj_name}' AS object_name) s
            ON t.run_id = s.run_id AND t.object_type = s.object_type AND t.object_name = s.object_name
            WHEN MATCHED THEN UPDATE SET
                status = '{status}',
                error_message = '{error_msg_sql}',
                completed_at = {completed_at_sql},
                attempt = {attempt},
                updated_at = current_timestamp()
            WHEN NOT MATCHED THEN INSERT (
                run_id, object_type, object_name, object_key,
                status, error_message, started_at, completed_at,
                attempt, created_at
            ) VALUES (
                '{run_id}', '{obj_type}', '{obj_name}', '{obj_key}',
                '{status}', '{error_msg_sql}', '{started_at}', {completed_at_sql},
                {attempt}, '{created_at}'
            )
        """)

        return object_status

    # -----------------------------
    # Ranger & Keycloak tasks returning list of statuses
    # -----------------------------
    @task
    def create_ranger_groups_and_policies(parsed_data: Dict[str, Any], run_id: str) -> List[Dict]:
        from ranger_utils import RangerPolicyManager
        from datetime import datetime

        cfg = get_config()
        manager = RangerPolicyManager(
            ranger_url=cfg["ranger_url"],
            ranger_username=cfg["ranger_username"],
            ranger_password=cfg["ranger_password"],
            service_name=cfg["service_name"]
        )

        policies = parsed_data['policies']
        role_groups = parsed_data['role_groups']

        statuses = []

        # Ensure groups
        all_roles = set(role_groups.keys())
        for policy_data in policies.values():
            for r in policy_data.get('roles', []):
                all_roles.add(r['role'])

        groups_result = manager.ensure_groups_exist(list(all_roles))
        for g, created in groups_result.items():
            statuses.append({
                "run_id": run_id,
                "object_type": "group",
                "object_name": g,
                "status": "SUCCESS",
                "started_at": datetime.utcnow(),
                "completed_at": datetime.utcnow()
            })

        # Sync policies
        policies_result = manager.sync_policies_from_dict(policies)
        for obj_list in ['created', 'updated', 'failed']:
            for p in policies_result.get(obj_list, []):
                statuses.append({
                    "run_id": run_id,
                    "object_type": "policy",
                    "object_name": p['name'],
                    "status": "FAILED" if obj_list == "failed" else "SUCCESS",
                    "error_message": p.get('error', ''),
                    "started_at": datetime.utcnow(),
                    "completed_at": datetime.utcnow()
                })

        return statuses

    @task
    def create_keycloak_roles(parsed_data: Dict[str, Any], run_id: str) -> List[Dict]:
        from ranger_utils import KeycloakRoleManager
        from datetime import datetime

        cfg = get_config()
        manager = KeycloakRoleManager(
            server_url=cfg["keycloak_url"],
            realm_name=cfg["keycloak_realm"],
            client_id=cfg["keycloak_client_id"],
            client_secret=cfg["keycloak_client_secret"]
        )

        role_groups = parsed_data['role_groups']
        result = manager.sync_roles_and_groups(role_groups)

        statuses = []
        for obj_type, obj_list in [("role", ['created_roles','existing_roles']), ("mapping", ['created_mappings','existing_mappings','failed'])]:
            for key in obj_list:
                for entry in result.get(key, []):
                    name = entry if isinstance(entry, str) else f"{entry.get('role','')}->{entry.get('group','')}"
                    statuses.append({
                        "run_id": run_id,
                        "object_type": obj_type if obj_type=="role" else "mapping",
                        "object_name": name,
                        "status": "FAILED" if key=="failed" else "SUCCESS",
                        "error_message": entry.get('error','') if isinstance(entry, dict) else "",
                        "started_at": datetime.utcnow(),
                        "completed_at": datetime.utcnow()
                    })
        return statuses

    @task.pyspark(conn_id="spark_default")
    def generate_policy_report(run_id: str, spark, sc) -> str:
        """
        Generate a multi-row formatted HTML report for Ranger & Keycloak policy run.
        Includes object counts, status badges, and error messages.
        Saves report to S3.
        """
        from datetime import datetime
        from collections import Counter

        config = get_config()
        tracking_db = config["tracking_database"]
        report_location = config["report_output_location"]

        # Fetch run-level info
        run_info = spark.sql(f"""
            SELECT * FROM {tracking_db}.ranger_policy_runs
            WHERE run_id = '{run_id}'
        """).collect()[0]

        # Fetch object-level info
        objects = spark.sql(f"""
            SELECT * FROM {tracking_db}.ranger_policy_object_status
            WHERE run_id = '{run_id}'
            ORDER BY object_type, object_name
        """).collect()

        total_objects = len(objects)
        successful_objects = sum(1 for o in objects if o.status == "SUCCESS")
        failed_objects = sum(1 for o in objects if o.status == "FAILED")
        type_counts = Counter(o.object_type for o in objects)

        # Build HTML
        html = f"""
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <title>Ranger Policy Run Report - {run_id}</title>
            <style>
                body {{ font-family: Arial, sans-serif; background: #f5f5f5; padding: 20px; }}
                .container {{ background: #fff; padding: 20px; border-radius: 8px; }}
                h1 {{ color: #2c3e50; }}
                .summary-card {{ display: inline-block; padding: 15px; margin: 5px; border-radius: 6px; color: #fff; }}
                .success {{ background-color: #28a745; }}
                .failed {{ background-color: #dc3545; }}
                .info {{ background-color: #17a2b8; }}
                table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
                th, td {{ border: 1px solid #ddd; padding: 8px; text-align: left; }}
                th {{ background-color: #343a40; color: white; }}
                tr:hover {{ background-color: #f1f1f1; }}
                .status-badge {{ padding: 4px 8px; border-radius: 12px; font-weight: bold; color: white; }}
                .status-success {{ background-color: #28a745; }}
                .status-failed {{ background-color: #dc3545; }}
                .status-partial {{ background-color: #ffc107; color: #212529; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>Ranger Policy Run Report</h1>
                <p>
                    <strong>Run ID:</strong> {run_id}<br>
                    <strong>DAG Run:</strong> {run_info.dag_run_id}<br>
                    <strong>Excel Path:</strong> {run_info.excel_file_path}<br>
                    <strong>Started At:</strong> {run_info.started_at}<br>
                    <strong>Completed At:</strong> {run_info.completed_at}
                </p>

                <div class="summary-cards">
                    <div class="summary-card info">TOTAL OBJECTS: {total_objects}</div>
                    <div class="summary-card success">SUCCESSFUL: {successful_objects}</div>
                    <div class="summary-card failed">FAILED: {failed_objects}</div>
                </div>

                <h2>Object Counts by Type</h2>
                <ul>
        """
        for obj_type, count in type_counts.items():
            html += f"<li>{obj_type}: {count}</li>"

        html += """
                </ul>

                <h2>Policy Object Details</h2>
                <table>
                    <thead>
                        <tr>
                            <th>Object Type</th>
                            <th>Object Name</th>
                            <th>Object Key</th>
                            <th>Status</th>
                            <th>Error Message</th>
                            <th>Attempt</th>
                            <th>Started At</th>
                            <th>Completed At</th>
                        </tr>
                    </thead>
                    <tbody>
        """

        for o in objects:
            status_class = (
                "status-success" if o.status == "SUCCESS"
                else "status-failed" if o.status == "FAILED"
                else "status-partial"
            )
            html += f"""
                        <tr>
                            <td>{o.object_type}</td>
                            <td>{o.object_name}</td>
                            <td>{o.object_key or ''}</td>
                            <td><span class="status-badge {status_class}">{o.status}</span></td>
                            <td>{o.error_message or ''}</td>
                            <td>{o.attempt}</td>
                            <td>{o.started_at}</td>
                            <td>{o.completed_at or ''}</td>
                        </tr>
            """

        html += f"""
                    </tbody>
                </table>

                <p style="margin-top:20px; font-size:12px; color:#6c757d;">
                    Report generated automatically by the Ranger Policy DAG on {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC.
                </p>
            </div>
        </body>
        </html>
        """

        # Save HTML to S3
        report_path = f"{report_location}/{run_id}_policy_report.html"
        spark.createDataFrame([(html,)], ["content"]).coalesce(1).write.mode("overwrite").text(report_path)

        return report_path
		
    # -----------------------------
    # DAG flow
    # -----------------------------
    excel_path = "{{ params.excel_file_path }}"
    dag_run_id = "{{ run_id }}"

    init_tables = init_policy_tracking_tables()
    run_id = create_policy_run(excel_path, dag_run_id)
    parsed_data = parse_excel_to_dicts(excel_path, run_id)

    # Task outputs
    ranger_statuses = create_ranger_groups_and_policies(parsed_data, run_id)
    keycloak_statuses = create_keycloak_roles(parsed_data, run_id)

    # Write statuses (dynamic map)
    write_ranger = write_policy_object_status.expand(object_status=ranger_statuses)
    write_keycloak = write_policy_object_status.expand(object_status=keycloak_statuses)

    # Generate report
    report_path = generate_policy_report(run_id)

    # Dependencies
    init_tables >> run_id >> parsed_data
    parsed_data >> [ranger_statuses, keycloak_statuses]
    [write_ranger, write_keycloak] >> report_path
