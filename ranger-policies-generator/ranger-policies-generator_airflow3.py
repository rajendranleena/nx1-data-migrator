"""
Airflow DAG for automated Ranger policy and Keycloak role management from Excel sheet.
Makes use of the ranger_utils module for the Ranger and Keycloak client APIs.

This DAG:
1. Parses an Excel sheet with policy definitions (including users, groups, rowfilter, and wildcards)
2. Creates Ranger groups (from roles and groups in the Excel)
3. Creates Ranger policies based on parsed data
4. Creates Keycloak realm roles and assigns groups to them
5. Tracks all actions and statuses in robust Iceberg tables for auditing
6. Generates an HTML report summarizing the run, policy statuses, and object-level results

"""
import sys
from datetime import datetime, timedelta, timezone
from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule 
from typing import Dict, List, Any
import logging

sys.path.append('/opt/airflow/utils/migrations') 

logger = logging.getLogger(__name__)

# Default args for DAG
default_args = {
    'owner': 'trino-admin',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Keycloak-specific task defaults (more retries due to external dependency)
keycloak_task_args = {
    'owner': 'trino-admin',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 4,
    'retry_delay': timedelta(minutes=2),  # Shorter delays, more attempts
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=10),
}
def sql_str(value: str) -> str:
    return value.replace("'", "''") if value else ""

def parse_bool_config(value: Any, default: bool = True) -> bool:
    """Parse bool-like config values from Airflow Variables."""
    if value is None:
        return default
    if isinstance(value, bool):
        return value
    text = str(value).strip().lower()
    if text in {'true', '1', 'yes', 'y', 'on'}:
        return True
    if text in {'false', '0', 'no', 'n', 'off'}:
        return False
    return default

def is_empty_like(value: Any) -> bool:
    """Return True for empty/null-like values coming from Excel/Pandas."""
    if value is None:
        return True
    text = str(value).strip()
    if not text:
        return True
    return text.lower() in {'nan', 'null', 'none'}

def validate_rowfilter(rowfilter: str) -> str:
    """
    Reject rowfilter values containing dangerous SQL characters (e.g., ; or newlines).
    Returns the rowfilter if valid, else raises ValueError.
    """
    if rowfilter:
        if any(c in rowfilter for c in [';', '\n', '\r']):
            raise ValueError(f"Rowfilter contains forbidden characters (semicolon or newline): {rowfilter}")
    return rowfilter

def get_config() -> dict:
    return {
        'ranger_url': Variable.get('ranger_url', default_var='http://ranger:6080'),
        'ranger_username': Variable.get('ranger_username'),
        'ranger_password': Variable.get('ranger_password', deserialize_json=False),
        'service_name': Variable.get('nx1_repo_name', default_var='nx1-unifiedsql'),
        'keycloak_url': Variable.get('keycloak_url'),
        'keycloak_realm': Variable.get('keycloak_realm'),
        'keycloak_client_id': Variable.get('keycloak_admin_client_id'),
        'keycloak_client_secret': Variable.get('keycloak_admin_client_secret', deserialize_json=False),
        'keycloak_verify_ssl': parse_bool_config(Variable.get('keycloak_verify_ssl', default_var='false'), default=False),
        'keycloak_cacert': Variable.get('keycloak_cacert', default_var=''),
        'smtp_conn_id': Variable.get('policy_smtp_conn_id', default_var='smtp_default'),
        'email_recipients': Variable.get('policy_email_recipients', default_var=''),
        'spark_conn_id': Variable.get('spark_conn_id', default_var='spark_default'),
        'tracking_database': Variable.get('policy_tracking_database', default_var='policy_tracking'),
        'tracking_location': Variable.get('policy_tracking_location', default_var='s3a://data-lake/policy_tracking'),
        'report_output_location': Variable.get('policy_report_location', default_var='s3a://data-lake/policy_reports')
    }

def parse_permission_string(permissions: str) -> List[str]:
    """Parse permission string into list."""
    if is_empty_like(permissions):
        return ['read']
    
    perm_list = [p.strip().lower() for p in str(permissions).split(',') if p.strip()]
    
    valid_perms = {'read', 'write', 'all', 'select', 'insert', 'update', 'delete', 'create', 'drop', 'alter', 'use', 'show', 'grant', 'revoke', 'execute', 'impersonate', 'read_sysinfo', 'write_sysinfo'}
    invalid_perms = [p for p in perm_list if p not in valid_perms]
    
    if invalid_perms:
        logger.warning(f"Unknown permission types found: {invalid_perms}. They will be passed as-is to Ranger.")
    
    return perm_list if perm_list else ['read']


def parse_csv_field(value: str) -> List[str]:
    """Parse comma-separated field into list."""
    if is_empty_like(value):
        return []
    return [v.strip() for v in str(value).split(',') if v.strip()]


def build_policy_name(catalog: str, database: str, table: str, column: str) -> str:
    """
    Build policy name following the convention:
    If database is '*', policy name is just <catalog>
    Otherwise, it's a dot-separated string of non-wildcard parts:
    iceberg.${database}.${table if not *}.${column if not *}
    """
    if database == '*':
        return catalog
    parts = [catalog, database]
    if table and table != '*':
        parts.append(table)
        if column and column != '*':
            parts.append(column)
    return '.'.join(parts)


with DAG(
    dag_id='ranger_policy_automation',
    default_args=default_args,
    description='Automate Ranger & Keycloak policy creation from Excel configuration',
    schedule=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ranger', 'keycloak', 'security', 'policy-automation'],
    params={
        'excel_file_path': 's3a://your-bucket/configs/ranger_policies.xlsx'
    }
) as dag:

    @task
    def build_initial_policy_statuses(parsed_data: Dict[str, Any], tracking_run_id: str) -> List[Dict]:
        from datetime import datetime as dt, timezone
        policies = parsed_data.get('policies', {})
        now = dt.now(timezone.utc)
        status_list = []
        for policy_name, pdata in policies.items():
            users = []
            groups = []
            permissions = []
            rowfilter = None
            for r in pdata.get('roles', []):
                role = r.get('role')
                users.extend(r.get('users', []))
                if not is_empty_like(role):
                    groups.append(role)
                permissions.extend(r.get('permissions', []))
                if r.get('rowfilter'):
                    if rowfilter and rowfilter != r['rowfilter']:
                        raise ValueError(
                            f"Conflicting rowfilters for policy {policy_name}: "
                            f"{rowfilter} vs {r['rowfilter']}"
                        )
                    rowfilter = r['rowfilter']
            status_list.append({
                'run_id': tracking_run_id,
                'policy_id': policy_name,
                'policy_name': policy_name,
                'users': list(set(users)),
                'groups': list(set(groups)),
                'permissions': list(set(permissions)),
                'rowfilter': rowfilter,
                'status': 'RUNNING',
                'error_message': '',
                'created_at': now,
                'updated_at': now,
                'attributes': None
            })
        return status_list



    @task.pyspark(conn_id='spark_default')
    def parse_excel_to_dicts(excel_file_path: str, spark) -> Dict[str, Any]:
        import pandas as pd
        from io import BytesIO
        
        # Read Excel from S3
        binary_df = spark.read.format("binaryFile").load(excel_file_path)
        row = binary_df.select("content").first()
        excel_bytes = bytes(row.content)
        df = pd.read_excel(BytesIO(excel_bytes), engine='openpyxl')
        df.columns = df.columns.str.lower().str.strip()

        policies = {}
        role_principals = {}
        skipped_rows = []

        for idx, row in df.iterrows():
            role = str(row.get('role', '')).strip()
            if is_empty_like(role):
                role = ''
                logger.debug(f"Row {idx}: Role field was empty/null-like, treating as empty")
            databases = str(row.get('database', '')).strip()
            tables = str(row.get('tables', '*')).strip() or '*'
            columns = str(row.get('columns', '*')).strip() or '*'
            url = str(row.get('url', '')).strip()
            permissions = str(row.get('permissions', 'read')).strip()
            groups = str(row.get('groups', '')).strip()
            users = str(row.get('users', '')).strip()  
            rowfilter = str(row.get('rowfilter', '')).strip()  
            policy_label_override = str(row.get('policy_label', '')).strip()
            if is_empty_like(rowfilter):
                rowfilter = ''
                logger.debug(f"Row {idx}: Rowfilter field was empty/null-like, treating as empty")
            # Enforce: Either database or url must be provided, not both or neither
            has_db = not is_empty_like(databases)
            has_url = bool(not is_empty_like(url) and url != '-' and url != '*')
            if (has_db and has_url) or (not has_db and not has_url):
                reason = f"Row must have either database or url (but not both/neither). Skipped. role={role}, database={databases}, url={url}"
                logger.error(reason)
                skipped_rows.append({'row_index': idx, 'role': role, 'database': databases, 'url': url, 'reason': reason})
                continue
            # Validate: Rowfilters are only valid for database policies, not URL policies
            if has_url and rowfilter:
                reason = f"Rowfilters are not supported for URL-based policies. Skipped. url={url}, rowfilter={rowfilter}"
                logger.error(reason)
                skipped_rows.append({'row_index': idx, 'role': role, 'database': databases, 'url': url, 'reason': reason})
                continue
            group_list = parse_csv_field(groups)
            user_list = parse_csv_field(users)
            if role:
                role_principals.setdefault(role, {'groups': [], 'users': []}) 
            if group_list:
                if not role: # If group input is present, role is mandatory
                    reason = f"Row with groups {group_list} must have a role. Skipping row."
                    logger.error(reason)
                    skipped_rows.append({'row_index': idx, 'role': role, 'database': databases, 'url': url, 'reason': reason})
                    continue
                for g in group_list:
                    if g not in role_principals[role]['groups']:
                        role_principals[role]['groups'].append(g)
            if user_list and role:
                for u in user_list:
                    if u not in role_principals[role]['users']:
                        role_principals[role]['users'].append(u)
            # If only users are present and no group, and no role is specified, create role_<username> for each user
            if user_list and not group_list and is_empty_like(role):
                for u in user_list:
                    user_role = f"role_{u}"
                    role_principals.setdefault(user_role, {'groups': [], 'users': []})
                    if u not in role_principals[user_role]['users']:
                        role_principals[user_role]['users'].append(u)

            # Build effective role bindings for policy assignment (normalized).
            # - explicit role: one role bound to all row users
            # - users-only row: one synthetic role per user (role_<user>)
            if role:
                effective_role_bindings = [{'role': role, 'users': user_list}]
            elif user_list and not group_list:
                effective_role_bindings = [{'role': f"role_{u}", 'users': [u]} for u in user_list]
            else:
                reason = (
                    f"Row does not define a valid role binding. Skipped. "
                    f"role={role}, groups={group_list}, users={user_list}"
                )
                logger.error(reason)
                skipped_rows.append({
                    'row_index': idx,
                    'role': role,
                    'database': databases,
                    'url': url,
                    'reason': reason,
                })
                continue

            perm_list = parse_permission_string(permissions)

            if not is_empty_like(url) and url != '-' and url != '*':
                policy_name = url
                policy_label = policy_label_override if policy_label_override else None
                if policy_name not in policies:
                    policies[policy_name] = {'type': 'url', 'url': url, 'roles': [], 'label': policy_label}
                for binding in effective_role_bindings:
                    effective_role = binding['role']
                    effective_users = binding['users']
                    existing_roles = [r['role'] for r in policies[policy_name]['roles']]
                    if effective_role not in existing_roles:
                        policies[policy_name]['roles'].append({'role': effective_role, 'permissions': perm_list, 'groups': group_list, 'users': effective_users, 'rowfilter': rowfilter})
                    else:
                        for r in policies[policy_name]['roles']:
                            if r['role'] == effective_role:
                                for p in perm_list:
                                    if p not in r['permissions']:
                                        r['permissions'].append(p)
                                # Merge groups, users, rowfilter
                                r['groups'] = list(set(r.get('groups', []) + group_list))
                                r['users'] = list(set(r.get('users', []) + effective_users))
                                if rowfilter:
                                    if r.get('rowfilter') and r['rowfilter'] != rowfilter:
                                        raise ValueError(
                                            f"Conflicting rowfilters for role {effective_role} in policy {policy_name}: "
                                            f"{r['rowfilter']} vs {rowfilter}")
                                    r['rowfilter'] = rowfilter
            else:
                db_list = parse_csv_field(databases)
                table_list = parse_csv_field(tables) or ['*']
                column_list = parse_csv_field(columns) or ['*']
                for database in db_list:
                    if is_empty_like(database):
                        continue
                    for table in table_list:
                        for column in column_list:
                            policy_name = build_policy_name('iceberg', database, table, column)
                            policy_label = policy_label_override if policy_label_override else None
                            if policy_name not in policies:
                                policies[policy_name] = {
                                    'type': 'table',
                                    'catalog': 'iceberg',
                                    'schema': database,
                                    'table': table or '*',
                                    'column': column or '*',
                                    'roles': [],
                                    'label': policy_label
                                }
                            for binding in effective_role_bindings:
                                effective_role = binding['role']
                                effective_users = binding['users']
                                existing_roles = [r['role'] for r in policies[policy_name]['roles']]
                                if effective_role not in existing_roles:
                                    policies[policy_name]['roles'].append({'role': effective_role, 'permissions': perm_list, 'groups': group_list, 'users': effective_users, 'rowfilter': rowfilter})
                                else:
                                    for r in policies[policy_name]['roles']:
                                        if r['role'] == effective_role:
                                            for p in perm_list:
                                                if p not in r['permissions']:
                                                    r['permissions'].append(p)
                                            # Merge groups, users, rowfilter
                                            r['groups'] = list(set(r.get('groups', []) + group_list))
                                            r['users'] = list(set(r.get('users', []) + effective_users))
                                            if rowfilter:
                                                r['rowfilter'] = rowfilter

        logger.info(f"Parsed {len(policies)} policies and {len(role_principals)} role-principal mappings, {len(skipped_rows)} rows skipped")
        return {'policies': policies, 'role_principals': role_principals, 'skipped_rows': skipped_rows}

    @task.pyspark(conn_id="spark_default")
    def write_skipped_rows(tracking_run_id: str, skipped_rows: list, spark) -> int:
        """
        Write skipped Excel rows (validation errors) to the skipped rows tracking table.
        """
        from datetime import datetime as dt, timezone
        cfg = get_config()
        db = cfg["tracking_database"]
        now = dt.now(timezone.utc)
        written_count = 0
        if not skipped_rows:
            return 0
        for row in skipped_rows:
            row_index = row.get('row_index')
            role = row.get('role', '')
            database = row.get('database', '')
            url = row.get('url', '')
            reason = row.get('reason', '')
            created_at_sql = f"to_timestamp('{now.strftime('%Y-%m-%d %H:%M:%S')}', 'yyyy-MM-dd HH:mm:ss')"
            try:
                safe_role = role.replace("'", "''")
                safe_database = database.replace("'", "''")
                safe_url = url.replace("'", "''")
                safe_reason = reason.replace("'", "''")
                sql = (
                    f"""
                    INSERT INTO {db}.tracking_ranger_policy_skipped_rows (
                        run_id, row_index, role, database, url, reason, created_at
                    ) VALUES ('{tracking_run_id}', {row_index if row_index is not None else 'NULL'}, """
                    f"'{safe_role}', '{safe_database}', "
                    f"'{safe_url}', '{safe_reason}', {created_at_sql})"
                )
                spark.sql(sql)
                written_count += 1
            except Exception as e:
                logger.error(f"Failed to write skipped row {row_index}: {e}")
        return written_count

    @task.pyspark(conn_id="spark_default")
    def init_policy_tracking_tables(spark) -> Dict:

        config = get_config()
        db = config["tracking_database"]
        loc = config["tracking_location"]

        spark.sql(f"""
            CREATE DATABASE IF NOT EXISTS {db} LOCATION '{loc}'
        """)

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {db}.tracking_ranger_policy_runs (
                run_id STRING,
                dag_run_id STRING,
                excel_file_path STRING,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                status STRING,
                total_objects INT,
                successful_objects INT,
                failed_objects INT,
                total_policies_parsed INT,
                total_role_mappings_parsed INT,
                groups_created INT,
                groups_existing INT,
                policies_created INT,
                policies_updated INT,
                policies_failed INT,
                roles_created INT,
                roles_existing INT,
                mappings_created INT,
                mappings_existing INT,
                failed_operations INT,
                error_message STRING,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
            USING iceberg
            LOCATION '{loc}/tracking_ranger_policy_runs'
        """)

        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {db}.tracking_ranger_policy_object_status (
                run_id STRING,
                object_type STRING,
                object_name STRING,
                policy_id STRING,
                policy_name STRING,
                status STRING,
                error_message STRING,
                attempt INT,
                started_at TIMESTAMP,
                completed_at TIMESTAMP,
                created_at TIMESTAMP,
                updated_at TIMESTAMP
            )
            USING iceberg
            LOCATION '{loc}/tracking_ranger_policy_object_status'
        """)

        # Create tracking_ranger_policy_status table for policy-level tracking
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {db}.tracking_ranger_policy_status (
                run_id STRING,
                policy_id STRING,
                policy_name STRING,
                users ARRAY<STRING>,
                groups ARRAY<STRING>,
                permissions ARRAY<STRING>,
                rowfilter STRING,
                status STRING,
                error_message STRING,
                created_at TIMESTAMP,
                updated_at TIMESTAMP,
                attributes STRING
            )
            USING iceberg
            LOCATION '{loc}/tracking_ranger_policy_status'
        """)

        # Create tracking_ranger_policy_skipped_rows table for skipped Excel rows
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {db}.tracking_ranger_policy_skipped_rows (
                run_id STRING,
                row_index INT,
                role STRING,
                database STRING,
                url STRING,
                reason STRING,
                created_at TIMESTAMP
            )
            USING iceberg
            LOCATION '{loc}/tracking_ranger_policy_skipped_rows'
        """)
        
        return {"status": "initialized", "tracking_db": db}

    @task.pyspark(conn_id="spark_default")
    def create_policy_run(excel_file_path: str, dag_run_identifier: str, spark) -> str:
        import uuid
        policy_run_id = f"run_{datetime.now(timezone.utc):%Y%m%d_%H%M%S}_{uuid.uuid4().hex[:8]}"
        db = get_config()["tracking_database"]
        safe_policy_run_id = sql_str(policy_run_id)
        safe_dag_run_identifier = sql_str(dag_run_identifier)
        safe_excel_file_path = sql_str(excel_file_path)
        spark.sql(f"""
            INSERT INTO {db}.tracking_ranger_policy_runs (
                run_id,
                dag_run_id,
                excel_file_path,
                started_at,
                completed_at,
                status,
                total_objects,
                successful_objects,
                failed_objects,
                total_policies_parsed,
                total_role_mappings_parsed,
                groups_created,
                groups_existing,
                policies_created,
                policies_updated,
                policies_failed,
                roles_created,
                roles_existing,
                mappings_created,
                mappings_existing,
                failed_operations,
                error_message,
                created_at,
                updated_at
            )
            VALUES (
                '{safe_policy_run_id}',
                '{safe_dag_run_identifier}',
                '{safe_excel_file_path}',
                current_timestamp(),
                NULL,
                'RUNNING',
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                0,
                NULL,
                current_timestamp(),
                current_timestamp()
            )
        """)
        return policy_run_id

    # -----------------------------
    # Ranger & Keycloak tasks
    # -----------------------------
    @task
    def create_ranger_groups_and_policies(parsed_data: Dict[str, Any], tracking_run_id: str, keycloak_result: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create Ranger groups and policies.
        Returns both summary (for finalize) and statuses (for tracking).
        """
        from ranger_utils import RangerPolicyManager
        from datetime import datetime as dt

        cfg = get_config()
        manager = RangerPolicyManager(
            ranger_url=cfg["ranger_url"],
            ranger_username=cfg["ranger_username"],
            ranger_password=cfg["ranger_password"],
            service_name=cfg["service_name"]
        )

        policies = parsed_data['policies']
        role_principals = parsed_data['role_principals']
        statuses = []
        groups_created = []
        groups_existing = []


        # Ensure Ranger role-groups exist (strict RBAC model)
        all_roles = set(role_principals.keys())
        for policy_data in policies.values():
            for r in policy_data.get('roles', []):
                role = r.get('role')
                # Only add valid roles (not empty, None, or 'nan')
                if not is_empty_like(role):
                    all_roles.add(role)

        # Ensure role groups exist in Ranger (Keycloak groups/users are handled on Keycloak side)
        groups_result = manager.ensure_groups_exist(list(all_roles))

        for g, created in groups_result.items():
            if created:
                groups_created.append(g)
                status = "CREATED"
            else:
                groups_existing.append(g)
                status = "ALREADY_EXISTS"
            statuses.append({
                "run_id": tracking_run_id,
                "object_type": "ranger_group",  # Explicitly track as Ranger group
                "object_name": g,
                "status": status,
                "error_message": "",
                "attempt": 1,
                "started_at": dt.now(timezone.utc),
                "completed_at": dt.now(timezone.utc)
            })
        
        # Users are not tracked in object_status as they are external (AD/Keycloak) users
        # Policy creation will validate user existence at runtime

        patched_policies = {}
        prevalidated_policy_failures = []
        applied_policy_roles = {}
        excluded_policy_roles = {}

        for policy_name, policy_data in policies.items():
            patched_policy = dict(policy_data)
            patched_policy['roles'] = []
            excluded_roles_in_policy = []
            applied_roles_in_policy = []
            for r in policy_data.get('roles', []):
                role_name = r.get('role')
                if is_empty_like(role_name):
                    logger.error(
                        "Skipping Ranger policy assignment for invalid role in policy %s: %s",
                        policy_name,
                        role_name,
                    )
                    continue

                users = r.get('users', []) or []
                groups = r.get('groups', []) or []
                # Find successfully mapped groups
                mapped_groups = [
                    g for g in groups
                    if {'role': role_name, 'principal': g, 'type': 'group'} in keycloak_result.get('summary', {}).get('created_mappings', [])
                    or {'role': role_name, 'principal': g, 'type': 'group'} in keycloak_result.get('summary', {}).get('existing_mappings', [])
                ]
                # Find successfully mapped users
                mapped_users = [
                    u for u in users
                    if {'role': role_name, 'principal': u, 'type': 'user'} in keycloak_result.get('summary', {}).get('created_mappings', [])
                    or {'role': role_name, 'principal': u, 'type': 'user'} in keycloak_result.get('summary', {}).get('existing_mappings', [])
                ]
                if not mapped_groups and not mapped_users:
                    logger.error(
                        "Excluding role principal %s from Ranger policy %s due to failed Keycloak principal mapping for all users/groups in this policy row",
                        role_name,
                        policy_name,
                    )
                    excluded_roles_in_policy.append(role_name)
                    continue
                patched_role = dict(r)
                # Only include successfully mapped groups and users
                patched_role['groups'] = mapped_groups if mapped_groups else []
                patched_role['users'] = mapped_users if mapped_users else []
                patched_role['rowfilter'] = r.get('rowfilter', '')
                patched_policy['roles'].append(patched_role)
                applied_roles_in_policy.append(role_name)
            if applied_roles_in_policy:
                applied_policy_roles[policy_name] = sorted(set(applied_roles_in_policy))
            if excluded_roles_in_policy:
                excluded_policy_roles[policy_name] = sorted(set(excluded_roles_in_policy))
            # Only create the Ranger policy if there is at least one applied role
            if not patched_policy['roles']:
                if excluded_roles_in_policy:
                    failure_reason = (
                        "Ranger policy was not created because required Keycloak principal mapping failed for "
                        f"role principal(s): {', '.join(sorted(set(excluded_roles_in_policy)))}."
                    )
                else:
                    failure_reason = (
                        "Ranger policy was not created because no valid role principal could be built from the input rows. "
                        "The role value is empty/invalid, and no users-only fallback was available."
                    )
                prevalidated_policy_failures.append({'name': policy_name, 'error': failure_reason})
                statuses.append({
                    "run_id": tracking_run_id,
                    "object_type": "policy",
                    "object_name": policy_name,
                    "status": "FAILED",
                    "error_message": failure_reason,
                    "attempt": 1,
                    "started_at": dt.now(timezone.utc),
                    "completed_at": dt.now(timezone.utc)
                })
                continue
            patched_policies[policy_name] = patched_policy

        policies_result = manager.sync_policies_from_dict(patched_policies)

        policies_created = []
        policies_updated = []
        policies_failed = []

        # Process created policies - now contains policy_id and optional rowfilter_policy_id
        for p in policies_result.get('created', []):
            policy_name = p.get('policy_name', 'UNKNOWN')
            policy_id = p.get('policy_id')  # Actual Ranger policy ID
            policies_created.append(policy_name)
            statuses.append({
                "run_id": tracking_run_id,
                "object_type": "policy",
                "object_name": policy_name,
                "policy_id": str(policy_id) if policy_id else "",  # Track policy ID for association
                "policy_name": policy_name,  # Track policy name for association
                "status": "CREATED",
                "error_message": "",
                "attempt": 1,
                "started_at": dt.now(timezone.utc),
                "completed_at": dt.now(timezone.utc)
            })
            
            # Track rowfilter policy if it was created separately
            if p.get('rowfilter_policy_id'):
                rowfilter_policy_name = p.get('rowfilter_policy_name', 'UNKNOWN')
                statuses.append({
                    "run_id": tracking_run_id,
                    "object_type": "policy",
                    "object_name": rowfilter_policy_name,
                    "policy_id": str(policy_id) if policy_id else "",  # Reference parent access policy ID
                    "policy_name": policy_name,  # Reference parent base policy name
                    "status": "CREATED",
                    "error_message": "",
                    "attempt": 1,
                    "started_at": dt.now(timezone.utc),
                    "completed_at": dt.now(timezone.utc)
                })
        
        # Process updated policies - now contains policy_id and optional rowfilter_policy_id
        for p in policies_result.get('updated', []):
            policy_name = p.get('policy_name', 'UNKNOWN')
            policy_id = p.get('policy_id')  # Actual Ranger policy ID
            policies_updated.append(policy_name)
            statuses.append({
                "run_id": tracking_run_id,
                "object_type": "policy",
                "object_name": policy_name,
                "policy_id": str(policy_id) if policy_id else "",  # Track policy ID for association
                "policy_name": policy_name,  # Track policy name for association
                "status": "UPDATED",
                "error_message": "",
                "attempt": 1,
                "started_at": dt.now(timezone.utc),
                "completed_at": dt.now(timezone.utc)
            })
            
            # Track rowfilter policy if it exists
            if p.get('rowfilter_policy_id'):
                rowfilter_policy_name = p.get('rowfilter_policy_name', 'UNKNOWN')
                statuses.append({
                    "run_id": tracking_run_id,
                    "object_type": "policy",
                    "object_name": rowfilter_policy_name,
                    "policy_id": str(policy_id) if policy_id else "",  # Reference parent access policy ID
                    "policy_name": policy_name,  # Reference parent base policy name
                    "status": "UPDATED",
                    "error_message": "",
                    "attempt": 1,
                    "started_at": dt.now(timezone.utc),
                    "completed_at": dt.now(timezone.utc)
                })
        
        # Process failed policies
        for p in policies_result.get('failed', []):
            policy_name = p.get('name', 'UNKNOWN')
            error = p.get('error', '')
            policies_failed.append({'name': policy_name, 'error': error})
            statuses.append({
                "run_id": tracking_run_id,
                "object_type": "policy",
                "object_name": policy_name,
                "status": "FAILED",
                "error_message": error,
                "attempt": 1,
                "started_at": dt.now(timezone.utc),
                "completed_at": dt.now(timezone.utc)
            })

        policies_failed.extend(prevalidated_policy_failures)

        # Return only summary and statuses; policy_statuses is not used downstream
        return {
            "summary": {
                "groups": {"created": groups_created, "existing": groups_existing},
                "policies": {"created": policies_created, "updated": policies_updated, "failed": policies_failed}
            },
            "policy_principals": {
                "applied_roles": applied_policy_roles,
                "excluded_roles": excluded_policy_roles,
            },
            "statuses": statuses
        }

    @task(retries=4, retry_delay=timedelta(minutes=2))
    def check_keycloak_health() -> Dict[str, Any]:
        """
        Health check task that verifies Keycloak connectivity before role creation.
        Returns connection status and diagnostic info.
        """
        from ranger_utils import KeycloakRoleManager
        
        cfg = get_config()
        try:
            # Try to initialize manager with timeout and retries
            manager = KeycloakRoleManager(
                server_url=cfg["keycloak_url"],
                realm_name=cfg["keycloak_realm"],
                client_id=cfg["keycloak_client_id"],
                client_secret=cfg["keycloak_client_secret"],
                verify_ssl=cfg.get("keycloak_verify_ssl", False),
                ca_cert_path=(cfg.get("keycloak_cacert") or None)
            )
            logger.info("✓ Keycloak health check passed")
            return {
                "status": "healthy",
                "server_url": cfg["keycloak_url"],
                "realm": cfg["keycloak_realm"],
                "message": "Keycloak server is reachable"
            }
        except ConnectionError as e:
            logger.error(f"✗ Keycloak health check failed: {e}")
            raise
        except Exception as e:
            logger.error(f"✗ Keycloak health check error: {e}", exc_info=True)
            raise

    @task(retries=4, retry_delay=timedelta(minutes=2))
    def create_keycloak_roles(parsed_data: Dict[str, Any], tracking_run_id: str, health_check: Dict[str, Any], context=None) -> Dict[str, Any]:
        """
        Create Keycloak roles and group mappings.
        Returns both summary (for finalize) and statuses (for tracking).
        """
        from ranger_utils import KeycloakRoleManager
        from datetime import datetime as dt

        cfg = get_config()
        
        # Get current attempt number from Airflow context for tracking retries
        attempt_num = 1
        if context and 'ti' in context:
            attempt_num = context['ti'].try_number
        
        # Extract role principals BEFORE attempting connection so we can track failed attempts
        filtered_role_principals = {k: v for k, v in parsed_data['role_principals'].items()}
        
        try:
            manager = KeycloakRoleManager(
                server_url=cfg["keycloak_url"],
                realm_name=cfg["keycloak_realm"],
                client_id=cfg["keycloak_client_id"],
                client_secret=cfg["keycloak_client_secret"],
                verify_ssl=cfg.get("keycloak_verify_ssl", False),
                ca_cert_path=(cfg.get("keycloak_cacert") or None)
            )
        except ConnectionError as e:
            # Connection failed - record all roles and mappings as FAILED for audit trail
            error_msg = str(e)[:500]  # Limit error message length
            logger.error(f"Failed to connect to Keycloak: {e}")
            
            # Build failure response with all intended roles marked as FAILED
            statuses = []
            
            # All roles that were supposed to be created failed
            for role_name in filtered_role_principals.keys():
                statuses.append({
                    "run_id": tracking_run_id,
                    "object_type": "role",
                    "object_name": role_name,
                    "status": "FAILED",
                    "error_message": f"Keycloak connection failed: {error_msg}",
                    "attempt": attempt_num,
                    "started_at": dt.now(timezone.utc),
                    "completed_at": dt.now(timezone.utc)
                })
            
            # All groups that would have been assigned also failed
            all_groups = set()
            for role_info in filtered_role_principals.values():
                all_groups.update(role_info.get('groups', []))
            
            for group_name in sorted(all_groups):
                statuses.append({
                    "run_id": tracking_run_id,
                    "object_type": "keycloak_group",
                    "object_name": group_name,
                    "status": "FAILED",
                    "error_message": f"Keycloak connection failed: {error_msg}",
                    "attempt": attempt_num,
                    "started_at": dt.now(timezone.utc),
                    "completed_at": dt.now(timezone.utc)
                })
            
            # All mappings that would have been created also failed
            for role_name, role_info in filtered_role_principals.items():
                for group_name in role_info.get('groups', []):
                    statuses.append({
                        "run_id": tracking_run_id,
                        "object_type": "mapping",
                        "object_name": f"{role_name}->{group_name} (group)",
                        "status": "FAILED",
                        "error_message": f"Keycloak connection failed: {error_msg}",
                        "attempt": attempt_num,
                        "started_at": dt.now(timezone.utc),
                        "completed_at": dt.now(timezone.utc)
                    })
                for user_name in role_info.get('users', []):
                    statuses.append({
                        "run_id": tracking_run_id,
                        "object_type": "mapping",
                        "object_name": f"{role_name}->{user_name} (user)",
                        "status": "FAILED",
                        "error_message": f"Keycloak connection failed: {error_msg}",
                        "attempt": attempt_num,
                        "started_at": dt.now(timezone.utc),
                        "completed_at": dt.now(timezone.utc)
                    })
            
            # Return failure response with audit trail
            return {
                "summary": {
                    "created_roles": [],
                    "existing_roles": [],
                    "created_groups": [],
                    "existing_groups": [],
                    "created_mappings": [],
                    "existing_mappings": [],
                    "failed": list(filtered_role_principals.keys())
                },
                "statuses": statuses,
                "connection_error": True
            }


        # Map roles to principals (groups and users) based on parsed data
        result = manager.sync_roles_and_principals(filtered_role_principals)

        statuses = []
        
        # Process roles - differentiate between created and existing
        for entry in result.get('created_roles', []):
            name = entry if isinstance(entry, str) else str(entry)
            statuses.append({
                "run_id": tracking_run_id,
                "object_type": "role",
                "object_name": name,
                "status": "CREATED",
                "error_message": "",
                "attempt": 1,
                "started_at": dt.now(timezone.utc),
                "completed_at": dt.now(timezone.utc)
            })
        
        for entry in result.get('existing_roles', []):
            name = entry if isinstance(entry, str) else str(entry)
            statuses.append({
                "run_id": tracking_run_id,
                "object_type": "role",
                "object_name": name,
                "status": "ALREADY_EXISTS",
                "error_message": "",
                "attempt": 1,
                "started_at": dt.now(timezone.utc),
                "completed_at": dt.now(timezone.utc)
            })
        
        # Process Keycloak groups - track separately from Ranger groups
        for group_name in result.get('created_groups', []):
            statuses.append({
                "run_id": tracking_run_id,
                "object_type": "keycloak_group",  # Explicitly track as Keycloak group
                "object_name": group_name,
                "status": "CREATED",
                "error_message": "",
                "attempt": 1,
                "started_at": dt.now(timezone.utc),
                "completed_at": dt.now(timezone.utc)
            })
        
        for group_name in result.get('existing_groups', []):
            statuses.append({
                "run_id": tracking_run_id,
                "object_type": "keycloak_group",  # Explicitly track as Keycloak group
                "object_name": group_name,
                "status": "ALREADY_EXISTS",
                "error_message": "",
                "attempt": 1,
                "started_at": dt.now(timezone.utc),
                "completed_at": dt.now(timezone.utc)
            })
        
        # Process mappings - differentiate between created and existing
        for entry in result.get('created_mappings', []):
            if isinstance(entry, dict):
                principal = entry.get('principal', entry.get('group', entry.get('user', '')))
                mtype = entry.get('type', 'group' if 'group' in entry else 'user')
                name = f"{entry.get('role', '')}->{principal} ({mtype})"
            else:
                name = str(entry)
            statuses.append({
                "run_id": tracking_run_id,
                "object_type": "mapping",
                "object_name": name,
                "status": "CREATED",
                "error_message": "",
                "attempt": 1,
                "started_at": dt.now(timezone.utc),
                "completed_at": dt.now(timezone.utc)
            })
        
        for entry in result.get('existing_mappings', []):
            if isinstance(entry, dict):
                principal = entry.get('principal', entry.get('group', entry.get('user', '')))
                mtype = entry.get('type', 'group' if 'group' in entry else 'user')
                name = f"{entry.get('role', '')}->{principal} ({mtype})"
            else:
                name = str(entry)
            statuses.append({
                "run_id": tracking_run_id,
                "object_type": "mapping",
                "object_name": name,
                "status": "ALREADY_EXISTS",
                "error_message": "",
                "attempt": 1,
                "started_at": dt.now(timezone.utc),
                "completed_at": dt.now(timezone.utc)
            })
        
        # Process failures
        for entry in result.get('failed', []):
            if isinstance(entry, dict):
                principal = entry.get('principal', entry.get('group', entry.get('user', '')))
                mtype = entry.get('type', 'group' if 'group' in entry else 'user')
                name = f"{entry.get('role', '')}->{principal} ({mtype})"
                error = entry.get('error', '')
            else:
                name = str(entry)
                error = ''
            statuses.append({
                "run_id": tracking_run_id,
                "object_type": "mapping",
                "object_name": name,
                "status": "FAILED",
                "error_message": error,
                "attempt": 1,
                "started_at": dt.now(timezone.utc),
                "completed_at": dt.now(timezone.utc)
            })

        return {
            "summary": {
                "created_roles": result.get('created_roles', []),
                "existing_roles": result.get('existing_roles', []),
                "created_groups": result.get('created_groups', []),  # Keycloak groups
                "existing_groups": result.get('existing_groups', []),  # Keycloak groups
                "created_mappings": result.get('created_mappings', []),
                "existing_mappings": result.get('existing_mappings', []),
                "failed": result.get('failed', [])
            },
            "statuses": statuses
        }


    @task(trigger_rule=TriggerRule.ALL_DONE)
    def debug_list_policies() -> Dict[str, Any]:
        """
        Debug task: Query Ranger backend to verify policies actually exist.
        This runs during DAG execution and logs all policies for the service.
        """
        from ranger_utils import RangerPolicyManager
        
        cfg = get_config()
        manager = RangerPolicyManager(
            ranger_url=cfg["ranger_url"],
            ranger_username=cfg["ranger_username"],
            ranger_password=cfg["ranger_password"],
            service_name=cfg["service_name"]
        )
        
        try:
            # Query all policies for the service
            all_policies = manager.get_all_policies()
            policy_names = [p.get('name', 'UNKNOWN') for p in all_policies]
            
            logger.info(f"DEBUG: Found {len(all_policies)} policies in Ranger service '{cfg['service_name']}'")
            logger.info(f"DEBUG: Policy names: {policy_names}")
            
            return {
                "status": "success",
                "total_policies": len(all_policies),
                "policy_names": policy_names,
                "all_policies": all_policies
            }
        except Exception as e:
            logger.error(f"DEBUG: Error querying Ranger policies: {e}", exc_info=True)
            return {
                "status": "error",
                "error": str(e),
                "total_policies": 0,
                "policy_names": []
            }
    @task
    def extract_statuses(ranger_result: Dict[str, Any], keycloak_result: Dict[str, Any]) -> List[Dict]:
        """Combine statuses from both Ranger and Keycloak results."""
        all_statuses = []
        all_statuses.extend(ranger_result.get("statuses", []))
        all_statuses.extend(keycloak_result.get("statuses", []))
        return all_statuses

    @task.pyspark(conn_id="spark_default")
    def finalize_policy_run(
        tracking_run_id: str,
        dag_run_identifier: str,
        excel_file_path: str,
        parsed_data: Dict[str, Any],
        ranger_result: Dict[str, Any],
        keycloak_result: Dict[str, Any],
        spark=None,
        sc=None
    ) -> Dict:
        """
        Finalize the policy run: compute metrics from parsed data and task results,
        update the tracking_ranger_policy_runs table, and return summary.
        """
        cfg = get_config()
        db = cfg["tracking_database"]

        # Extract summaries
        ranger_summary = ranger_result.get("summary", {})
        keycloak_summary = keycloak_result.get("summary", {})

        # Compute run-level totals
        groups_created = len(ranger_summary.get('groups', {}).get('created', []))
        groups_existing = len(ranger_summary.get('groups', {}).get('existing', []))
        policies_created = len(ranger_summary.get('policies', {}).get('created', []))
        policies_updated = len(ranger_summary.get('policies', {}).get('updated', []))
        policies_failed = len(ranger_summary.get('policies', {}).get('failed', []))
        
        roles_created = len(keycloak_summary.get('created_roles', []))
        roles_existing = len(keycloak_summary.get('existing_roles', []))
        mappings_created = len(keycloak_summary.get('created_mappings', []))
        mappings_existing = len(keycloak_summary.get('existing_mappings', []))

        all_statuses = []
        all_statuses.extend(ranger_result.get("statuses", []))
        all_statuses.extend(keycloak_result.get("statuses", []))

        total_objects = len(all_statuses)
        successful_objects = sum(
            1 for s in all_statuses if s.get("status") in ("CREATED", "UPDATED", "ALREADY_EXISTS")
        )
        failed_objects = sum(1 for s in all_statuses if s.get("status") == "FAILED")

        overall_status = "COMPLETED" if failed_objects == 0 else "PARTIAL_FAILURE"
        safe_overall_status = sql_str(overall_status)
        safe_tracking_run_id = sql_str(tracking_run_id)
        # Update the tracking_ranger_policy_runs table with all metrics
        spark.sql(f"""
                UPDATE {db}.tracking_ranger_policy_runs
            SET
                completed_at = current_timestamp(),
                status = '{safe_overall_status}',
                total_objects = {total_objects},
                successful_objects = {successful_objects},
                failed_objects = {failed_objects},
                total_policies_parsed = {len(parsed_data.get('policies', {}))},
                total_role_mappings_parsed = {len(parsed_data.get('role_principals', {}))},
                groups_created = {groups_created},
                groups_existing = {groups_existing},
                policies_created = {policies_created},
                policies_updated = {policies_updated},
                policies_failed = {policies_failed},
                roles_created = {roles_created},
                roles_existing = {roles_existing},
                mappings_created = {mappings_created},
                mappings_existing = {mappings_existing},
                failed_operations = {failed_objects},
                updated_at = current_timestamp()
            WHERE run_id = '{safe_tracking_run_id}'
        """)
        fallback_message = "Final policy status not recorded; marking as FAILED"
        safe_fallback_message = sql_str(fallback_message)
        spark.sql(f"""
            UPDATE {db}.tracking_ranger_policy_status
           SET
                status = 'FAILED',
                error_message = '{safe_fallback_message}',
                updated_at = current_timestamp()
            WHERE run_id = '{safe_tracking_run_id}' AND status = 'RUNNING'
        """)

        spark.sql(f"""
            MERGE INTO {db}.tracking_ranger_policy_object_status t
            USING (
                SELECT run_id, policy_name AS object_name, policy_name, policy_id
                FROM {db}.tracking_ranger_policy_status
                                WHERE run_id = '{safe_tracking_run_id}'
                                    AND status IN ('CREATED', 'UPDATED')
                                    AND policy_name IS NOT NULL AND trim(policy_name) <> ''
                                    AND policy_id IS NOT NULL AND trim(policy_id) <> ''
                UNION ALL
                SELECT run_id, concat(policy_name, '__rowfilter') AS object_name, policy_name, policy_id
                FROM {db}.tracking_ranger_policy_status
                                WHERE run_id = '{safe_tracking_run_id}'
                                    AND status IN ('CREATED', 'UPDATED')
                                    AND policy_name IS NOT NULL AND trim(policy_name) <> ''
                                    AND policy_id IS NOT NULL AND trim(policy_id) <> ''
            ) s
            ON t.run_id = s.run_id
               AND t.object_type = 'policy'
               AND t.object_name = s.object_name
            WHEN MATCHED
              AND ((t.policy_id IS NULL OR t.policy_id = '') OR (t.policy_name IS NULL OR t.policy_name = ''))
            THEN UPDATE SET
              t.policy_name = s.policy_name,
              t.policy_id = s.policy_id,
              t.updated_at = current_timestamp()
        """)

        spark.sql(f"""
            MERGE INTO {db}.tracking_ranger_policy_object_status t
            USING (
                SELECT
                    principal,
                    concat_ws(', ', sort_array(collect_set(policy_name))) AS policy_name,
                    concat_ws(', ', sort_array(collect_set(policy_id))) AS policy_id
                FROM (
                    SELECT explode(groups) AS principal, policy_name, policy_id
                    FROM {db}.tracking_ranger_policy_status
                                        WHERE run_id = '{safe_tracking_run_id}'
                                            AND status IN ('CREATED', 'UPDATED')
                                            AND policy_name IS NOT NULL AND trim(policy_name) <> ''
                                            AND policy_id IS NOT NULL AND trim(policy_id) <> ''
                    UNION ALL
                    SELECT explode(users) AS principal, policy_name, policy_id
                    FROM {db}.tracking_ranger_policy_status
                                        WHERE run_id = '{safe_tracking_run_id}'
                                            AND status IN ('CREATED', 'UPDATED')
                                            AND policy_name IS NOT NULL AND trim(policy_name) <> ''
                                            AND policy_id IS NOT NULL AND trim(policy_id) <> ''
                ) principals
                WHERE principal IS NOT NULL AND trim(principal) <> ''
                GROUP BY principal
            ) s
            ON t.run_id = '{safe_tracking_run_id}'
               AND t.object_type IN ('ranger_group', 'keycloak_group')
               AND t.object_name = s.principal
            WHEN MATCHED
              AND ((t.policy_id IS NULL OR t.policy_id = '') OR (t.policy_name IS NULL OR t.policy_name = ''))
            THEN UPDATE SET
              t.policy_name = s.policy_name,
              t.policy_id = s.policy_id,
              t.updated_at = current_timestamp()
        """)

        spark.sql(f"""
            MERGE INTO {db}.tracking_ranger_policy_object_status t
            USING (
                SELECT
                    principal_assoc.principal,
                    principal_assoc.policy_name,
                    principal_assoc.policy_id
                FROM (
                    SELECT
                        principal,
                        concat_ws(', ', sort_array(collect_set(policy_name))) AS policy_name,
                        concat_ws(', ', sort_array(collect_set(policy_id))) AS policy_id
                    FROM (
                        SELECT explode(groups) AS principal, policy_name, policy_id
                        FROM {db}.tracking_ranger_policy_status
                                                WHERE run_id = '{safe_tracking_run_id}'
                                                    AND status IN ('CREATED', 'UPDATED')
                                                    AND policy_name IS NOT NULL AND trim(policy_name) <> ''
                                                    AND policy_id IS NOT NULL AND trim(policy_id) <> ''
                        UNION ALL
                        SELECT explode(users) AS principal, policy_name, policy_id
                        FROM {db}.tracking_ranger_policy_status
                                                WHERE run_id = '{safe_tracking_run_id}'
                                                    AND status IN ('CREATED', 'UPDATED')
                                                    AND policy_name IS NOT NULL AND trim(policy_name) <> ''
                                                    AND policy_id IS NOT NULL AND trim(policy_id) <> ''
                    ) principals
                    WHERE principal IS NOT NULL AND trim(principal) <> ''
                    GROUP BY principal
                ) principal_assoc
            ) s
            ON t.run_id = '{safe_tracking_run_id}'
               AND t.object_type = 'mapping'
               AND trim(regexp_extract(t.object_name, '->([^\\(]+)\\s*\\((group|user)\\)$', 1)) = s.principal
            WHEN MATCHED
              AND ((t.policy_id IS NULL OR t.policy_id = '') OR (t.policy_name IS NULL OR t.policy_name = ''))
            THEN UPDATE SET
              t.policy_name = s.policy_name,
              t.policy_id = s.policy_id,
              t.updated_at = current_timestamp()
        """)

        spark.sql(f"""
            MERGE INTO {db}.tracking_ranger_policy_object_status t
            USING (
                SELECT
                    role_name,
                    concat_ws(', ', sort_array(collect_set(policy_name))) AS policy_name,
                    concat_ws(', ', sort_array(collect_set(policy_id))) AS policy_id
                FROM (
                    SELECT
                        trim(regexp_extract(object_name, '^(.+?)->', 1)) AS role_name,
                        policy_name,
                        policy_id
                    FROM {db}.tracking_ranger_policy_object_status
                    WHERE run_id = '{safe_tracking_run_id}'
                      AND object_type = 'mapping'
                      AND policy_name IS NOT NULL
                      AND policy_name <> ''
                ) mapped_roles
                WHERE role_name IS NOT NULL AND role_name <> ''
                GROUP BY role_name
            ) s
            ON t.run_id = '{safe_tracking_run_id}'
               AND t.object_type = 'role'
               AND t.object_name = s.role_name
            WHEN MATCHED
              AND ((t.policy_id IS NULL OR t.policy_id = '') OR (t.policy_name IS NULL OR t.policy_name = ''))
            THEN UPDATE SET
              t.policy_name = s.policy_name,
              t.policy_id = s.policy_id,
              t.updated_at = current_timestamp()
        """)

        spark.sql(f"""
                    MERGE INTO {db}.tracking_ranger_policy_object_status t
                    USING (
                            SELECT object_name, policy_name, policy_id
                            FROM {db}.tracking_ranger_policy_object_status
                            WHERE run_id = '{safe_tracking_run_id}'
                                AND object_type = 'role'
                                AND policy_name IS NOT NULL
                                AND policy_name <> ''
                    ) s
                    ON t.run_id = '{safe_tracking_run_id}'
                            AND t.object_type = 'ranger_group'
                            AND t.object_name = s.object_name
                    WHEN MATCHED
                        AND ((t.policy_id IS NULL OR t.policy_id = '') OR (t.policy_name IS NULL OR t.policy_name = ''))
                    THEN UPDATE SET
                        t.policy_name = s.policy_name,
                        t.policy_id = s.policy_id,
                        t.updated_at = current_timestamp()
            """)

        spark.sql(f"""
                MERGE INTO {db}.tracking_ranger_policy_object_status t
                USING (
                        SELECT
                                trim(regexp_extract(object_name, '->([^\\(]+)\\s*\\(group\\)$', 1)) AS group_name,
                                concat_ws(', ', sort_array(collect_set(policy_name))) AS policy_name,
                                concat_ws(', ', sort_array(collect_set(policy_id))) AS policy_id
                        FROM {db}.tracking_ranger_policy_object_status
                        WHERE run_id = '{safe_tracking_run_id}'
                            AND object_type = 'mapping'
                            AND object_name RLIKE '->[^\\(]+\\s*\\(group\\)$'
                            AND policy_name IS NOT NULL
                            AND policy_name <> ''
                        GROUP BY trim(regexp_extract(object_name, '->([^\\(]+)\\s*\\(group\\)$', 1))
                ) s
                ON t.run_id = '{safe_tracking_run_id}'
                        AND t.object_type = 'keycloak_group'
                        AND t.object_name = s.group_name
                WHEN MATCHED
                    AND ((t.policy_id IS NULL OR t.policy_id = '') OR (t.policy_name IS NULL OR t.policy_name = ''))
                THEN UPDATE SET
                    t.policy_name = s.policy_name,
                    t.policy_id = s.policy_id,
                    t.updated_at = current_timestamp()
        """)
        return {
            "run_id": tracking_run_id,
            "status": overall_status,
            "total_objects": total_objects,
            "successful_objects": successful_objects,
            "failed_objects": failed_objects,
            "dag_run_id": dag_run_identifier,
            "excel_file_path": excel_file_path,
            "ranger_summary": ranger_summary,
            "keycloak_summary": keycloak_summary
        }


    @task.pyspark(conn_id="spark_default")
    def generate_policy_report(tracking_run_id: str, skipped_rows: list, spark) -> Dict[str, str]:
        """
        Generate a multi-row formatted HTML report for Ranger & Keycloak policy run.
        Includes object counts, status badges, and error messages.
        Saves report to S3.
        """
        from datetime import datetime as dt
        from collections import Counter

        config = get_config()
        tracking_db = config["tracking_database"]
        report_location = config["report_output_location"]

        # Fetch run-level info
        rows = spark.sql(f"""
                SELECT * FROM {tracking_db}.tracking_ranger_policy_runs
            WHERE run_id = '{tracking_run_id}'
        """).collect()
        if not rows:
            raise ValueError(f"No run found for run_id={tracking_run_id}")
        run_info = rows[0]

        # Fetch object-level info
        objects = spark.sql(f"""
                SELECT * FROM {tracking_db}.tracking_ranger_policy_object_status
            WHERE run_id = '{tracking_run_id}'
            ORDER BY object_type, object_name
        """).collect()

        # Fetch policy-level info
        policy_statuses = spark.sql(f"""
                SELECT * FROM {tracking_db}.tracking_ranger_policy_status
            WHERE run_id = '{tracking_run_id}'
            ORDER BY policy_name
        """).collect()


        # Fetch skipped rows from tracking table
        skipped_rows = spark.sql(f"""
            SELECT row_index, role, database, url, reason
            FROM {tracking_db}.tracking_ranger_policy_skipped_rows
            WHERE run_id = '{tracking_run_id}'
            ORDER BY row_index
        """).collect()

        total_objects = len(objects)
        successful_objects = sum(1 for o in objects if o.status in ["CREATED", "UPDATED", "ALREADY_EXISTS"])
        failed_objects = sum(1 for o in objects if o.status == "FAILED")
        type_counts = Counter(o.object_type for o in objects)

        # Determine status class for run
        run_status_class = 'status-success' if run_info.status == 'COMPLETED' else 'status-failed'

        # Build HTML
        html = f"""
        <!DOCTYPE html>
        <html lang=\"en\">
        <head>
            <meta charset=\"UTF-8\">
            <title>Ranger Policy Run Report - {tracking_run_id}</title>
            <style>
                body {{ font-family: Arial, sans-serif; background: #f5f5f5; padding: 20px; }}
                .container {{ background: #fff; padding: 20px; border-radius: 8px; max-width: 1400px; margin: 0 auto; }}
                h1 {{ color: #2c3e50; }}
                h2 {{ color: #34495e; margin-top: 30px; }}
                .summary-cards {{ display: flex; flex-wrap: wrap; gap: 10px; margin: 20px 0; }}
                .summary-card {{ padding: 15px 20px; border-radius: 6px; color: #fff; min-width: 150px; text-align: center; }}
                .success {{ background-color: #28a745; }}
                .failed {{ background-color: #dc3545; }}
                .info {{ background-color: #17a2b8; }}
                table {{ width: 100%; border-collapse: collapse; margin-top: 20px; }}
                th, td {{ border: 1px solid #ddd; padding: 10px; text-align: left; }}
                th {{ background-color: #343a40; color: white; }}
                tr:nth-child(even) {{ background-color: #f8f9fa; }}
                tr:hover {{ background-color: #e9ecef; }}
                .status-badge {{ padding: 4px 10px; border-radius: 12px; font-weight: bold; color: white; font-size: 12px; }}
                .status-success {{ background-color: #28a745; }}
                .status-failed {{ background-color: #dc3545; }}
                .status-partial {{ background-color: #ffc107; color: #212529; }}
                .error-cell {{ color: #dc3545; font-size: 12px; max-width: 300px; word-wrap: break-word; }}
                .run-info {{ background: #f8f9fa; padding: 15px; border-radius: 6px; margin-bottom: 20px; }}
                .run-info p {{ margin: 5px 0; }}
            </style>
        </head>
        <body>
            <div class=\"container\">
                <h1>Ranger Policy Run Report</h1>
                
                <div class=\"run-info\">
                    <p><strong>Run ID:</strong> {tracking_run_id}</p>
                    <p><strong>DAG Run:</strong> {run_info.dag_run_id}</p>
                    <p><strong>Excel Path:</strong> {run_info.excel_file_path}</p>
                    <p><strong>Started At:</strong> {run_info.started_at}</p>
                    <p><strong>Completed At:</strong> {run_info.completed_at}</p>
                    <p><strong>Status:</strong> <span class=\"status-badge {run_status_class}\">{run_info.status}</span></p>
                </div>

                <h2>Summary</h2>
                <div class=\"summary-cards\">
                    <div class=\"summary-card info\">Total Objects<br><strong>{total_objects}</strong></div>
                    <div class=\"summary-card success\">Successful<br><strong>{successful_objects}</strong></div>
                    <div class=\"summary-card failed\">Failed<br><strong>{failed_objects}</strong></div>
                </div>
                
                <div class=\"summary-cards\">
                    <div class=\"summary-card info\">Policies Parsed<br><strong>{run_info.total_policies_parsed or 0}</strong></div>
                    <div class=\"summary-card info\">Role Mappings Parsed<br><strong>{run_info.total_role_mappings_parsed or 0}</strong></div>
                </div>
                
                <div class=\"summary-cards\">
                    <div class=\"summary-card success\">Groups Created<br><strong>{run_info.groups_created or 0}</strong></div>
                    <div class=\"summary-card info\">Groups Existing<br><strong>{run_info.groups_existing or 0}</strong></div>
                    <div class=\"summary-card success\">Policies Created<br><strong>{run_info.policies_created or 0}</strong></div>
                    <div class=\"summary-card info\">Policies Updated<br><strong>{run_info.policies_updated or 0}</strong></div>
                    <div class=\"summary-card failed\">Policies Failed<br><strong>{run_info.policies_failed or 0}</strong></div>
                </div>
                
                <div class=\"summary-cards\">
                    <div class=\"summary-card success\">Roles Created<br><strong>{run_info.roles_created or 0}</strong></div>
                    <div class=\"summary-card info\">Roles Existing<br><strong>{run_info.roles_existing or 0}</strong></div>
                    <div class=\"summary-card success\">Mappings Created<br><strong>{run_info.mappings_created or 0}</strong></div>
                    <div class=\"summary-card info\">Mappings Existing<br><strong>{run_info.mappings_existing or 0}</strong></div>
                    <div class=\"summary-card failed\">Failed Operations<br><strong>{run_info.failed_operations or 0}</strong></div>
                </div>

                <h2>Object Counts By Type</h2>
                <ul>
        """
        for obj_type, count in type_counts.items():
            html += f"<li><strong>{obj_type}:</strong> {count}</li>"

        html += """
                </ul>

                <h2>Policy Status Details</h2>
                <table>
                    <thead>
                        <tr>
                            <th>Policy Name</th>
                            <th>Users</th>
                            <th>Groups</th>
                            <th>Permissions</th>
                            <th>Rowfilter</th>
                            <th>Status</th>
                            <th>Error Message</th>
                            <th>Created At</th>
                            <th>Updated At</th>
                        </tr>
                    </thead>
                    <tbody>
        """
        for p in policy_statuses:
            status_class = (
                "status-success" if getattr(p, 'status', None) in ["CREATED", "UPDATED", "ALREADY_EXISTS"]
                else "status-failed" if getattr(p, 'status', None) == "FAILED"
                else "status-partial"
            )
            users = ', '.join(getattr(p, 'users', []) or [])
            groups = ', '.join(getattr(p, 'groups', []) or [])
            permissions = ', '.join(getattr(p, 'permissions', []) or [])
            rowfilter = getattr(p, 'rowfilter', '') or ''
            error_display = getattr(p, 'error_message', '') or ''
            html += f"""
                        <tr>
                            <td>{getattr(p, 'policy_name', '')}</td>
                            <td>{users}</td>
                            <td>{groups}</td>
                            <td>{permissions}</td>
                            <td>{rowfilter}</td>
                            <td><span class=\"status-badge {status_class}\">{getattr(p, 'status', '')}</span></td>
                            <td class=\"error-cell\">{error_display}</td>
                            <td>{getattr(p, 'created_at', '')}</td>
                            <td>{getattr(p, 'updated_at', '')}</td>
                        </tr>
            """

        html += """
                </tbody>
                </table>
                <h2>Skipped Excel Rows (Validation Errors)</h2>
                <table>
                    <thead>
                        <tr>
                            <th>Row Index</th>
                            <th>Role</th>
                            <th>Database</th>
                            <th>URL</th>
                            <th>Reason</th>
                        </tr>
                    </thead>
                    <tbody>
        """
        for row in skipped_rows:
            row_index = row['row_index'] if row['row_index'] is not None else ''
            role = row['role'] or ''
            database = row['database'] or ''
            url = row['url'] or ''
            reason = row['reason'] or ''
            html += f"""
                                <tr>
                                    <td>{row_index}</td>
                                    <td>{role}</td>
                                    <td>{database}</td>
                                    <td>{url}</td>
                                    <td>{reason}</td>
                                </tr>
                    """
        html += """
                    </tbody>
                </table>
                <h2>Object Details</h2>
                <table>
                    <thead>
                        <tr>
                            <th>Object Type</th>
                            <th>Object Name</th>
                            <th>Policy Name</th>
                            <th>Policy ID</th>
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
                "status-success" if o.status in ["CREATED", "UPDATED", "ALREADY_EXISTS"]
                else "status-failed" if o.status == "FAILED"
                else "status-partial"
            )
            error_display = o.error_message or ''
            policy_name_display = getattr(o, 'policy_name', '') or ''
            policy_id_display = getattr(o, 'policy_id', '') or ''
            html += f"""
                        <tr>
                            <td>{o.object_type}</td>
                            <td>{o.object_name}</td>
                            <td>{policy_name_display}</td>
                            <td>{policy_id_display}</td>
                            <td><span class=\"status-badge {status_class}\">{o.status}</span></td>
                            <td class=\"error-cell\">{error_display}</td>
                            <td>{o.attempt}</td>
                            <td>{o.started_at}</td>
                            <td>{o.completed_at or ''}</td>
                        </tr>
            """

        html += f"""
                    </tbody>
                </table>

                <p style=\"margin-top:30px; font-size:12px; color:#6c757d; text-align: center;\">
                    Report generated automatically by the Ranger Policy DAG on {dt.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S')} UTC.
                </p>
            </div>
        </body>
        </html>
        """

        report_filename = f"{tracking_run_id}_policy_report.html"
        report_path = f"{report_location}/{report_filename}"

        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(report_path),
            hadoop_conf
        )
        output_path = spark._jvm.org.apache.hadoop.fs.Path(report_path)
        output_stream = fs.create(output_path, True)
        output_stream.write(html.encode('utf-8'))
        output_stream.close()

        logger.info(f"Report generated at: {report_path}")
        return {
            "report_path": report_path,
            "html_content": html,
        }

    @task.pyspark(conn_id='spark_default')
    def send_policy_report_email(report_result: Dict[str, Any], tracking_run_id: str, spark) -> Dict[str, Any]:
        """Send HTML policy report via email using SMTP."""
        cfg = get_config()
        smtp_conn_id = cfg.get('smtp_conn_id', 'smtp_default')
        recipients_str = cfg.get('email_recipients', '')

        if not recipients_str:
            logger.warning("[Email] No recipients configured in 'policy_email_recipients' variable. Skipping email.")
            return {'sent': False, 'reason': 'no_recipients'}

        recipients = [r.strip() for r in recipients_str.split(',') if r.strip()]
        html_content = report_result.get('html_content', '')
        report_path = report_result.get('report_path', '')

        try:
            import tempfile
            import os
            from airflow.utils.email import send_email

            tmp = tempfile.NamedTemporaryFile(
                mode='w',
                suffix='.html',
                prefix=f'{tracking_run_id}_report_',
                delete=False
            )
            tmp.write(html_content)
            tmp.close()

            send_email(
                to=recipients,
                subject=f"Ranger Policy Report - {tracking_run_id}",
                html_content=(
                    f"<p>Please find the Ranger policy report for run "
                    f"<strong>{tracking_run_id}</strong> attached.</p>"
                ),
                files=[tmp.name],
                conn_id=smtp_conn_id,
            )
            os.unlink(tmp.name)
            logger.info(f"[Email] Report sent successfully to: {recipients}")
            return {'sent': True, 'recipients': recipients, 'report_path': report_path}
        except Exception as e:
            logger.error(f"[Email] Failed to send report: {str(e)}")
            raise Exception(f"Failed to send policy report email: {str(e)}") from e

    @task.pyspark(conn_id="spark_default")
    def write_policy_statuses(initial_policy_statuses: List[Dict], spark) -> int:
        """
        Write initial policy statuses to the policy_status table (status = RUNNING or PENDING).
        """
        from datetime import datetime as dt, timezone
        cfg = get_config()
        db = cfg["tracking_database"]
        now = dt.now(timezone.utc)
        written_count = 0
        for p in initial_policy_statuses:
            run_id = sql_str(p["run_id"])
            policy_id = sql_str(p.get("policy_id", ""))
            policy_name = sql_str(p.get("policy_name", ""))
            users = p.get("users", [])
            groups = p.get("groups", [])
            permissions = p.get("permissions", [])
            rowfilter = p.get("rowfilter", None)
            if rowfilter:
                try:
                    validate_rowfilter(rowfilter)
                except ValueError as ve:
                    logger.error(f"Rejected rowfilter for policy {policy_name}: {ve}")
                    continue
            status = sql_str(p.get("status", "RUNNING"))
            error_message = sql_str(p.get("error_message", ""))
            created_at = p.get("created_at", now)
            updated_at = p.get("updated_at", now)
            users_sql = "array(" + ", ".join(["'" + sql_str(u) + "'" for u in users]) + ")" if users else "array()"
            groups_sql = "array(" + ", ".join(["'" + sql_str(g) + "'" for g in groups]) + ")" if groups else "array()"
            permissions_sql = "array(" + ", ".join(["'" + sql_str(perm) + "'" for perm in permissions]) + ")" if permissions else "array()"
            rowfilter_sql = f"'" + sql_str(rowfilter) + "'" if rowfilter else "NULL"
            created_at_sql = f"to_timestamp('{created_at.strftime('%Y-%m-%d %H:%M:%S')}', 'yyyy-MM-dd HH:mm:ss')"
            updated_at_sql = f"to_timestamp('{updated_at.strftime('%Y-%m-%d %H:%M:%S')}', 'yyyy-MM-dd HH:mm:ss')"
            try:
                spark.sql(f"""
                    MERGE INTO {db}.tracking_ranger_policy_status t
                    USING (SELECT '{run_id}' AS run_id, '{policy_name}' AS policy_name) s
                    ON t.run_id = s.run_id AND t.policy_name = s.policy_name
                    WHEN MATCHED THEN UPDATE SET
                        policy_id = '{policy_id}',
                        users = {users_sql},
                        groups = {groups_sql},
                        permissions = {permissions_sql},
                        rowfilter = {rowfilter_sql},
                        status = '{status}',
                        error_message = '{error_message}',
                        updated_at = {updated_at_sql}
                    WHEN NOT MATCHED THEN INSERT (
                        run_id, policy_id, policy_name, users, groups, permissions, rowfilter, status, error_message, created_at, updated_at
                    ) VALUES (
                        '{run_id}', '{policy_id}', '{policy_name}', {users_sql}, {groups_sql}, {permissions_sql}, {rowfilter_sql}, '{status}', '{error_message}', {created_at_sql}, {updated_at_sql}
                    )
                """)
                written_count += 1
            except Exception as e:
                logger.error(f"Failed to write initial policy status for {policy_name}: {e}")
        return written_count
    
    # Build and write final policy statuses after policy processing
    @task(trigger_rule=TriggerRule.ALL_DONE)
    def build_final_policy_statuses(parsed_data: Dict[str, Any], tracking_run_id: str, ranger_result: Dict[str, Any]) -> List[Dict]:
        from datetime import datetime as dt, timezone
        policies = parsed_data.get('policies', {})
        policy_principals = ranger_result.get('policy_principals', {})
        applied_role_map = policy_principals.get('applied_roles', {})
        excluded_role_map = policy_principals.get('excluded_roles', {})
        now = dt.now(timezone.utc)
        # Build map of base policy names to their numeric IDs from ranger_result
        policy_id_map = {}
        for obj in ranger_result.get("statuses", []):
            if obj["object_type"] == "policy":
                # Extract base policy name (remove __rowfilter suffix if present)
                full_name = obj["object_name"]
                base_name = full_name.replace("__rowfilter", "") if full_name.endswith("__rowfilter") else full_name
                # Only store base policy numeric ID (not the rowfilter policy)
                if not full_name.endswith("__rowfilter"):
                    policy_id_map[base_name] = obj.get("policy_id", "")
        policy_status_map = {}
        for obj in ranger_result.get("statuses", []):
            if obj["object_type"] == "policy":
                full_name = obj["object_name"]
                base_name = full_name.replace("__rowfilter", "") if full_name.endswith("__rowfilter") else full_name
                policy_status_map[base_name] = {
                    "status": obj["status"],
                    "error_message": obj["error_message"]
                }
        status_list = []
        for policy_name, pdata in policies.items():
            users = []
            parsed_groups = []
            permissions = []
            rowfilter = None
            for r in pdata.get('roles', []):
                role = r.get('role')
                users.extend(r.get('users', []))
                if not is_empty_like(role):
                    parsed_groups.append(role)
                permissions.extend(r.get('permissions', []))
                if r.get('rowfilter'):
                    if rowfilter and rowfilter != r['rowfilter']:
                        raise ValueError(
                            f"Conflicting rowfilters for policy {policy_name}: "
                            f"{rowfilter} vs {r['rowfilter']}"
                        )
                    rowfilter = r['rowfilter']
            status = policy_status_map.get(policy_name, {}).get("status", "RUNNING")
            error_message = policy_status_map.get(policy_name, {}).get("error_message", "")
            excluded_roles = excluded_role_map.get(policy_name, [])
            if excluded_roles and status != "FAILED":
                exclusion_note = (
                    f"Excluded role principal(s) due to failed Keycloak principal mapping (no successful group/user assignment): {', '.join(excluded_roles)}"
                )
                error_message = f"{error_message} | {exclusion_note}" if error_message else exclusion_note

            effective_groups = applied_role_map.get(policy_name)
            groups = effective_groups if effective_groups is not None else list(set(parsed_groups))
            # Use numeric policy_id from ranger_result if available, otherwise use policy_name
            numeric_policy_id = policy_id_map.get(policy_name, policy_name)
            status_list.append({
                'run_id': tracking_run_id,
                'policy_id': numeric_policy_id,
                'policy_name': policy_name,
                'users': list(set(users)),
                'groups': groups,
                'permissions': list(set(permissions)),
                'rowfilter': rowfilter,
                'status': status,
                'error_message': error_message,
                'created_at': now,
                'updated_at': now,
                'attributes': None
            })
        return status_list

    @task.pyspark(conn_id="spark_default")
    def write_policy_object_statuses(statuses: List[Dict], spark) -> Dict:
        """
        Write all object statuses in a single batch operation.
        This avoids the mapped task complexity and type mismatches.
        """
        from datetime import datetime as dt
        
        cfg = get_config()
        db = cfg["tracking_database"]
        
        if not statuses:
            return {"written": 0}
        
        written_count = 0
        
        for obj in statuses:
            obj_run_id = sql_str(obj["run_id"])
            obj_type = sql_str(obj["object_type"])
            obj_name = sql_str(obj["object_name"])
            status = sql_str(obj.get("status", "SUCCESS"))
            error_msg = sql_str(obj.get("error_message", ""))
            attempt = obj.get("attempt", 1)
            started_at = obj.get("started_at", dt.now(timezone.utc))
            completed_at = obj.get("completed_at", dt.now(timezone.utc))
            policy_id = sql_str(obj.get("policy_id", ""))
            policy_name_val = sql_str(obj.get("policy_name", ""))

            # Format timestamps using to_timestamp() for proper casting
            started_at_sql = f"to_timestamp('{started_at.strftime('%Y-%m-%d %H:%M:%S')}', 'yyyy-MM-dd HH:mm:ss')"
            completed_at_sql = f"to_timestamp('{completed_at.strftime('%Y-%m-%d %H:%M:%S')}', 'yyyy-MM-dd HH:mm:ss')" if completed_at else "NULL"

            try:
                spark.sql(f"""
                    MERGE INTO {db}.tracking_ranger_policy_object_status t
                    USING (SELECT '{obj_run_id}' AS run_id, '{obj_type}' AS object_type, '{obj_name}' AS object_name) s
                    ON t.run_id = s.run_id AND t.object_type = s.object_type AND t.object_name = s.object_name
                    WHEN MATCHED THEN UPDATE SET
                        policy_id = '{policy_id}',
                        policy_name = '{policy_name_val}',
                        status = '{status}',
                        error_message = '{error_msg}',
                        completed_at = {completed_at_sql},
                        attempt = {attempt},
                        updated_at = current_timestamp()
                    WHEN NOT MATCHED THEN INSERT (
                        run_id, object_type, object_name,
                        policy_id, policy_name,
                        status, error_message, started_at, completed_at,
                        attempt, created_at, updated_at
                    ) VALUES (
                        '{obj_run_id}', '{obj_type}', '{obj_name}',
                        '{policy_id}', '{policy_name_val}',
                        '{status}', '{error_msg}', {started_at_sql}, {completed_at_sql},
                        {attempt}, current_timestamp(), current_timestamp()
                    )
                """)
                written_count += 1
            except Exception as e:
                logger.error(f"Failed to write status for {obj_type}/{obj_name}: {e}")
        
        return {"written": written_count, "total": len(statuses)}
    
    # -----------------------------
    # DAG flow 
    # -----------------------------
    excel_path = "{{ params.excel_file_path }}"
    dag_run_identifier = "{{ run_id }}"

    init_tables = init_policy_tracking_tables()
    tracking_run_id = create_policy_run(excel_path, dag_run_identifier)
    parsed_data = parse_excel_to_dicts(excel_path)

    # Write skipped rows to tracking table
    write_skipped = write_skipped_rows(tracking_run_id, parsed_data['skipped_rows'])

    initial_policy_statuses = build_initial_policy_statuses(parsed_data, tracking_run_id)
    write_initial_policy_statuses = write_policy_statuses.override(
                    task_id="write_initial_policy_statuses")(initial_policy_statuses)

    # Check Keycloak health before attempting role creation
    keycloak_health = check_keycloak_health()
    keycloak_result = create_keycloak_roles(parsed_data, tracking_run_id, keycloak_health)

    ranger_result = create_ranger_groups_and_policies(parsed_data, tracking_run_id, keycloak_result)

    final_policy_statuses = build_final_policy_statuses(parsed_data, tracking_run_id, ranger_result)
    write_final_policy_statuses = write_policy_statuses.override(
                            task_id="write_final_policy_statuses",
                            trigger_rule=TriggerRule.ALL_DONE)(final_policy_statuses)
    debug_result = debug_list_policies()
    all_statuses = extract_statuses(ranger_result, keycloak_result)
    write_statuses = write_policy_object_statuses(all_statuses)

    finalize = finalize_policy_run(
        tracking_run_id=tracking_run_id,
        dag_run_identifier=dag_run_identifier,
        excel_file_path=excel_path,
        parsed_data=parsed_data,
        ranger_result=ranger_result,
        keycloak_result=keycloak_result,
    )


    report_result = generate_policy_report(tracking_run_id, parsed_data['skipped_rows'])
    send_email_result = send_policy_report_email(report_result, tracking_run_id)

    init_tables >> tracking_run_id
    tracking_run_id >> parsed_data
    parsed_data >> write_skipped
    parsed_data >> initial_policy_statuses
    parsed_data >> keycloak_health
    keycloak_health >> keycloak_result
    [parsed_data, keycloak_result] >> ranger_result
    initial_policy_statuses >> write_initial_policy_statuses
    ranger_result >> [debug_result, final_policy_statuses]
    final_policy_statuses >> write_final_policy_statuses
    write_initial_policy_statuses >> write_final_policy_statuses 
    [ranger_result, keycloak_result] >> all_statuses
    all_statuses >> write_statuses
    [write_statuses, write_final_policy_statuses] >> finalize
    finalize >> report_result >> send_email_result