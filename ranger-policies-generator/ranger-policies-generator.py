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


default_args = {
    'owner': 'trino-admin',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


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


with DAG(
    dag_id='ranger_policy_automation_v2',
    default_args=default_args,
    description='Automate Ranger policy creation from Excel configuration (v2)',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['ranger', 'security', 'policy-automation'],
    params={
        'excel_file_path': 's3a://your-bucket/configs/ranger_policies.xlsx',
        'dry_run': False,
    }
) as dag:

    @task.pyspark(conn_id='spark_default')
    def parse_excel_to_dicts(excel_file_path: str, run_id: str, spark, sc) -> Dict[str, Any]:
        """
        Parse Excel file into two dictionaries:
        1. policies: {policy_name: {type, catalog, schema, table, column, url, roles: [{role, permissions}]}}
        2. role_groups: {role_name: [group_names]}
        """
        import pandas as pd
        from io import BytesIO
        
        # Read Excel from S3
        binary_df = spark.read.format("binaryFile").load(excel_file_path)
        row = binary_df.select("content").first()
        excel_bytes = bytes(row.content)
        df = pd.read_excel(BytesIO(excel_bytes), engine='openpyxl')
        
        # Normalize column names
        df.columns = df.columns.str.lower().str.strip()
        
        policies = {}
        role_groups = {}
        
        for _, row in df.iterrows():
            # Extract row values
            role = str(row.get('role', '')).strip()
            databases = str(row.get('database', '')).strip()
            tables = str(row.get('tables', '*')).strip() or '*'
            columns = str(row.get('columns', '*')).strip() or '*'
            url = str(row.get('url', '')).strip()
            permissions = str(row.get('permissions', 'read')).strip()
            groups = str(row.get('groups', '')).strip()
            
            # Skip empty rows
            if not role or role.lower() == 'nan':
                continue
            
            # Build role_groups dictionary
            group_list = parse_csv_field(groups)
            if group_list:
                if role not in role_groups:
                    role_groups[role] = []
                for g in group_list:
                    if g not in role_groups[role]:
                        role_groups[role].append(g)
            
            # Parse permissions
            perm_list = parse_permission_string(permissions)
            
            # Handle URL-based policies
            if url and url.lower() != 'nan':
                policy_name = url
                
                if policy_name not in policies:
                    policies[policy_name] = {
                        'type': 'url',
                        'url': url,
                        'roles': []
                    }
                
                # Check if role already exists in this policy
                existing_roles = [r['role'] for r in policies[policy_name]['roles']]
                if role not in existing_roles:
                    policies[policy_name]['roles'].append({
                        'role': role,
                        'permissions': perm_list
                    })
                else:
                    # Merge permissions
                    for r in policies[policy_name]['roles']:
                        if r['role'] == role:
                            for p in perm_list:
                                if p not in r['permissions']:
                                    r['permissions'].append(p)
            else:
                # Handle table-based policies
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
                                    'table': table if table else '*',
                                    'column': column if column else '*',
                                    'roles': []
                                }
                            
                            # Check if role already exists
                            existing_roles = [r['role'] for r in policies[policy_name]['roles']]
                            if role not in existing_roles:
                                policies[policy_name]['roles'].append({
                                    'role': role,
                                    'permissions': perm_list
                                })
                            else:
                                # Merge permissions
                                for r in policies[policy_name]['roles']:
                                    if r['role'] == role:
                                        for p in perm_list:
                                            if p not in r['permissions']:
                                                r['permissions'].append(p)
        
        logger.info(f"Parsed {len(policies)} policies and {len(role_groups)} role-group mappings")
        
        return {
            'policies': policies,
            'role_groups': role_groups
        }

    @task
    def create_ranger_groups_and_policies(parsed_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create Ranger groups and policies from parsed data.
        Uses the RangerPolicyManager utility class.
        """
        from ranger_utils import RangerPolicyManager
        
        # Get configuration from Airflow Variables
        ranger_url = Variable.get('ranger_url')
        ranger_username = Variable.get('ranger_username')
        ranger_password = Variable.get('ranger_password', deserialize_json=False)
        service_name = Variable.get('nx1_repo_name', default_var='nx1-unifiedsql')
        
        # Initialize manager
        manager = RangerPolicyManager(
            ranger_url=ranger_url,
            ranger_username=ranger_username,
            ranger_password=ranger_password,
            service_name=service_name
        )
        
        policies = parsed_data['policies']
        role_groups = parsed_data['role_groups']
        
        # Collect all unique roles from both policies and role_groups
        all_roles = set(role_groups.keys())
        for policy_data in policies.values():
            for role_entry in policy_data.get('roles', []):
                all_roles.add(role_entry['role'])
        
        # Create groups
        groups_result = manager.ensure_groups_exist(list(all_roles))
        created_groups = [g for g, created in groups_result.items() if created]
        existing_groups = [g for g, created in groups_result.items() if not created]
        
        logger.info(f"Created {len(created_groups)} groups, {len(existing_groups)} already existed")
        
        # Create policies
        policies_result = manager.sync_policies_from_dict(policies)
        
        logger.info(f"Created {len(policies_result['created'])} policies, "
                   f"updated {len(policies_result['updated'])}, "
                   f"failed {len(policies_result['failed'])}")
        
        return {
            'groups': {
                'created': created_groups,
                'existing': existing_groups
            },
            'policies': policies_result
        }

    @task
    def create_keycloak_roles(parsed_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create Keycloak realm roles and assign groups to them.
        Uses the KeycloakRoleManager utility class.
        """
        from ranger_utils import KeycloakRoleManager
        
        # Get Keycloak configuration from Airflow Variables
        keycloak_url = Variable.get('keycloak_url')
        keycloak_realm = Variable.get('keycloak_realm')
        keycloak_client_id = Variable.get('keycloak_admin_client_id')
        keycloak_client_secret = Variable.get('keycloak_admin_client_secret', deserialize_json=False)
        
        # Initialize manager
        manager = KeycloakRoleManager(
            server_url=keycloak_url,
            realm_name=keycloak_realm,
            client_id=keycloak_client_id,
            client_secret=keycloak_client_secret
        )
        
        role_groups = parsed_data['role_groups']
        
        # Sync roles and group mappings
        result = manager.sync_roles_and_groups(role_groups)
        
        logger.info(f"Created {len(result['created_roles'])} Keycloak roles, "
                   f"created {len(result['created_mappings'])} group mappings, "
                   f"failed {len(result['failed'])}")
        
        return result

    @task
    def generate_report(
        parsed_data: Dict[str, Any],
        ranger_result: Dict[str, Any],
        keycloak_result: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Generate execution report."""
        
        report = {
            'execution_time': datetime.now().isoformat(),
            'summary': {
                'total_policies_parsed': len(parsed_data['policies']),
                'total_role_mappings_parsed': len(parsed_data['role_groups'])
            },
            'ranger': {
                'groups_created': len(ranger_result['groups']['created']),
                'groups_existing': len(ranger_result['groups']['existing']),
                'policies_created': len(ranger_result['policies']['created']),
                'policies_updated': len(ranger_result['policies']['updated']),
                'policies_failed': len(ranger_result['policies']['failed'])
            },
            'keycloak': {
                'roles_created': len(keycloak_result['created_roles']),
                'roles_existing': len(keycloak_result['existing_roles']),
                'mappings_created': len(keycloak_result['created_mappings']),
                'mappings_existing': len(keycloak_result['existing_mappings']),
                'failed_operations': len(keycloak_result['failed'])
            },
            'details': {
                'created_policies': ranger_result['policies']['created'],
                'updated_policies': ranger_result['policies']['updated'],
                'failed_policies': ranger_result['policies']['failed'],
                'created_keycloak_roles': keycloak_result['created_roles'],
                'keycloak_failures': keycloak_result['failed']
            }
        }
        
        # Print summary
        print("\n" + "=" * 70)
        print("RANGER POLICY AUTOMATION - EXECUTION REPORT")
        print("=" * 70)
        print(f"\nExecution Time: {report['execution_time']}")
        print(f"\n--- Input Summary ---")
        print(f"  Policies parsed: {report['summary']['total_policies_parsed']}")
        print(f"  Role mappings parsed: {report['summary']['total_role_mappings_parsed']}")
        print(f"\n--- Ranger Results ---")
        print(f"  Groups created: {report['ranger']['groups_created']}")
        print(f"  Groups existing: {report['ranger']['groups_existing']}")
        print(f"  Policies created: {report['ranger']['policies_created']}")
        print(f"  Policies updated: {report['ranger']['policies_updated']}")
        print(f"  Policies failed: {report['ranger']['policies_failed']}")
        print(f"\n--- Keycloak Results ---")
        print(f"  Roles created: {report['keycloak']['roles_created']}")
        print(f"  Roles existing: {report['keycloak']['roles_existing']}")
        print(f"  Mappings created: {report['keycloak']['mappings_created']}")
        print(f"  Mappings existing: {report['keycloak']['mappings_existing']}")
        print(f"  Failed operations: {report['keycloak']['failed_operations']}")
        
        if report['details']['failed_policies']:
            print(f"\n--- Failed Policies ---")
            for fp in report['details']['failed_policies']:
                print(f"  - {fp['name']}: {fp['error']}")
        
        if report['details']['keycloak_failures']:
            print(f"\n--- Keycloak Failures ---")
            for kf in report['details']['keycloak_failures']:
                print(f"  - {kf['operation']}: {kf.get('role', '')} / {kf.get('group', '')} - {kf['error']}")
        
        print("\n" + "=" * 70)
        
        return report

    # Task dependencies
    excel_path = "{{ params.excel_file_path }}"
    run_id = "{{ run_id }}"
    
    parsed_data = parse_excel_to_dicts(excel_path, run_id)
    ranger_result = create_ranger_groups_and_policies(parsed_data)
    keycloak_result = create_keycloak_roles(parsed_data)
    report = generate_report(parsed_data, ranger_result, keycloak_result)
    
    # Define execution order
    parsed_data >> [ranger_result, keycloak_result] >> report
