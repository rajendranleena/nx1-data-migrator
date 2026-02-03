# Ranger Policy Automation - Setup Guide

## Overview

This solution automates the creation of Apache Ranger policies and Keycloak role mappings based on an Excel configuration file.

## Excel Sheet Format

The Excel file should have the following columns:

| Column | Description | Required | Example |
|--------|-------------|----------|---------|
| role | Role name (becomes Ranger group) | Yes | `data_analysts` |
| database | Database/schema names (comma-separated) | Yes* | `sales,marketing` |
| tables | Table names (* for all, comma-separated) | No | `*` or `customers,orders` |
| columns | Column names (* for all, comma-separated) | No | `*` or `name,email,phone` |
| url | URL for storage-based policies | Yes* | `s3a://bucket/path/*` |
| permissions | Access level (read, write, or both) | Yes | `read` or `read,write` |
| groups | Keycloak groups to assign the role | No | `engineering,data-team` |

*Note: Either `database` OR `url` must be provided, not both.

### Example Excel Data

```
| role           | database         | tables    | columns | url                        | permissions  | groups               |
|----------------|------------------|-----------|---------|----------------------------|--------------|----------------------|
| data_analysts  | sales,marketing  | *         | *       |                            | read         | analysts,bi-team     |
| data_engineers | sales            | customers | *       |                            | read,write   | engineering          |
| data_engineers | raw_data         | *         | *       |                            | write        | engineering          |
| ml_team        |                  |           |         | s3a://ml-bucket/models/*   | read,write   | data-science         |
| finance_team   | finance          | transactions | amount,date |                     | read         | finance,accounting   |
```

## Generated Policy Names

Based on the Excel data, policy names are generated as follows:

1. **For database-level access (tables=*, columns=*):**
   - Policy name: `iceberg.{database}`
   - Example: `iceberg.sales`

2. **For table-level access (columns=*):**
   - Policy name: `iceberg.{database}.{table}`
   - Example: `iceberg.sales.customers`

3. **For column-level access:**
   - Policy name: `iceberg.{database}.{table}.{column}`
   - Example: `iceberg.finance.transactions.amount`

4. **For URL-based policies:**
   - Policy name: `{url}`
   - Example: `s3a://ml-bucket/models/*`

*Note: Policy names are prefixed with `iceberg` to match the catalog used for table-type policies in Ranger. The `iceberg` catalog supports both Iceberg and Hive tables.

## Permission Mappings

| Excel Permission | Ranger Access Types |
|-----------------|---------------------|
| `read` | select, use, execute, show, read_sysinfo |
| `write` | select, insert, update, delete, create, drop, alter, use, show, grant, revoke, execute, read, write, read_sysinfo, write_sysinfo |

## Airflow Variables Required

Set these Airflow Variables before running the DAG:

```python
# Ranger Configuration
Variable.set("ranger_url", "https://ranger.example.com")
Variable.set("ranger_username", "admin")
Variable.set("ranger_password", "your_ranger_password")
Variable.set("nx1_repo_name", "nx1-unifiedsql")

# Keycloak Configuration
Variable.set("keycloak_url", "https://keycloak.example.com/auth")
Variable.set("keycloak_realm", "your-realm")
Variable.set("keycloak_admin_client_id", "ranger-user-sync")
Variable.set("keycloak_admin_client_secret", "your_client_secret")
```

## DAG Parameters

When triggering the DAG, you can pass these parameters:

```json
{
    "excel_file_path": "s3a://your-bucket/configs/ranger_policies.xlsx",
    "dry_run": false
}
```

## Usage

### 1. Prepare Your Excel File

Create an Excel file (.xlsx) with the policy definitions following the format above.

### 2. Upload to S3

Upload the Excel file to an S3 location accessible by your Airflow cluster:

```bash
s3Cli put ranger_policies.xlsx s3://your-bucket/configs/
```

### 3. Set Airflow Variables

Configure the required Airflow Variables via the UI or CLI:

```bash
airflow variables set ranger_url "https://ranger.example.com"
airflow variables set ranger_username "admin"
# ... etc
```

### 4. Trigger the DAG

Trigger the DAG via Airflow UI or CLI:

```bash
airflow dags trigger ranger_policy_automation \
    --conf '{"excel_file_path": "s3a://your-bucket/configs/ranger_policies.xlsx"}'
```
## Ranger Utils install
place the utils under utils/migrations directory in airflow/jupyter

## Task Flow

```
parse_excel
    │
    ▼
create_ranger_groups
    │
    ├────────────────────┐
    ▼                    ▼
create_ranger_policies   create_keycloak_roles_and_mappings
    │                    │
    └────────────────────┘
              │
              ▼
        generate_summary
```

## Outputs

The DAG produces a summary with:
- Number of policies created/updated/failed
- Number of Ranger groups created
- Number of Keycloak roles created
- Number of group-role mappings created
- List of any failures with error messages


### Logging

Check Airflow task logs for detailed output:
- Policy creation/update results
- Group creation results
- Keycloak role/group mapping results
