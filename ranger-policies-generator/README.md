# Ranger Policy Automation - Setup Guide

## Overview

This solution automates the creation of Apache Ranger policies and Keycloak role mappings based on an Excel configuration file.

**Note on Skipped Rows:**
Any Excel rows that fail validation (e.g., missing required fields, invalid rowfilter, ambiguous mapping) are tracked, persisted in a dedicated table, and included in the HTML report with reasons for skipping. This ensures full auditability and transparency for all input errors.

## Excel Sheet Format

The Excel file should have the following columns:

| Column | Description | Required | Example |
|--------|-------------|----------|---------|
| role | Role name (becomes Ranger group) | Yes | `data_analysts` |
| database | Database/schema names (comma-separated, or `*` for wildcard) | Yes* | `sales,marketing` or `*` |
| tables | Table names (* for all, comma-separated) | No | `*` or `customers,orders` |
| columns | Column names (* for all, comma-separated) | No | `*` or `name,email,phone` |
| url | URL for storage-based policies | Yes* | `s3a://bucket/path/*` |
| permissions | Access level (read, write, or both) | Yes | `read` or `read,write` |
| groups | Keycloak groups to assign the role | No | `engineering,data-team` |
| users | Users to assign the role (optional, derived from roles) | No | `alice,bob` |
| rowfilter | Row-level filter expression for table policies only (optional, must not contain `;` or newlines) | No | `region = 'US'` |

*Note: 
For database, you may use `*` as a wildcard to apply the policy to all databases. 
Rowfilters are only supported for table policies; URL-based rows with a rowfilter are skipped.
*

# Policy Name Generation

1. **For all-database access (database=`*`):**
    - Policy name: `iceberg`
    - Example: `iceberg`
2. **For database-level access (tables=*, columns=*):**
    - Policy name: `iceberg.{database}`
    - Example: `iceberg.sales`
3. **For table-level access (columns=*):**
    - Policy name: `iceberg.{database}.{table}`
    - Example: `iceberg.sales.customers`
4. **For column-level access:**
    - Policy name: `iceberg.{database}.{table}.{column}`
    - Example: `iceberg.finance.transactions.amount`
5. **For URL-based policies:**
    - Policy name: `{url}`
    - Example: `s3a://ml-bucket/models/*`


## Excel Sheet Sample

The Excel file with sample values:
```
| role           | database         | tables    | columns | url                        | permissions  | groups               | users         | rowfilter         |
|----------------|------------------|-----------|---------|----------------------------|--------------|----------------------|---------------|-------------------|
| data_analysts  | sales,marketing  | *         | *       |                            | read         | analysts,bi-team     | alice,bob     | region = 'US'     |
| data_engineers | sales            | customers | *       |                            | read,write   | engineering          | carol         |                   |
| data_engineers | raw_data         | *         | *       |                            | write        | engineering          |               |                   |
| ml_team        |                  |           |         | s3a://ml-bucket/models/*   | read,write   | data-science         |               |                   |
| finance_team   | finance          | transactions | amount,date |                     | read         | finance,accounting   | dave,erin     | dept = 'acct'     |
```

# Role, Group, and User Mapping Logic

- If a row specifies groups, a role is required. Groups are mapped to the specified role.
- If a row specifies users, and a role is specified -> Users are mapped to the specified role.
- If a row specifies only users (no groups, no role), a role named `role_<username>` is created for each user and mapped to that user.
- If both users and groups are present with a role, both are mapped to the role.
- Roles and mappings are created in Keycloak, and groups are created in Ranger as needed.

# SQL Injection Safeguards

- All policy names and rowfilters are sanitized before being used in SQL.
- Rowfilters containing `;` or newlines are rejected and logged.

## Generated Policy Names

Based on the Excel data, policy names are generated as follows:

0. **For all-database access (database=`*`):**
    - Policy name: `iceberg`
    - Example: `iceberg`

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

When triggering the DAG, you can pass this parameter:

```json
{
    "excel_file_path": "s3a://your-bucket/configs/ranger_policies.xlsx"
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

## HTML Report

After each run, an HTML report is generated and saved to the configured output location (e.g., S3). This report provides:

- Run metadata and summary statistics
- Policy-level status details (users, groups, permissions, rowfilter, status, error messages)
- Object-level status details (type, name, status, error, timestamps)
- Skipped/invalid Excel rows with reasons for skipping (for audit and troubleshooting)

You can use this report for auditing, troubleshooting, and compliance tracking.

## Tracking Tables

This solution creates the following tracking tables in your configured Iceberg database to provide robust, auditable records of all policy automation runs:

- **tracking_ranger_policy_runs**: Run-level summary and metrics
- **tracking_ranger_policy_object_status**: Status for each object (policy, group, role, etc.)
- **tracking_ranger_policy_status**: Detailed status for each policy, including users, groups, permissions, rowfilter, and error messages
- **tracking_ranger_policy_skipped_rows**: All skipped/invalid Excel rows with reasons for skipping (for audit and troubleshooting)

## Row-Level Filtering (Rowfilters)

When you specify a rowfilter expression in the Excel sheet, the system automatically creates TWO policies in Ranger:

1. **Access Policy**: Grants the full set of requested permissions (read/write/all) expanded to their appropriate Ranger access types (e.g., read → select, use, execute, show, etc.)
2. **Row Filter Policy**: Applies the SQL filter expression to limit which rows are visible (restricted to SELECT access for row visibility filtering)

Both policies work together to provide secure, filtered access. You specify a rowfilter once in Excel, but they are tracked as separate policies in Ranger and appear as separate entries in the tracking tables and HTML report. 