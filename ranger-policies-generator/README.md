# Ranger Policy Automation - Setup Guide

Users responsible for authoring the Excel input and running the Ranger policy automation DAG, start with [USER_GUIDE.md](USER_GUIDE.md).

## Overview

This solution automates the creation of Apache Ranger policies and Keycloak role mappings based on an Excel configuration file.

**Note on Skipped Rows:**
Any Excel rows that fail validation (e.g., missing required fields, invalid rowfilter, ambiguous mapping) are tracked, persisted in a dedicated table, and included in the HTML report with reasons for skipping. This ensures full auditability and transparency for all input errors.

## Excel Sheet Format

The Excel file should have the following columns:

| Column | Description | Required | Example |
|--------|-------------|----------|---------|
| role | Role name (becomes Ranger group); optional for users-only rows (synthetic role is created) | Conditional | `data_analysts` |
| database | Database/schema names (comma-separated, or `*` for wildcard). **Fill-down**: if blank, the value from the previous row is used. | Yes* | `sales,marketing` or `*` |
| tables | Table names (* for all, comma-separated). **Fill-down**: if blank *and `database` is also blank*, the value from the previous row is used; otherwise defaults to `*`. | No | `*` or `customers,orders` |
| columns | Column names (* for all, comma-separated). **Fill-down**: if blank *and `database` is also blank*, the value from the previous row is used; otherwise defaults to `*`. | No | `*` or `name,email,phone` |
| url | URL for storage-based policies. **Fill-down**: if blank, the value from the previous row is used. | Yes* | `s3a://bucket/path/*` |
| permissions | Access level (read, write, or both) | Yes | `read` or `read,write` |
| groups | Keycloak groups to assign the role | No | `engineering,data-team` |
| users | Users to assign the role (users must already exist in Keycloak) | No | `alice,bob` |
| rowfilter | Row-level filter expression for table policies only (optional, must not contain `;` or newlines). Only valid for `item_type=allow`. | No | `region = 'US'` |
| policy_label | Policy label(s) for Ranger policy. **Fill-down**: if blank, the value from the previous row is used. | No | `label1,label2` |
| item_type | Which Ranger policy item list this row contributes to. One of: `allow` (default), `allow_exception`, `deny`, `deny_exception`. | No | `deny` |

*Note:
- Exactly one of `database` or `url` must be provided per row (not both, not neither).
- For database, you may use `*` as a wildcard to apply the policy to all databases.
- Rowfilters are only supported for table policies with `item_type=allow`; any other combination is skipped.
- **Fill-down columns** (`database`, `tables`, `columns`, `url`, `policy_label`) carry their value forward to subsequent rows that leave them blank — but with one important rule: `tables` and `columns` only fill down when `database` is also blank on that row. When a row introduces a new `database` value, blank `tables`/`columns` reset to their defaults (`*`), not the previous row's values. Item-level columns (`role`, `permissions`, `groups`, `users`, `rowfilter`, `item_type`) never fill down.
*



# Policy Name Generation

- If the `policy_label` column is provided and non-empty, its value will be used as the policy label(s) for that row in Ranger.
- Otherwise, the default logic applies:
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

### Best Practice: Trust Default Policy Name Generation

The default policy name generation logic (`{catalog}.{database}.{table}.{column}`) ensures that all permissions and principals for the same resource are merged into a single policy. If multiple rows in your Excel input refer to the same resource, they will generate the same policy name. The automation will:
- **Create** a new policy if the policy name does not exist yet.
- **Update** the existing policy if the policy name already exists, merging new roles, permissions, groups, users, and rowfilters as needed.

This prevents duplicate policies and keeps access definitions consolidated. Unless you have a specific need for a custom policy name, it is best practice to trust the default naming logic.

**How does it work?**
- The system checks if a policy name already exists in the internal policies dictionary.
- If it exists, it updates the policy; if not, it creates a new one.
- For example, if row 1 and row 100 both refer to the same resource, only one policy is created and all access is merged.


## Excel Sheet Sample

The Excel file with sample values:
```
| role           | database         | tables       | columns     | url                        | permissions  | groups               | users         | rowfilter         | policy_label  | item_type        |
|----------------|------------------|--------------|-------------|----------------------------|--------------|----------------------|---------------|-------------------|---------------|------------------|
| data_analysts  | sales,marketing  | *            | *           |                            | read         | analysts,bi-team     | alice,bob     | region = 'US'     | analytics,us  | allow            |
| blocked_users  |                  |              |             |                            | read         | blocked-team         |               |                   |               | deny             |
| data_engineers | sales            | customers    | *           |                            | read,write   | engineering          | carol         |                   | engineering   | allow            |
| data_engineers |                  |              |             |                            | read         | engineering-exc      |               |                   |               | allow_exception  |
| data_engineers | raw_data         | *            | *           |                            | write        | engineering          |               |                   | raw           | allow            |
| ml_team        |                  |              |             | s3a://ml-bucket/models/*   | read,write   | data-science         |               |                   | ml,models     | allow            |
| finance_team   | finance          | transactions | amount,date |                            | read         | finance,accounting   | dave,erin     | dept = 'acct'     | finance,acct  | allow            |
```

In rows 2 and 4, `database`/`tables`/`columns` are left blank and fill down from row 1 and 3 respectively — both rows contribute to the same Ranger policy as their predecessor but go into different item lists (`deny` and `allow_exception`).

# Role, Group, and User Mapping Logic

- If a row specifies groups, a role is required. Groups are mapped to the specified role.
- If a row specifies users, and a role is specified -> Users are mapped to the specified role.
- If a row specifies only users (no groups, no role), a role named `role_<username>` is created for each user and mapped to that user.
- If both users and groups are present with a role, both are mapped to the role.
- Roles and mappings are created in Keycloak, and groups are created in Ranger as needed.
- Ranger policy grants are role/group-based only (no direct Ranger user grants).
- Ranger policy assignment for a role proceeds when at least one Keycloak principal mapping (group or user) is successful/already exists.
- A role is blocked for Ranger policy assignment only when all attempted Keycloak principal mappings fail for that role (or role creation fails).
- **`deny`, `deny_exception`, and `allow_exception` rows require KC provisioning just like `allow` rows.** The KC realm role (= Ranger group) must be created and have KC group/user mappings before it can be placed in any Ranger policy item list. All four item types go through the same KC → Ranger group provisioning flow.

# Policy Item Types (`item_type`)

The `item_type` column controls which of the four Ranger policy item lists a row's role entry is placed into:

| `item_type` | Ranger API field | Meaning |
|---|---|---|
| `allow` (default) | `policyItems` | Grants access |
| `allow_exception` | `allowExceptions` | Excludes principals from an allow rule |
| `deny` | `denyPolicyItems` | Explicitly denies access |
| `deny_exception` | `denyExceptions` | Excludes principals from a deny rule |

If `item_type` is omitted or blank, it defaults to `allow`.

All rows sharing the same resource (same `database`/`tables`/`columns` or `url`) are merged into a single Ranger policy, regardless of their `item_type`. The `item_type` only determines which item list within that policy they belong to.

# Validation and SQL Safeguards

- All policy names and rowfilters are sanitized before being used in SQL.
- Rowfilters containing `;` or newlines are rejected and logged.
- Invalid Excel rows are skipped, persisted in `tracking_ranger_policy_skipped_rows`, and shown in the HTML report.

**Note on Row Filter Policy Architecture (Apache Ranger):**
In Apache Ranger, row filter policies (`policyType=2`) are implemented as a distinct policy type, separate from standard access policies and their deny/exception items. Row filters apply only to `allow`-type policy entries and define a filter condition that is enforced when access is granted. Ranger does not support attaching row filters to deny rules. Consequently, a `rowfilter` value on a `deny`, `deny_exception`, or `allow_exception` row in the Excel input is ignored — only `allow` rows contribute to the row filter policy.

*Note: Policy names are prefixed with `iceberg` to match the catalog used for table-type policies in Ranger.*

## Permission Mappings

| Excel Permission | Ranger Access Types |
|-----------------|---------------------|
| `read` | select, use, execute, show, read_sysinfo |
| `write` | select, insert, update, delete, create, drop, alter, use, show, grant, revoke, execute, read, write, read_sysinfo, write_sysinfo |
| `all` | select, insert, update, delete, create, drop, alter, use, show, grant, revoke, impersonate, execute, read, write, read_sysinfo, write_sysinfo |

Explicit permissions such as `select,update,create,drop,alter,all,read,write` are also accepted and passed through.

If a permissions list contains unsupported values, those values are ignored and policy creation continues with the remaining supported permissions.

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
Variable.set("keycloak_verify_ssl", "false")  # optional, default false
Variable.set("keycloak_cacert", "/path/to/ca-bundle.pem")  # optional, used when verify_ssl=true

# Email / SMTP Configuration (optional)
Variable.set("policy_smtp_conn_id", "smtp_default")
Variable.set("policy_email_recipients", "ops@example.com,security@example.com")
```

`keycloak_verify_ssl=false` disables TLS certificate verification for Keycloak connections.
When `keycloak_verify_ssl=true`, `keycloak_cacert` can be set to a custom CA bundle path.

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
airflow variables set keycloak_verify_ssl "false"
# Optional when using a private/internal CA:
airflow variables set keycloak_cacert "/path/to/ca-bundle.pem"
# Optional email report delivery:
airflow variables set policy_smtp_conn_id "smtp_default"
airflow variables set policy_email_recipients "ops@example.com,security@example.com"
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
init_tracking_tables
    │
    ▼
create_run_record
    │
    ▼
parse_excel ──► write_skipped_rows ──► write_initial_policy_status
    │
    ├──► check_keycloak_health ──► create_keycloak_roles_and_mappings
    │                                  │
    └──────────────────────────────────┼──► create_ranger_groups_and_policies
                                       │
                                       └──► write_object_statuses

create_ranger_groups_and_policies ──► build_final_policy_status ──► write_final_policy_status

write_object_statuses + write_final_policy_status ──► finalize_run ──► generate_policy_report
generate_policy_report ──► send_policy_report_email
```

## Outputs

The DAG produces a summary with:
- Number of policies created/updated/failed
- Number of Ranger groups created/existing
- Number of Keycloak roles created
- Number of mappings created/existing/failed
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

If `policy_email_recipients` is configured, the generated report is also emailed as an HTML attachment via the SMTP connection defined by `policy_smtp_conn_id`.

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

## Running Tests

From the repo root, install dev dependencies:

```bash
pip install ".[dev]"
```

Run the test suite from the project directory:

```bash
cd ranger-policies-generator
pytest tests/           # fast, no coverage
pytest tests/ --cov     # with coverage
```

Coverage settings (source, 80% threshold) are configured in `.coveragerc`.


## Notes for Dev

Env files are loaded from `/opt/airflow/utils/migration_configs/`:

- `env.shared` — shared config (S3, SSH, Spark, Ranger, Keycloak credentials, etc.)
- `env.<dag_stem>` — per-developer overrides (e.g. `env.ranger-policies-generator_airflow3`)

Copy the `env.*.example` files there, drop the `.example` suffix, and fill in your values. If the directory doesn't exist the DAG logs a warning and falls back to Airflow Variables / defaults.

Config resolution: Airflow Variable → `os.getenv()` → hardcoded default in `get_config()`.
