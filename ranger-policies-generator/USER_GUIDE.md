

# Ranger Policy Automation - User Guide

This guide is for security administrators, data stewards, or anyone responsible for authoring the Excel input and running the Ranger policy automation.

## What this automation does

When you run `ranger_policy_automation`, it:
- Reads your `.xlsx` input file
- Validates each row
- Creates/updates Ranger policies
- Creates Keycloak roles and assigns groups/users to those roles
- Writes audit records to tracking tables
- Generates an HTML report (and optionally emails it)

---

## Excel input: required columns and rules

Your Excel sheet must include these columns (case-insensitive):

| Column | Meaning | Required | Fill-down? | Example |
|---|---|---|---|---|
| `role` | Role name used for policy principals | Conditional | No | `data_analysts` |
| `database` | One or more schema names (comma-separated) or `*` | Yes* | **Yes** | `sales,marketing` |
| `tables` | Table names (comma-separated) or `*` | No (defaults to `*`) | **Conditional** | `orders,customers` |
| `columns` | Column names (comma-separated) or `*` | No (defaults to `*`) | **Conditional** | `id,email` |
| `url` | Storage URL path for URL policies | Yes* | **Yes** | `s3a://bucket/path/*` |
| `permissions` | Access level(s), comma-separated | No (defaults to `read`) | No | `read,write` |
| `groups` | Keycloak groups to map to the role | No | No | `finance,bi-team` |
| `users` | Keycloak users to map to the role | No | No | `alice,bob` |
| `rowfilter` | SQL row filter for table policies only. Only valid for `item_type=allow`. | No | No | `region = 'US'` |
| `policy_label` | Policy label(s) for Ranger policy | No | **Yes** | `label1,label2` |
| `item_type` | Which Ranger policy item list this row goes into. Valid values: `allow` (default), `allow_exception`, `deny`, `deny_exception`. | No | No | `deny` |

`*` Exactly one of `database` or `url` must be provided in each row:
- `database` set and `url` empty ✅
- `url` set and `database` empty ✅
- both set ❌
- both empty ❌

**Fill-down columns:** Fill-down applies to `database`, `tables`, `columns`, `url`, and `policy_label` — but only when the **primary resource identifier** for the row is also blank.

- `tables` and `columns` fill down **only when `database` is also blank** on that row.
- `url` and `policy_label` fill down when their respective cells are blank.
- When a row specifies a new `database` (or `url`) value, `tables` and `columns` on that row are read as written — blank means default (`*`), **not** the previous row's value.

This means fill-down is scoped to a "same resource block": consecutive rows that share the same resource by leaving `database`/`url` blank inherit the full resource definition. As soon as a new `database`/`url` value appears, `tables`/`columns` start fresh.

This lets you stack multiple item types for the same resource without repeating the resource definition:

```
| role          | database | tables | columns | permissions | groups       | item_type       |
|---------------|----------|--------|---------|-------------|--------------|------------------|
| analysts      | sales    | orders | *       | read        | analytics    | allow            |
| blocked       |          |        |         | read        | contractors  | deny             |
```
Both rows target the same `iceberg.sales.orders.*` policy; row 2 fills all resource columns down from row 1.

Contrast with a new database row where blank columns mean default:
```
| role          | database | tables     | columns | permissions | item_type |
|---------------|----------|------------|---------|-------------|-----------|
| analysts      | sales    | orders     | name    | read        | allow     |
| readers       | finance  |            |         | read        | allow     |
```
Row 2 has a new `database=finance`, so blank `tables` and `columns` default to `*` (not filled from row 1). Policy: `iceberg.finance.*.*`.

Item-level columns (`role`, `permissions`, `groups`, `users`, `rowfilter`, `item_type`) never fill down.

### Important validation behavior

Rows are **skipped** (not fatal for whole run) when invalid, for example:
- Both `database` and `url` are set, or both are empty
- `rowfilter` is present on a `deny`, `deny_exception`, or `allow_exception` row (rowfilter is only valid for `allow`)
- `rowfilter` is present on URL rows
- `groups` is provided but `role` is empty
- Role binding cannot be derived from row content

Skipped rows are recorded in `tracking_ranger_policy_skipped_rows` and shown in the report.

---

## How role mapping works

- If `role` is present, that role is used.
- If `users` are present and `role` is empty (and `groups` empty), synthetic roles are created:
  - user `alice` -> role `role_alice`
- If `groups` are present, `role` is mandatory.

Ranger policy grants are role/group-based. Direct Ranger user grants are not used.

---

## Permissions behavior

### Supported shorthand
- `read`: Grants read-only access (e.g., SELECT for tables, download for storage)
- `write`: Grants write access (e.g., INSERT/UPDATE/DELETE for tables, upload for storage)
- `all`: Grants all available permissions (both read and write, plus any admin actions)

### Also supported
Explicit access values like `select,insert,update,...`.

If a row contains unsupported access types for the target Ranger service, unsupported values are dropped and policy creation continues with supported ones.

If nothing valid remains after filtering for a role in a policy, that role is skipped for that policy.
If all roles are skipped, that policy is marked failed.

---


## Policy naming

- If the `policy_label` column is provided and non-empty, its value will be used as the policy label(s) for that row in Ranger.
- Otherwise, the default logic applies:
  - Table-based rows (`database` mode):
    - `database='*'` -> `iceberg`
    - database only -> `iceberg.<database>`
    - database + table -> `iceberg.<database>.<table>`
    - database + table + column -> `iceberg.<database>.<table>.<column>`
  - URL-based rows (`url` mode):
    - policy name is the URL string itself

### Best Practice: Trust Default Policy Name Generation

The default policy name generation logic (`{catalog}.{database}.{table}.{column}`) ensures that all permissions and principals for the same resource are merged into a single policy. If multiple rows in your Excel input refer to the same resource, they will generate the same policy name. The automation will:
- **Create** a new policy if the policy name does not exist yet.
- **Update** the existing policy if the policy name already exists, merging new roles, permissions, groups, users, and rowfilters as needed.

This prevents duplicate policies and keeps access definitions consolidated. Unless you have a specific need for a custom policy name, it is best practice to trust the default naming logic.

**How does it work?**
- The system checks if a policy name already exists in the internal policies dictionary.
- If it exists, it updates the policy; if not, it creates a new one.
- For example, if row 1 and row 100 both refer to the same resource, only one policy is created and all access is merged.

---

## Rowfilter behavior

For rows with `rowfilter` (table policies with `item_type=allow` only), the automation creates two Ranger policies:
1. Access policy (normal permissions) — policyType 0
2. Row-filter policy (`<base_policy_name>__rowfilter`) — policyType 2

This is expected and both can appear in status/report outputs.

Ranger does not support row filters on deny or exception items. A `rowfilter` value on a `deny`, `deny_exception`, or `allow_exception` row is ignored and the row is skipped with a warning.

---

## Deny and exception item types

The `item_type` column controls which Ranger policy item list a row's role entry is placed in:

| `item_type` | Ranger list | Use case |
|---|---|---|
| `allow` (default) | `policyItems` | Grant access |
| `allow_exception` | `allowExceptions` | Exclude a principal from an allow rule |
| `deny` | `denyPolicyItems` | Explicitly deny access |
| `deny_exception` | `denyExceptions` | Exclude a principal from a deny rule |

**Keycloak provisioning applies to all item types equally.** The KC realm role (= Ranger group) must be created and have KC group/user mappings before it can be placed in any Ranger policy item list — whether `allow`, `deny`, `allow_exception`, or `deny_exception`. All four item types go through the same KC → Ranger group provisioning flow.

All rows that share the same resource (same `database`/`tables`/`columns` or `url`) are merged into one Ranger policy, with each row contributing to its respective item list within that policy.

---


## Example Excel rows

| role           | database | tables | columns | url | permissions | groups | users | rowfilter | policy_label           |
|----------------|----------|--------|---------|-----|-------------|--------|-------|-----------|------------------------|
| `data_analysts`| `sales`  | `*`    | `*`     |     | `read`      | `analytics` | `alice,bob` | `region = 'US'` | analytics,us             |
| `etl_engineers`| `raw`    | `*`    | `*`     |     | `write`     | `engineering` |           |                   | engineering              |
|                | `finance`| `transactions` | `amount` | | `read` |        | `dave`| `dept = 'acct'` | finance,acct             |
| `ml_team`      |          |        |         | `s3a://ml-bucket/models/*` | `read,write` | `data-science` | | | ml,models              |
| `custom_team`  | `hr`     | `employees` | `salary` | | `read` | `hr-group` | `eve` |           | hr,salary               |

In the last row, the `policy_name` column is used to explicitly set the policy name to `custom_policy_hr_salary` for that policy. For all other rows, the default naming logic applies.

---

## What to expect during a DAG run

## 1) Parse and validate input
- Invalid rows are skipped and tracked.
- Valid rows are normalized into policies + role/principal mappings.

## 2) Keycloak processing
- Health check runs first.
- Roles are created/verified.
- Groups/users are mapped to roles.

## 3) Ranger processing
- Ranger groups (role names) are created/verified.
- Policies are created/updated.
- If Keycloak principal mapping fully fails for a role, that role is excluded from Ranger policy assignment.

## 4) Tracking + finalization
- Object-level and policy-level statuses are written.
- Run-level summary metrics are computed.
- Final state is `COMPLETED` or `PARTIAL_FAILURE`.

## 5) Reporting
- HTML report is generated at configured output path.
- If recipients are configured, report is emailed as attachment.

---

## Run status expectations

### Common object statuses
- `CREATED`
- `UPDATED`
- `ALREADY_EXISTS`
- `FAILED`

### Run status
- `COMPLETED`: no failed objects
- `PARTIAL_FAILURE`: one or more failed objects

A `PARTIAL_FAILURE` run can still have many successful policies.

---

## Where to check results

## Airflow task logs
Use task logs for detailed API-level errors and retry behavior.

## Tracking tables
- `tracking_ranger_policy_runs` (run summary)
- `tracking_ranger_policy_object_status` (object-level status)
- `tracking_ranger_policy_status` (policy-level details)
- `tracking_ranger_policy_skipped_rows` (skipped input rows)

## HTML report
Contains:
- Run summary and counts
- Policy status details (permissions, groups, rowfilter, errors)
- Object status details
- Skipped rows and reasons

---

## DAG trigger input

Trigger with:

```json
{
  "excel_file_path": "s3a://your-bucket/configs/ranger_policies.xlsx"
}
```

---

## Best practices for users

### 1) Excel column guidelines

Use this as the authoring standard for input files:

| Column | Required | Type | Notes | Example |
|---|---|---|---|---|
| `role` | Conditional | String | Required when `groups` is used. Optional for users-only rows (auto role is generated). | `data_analysts` |
| `database` | Conditional* | CSV String | Comma-separated DB names or `*` | `sales,marketing` |
| `tables` | No | CSV String | Comma-separated table names or `*` | `customers,orders` |
| `columns` | No | CSV String | Comma-separated column names or `*` | `name,email` |
| `url` | Conditional* | String | Storage URL policy target | `s3a://bucket/path/*` |
| `permissions` | No | CSV String | Defaults to `read` when omitted | `read,write` |
| `groups` | No | CSV String | Keycloak groups | `engineering,bi-team` |
| `users` | No | CSV String | Keycloak user IDs/usernames | `john.doe,jane.smith` |
| `rowfilter` | No | String | SQL predicate for `allow` table policies only | `country='US'` |
| `item_type` | No | String | `allow` (default), `allow_exception`, `deny`, `deny_exception` | `deny` |

`*` Exactly one of `database` or `url` must be provided per row.

### 2) Data entry DOs and DON'Ts

**DO**
- Keep one clear intent per row.
- Use consistent role naming for repeated access patterns.
- Use comma-separated lists for multiple DB/table/column/group values.
- Keep table and URL policies in separate rows.

**DON'T**
- Don’t set both `database` and `url` in one row.
- Don’t leave `role` empty when `groups` is populated.
- Don’t add `rowfilter` to URL rows.- Don't add `rowfilter` to `deny`, `deny_exception`, or `allow_exception` rows — it will be ignored.
- Don't use temporary/unclear role names that will be hard to maintain.

### 3) Naming conventions

Use stable, descriptive names because policy updates depend on deterministic naming.

**Role naming (recommended):**
- Pattern: `<team>_<function>` or `<function>_<access>`
- Examples: `data_analysts`, `finance_viewers`, `ml_engineers`

**Keycloak group naming (recommended):**
- Use organization-aligned names: `engineering`, `bi-team`, `finance-department`

**Auto-generated Ranger policy names:**
- Table policies: `iceberg.<database>[.<table>[.<column>]]`
- URL policies: `<url>`

### 4) Policy design principles

#### Principle of least privilege
- Start with `read`; escalate to `write`/`all` only when required.
- Use `all` only for limited admin scenarios.

#### Prefer hierarchical scoping
- Database-level: broadest (`*` tables/columns)
- Table-level: narrower
- Column-level: narrowest

#### Consolidate where access is identical
- Prefer one row with `sales,marketing` over two rows when permission scope is the same.
- Split rows only when access differs (for example, `sales=read,write` and `marketing=read`).

### 5) Permission management guidance

- Use `read` for analytics/reporting and audit consumers.
- Use `write` for ETL/engineering workloads that modify data.
- Use `all` only for platform/admin operations.
- For URL policies, access effectively maps to storage read/write semantics.
- Unsupported permission values are filtered out; valid values still apply.

### 6) Row filter best practices

- Keep row filters simple and readable.
- Prefer indexed/filter-friendly columns when possible.
- Avoid complex expressions that are hard to maintain.
- Use row filters only for table policies.
- Remember: row filters create an additional `<policy_name>__rowfilter` policy.

### 7) Role and group lifecycle guidance

- Model access through roles, then assign groups/users to roles.
- Prefer group-based assignment for long-term maintainability.
- Use users-only rows sparingly (they create synthetic roles like `role_<user>`).
- When personnel changes happen, update Keycloak group membership rather than rewriting policy rows.

### 8) Identity integration note

- This automation grants access through Keycloak realm roles mapped to groups/users.
- The `groups` column specifies Keycloak groups that will be mapped to the corresponding Keycloak role during automation. Ranger policy group assignments are determined by the role column, not the groups column.
- External identity-provider groups must be synchronized/mapped into Keycloak before they can participate in this flow.

### 9) Operational checklist

- Start with a small test Excel (3-5 rows) before bulk rollout.
- Validate one end-to-end role mapping (group/user -> role -> policy) first.
- Review `tracking_ranger_policy_skipped_rows` and failed object statuses after every run.
- Treat `PARTIAL_FAILURE` as actionable: fix inputs/integration issues and re-run.
