RANGER POLICY DAG - README
===========================

Overview
--------
This Airflow DAG setup automates creating and tracking Apache Ranger policies based on an Excel input file. 
It includes two DAGs:

1. ranger_policy_creation
   - Parses an Excel file containing policy definitions.
   - Creates or updates policies in Ranger.
   - Tracks results in Iceberg tables.
   - Finalizes the run by aggregating success/failure counts.

2. ranger_policy_retry
   - Triggered manually to retry failed policies from a previous run.
   - Updates the status table and re-aggregates totals in the runs table.

Configuration
-------------
All configuration values are stored in Airflow Variables:

- policy_spark_conn_id : Spark connection ID (default: spark_default)
- policy_tracking_database : Name of the Iceberg database to track policy runs (default: policy_tracking)
- policy_tracking_location : S3 path for tracking tables (default: s3a://policy-tracking)
- ranger_endpoint : Ranger REST API endpoint (default: http://ranger:6080)
- ranger_user : Ranger username (default: admin)
- ranger_password : Ranger password (default: admin)
- ranger_service_name : Ranger service name (e.g., hive_prod)

Excel Input
-----------
Expected columns in Excel (case-insensitive, spaces replaced by underscores):
- role : Name of the policy
- database : Comma-separated list of databases (use '*' for all)
- table : Comma-separated list of tables (use '*' for all)
- column : Comma-separated list of columns (use '*' for all)
- path/url : Comma-separated list of HDFS/S3 paths (use '*' for all)
- groups : Comma-separated list of Ranger groups
- access_level : Comma-separated list of access levels

Notes:
- DB/Table access levels are mapped automatically: 
  read -> select, write -> update, create -> create, delete -> drop, all -> all
- Path/URL access uses the exact access level provided.

Iceberg Tracking Tables
-----------------------
1. ranger_policy_runs
   - run_id STRING : Unique run ID
   - dag_run_id STRING : Airflow DAG run ID
   - excel_file_path STRING : Excel file used
   - started_at TIMESTAMP
   - completed_at TIMESTAMP
   - total_policies INT
   - successful_policies INT
   - failed_policies INT
   - status STRING : COMPLETED / PARTIAL_FAILURE
   - config_json STRING : Reserved for future config snapshot

2. ranger_policy_status
   - run_id STRING
   - role STRING
   - database STRING
   - table STRING
   - column STRING
   - path STRING
   - groups STRING
   - access_level STRING
   - status STRING : SUCCESS / FAILED
   - created_at TIMESTAMP
   - message STRING : Success or error message

DAG Flow
--------
ranger_policy_creation:
  init_tracking_tables -> create_policy_run -> parse_policy_excel -> apply_policy (expand for all policies) 
  -> write_policy_status_iceberg -> finalize_policy_run

ranger_policy_retry:
  fetch_failed_policies -> apply_policy (expand) -> write_policy_status_iceberg -> finalize_policy_run

Usage
-----
1. Place your Excel file in S3 (or accessible location) and update `excel_file_path` parameter.
2. Trigger `ranger_policy_creation` DAG manually or via schedule.
3. Check Iceberg tables for run and policy status.
4. If any policies failed, trigger `ranger_policy_retry` DAG with the original `run_id`.
5. After retry, `ranger_policy_runs` table will reflect the updated totals.

Logging & Debugging
------------------
- Airflow logs show which policies were applied or failed.
- ranger_policy_status table stores per-policy success/failure messages.
- Retry DAG keeps all original and retried attempts for auditing.

Notes
-----
- Ensure Ranger service user has sufficient privileges to create/update policies.
- All paths, databases, tables, and columns can use '*' wildcard.
- The DAG supports multiple groups per policy; if no group is provided, uses '{USER}' placeholder.


