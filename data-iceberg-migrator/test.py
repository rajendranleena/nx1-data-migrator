"""
Test Env Combined Migration DAGs - v4

This file contains two independent DAGs:
1. mapr_to_s3_migration: Migrates data and Hive tables (metadata) from MapR or HDFS to S3
2. iceberg_migration: Converts existing Hive tables in S3 to Apache Iceberg format

Both DAGs can be run independently. The iceberg_migration DAG is typically run after mapr_to_s3_migration is complete, but they are not automatically chained.


1. MapR/HDFS to S3 Migration DAG

Orchestrates migration of Hive tables from MapR or HDFS to S3:
- Excel config from S3 (only DAG parameter)
- SSH operations for MapR or Kerberos authentication, beeline discovery, distcp (24h timeout)
- PySpark tasks for Hive table creation
- Incremental support (distcp -update, table repair)
- Comprehensive validation (row counts, partitions, schema)

Excel columns: database | table | dest database | bucket

2. Iceberg Migration DAG

Converts existing Hive tables in S3 to Apache Iceberg format.
This DAG runs independently after the main MapR-to-S3 migration is complete.

Two migration strategies supported:
1. In-place migration: Convert existing Hive table to Iceberg (overwrites metadata)
2. Snapshot migration: Create separate Iceberg table alongside Hive table

Excel columns: database | table | inplace_migration | destination_iceberg_database
"""

import json
import logging
import os
import random
import time
import urllib.parse
import re as _re
from datetime import datetime, timedelta
from functools import wraps
from pathlib import Path

from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.models.param import Param
from dotenv import load_dotenv

_dag_stem = Path(__file__).stem
logger = logging.getLogger(__name__)

_dag_dir = Path(__file__).resolve().parent
_config_dir = str(_dag_dir / 'utils' / 'migration_configs')
if os.path.isdir(_config_dir):
    load_dotenv(os.path.join(_config_dir, 'env.shared'))
    load_dotenv(os.path.join(_config_dir, f'env.{_dag_stem}'), override=True)
else:
    logger.warning(f"Config directory {_config_dir} not found — env files not loaded, using Airflow Variables / defaults")

# =============================================================================
# Duration tracking decorator using XCom
# =============================================================================
def track_duration(func):
    """Decorator to automatically track task duration via result dict."""
    @wraps(func)
    def wrapper(*args, **kwargs):
        from datetime import datetime as dt
        start_time = dt.utcnow()
        result = func(*args, **kwargs)
        end_time = dt.utcnow()
        duration = (end_time - start_time).total_seconds()

        # Add duration to result if it's a dict
        if isinstance(result, dict):
            result['_task_duration'] = duration

        return result

    return wrapper

def execute_with_iceberg_retry(spark, sql: str, max_retries: int = 6, task_label: str = ""):
    """Execute Spark SQL with retry logic for Iceberg commit conflicts."""
    status = False
    counter = 0
    last_exception = None

    while not status and counter < max_retries:
        try:
            spark.sql(sql)
            status = True
        except Exception as e:
            last_exception = e
            counter += 1
            if counter < max_retries:
                sleep_secs = random.choice([10, 20, 30, 40, 50])
                logger.warning(
                    f"[IcebergRetry] {task_label} hit commit conflict "
                    f"(attempt {counter}/{max_retries}). Retrying in {sleep_secs}s... Error: {str(e)[:200]}"
                )
                time.sleep(sleep_secs)
            else:
                logger.error(f"[IcebergRetry] {task_label} failed after {max_retries} attempts.")

    if not status:
        raise last_exception
    
def _apply_partition_filter(partitions: list, filter_expr) -> list:
    """Filter Hive partition strings against a filter expression."""
    import urllib.parse
    import re

    if not filter_expr:
        return partitions

    def parse_partition(part_str):
        """Parse 'year=2023/month=01' -> {'year': '2023', 'month': '01'}"""
        result = {}
        for segment in part_str.split('/'):
            if '=' in segment:
                k, _, v = segment.partition('=')
                result[k.strip()] = urllib.parse.unquote(v.strip())
        return result

    def try_numeric(val):
        try:
            return int(val)
        except ValueError:
            return val

    terms = [t.strip() for t in filter_expr.split(',') if t.strip()]
    matched = set()

    for term in terms:
        m = re.match(r'^last_n_partitions=(\d+)$', term.strip())
        if m:
            n = int(m.group(1))
            sorted_parts = sorted(partitions, reverse=True)
            for p in sorted_parts[:n]:
                matched.add(p)
            continue

        if not any(op in term for op in ('>=', '<=', '>', '<')):
            if term.endswith('/*') or term.endswith('*'):
                prefix = term.rstrip('*').rstrip('/')
                for p in partitions:
                    if p.startswith(prefix):
                        matched.add(p)
            else:
                for p in partitions:
                    if p == term:
                        matched.add(p)
            continue

        op_match = re.match(r'^([a-zA-Z_][a-zA-Z0-9_]*)(>=|<=|>|<)(.+)$', term.strip())
        if op_match:
            key, op, threshold = op_match.group(1), op_match.group(2), op_match.group(3).strip()
            threshold_cmp = try_numeric(threshold)
            for p in partitions:
                pdict = parse_partition(p)
                if key not in pdict:
                    continue
                pval_cmp = try_numeric(pdict[key])
                try:
                    if op == '>='  and pval_cmp >= threshold_cmp: matched.add(p)
                    elif op == '<=' and pval_cmp <= threshold_cmp: matched.add(p)
                    elif op == '>'  and pval_cmp >  threshold_cmp: matched.add(p)
                    elif op == '<'  and pval_cmp <  threshold_cmp: matched.add(p)
                except TypeError:
                    if op == '>='  and str(pdict[key]) >= str(threshold): matched.add(p)
                    elif op == '<=' and str(pdict[key]) <= str(threshold): matched.add(p)
                    elif op == '>'  and str(pdict[key]) >  str(threshold): matched.add(p)
                    elif op == '<'  and str(pdict[key]) <  str(threshold): matched.add(p)
            continue

        logger.warning(f"[PartitionFilter] Unrecognised filter term '{term}' — skipping.")

    return [p for p in partitions if p in matched]


def _partitions_to_where_clause(partitions: list) -> str:
    """Convert partition strings to a SQL WHERE clause."""
    if not partitions:
        return "1=0"
    clauses = []
    for part_str in partitions:
        conditions = []
        for segment in part_str.split('/'):
            if '=' in segment:
                k, _, v = segment.partition('=')
                conditions.append(f"{k.strip()}='{v.strip()}'")
        if conditions:
            clauses.append("(" + " AND ".join(conditions) + ")")
    return " OR ".join(clauses) if clauses else "1=1"

# =============================================================================
# SHARED CONFIGURATION
# =============================================================================

def get_config() -> dict:
    """Shared configuration for all DAGs (mapr_to_s3_migration, iceberg_migration, folder_only_data_copy)"""
    return {
        # SSH Configuration (for MapR migration)
        'ssh_conn_id': Variable.get('cluster_ssh_conn_id', default_var=os.getenv('CLUSTER_SSH_CONN_ID', 'cluster_edge_ssh')),
        'edge_temp_path': Variable.get('cluster_edge_temp_path', default_var=os.getenv('CLUSTER_EDGE_TEMP_PATH', '/tmp/migration')),

        # S3 Configuration
        'default_s3_bucket': Variable.get('migration_default_s3_bucket', default_var=os.getenv('MIGRATION_DEFAULT_S3_BUCKET', 's3a://data-lake')),
        's3_endpoint': Variable.get('s3_endpoint', default_var=os.getenv('S3_ENDPOINT', '')),
        's3_access_key': Variable.get('s3_access_key', default_var=os.getenv('S3_ACCESS_KEY', '')),
        's3_secret_key': Variable.get('s3_secret_key', default_var=os.getenv('S3_SECRET_KEY', '')),

        # DistCp Configuration
        'distcp_mappers': Variable.get('migration_distcp_mappers', default_var=os.getenv('MIGRATION_DISTCP_MAPPERS', '50')),
        'distcp_bandwidth': Variable.get('migration_distcp_bandwidth', default_var=os.getenv('MIGRATION_DISTCP_BANDWIDTH', '100')),

        # Spark Configuration
        'spark_conn_id': Variable.get('migration_spark_conn_id', default_var=os.getenv('MIGRATION_SPARK_CONN_ID', 'spark_default')),

        # Tracking Configuration
        'tracking_database': Variable.get('migration_tracking_database', default_var=os.getenv('MIGRATION_TRACKING_DATABASE', 'migration_tracking')),
        'tracking_location': Variable.get('migration_tracking_location', default_var=os.getenv('MIGRATION_TRACKING_LOCATION', 's3a://data-lake/migration_tracking')),
        'report_output_location': Variable.get('migration_report_location', default_var=os.getenv('MIGRATION_REPORT_LOCATION', 's3a://data-lake/migration_reports')),

        # Cluster Authentication (MapR or Kerberos)
        'auth_method': Variable.get('auth_method', default_var=os.getenv('AUTH_METHOD', 'mapr')),  # 'mapr' or 'kinit'
        'mapr_user': Variable.get('mapr_user', default_var=os.getenv('MAPR_USER', '')),
        'mapr_ticketfile_location': Variable.get('mapr_ticketfile_location', default_var=os.getenv('MAPR_TICKETFILE_LOCATION', '/tmp/maprticket_${USER}')),
        'kinit_principal': Variable.get('kinit_principal', default_var=os.getenv('KINIT_PRINCIPAL', '')),
        'kinit_keytab': Variable.get('kinit_keytab', default_var=os.getenv('KINIT_KEYTAB', '')),
        'kinit_password': Variable.get('kinit_password', default_var=os.getenv('KINIT_PASSWORD', '')),

        # Listing tool
        's3_listing_tool': Variable.get('s3_listing_tool', default_var=os.getenv('S3_LISTING_TOOL', 'hadoop')),

        # Email / SMTP Configuration
        'smtp_conn_id': Variable.get('migration_smtp_conn_id', default_var=os.getenv('MIGRATION_SMTP_CONN_ID', 'smtp_default')),
        'email_recipients': Variable.get('migration_email_recipients', default_var=os.getenv('MIGRATION_EMAIL_RECIPIENTS', '')),
    }


DEFAULT_ARGS = {
    'owner': 'gsomisetty@nexuscognitive.com',
    'depends_on_past': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# =============================================================================
# TEST DATASET SETUP  (v4: adds `sales` table for rich partition_filter tests)
# =============================================================================

@task.pyspark(conn_id='spark_default')
def setup_test_datasets(spark) -> dict:
    """
    Create source datasets in S3.

    migration_test_src_es_v2_3:
      1.  customers      – Parquet, partitioned by country
      2.  orders         – ORC,     partitioned by order_date
      3.  products       – Parquet, unpartitioned
      4.  pipe_delimited – TEXTFILE (|), unpartitioned, with NULL values
      5.  tab_delimited  – TEXTFILE (\\t), unpartitioned
      6.  sales          – Parquet, partitioned by year + month
      7.  customers_2    – Parquet, partitioned by country   ← NEW (wildcard expansion)
      8.  products_2     – Parquet, unpartitioned            ← NEW (csv list / multirow)

    migration_test_src_2_es_v2_3:
      9.  events         – Parquet, partitioned by region
      10. events_2       – Parquet, partitioned by region    ← NEW (multirow same db)
    """
    from decimal import Decimal

    from pyspark.sql import Row
    from pyspark.sql.types import (
        DateType, DecimalType, IntegerType, StringType,
        StructField, StructType,
    )

    config = get_config()
    bucket = config['default_s3_bucket']
    created = []

    # ── helper ────────────────────────────────────────────────────────────────
    def create_external_table(df, db, tbl, fmt, location,
                              partition_cols=None, field_delim=None,
                              escape_delim=None, null_format=None):
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {db} LOCATION '{bucket}/{db}'")
        spark.sql(f"DROP TABLE IF EXISTS {db}.{tbl}")

        if fmt.upper() == 'TEXTFILE':
            sep  = field_delim if field_delim else ','
            opts = {'sep': sep, 'header': 'false', 'nullValue': null_format or ''}
            if partition_cols:
                df.write.mode('overwrite').partitionBy(*partition_cols).options(**opts).csv(location)
            else:
                df.write.mode('overwrite').options(**opts).csv(location)

            non_part  = [c for c in df.columns if c not in (partition_cols or [])]
            col_defs  = ', '.join(f"`{c}` {df.schema[c].dataType.simpleString()}" for c in non_part)
            part_cls  = ''
            if partition_cols:
                part_cls = "PARTITIONED BY (" + ', '.join(
                    f"`{c}` {df.schema[c].dataType.simpleString()}" for c in partition_cols
                ) + ")"
            esc_cls   = f"ESCAPED BY '{escape_delim}'" if escape_delim else ''
            null_prop = (f"TBLPROPERTIES ('serialization.null.format'='{null_format}')"
                         if null_format else '')
            spark.sql(f"""
                CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{tbl} ({col_defs})
                {part_cls}
                ROW FORMAT DELIMITED FIELDS TERMINATED BY '{sep}' {esc_cls}
                STORED AS TEXTFILE LOCATION '{location}' {null_prop}
            """)
            if partition_cols:
                spark.sql(f"MSCK REPAIR TABLE {db}.{tbl}")
        else:
            if partition_cols:
                df.write.mode('overwrite').format(fmt.lower()).partitionBy(*partition_cols).save(location)
            else:
                df.write.mode('overwrite').format(fmt.lower()).save(location)

            non_part = [c for c in df.columns if c not in (partition_cols or [])]
            col_defs = ', '.join(f"`{c}` {df.schema[c].dataType.simpleString()}" for c in non_part)
            part_cls = ''
            if partition_cols:
                part_cls = "PARTITIONED BY (" + ', '.join(
                    f"`{c}` {df.schema[c].dataType.simpleString()}" for c in partition_cols
                ) + ")"
            spark.sql(f"""
                CREATE EXTERNAL TABLE IF NOT EXISTS {db}.{tbl} ({col_defs})
                {part_cls}
                STORED AS {fmt.upper()} LOCATION '{location}'
            """)
            if partition_cols:
                spark.sql(f"MSCK REPAIR TABLE {db}.{tbl}")

        row_count = spark.sql(f"SELECT COUNT(*) as c FROM {db}.{tbl}").collect()[0]['c']
        logger.info(f"[SetupDatasets] {db}.{tbl} | fmt={fmt} | rows={row_count}")
        created.append({'database': db, 'table': tbl, 'format': fmt, 'rows': row_count})

    SRC1 = 'migration_test_src_es_v2_3'
    SRC2 = 'migration_test_src_2_es_v2_3'

    # ── 1. customers ──────────────────────────────────────────────────────────
    customers_data = [
        Row(customer_id=1,  name='Alice Smith',   email='alice@example.com',  age=30, country='US'),
        Row(customer_id=2,  name='Bob Jones',     email='bob@example.com',    age=25, country='UK'),
        Row(customer_id=3,  name='Carol Lee',     email='carol@example.com',  age=35, country='US'),
        Row(customer_id=4,  name='Dave Brown',    email='dave@example.com',   age=28, country='CA'),
        Row(customer_id=5,  name='Eve Wilson',    email='eve@example.com',    age=32, country='UK'),
        Row(customer_id=6,  name='Frank Garcia',  email='frank@example.com',  age=45, country='US'),
        Row(customer_id=7,  name="Grace O'Brien", email='grace@example.com',  age=29, country='CA'),
        Row(customer_id=8,  name='Hank Martinez', email='hank@example.com',   age=38, country='US'),
    ]
    create_external_table(
        spark.createDataFrame(customers_data),
        SRC1, 'customers', 'PARQUET',
        f'{bucket}/{SRC1}/customers',
        partition_cols=['country'],
    )

    # ── 2. orders ─────────────────────────────────────────────────────────────
    orders_schema = StructType([
        StructField('order_id',    IntegerType(),      True),
        StructField('customer_id', IntegerType(),      True),
        StructField('product_id',  IntegerType(),      True),
        StructField('quantity',    IntegerType(),      True),
        StructField('amount',      DecimalType(10, 2), True),
        StructField('order_date',  StringType(),       True),
    ])
    orders_data = [
        (101, 1, 10, 2, Decimal('49.98'),  '2024-01-15'),
        (102, 2, 11, 1, Decimal('29.99'),  '2024-01-15'),
        (103, 3, 12, 3, Decimal('89.97'),  '2024-01-16'),
        (104, 1, 10, 1, Decimal('24.99'),  '2024-01-16'),
        (105, 4, 13, 2, Decimal('59.98'),  '2024-01-17'),
        (106, 5, 11, 4, Decimal('119.96'), '2024-01-17'),
        (107, 6, 14, 1, Decimal('39.99'),  '2024-01-18'),
        (108, 7, 12, 2, Decimal('59.98'),  '2024-01-18'),
    ]
    create_external_table(
        spark.createDataFrame(orders_data, schema=orders_schema),
        SRC1, 'orders', 'ORC',
        f'{bucket}/{SRC1}/orders',
        partition_cols=['order_date'],
    )

    # ── 3. products ───────────────────────────────────────────────────────────
    products_data = [
        Row(product_id=10, name='Widget A', category='Electronics', price=24.99, in_stock=True),
        Row(product_id=11, name='Widget B', category='Electronics', price=29.99, in_stock=True),
        Row(product_id=12, name='Gadget X', category='Tools',       price=29.99, in_stock=False),
        Row(product_id=13, name='Gadget Y', category='Tools',       price=29.99, in_stock=True),
        Row(product_id=14, name='Thing Z',  category='Misc',        price=39.99, in_stock=True),
    ]
    create_external_table(
        spark.createDataFrame(products_data),
        SRC1, 'products', 'PARQUET',
        f'{bucket}/{SRC1}/products',
    )

    # ── 4. pipe_delimited ─────────────────────────────────────────────────────
    pipe_schema = StructType([
        StructField('employee_id', IntegerType(),      True),
        StructField('full_name',   StringType(),       True),
        StructField('department',  StringType(),       True),
        StructField('salary',      DecimalType(10, 2), True),
        StructField('notes',       StringType(),       True),
    ])
    pipe_data = [
        (1,  'Alice Smith',   'Engineering', Decimal('95000.00'), 'Lead engineer'),
        (2,  'Bob Jones',     'Marketing',   Decimal('72000.00'), 'Campaign manager'),
        (3,  'Carol Lee',     'Engineering', Decimal('88000.00'), None),
        (4,  'Dave Brown',    'HR',          Decimal('65000.00'), 'Handles onboarding'),
        (5,  'Eve Wilson',    'Engineering', Decimal('91000.00'), 'Backend focus'),
        (6,  'Frank Garcia',  'Finance',     Decimal('80000.00'), None),
        (7,  'Grace OBrien',  'Marketing',   Decimal('70000.00'), 'Social media lead'),
        (8,  'Hank Martinez', 'Engineering', Decimal('99000.00'), 'Architect'),
        (9,  'Irene Chu',     'HR',          Decimal('67000.00'), 'Recruiter'),
        (10, 'Jake Patel',    'Finance',     Decimal('83000.00'), 'Tax specialist'),
    ]
    create_external_table(
        spark.createDataFrame(pipe_data, schema=pipe_schema),
        SRC1, 'pipe_delimited', 'TEXTFILE',
        f'{bucket}/{SRC1}/pipe_delimited',
        field_delim='|', escape_delim='\\\\', null_format='\\N',
    )

    # ── 5. tab_delimited ──────────────────────────────────────────────────────
    tab_schema = StructType([
        StructField('log_id',    IntegerType(), True),
        StructField('log_level', StringType(),  True),
        StructField('message',   StringType(),  True),
        StructField('log_date',  StringType(),  True),
    ])
    tab_data = [
        (1, 'INFO',  'Application started',   '2024-01-15'),
        (2, 'WARN',  'Low memory warning',     '2024-01-15'),
        (3, 'ERROR', 'Connection timeout',     '2024-01-16'),
        (4, 'INFO',  'Job completed',          '2024-01-16'),
        (5, 'DEBUG', 'Processing record 42',   '2024-01-17'),
        (6, 'ERROR', 'Null pointer exception', '2024-01-17'),
    ]
    create_external_table(
        spark.createDataFrame(tab_data, schema=tab_schema),
        SRC1, 'tab_delimited', 'TEXTFILE',
        f'{bucket}/{SRC1}/tab_delimited',
        field_delim='\t',
    )

    # ── 6. sales (multi-level partition: year + month) ────────────────────────
    sales_schema = StructType([
        StructField('sale_id',    IntegerType(),      True),
        StructField('product_id', IntegerType(),      True),
        StructField('region',     StringType(),       True),
        StructField('amount',     DecimalType(12, 2), True),
        StructField('year',       StringType(),       True),
        StructField('month',      StringType(),       True),
    ])
    sales_data = [
        (1001, 10, 'US', Decimal('1500.00'), '2023', '10'),
        (1002, 11, 'UK', Decimal('800.00'),  '2023', '10'),
        (1003, 12, 'CA', Decimal('600.00'),  '2023', '11'),
        (1004, 10, 'US', Decimal('2000.00'), '2023', '11'),
        (1005, 13, 'UK', Decimal('950.00'),  '2023', '12'),
        (1006, 14, 'CA', Decimal('1200.00'), '2023', '12'),
        (1007, 10, 'US', Decimal('1800.00'), '2024', '01'),
        (1008, 11, 'UK', Decimal('900.00'),  '2024', '01'),
        (1009, 12, 'CA', Decimal('700.00'),  '2024', '02'),
        (1010, 10, 'US', Decimal('2100.00'), '2024', '02'),
        (1011, 13, 'UK', Decimal('1050.00'), '2024', '03'),
        (1012, 14, 'CA', Decimal('1300.00'), '2024', '03'),
    ]
    create_external_table(
        spark.createDataFrame(sales_data, schema=sales_schema),
        SRC1, 'sales', 'PARQUET',
        f'{bucket}/{SRC1}/sales',
        partition_cols=['year', 'month'],
    )

    # ── 7. customers_2  (NEW – wildcard expansion: cust* matches customers + customers_2) ──
    # Same schema as customers, different data, adds AU partition not in customers
    customers_2_data = [
        Row(customer_id=101, name='Liam Chen',    email='liam@example.com',   age=27, country='US'),
        Row(customer_id=102, name='Mia Patel',    email='mia@example.com',    age=31, country='AU'),
        Row(customer_id=103, name='Noah Kim',     email='noah@example.com',   age=24, country='UK'),
        Row(customer_id=104, name='Olivia Cruz',  email='olivia@example.com', age=36, country='US'),
        Row(customer_id=105, name='Paul Nguyen',  email='paul@example.com',   age=42, country='CA'),
        Row(customer_id=106, name='Quinn Lee',    email='quinn@example.com',  age=33, country='AU'),
    ]
    create_external_table(
        spark.createDataFrame(customers_2_data),
        SRC1, 'customers_2', 'PARQUET',
        f'{bucket}/{SRC1}/customers_2',
        partition_cols=['country'],
    )

    # ── 8. products_2  (NEW – comma-separated list & multirow no-filter tests) ──
    products_2_data = [
        Row(product_id=20, name='Gizmo A',  category='Electronics', price=19.99, in_stock=True),
        Row(product_id=21, name='Gizmo B',  category='Electronics', price=34.99, in_stock=False),
        Row(product_id=22, name='Thingamajig', category='Misc',     price=9.99,  in_stock=True),
        Row(product_id=23, name='Doohickey', category='Tools',      price=14.99, in_stock=True),
    ]
    create_external_table(
        spark.createDataFrame(products_2_data),
        SRC1, 'products_2', 'PARQUET',
        f'{bucket}/{SRC1}/products_2',
    )

    # ── 9. events (original) ──────────────────────────────────────────────────
    events_data = [
        Row(event_id=1001, event_type='click',    user_id=1, event_ts='2024-01-15 10:00:00', region='us-east'),
        Row(event_id=1002, event_type='purchase', user_id=2, event_ts='2024-01-15 10:05:00', region='eu-west'),
        Row(event_id=1003, event_type='click',    user_id=3, event_ts='2024-01-15 10:10:00', region='us-east'),
        Row(event_id=1004, event_type='login',    user_id=1, event_ts='2024-01-16 09:00:00', region='us-east'),
        Row(event_id=1005, event_type='purchase', user_id=4, event_ts='2024-01-16 11:30:00', region='ap-south'),
    ]
    create_external_table(
        spark.createDataFrame(events_data),
        SRC2, 'events', 'PARQUET',
        f'{bucket}/{SRC2}/events',
        partition_cols=['region'],
    )

    # ── 10. events_2  (NEW – multirow same-db no-filter test) ─────────────────
    events_2_data = [
        Row(event_id=2001, event_type='scroll',   user_id=5, event_ts='2024-02-01 08:00:00', region='us-east'),
        Row(event_id=2002, event_type='click',    user_id=6, event_ts='2024-02-01 08:15:00', region='eu-west'),
        Row(event_id=2003, event_type='login',    user_id=7, event_ts='2024-02-02 09:30:00', region='ap-south'),
        Row(event_id=2004, event_type='purchase', user_id=5, event_ts='2024-02-02 10:00:00', region='us-east'),
        Row(event_id=2005, event_type='scroll',   user_id=8, event_ts='2024-02-03 11:00:00', region='eu-west'),
        Row(event_id=2006, event_type='click',    user_id=9, event_ts='2024-02-03 12:00:00', region='ap-south'),
    ]
    create_external_table(
        spark.createDataFrame(events_2_data),
        SRC2, 'events_2', 'PARQUET',
        f'{bucket}/{SRC2}/events_2',
        partition_cols=['region'],
    )

    logger.info(
        f"[SetupDatasets] Done — {len(created)} table(s): "
        + ', '.join(f"{t['database']}.{t['table']}" for t in created)
    )
    return {'bucket': bucket, 'tables_created': created}


# =============================================================================
# DAG 1: S3-TO-S3 MIGRATION TASKS
# =============================================================================

@task
def validate_prerequisites(run_id: str) -> dict:
    """Pre-DAG validation (Test Env – no SSH)."""
    validation_results = {'pyspark_available': False, 'hive_available': False, 's3_accessible': False, 'errors': []}
    logger.info("=" * 60)
    logger.info("STARTING PRE-DAG VALIDATION (Test Environment)")
    logger.info("=" * 60)

    logger.info("[1/3] Testing PySpark…")
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("validation_test").getOrCreate()
        spark.range(1).collect()
        validation_results['pyspark_available'] = True
        logger.info("PySpark: PASSED")
        spark.stop()
    except Exception as e:
        validation_results['errors'].append(f"PySpark: {str(e)}")
        logger.error(f"PySpark: FAILED — {str(e)}")

    logger.info("[2/3] Testing Hive…")
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("hive_test").enableHiveSupport().getOrCreate()
        spark.sql("SHOW DATABASES").collect()
        validation_results['hive_available'] = True
        logger.info("Hive: PASSED")
        spark.stop()
    except Exception as e:
        validation_results['errors'].append(f"Hive: {str(e)}")
        logger.error(f"Hive: FAILED — {str(e)}")

    logger.info("[3/3] Testing S3 access…")
    try:
        config = get_config()
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("s3_test").getOrCreate()
        test_path = config['default_s3_bucket']
        hconf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jvm.java.net.URI(test_path), hconf)
        fs.exists(spark._jvm.org.apache.hadoop.fs.Path(test_path))
        validation_results['s3_accessible'] = True
        logger.info("S3 Access: PASSED")
        spark.stop()
    except Exception as e:
        validation_results['errors'].append(f"S3: {str(e)}")
        logger.error(f"S3: FAILED — {str(e)}")

    all_passed = all([validation_results['pyspark_available'],
                      validation_results['hive_available'],
                      validation_results['s3_accessible']])
    if all_passed:
        logger.info("ALL PRE-DAG CHECKS PASSED")
        return validation_results
    logger.error("SOME PRE-DAG CHECKS FAILED")
    raise Exception(
        f"Pre-DAG validation failed — {len(validation_results['errors'])} check(s) failed:\n"
        + "\n".join(f"  - {e}" for e in validation_results['errors'])
    )


@task
def cluster_login_setup(run_id: str) -> dict:
    """No-op stub – mirrors production task graph position; no SSH in test env."""
    config = get_config()
    temp_dir = f"{config['edge_temp_path']}/{run_id}"
    logger.info(f"[ClusterLogin] Test env — stub. temp_dir={temp_dir}")
    return {'temp_dir': temp_dir, 'run_id': run_id}


@task.pyspark(conn_id='spark_default')
def init_tracking_tables(spark) -> dict:
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
                source_location STRING,
                file_format STRING,
                partition_count INT,
                is_partitioned BOOLEAN,
                schema_json STRING,
                partitions_json STRING,
                partition_columns STRING,
                table_type STRING,
                source_row_count BIGINT,
                source_total_size_bytes BIGINT,
                source_file_count BIGINT,
                s3_total_size_bytes_before BIGINT,
                s3_file_count_before BIGINT,
                s3_total_size_bytes_after BIGINT,
                s3_file_count_after BIGINT,
                s3_bytes_transferred BIGINT,
                s3_files_transferred BIGINT,
                file_size_match BOOLEAN,
                file_count_match BOOLEAN,
                partition_filter STRING,
                filtered_partition_count INT,
                full_table_row_count BIGINT,
                full_table_partition_count INT,
                discovery_status STRING,
                discovery_completed_at TIMESTAMP,
                discovery_duration_seconds DOUBLE,
                distcp_status STRING,
                distcp_started_at TIMESTAMP,
                distcp_completed_at TIMESTAMP,
                distcp_duration_seconds DOUBLE,
                distcp_is_incremental BOOLEAN,
                distcp_bytes_copied BIGINT,
                distcp_files_copied BIGINT,
                table_create_status STRING,
                table_create_completed_at TIMESTAMP,
                table_create_duration_seconds DOUBLE,
                table_already_existed BOOLEAN,
                validation_status STRING,
                validation_completed_at TIMESTAMP,
                validation_duration_seconds DOUBLE,
                dest_hive_row_count BIGINT,
                source_partition_count INT,
                unregistered_partitions BOOLEAN,
                dest_partition_count INT,
                row_count_match BOOLEAN,
                partition_count_match BOOLEAN,
                schema_match BOOLEAN,
                schema_differences STRING,
                overall_status STRING,
                error_message STRING,
                updated_at TIMESTAMP
            )
            USING iceberg
            PARTITIONED BY (source_database)
            LOCATION '{tracking_loc}/migration_table_status'
        """)

    return {'status': 'initialized', 'database': tracking_db}


@task.pyspark(conn_id='spark_default')
def create_migration_run(excel_file_path: str, dag_run_id: str, spark) -> str:
    """Create migration run record in Iceberg tracking table."""
    import uuid
    from datetime import datetime

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
def parse_excel(excel_file_path: str, run_id: str, spark) -> list:
    """Read Excel config from S3 using pandas.read_excel."""
    from io import BytesIO
    import math
    import pandas as ps

    config = get_config()
    binary_df = spark.read.format("binaryFile").load(excel_file_path)
    row = binary_df.select("content").first()
    excel_bytes = bytes(row.content)
    df = ps.read_excel(BytesIO(excel_bytes), engine='openpyxl')

    # Normalize column names
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

    def normalize_bucket(raw: str) -> str:
        val = raw.strip()
        if val.startswith('s3n://'):
            val = 's3a://' + val[6:]
        elif val.startswith('s3://'):
            val = 's3a://' + val[5:]
        elif not val.startswith('s3a://'):
            val = f"s3a://{val}"
        return val.rstrip('/') 

    # Convert to list of dicts
    grouped = {}
    for _, row in df.iterrows():
        src_db = str(row.get('database', '') or '').strip()
        if not src_db:
            continue

        raw_pf = row.get('partition_filter', '')
        partition_filter = None
        if raw_pf is not None and not (isinstance(raw_pf, float) and math.isnan(raw_pf)):
            stripped = str(raw_pf).strip()
            if stripped and stripped.lower() not in ('', 'nan', 'none'):
                partition_filter = stripped

        raw_cell_val = row.get('table', '')
        raw_cell = '*' if (raw_cell_val is None or (isinstance(raw_cell_val, float) and __import__('math').isnan(raw_cell_val)) or str(raw_cell_val).strip().lower() in ('', 'nan')) else str(raw_cell_val).strip() or '*'
        dest_db_val = row.get('dest_database', '')
        dest_db = src_db if (dest_db_val is None or (isinstance(dest_db_val, float) and __import__('math').isnan(dest_db_val)) or str(dest_db_val).strip().lower() in ('', 'nan')) else str(dest_db_val).strip() or src_db
        raw_bucket_val = row.get('bucket', '')
        raw_bucket = '' if (raw_bucket_val is None or (isinstance(raw_bucket_val, float) and __import__('math').isnan(raw_bucket_val)) or str(raw_bucket_val).strip().lower() == 'nan') else str(raw_bucket_val).strip()
        bucket_val = normalize_bucket(raw_bucket) if raw_bucket else config['default_s3_bucket']

        # Warn if wildcard used with a filter
        if '*' in [t.strip() for t in raw_cell.split(',')] and partition_filter:
            logger.warning(
                f"[ParseExcel] Wildcard '*' combined with partition_filter='{partition_filter}' "
                f"in {src_db}. Wildcard will be expanded per-table with filter applied to each."
            )

        # Warn if comma-separated tables used with a filter
        tokens_in_row = [t.strip() for t in raw_cell.split(',') if t.strip()]
        if len(tokens_in_row) > 1 and partition_filter:
            logger.warning(
                f"[ParseExcel] Comma-separated tables {tokens_in_row} combined with "
                f"partition_filter='{partition_filter}' in {src_db}. "
                f"Each table will inherit the same filter."
            )

        key = (src_db, dest_db, bucket_val, partition_filter)
        if key not in grouped:
            grouped[key] = {'bucket': bucket_val, 'tokens': [], 'partition_filter': partition_filter}

        for tok in raw_cell.split(','):
            tok = tok.strip()
            if tok:
                grouped[key]['tokens'].append(tok)

    configs = []

    for (src_db, dest_db, bucket_val, partition_filter), group in grouped.items():
        unique_tokens = list(dict.fromkeys(group['tokens']))
        if '*' in unique_tokens:
            unique_tokens = ['*']

        bucket_val = group['bucket']

        logger.info(
            f"[ParseExcel] {src_db} -> dest={dest_db} | bucket={bucket_val} | "
            f"tokens={unique_tokens[:10]}"
            + (" ..." if len(unique_tokens) > 10 else "")
        )

        configs.append({
            'source_database': src_db,
            'table_tokens': unique_tokens,
            'dest_database': dest_db,
            'dest_bucket': bucket_val,
            'run_id': run_id,
            'partition_filter': partition_filter, 
        })

    logger.info(f"[ParseExcel] Total database configs emitted: {len(configs)}")
    return configs


@task.pyspark(conn_id='spark_default')
@track_duration
def discover_tables_via_spark(db_config: dict, spark) -> dict:
    """
    Discover tables and metadata using Spark SQL.

    v4 additions (full parity with production SSH-based task):
      - Reads partition_filter from db_config
      - Applies _apply_partition_filter inline
      - Captures filtered_partitions, filtered_row_count, filtered_source_size_bytes,
        filtered_file_count, full_table_row_count, full_table_partition_count,
        partition_filter_active flag
      - Emits warnings for edge cases (unpartitioned table + filter, 0 matches)
      - Captures serde_properties (field.delim, escape.delim, null.format)
    """
    import urllib.parse as _urlparse

    run_id          = db_config['run_id']
    src_db          = db_config['source_database']
    raw_tokens      = db_config.get('table_tokens') or []
    if not raw_tokens:
        pattern_str = db_config.get('table_pattern', '*')
        raw_tokens  = [t.strip() for t in pattern_str.split(',') if t.strip()] or ['*']
    dest_db         = db_config['dest_database']
    dest_bucket     = db_config['dest_bucket']
    partition_filter = db_config.get('partition_filter') or ''

    def resolve_tokens(spark, db, tokens):
        resolved, seen = [], set()
        for tok in tokens:
            if tok == '*':
                rows = spark.sql(f"SHOW TABLES IN {db}").collect()
                for r in rows:
                    if r.tableName not in seen:
                        seen.add(r.tableName); resolved.append(r.tableName)
            elif '*' in tok or '?' in tok:
                rows = spark.sql(f"SHOW TABLES IN {db} LIKE '{tok}'").collect()
                for r in rows:
                    if r.tableName not in seen:
                        seen.add(r.tableName); resolved.append(r.tableName)
            else:
                if tok not in seen:
                    seen.add(tok); resolved.append(tok)
        return resolved

    table_list = resolve_tokens(spark, src_db, raw_tokens)
    logger.info(f"[Discovery] '{src_db}': {len(table_list)} table(s) resolved | partition_filter={partition_filter!r}")

    metadata = []

    for tbl in table_list:
        try:
            desc_df   = spark.sql(f"DESCRIBE FORMATTED {src_db}.{tbl}")
            desc_rows = desc_df.collect()

            loc = None; table_type = "UNKNOWN"; input_format = None
            serde_properties = {}; in_serde_section = False

            for row in desc_rows:
                col_name  = (row.col_name  or "").strip().rstrip(":").lower()
                data_type = (row.data_type or "").strip()

                if col_name == "location":
                    loc = data_type
                elif col_name in ("type", "table type"):
                    table_type = data_type.replace("_TABLE", "")
                elif col_name == "inputformat":
                    input_format = data_type
                elif col_name in ("storage properties", "serde library", "serialization lib",
                                  "serde:", "storage information"):
                    in_serde_section = True
                elif in_serde_section and col_name and not col_name.startswith("#"):
                    if col_name in ("field.delim", "serialization.format", "escape.delim",
                                    "quote.delim", "null.format", "field delimiter",
                                    "fields.terminated.by"):
                        serde_properties[col_name] = data_type
                elif col_name.startswith("#"):
                    in_serde_section = False

            # Auto-detect delimiter for TEXTFILE when DESCRIBE doesn't surface it
            if input_format and 'text' in input_format.lower() and not serde_properties.get('field.delim'):
                try:
                    if loc:
                        sample_rdd = spark.sparkContext.textFile(loc).take(2)
                        if sample_rdd:
                            for candidate in ('|', '\t', ';', ','):
                                if candidate in sample_rdd[0]:
                                    serde_properties['field.delim'] = candidate
                                    logger.info(f"[Discovery] Auto-detected delimiter '{candidate}' for {src_db}.{tbl}")
                                    break
                except Exception:
                    pass

            # Size metrics
            source_total_size = 0; source_file_count = 0
            if loc:
                try:
                    from py4j.java_gateway import java_import
                    java_import(spark._jvm, "org.apache.hadoop.fs.*")
                    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                        spark._jvm.java.net.URI(loc), spark._jsc.hadoopConfiguration()
                    )
                    path = spark._jvm.org.apache.hadoop.fs.Path(loc)
                    if fs.exists(path):
                        cs = fs.getContentSummary(path)
                        source_total_size = int(cs.getLength())
                        source_file_count = int(cs.getFileCount())
                except Exception:
                    pass

            # File format
            file_format = "PARQUET"
            if input_format:
                if "parquet" in input_format.lower():   file_format = "PARQUET"
                elif "orc"    in input_format.lower():  file_format = "ORC"
                elif "avro"   in input_format.lower():  file_format = "AVRO"
                elif "text"   in input_format.lower():  file_format = "TEXTFILE"

            # Partition columns
            partition_cols = []
            in_part_section = False
            for row in desc_rows:
                col_name = (row.col_name or "").strip()
                if col_name == "# Partition Information": in_part_section = True; continue
                if in_part_section and col_name == "# col_name": continue
                if in_part_section and col_name.startswith("#"): break
                if in_part_section and col_name: partition_cols.append(col_name)

            partition_definition = len(partition_cols) > 0
            partition_columns    = ",".join(partition_cols)

            # All partitions
            all_partitions = []; registered_partition_count = 0
            try:
                parts_df      = spark.sql(f"SHOW PARTITIONS {src_db}.{tbl}")
                all_partitions = [r.partition for r in parts_df.collect()]
                registered_partition_count = len(all_partitions)
            except Exception:
                pass

            full_partition_count = len(all_partitions)

            # Row count (full table – before filter)
            full_row_count = 0
            try:
                full_row_count = spark.sql(
                    f"SELECT COUNT(*) as c FROM {src_db}.{tbl}"
                ).collect()[0].c
            except Exception:
                pass

            # ── Apply partition filter ─────────────────────────────────────
            filtered_partitions   = _apply_partition_filter(all_partitions, partition_filter)
            partition_filter_active = (
                bool(partition_filter)
                and len(filtered_partitions) < len(all_partitions)
            )

            # Edge-case warnings
            if partition_filter and not partition_definition:
                logger.warning(
                    f"[Discovery] partition_filter set but {src_db}.{tbl} is unpartitioned "
                    "— filter will be ignored."
                )
                partition_filter_active = False
                filtered_partitions     = all_partitions

            if partition_filter and partition_definition and len(filtered_partitions) == 0:
                logger.warning(
                    f"[Discovery] partition_filter '{partition_filter}' matched 0 "
                    f"partitions for {src_db}.{tbl}"
                )

            # Filtered size / file count
            filtered_source_size = source_total_size
            filtered_file_count  = source_file_count
            if partition_filter_active and filtered_partitions and loc:
                filtered_source_size = 0; filtered_file_count = 0
                try:
                    from py4j.java_gateway import java_import
                    java_import(spark._jvm, "org.apache.hadoop.fs.*")
                    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                        spark._jvm.java.net.URI(loc), spark._jsc.hadoopConfiguration()
                    )
                    for part_str in filtered_partitions:
                        try:
                            part_path = spark._jvm.org.apache.hadoop.fs.Path(loc + "/" + part_str)
                            if fs.exists(part_path):
                                cs = fs.getContentSummary(part_path)
                                filtered_source_size += int(cs.getLength())
                                filtered_file_count  += int(cs.getFileCount())
                        except Exception:
                            pass
                except Exception:
                    pass

            # Filtered row count
            row_count = full_row_count
            if partition_filter_active and filtered_partitions:
                try:
                    where_clause = _partitions_to_where_clause(filtered_partitions)
                    row_count = spark.sql(
                        f"SELECT COUNT(*) as c FROM {src_db}.{tbl} WHERE {where_clause}"
                    ).collect()[0].c
                except Exception:
                    pass

            # Tracking counts
            tracking_row_count       = row_count if partition_filter_active else full_row_count
            tracking_partition_count = len(filtered_partitions) if partition_filter_active else full_partition_count
            tracking_size_bytes      = filtered_source_size if partition_filter_active else source_total_size
            tracking_file_count      = filtered_file_count  if partition_filter_active else source_file_count

            is_partitioned          = partition_definition
            unregistered_partitions = partition_definition and registered_partition_count == 0

            # Schema
            schema = []
            for row in spark.sql(f"DESCRIBE {src_db}.{tbl}").collect():
                cn = row.col_name.strip() if row.col_name else ""
                ct = row.data_type.strip() if row.data_type else ""
                if cn.startswith("#") or cn in ("", "col_name"): break
                schema.append({"name": cn, "type": ct})

            s3_location = f"{dest_bucket}/{dest_db}/{tbl}"

            partitions_for_output = filtered_partitions if partition_filter_active else all_partitions

            logger.info(
                f"[Discovery] {src_db}.{tbl} | fmt={file_format} | "
                f"total_partitions={full_partition_count} | "
                f"filtered_partitions={len(filtered_partitions)} | "
                f"tracking_rows={tracking_row_count} | "
                f"size={tracking_size_bytes/(1024**2):.1f}MB | "
                f"serde={serde_properties} | filter_active={partition_filter_active}"
            )

            metadata.append({
                "source_database":            src_db,
                "source_table":               tbl,
                "dest_database":              dest_db,
                "dest_bucket":                dest_bucket,
                "source_location":            loc or "",
                "s3_location":                s3_location,
                "file_format":                file_format,
                "schema":                     schema,
                "partitions":                 partitions_for_output,
                "partition_columns":          partition_columns,
                "partition_count":            len(all_partitions),
                "row_count":                  tracking_row_count,
                "is_partitioned":             is_partitioned,
                "unregistered_partitions":    unregistered_partitions,
                "table_type":                 table_type,
                "source_total_size_bytes":    source_total_size,
                "source_file_count":          source_file_count,
                "serde_properties":           serde_properties,
                "partition_filter":           partition_filter or None,
                "filtered_partitions":        filtered_partitions,
                "partition_filter_active":    partition_filter_active,
                "filtered_row_count":         tracking_row_count,
                "filtered_source_size_bytes": tracking_size_bytes,
                "filtered_file_count":        tracking_file_count,
                "full_table_row_count":       full_row_count,
                "full_table_partition_count": full_partition_count,
            })

        except Exception as e:
            logger.error(f"[Discovery] FAILED: {src_db}.{tbl} — {str(e)[:500]}")
            metadata.append({
                "source_database":            src_db,
                "source_table":               tbl,
                "dest_database":              dest_db,
                "dest_bucket":                dest_bucket,
                "source_location":            "",
                "s3_location":                f"{dest_bucket}/{dest_db}/{tbl}",
                "file_format":                "PARQUET",
                "schema":                     [],
                "partitions":                 [],
                "partition_columns":          "",
                "partition_count":            0,
                "row_count":                  0,
                "is_partitioned":             False,
                "unregistered_partitions":    False,
                "table_type":                 "UNKNOWN",
                "source_total_size_bytes":    0,
                "source_file_count":          0,
                "serde_properties":           {},
                "partition_filter":           partition_filter or None,
                "filtered_partitions":        [],
                "partition_filter_active":    False,
                "filtered_row_count":         0,
                "filtered_source_size_bytes": 0,
                "filtered_file_count":        0,
                "full_table_row_count":       0,
                "full_table_partition_count": 0,
                "error":                      str(e)[:2000],
            })

    failed = [t for t in metadata if 'error' in t]
    if failed:
        names = ', '.join(t['source_table'] for t in failed[:3])
        raise Exception(f"Discovery failed for {len(failed)}/{len(metadata)} table(s) in {src_db}: {names}.")

    return {
        'run_id':          run_id,
        'source_database': src_db,
        'dest_database':   dest_db,
        'dest_bucket':     dest_bucket,
        'tables':          metadata,
    }


@task.pyspark(conn_id='spark_default')
def record_discovered_tables(discovery: dict, spark) -> dict:
    """Record discovered tables — v4: persists partition_filter tracking columns."""
    if not isinstance(discovery, dict) or 'tables' not in discovery:
        logger.warning(f"[record_discovered_tables] Skipping invalid input: {type(discovery)}")
        return {}

    config      = get_config()
    tracking_db = config['tracking_database']
    run_id      = discovery['run_id']
    discovery_duration = discovery.get('_task_duration', 0.0)

    for t in discovery['tables']:
        parts = t.get('partitions', [])
        if isinstance(parts, str):
            parts = [p for p in parts.split(',') if p]

        schema_json = json.dumps(t.get('schema', [])).replace("'", "''")
        parts_json  = json.dumps(parts).replace("'", "''")
        table_type  = t.get('table_type', 'UNKNOWN')

        partition_filter_active  = t.get('partition_filter_active', False)
        tracking_row_count       = t.get('filtered_row_count', 0) if partition_filter_active else t.get('row_count', 0)
        tracking_partition_count = len(t.get('filtered_partitions', [])) if partition_filter_active else t.get('partition_count', 0)
        tracking_size_bytes      = t.get('filtered_source_size_bytes', 0) if partition_filter_active else t.get('source_total_size_bytes', 0)
        tracking_file_count      = t.get('filtered_file_count', 0) if partition_filter_active else t.get('source_file_count', 0)

        full_table_row_count       = t.get('full_table_row_count', t.get('row_count', 0))
        full_table_partition_count = t.get('full_table_partition_count', t.get('partition_count', 0))
        partition_filter_val       = (t.get('partition_filter') or '').replace("'", "''")
        filtered_partition_count   = len(t.get('filtered_partitions', [])) if partition_filter_active else None
        fpc_sql = str(filtered_partition_count) if filtered_partition_count is not None else 'NULL'

        existing = spark.sql(f"""
            SELECT COUNT(*) as cnt FROM {tracking_db}.migration_table_status
            WHERE run_id='{run_id}'
              AND source_database='{t['source_database']}'
              AND source_table='{t['source_table']}'
              AND dest_database='{t['dest_database']}'
        """).collect()[0]['cnt']

        if existing > 0:
            execute_with_iceberg_retry(spark, f"""
                UPDATE {tracking_db}.migration_table_status
                SET discovery_status           = 'COMPLETED',
                    discovery_completed_at     = current_timestamp(),
                    discovery_duration_seconds = {discovery_duration},
                    source_location            = '{t['source_location']}',
                    file_format                = '{t['file_format']}',
                    table_type                 = '{table_type}',
                    source_row_count           = {tracking_row_count},
                    source_total_size_bytes    = {tracking_size_bytes},
                    source_file_count          = {tracking_file_count},
                    source_partition_count     = {tracking_partition_count},
                    unregistered_partitions    = {str(t.get('unregistered_partitions', False)).lower()},
                    partition_filter           = '{partition_filter_val}',
                    filtered_partition_count   = {fpc_sql},
                    full_table_row_count       = {full_table_row_count},
                    full_table_partition_count = {full_table_partition_count},
                    updated_at                 = current_timestamp()
                WHERE run_id='{run_id}'
                  AND source_database='{t['source_database']}'
                  AND dest_database='{t['dest_database']}'
                  AND source_table='{t['source_table']}'
            """, task_label=f"record_discovered:{t['source_table']}")
        else:
            execute_with_iceberg_retry(spark, f"""
                INSERT INTO {tracking_db}.migration_table_status (
                    run_id, source_database, source_table, dest_database, dest_bucket,
                    dest_location, source_location, file_format,
                    partition_count, is_partitioned, schema_json, partitions_json,
                    partition_columns, table_type, source_row_count,
                    source_total_size_bytes, source_file_count,
                    source_partition_count, unregistered_partitions,
                    s3_total_size_bytes_before, s3_file_count_before,
                    s3_total_size_bytes_after, s3_file_count_after,
                    s3_bytes_transferred, s3_files_transferred,
                    file_size_match, file_count_match,
                    partition_filter, filtered_partition_count,
                    full_table_row_count, full_table_partition_count,
                    discovery_status, discovery_completed_at, discovery_duration_seconds,
                    distcp_status, distcp_started_at, distcp_completed_at, distcp_duration_seconds,
                    distcp_is_incremental, distcp_bytes_copied, distcp_files_copied,
                    table_create_status, table_create_completed_at, table_create_duration_seconds,
                    table_already_existed,
                    validation_status, validation_completed_at, validation_duration_seconds,
                    dest_hive_row_count, dest_partition_count,
                    row_count_match, partition_count_match, schema_match,
                    schema_differences, overall_status, error_message, updated_at
                ) VALUES (
                    '{run_id}', '{t['source_database']}', '{t['source_table']}',
                    '{t['dest_database']}', '{t['dest_bucket']}', '{t['s3_location']}',
                    '{t['source_location']}', '{t['file_format']}',
                    {t.get('partition_count', 0)}, {str(t.get('is_partitioned', False)).lower()},
                    '{schema_json}', '{parts_json}', '{t.get('partition_columns', '')}',
                    '{table_type}', {tracking_row_count},
                    {tracking_size_bytes}, {tracking_file_count},
                    {tracking_partition_count}, {str(t.get('unregistered_partitions', False)).lower()},
                    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                    '{partition_filter_val}', {fpc_sql},
                    {full_table_row_count}, {full_table_partition_count},
                    'COMPLETED', current_timestamp(), {discovery_duration},
                    NULL, NULL, NULL, NULL, NULL, NULL, NULL,
                    NULL, NULL, NULL, NULL,
                    NULL, NULL, NULL, NULL, NULL,
                    NULL, NULL, NULL, NULL, NULL, NULL, current_timestamp()
                )
            """, task_label=f"record_discovered:{t['source_table']}")

    return discovery


@task.pyspark(conn_id='spark_default')
@track_duration
def run_s3_copy(discovery: dict, spark, **context) -> dict:
    """
    Copy data from source S3 to destination S3 using Hadoop FileUtil.

    v4 partition_filter support:
      - When partition_filter_active AND filtered_partitions is non-empty:
          copies only filtered partition sub-directories one by one
      - When partition_filter_active AND filtered_partitions is empty:
          SKIPS the table (mirrors production distcp path-list-mode skip)
      - Otherwise: copies the full table directory
    """
    from datetime import datetime as _dt
    from py4j.java_gateway import java_import

    if not isinstance(discovery, dict) or 'tables' not in discovery:
        logger.warning(f"[run_s3_copy] Skipping invalid input: {type(discovery)}")
        return {}

    run_id = discovery['run_id']
    tables = discovery['tables']
    results = []

    for t in tables:
        if t.get('error'):
            results.append({
                'source_database': t['source_database'], 'source_table': t['source_table'], 'dest_database': t['dest_database'],
                'status': 'SKIPPED', 'distcp_started_at': _dt.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                'distcp_completed_at': _dt.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                'distcp_duration_secs': 0.0, 'is_incremental': False,
                'bytes_copied': 0, 'files_copied': 0,
                's3_total_size_bytes_before': 0, 's3_file_count_before': 0,
                's3_total_size_bytes_after': 0, 's3_file_count_after': 0,
                's3_bytes_transferred': 0, 's3_files_transferred': 0, 'error': None,
            })
            continue

        partition_filter_active = t.get('partition_filter_active', False)
        filtered_partitions     = t.get('filtered_partitions', [])

        # Skip tables where filter matched 0 partitions
        if partition_filter_active and len(filtered_partitions) == 0:
            logger.warning(
                f"[S3Copy] SKIPPED {t['source_database']}.{t['source_table']} — "
                f"partition_filter '{t.get('partition_filter')}' matched 0 partitions."
            )
            results.append({
                'source_database': t['source_database'], 'source_table': t['source_table'],
                'dest_database': t['dest_database'],
                'status': 'SKIPPED', 'distcp_started_at': _dt.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                'distcp_completed_at': _dt.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                'distcp_duration_secs': 0.0, 'is_incremental': False,
                'bytes_copied': 0, 'files_copied': 0,
                's3_total_size_bytes_before': 0, 's3_file_count_before': 0,
                's3_total_size_bytes_after': 0, 's3_file_count_after': 0,
                's3_bytes_transferred': 0, 's3_files_transferred': 0,
                'error': f"partition_filter matched 0 partitions: {t.get('partition_filter')}",
            })
            continue

        src_db     = t['source_database']
        tbl        = t['source_table']
        source_loc = t['source_location']
        s3_loc     = t['s3_location']

        distcp_started_at = _dt.utcnow().strftime('%Y-%m-%d %H:%M:%S')
        logger.info(f"[S3Copy] {src_db}.{tbl} | src={source_loc} | dest={s3_loc} | filter_active={partition_filter_active}")

        try:
            java_import(spark._jvm, "org.apache.hadoop.fs.*")
            hconf   = spark._jsc.hadoopConfiguration()
            fs      = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark._jvm.java.net.URI(source_loc), hconf
            )
            dst_fs  = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                spark._jvm.java.net.URI(s3_loc), hconf
            )
            dst_path = spark._jvm.org.apache.hadoop.fs.Path(s3_loc)
            is_incr  = dst_fs.exists(dst_path)

            s3_size_before = 0; s3_files_before = 0
            if is_incr:
                try:
                    cs = dst_fs.getContentSummary(dst_path)
                    s3_size_before  = int(cs.getLength())
                    s3_files_before = int(cs.getFileCount())
                except Exception: pass

            if partition_filter_active and filtered_partitions:
                # Copy only the filtered partitions individually
                logger.info(
                    f"[S3Copy] Partition-filtered copy: {len(filtered_partitions)} "
                    f"partition(s) for {src_db}.{tbl}"
                )
                for part_str in filtered_partitions:
                    src_part = spark._jvm.org.apache.hadoop.fs.Path(f"{source_loc}/{part_str}")
                    dst_part = spark._jvm.org.apache.hadoop.fs.Path(f"{s3_loc}/{part_str}")
                    success  = spark._jvm.org.apache.hadoop.fs.FileUtil.copy(
                        fs, src_part, dst_fs, dst_part, False, True, hconf
                    )
                    if not success:
                        raise Exception(f"FileUtil.copy returned false for partition {part_str}")
            else:
                # Full table copy
                src_path = spark._jvm.org.apache.hadoop.fs.Path(source_loc)

                # Ensure destination directory exists
                if not dst_fs.exists(dst_path):
                    dst_fs.mkdirs(dst_path)
                
                # List source contents and copy each item individually
                file_statuses = fs.listStatus(src_path)
                if file_statuses is None or len(file_statuses) == 0:
                    raise Exception(f"Source path is empty or does not exist: {source_loc}")
                
                for file_status in file_statuses:
                    item_src  = file_status.getPath()
                    item_name = item_src.getName()
                    item_dst  = spark._jvm.org.apache.hadoop.fs.Path(s3_loc + "/" + item_name)
                    success   = spark._jvm.org.apache.hadoop.fs.FileUtil.copy(
                        fs, item_src, dst_fs, item_dst, False, True, hconf
                    )
                    if not success:
                        raise Exception(f"FileUtil.copy returned false for item: {item_name}")

            s3_size_after = 0; s3_files_after = 0
            try:
                cs = dst_fs.getContentSummary(dst_path)
                s3_size_after  = int(cs.getLength())
                s3_files_after = int(cs.getFileCount())
            except Exception: pass

            _end_dt = _dt.utcnow()
            distcp_duration_secs = (_end_dt - _dt.strptime(distcp_started_at, '%Y-%m-%d %H:%M:%S')).total_seconds()

            logger.info(
                f"[S3Copy] COMPLETED: {src_db}.{tbl} | incremental={is_incr} | "
                f"s3_size_after={s3_size_after/(1024**2):.1f}MB"
            )
            results.append({
                'source_database':            src_db,
                'source_table':               tbl,
                'dest_database':   t['dest_database'], 
                'status':                     'COMPLETED',
                'distcp_started_at':          distcp_started_at,
                'distcp_completed_at':        _end_dt.strftime('%Y-%m-%d %H:%M:%S'),
                'distcp_duration_secs':       distcp_duration_secs,
                'is_incremental':             is_incr,
                'bytes_copied':               s3_size_after,
                'files_copied':               s3_files_after,
                's3_total_size_bytes_before': s3_size_before,
                's3_file_count_before':       s3_files_before,
                's3_total_size_bytes_after':  s3_size_after,
                's3_file_count_after':        s3_files_after,
                's3_bytes_transferred':       s3_size_after - s3_size_before,
                's3_files_transferred':       s3_files_after - s3_files_before,
                'partition_filter_active':    partition_filter_active,
                'partitions_requested':       len(filtered_partitions) if partition_filter_active else None,
                'error':                      None,
            })

        except Exception as e:
            _fail_dt = _dt.utcnow()
            results.append({
                'source_database':            src_db,
                'source_table':               tbl,
                'dest_database': t['dest_database'],
                'status':                     'FAILED',
                'distcp_started_at':          distcp_started_at,
                'distcp_completed_at':        _fail_dt.strftime('%Y-%m-%d %H:%M:%S'),
                'distcp_duration_secs': (_fail_dt - _dt.strptime(distcp_started_at, '%Y-%m-%d %H:%M:%S')).total_seconds(),
                'is_incremental': False,
                'bytes_copied': 0, 'files_copied': 0,
                's3_total_size_bytes_before': 0, 's3_file_count_before': 0,
                's3_total_size_bytes_after': 0, 's3_file_count_after': 0,
                's3_bytes_transferred': 0, 's3_files_transferred': 0,
                'error': str(e)[:2000],
            })
            logger.error(f"[S3Copy] ERROR {src_db}.{tbl}: {str(e)[:500]}")

    failed_tables = [r for r in results if r['status'] == 'FAILED']
    has_failures  = len(failed_tables) > 0
    result_dict   = {
        **discovery,
        'distcp_results':   results,
        '_has_failures':    has_failures,
        '_failure_summary': (
            f"S3 copy failed for {len(failed_tables)}/{len(results)} table(s)"
            if has_failures else None
        ),
    }
    context['ti'].xcom_push(key='return_value', value=result_dict)
    if has_failures:
        raise Exception(f"S3 copy failed — {result_dict['_failure_summary']}.")
    return result_dict


@task.pyspark(conn_id='spark_default')
def update_distcp_status(distcp_result: dict, spark) -> dict:
    """Update Iceberg tracking with S3 copy results."""
    if not isinstance(distcp_result, dict) or 'run_id' not in distcp_result:
        logger.warning(f"[update_distcp_status] Skipping invalid input: {type(distcp_result)}")
        return {}

    config      = get_config()
    tracking_db = config['tracking_database']
    run_id      = distcp_result['run_id']
    src_db      = distcp_result['source_database']

    for r in distcp_result.get('distcp_results', []):
        if r.get('status') == 'SKIPPED':
            continue
        overall      = 'COPIED' if r['status'] == 'COMPLETED' else 'FAILED'
        error_msg    = (r.get('error', '') or '').replace("'", "''")
        started_at   = r.get('distcp_started_at', '')
        completed_at = r.get('distcp_completed_at', '')

        execute_with_iceberg_retry(spark, f"""
            UPDATE {tracking_db}.migration_table_status
            SET distcp_status             = '{r['status']}',
                distcp_started_at         = CAST('{started_at}' AS TIMESTAMP),
                distcp_completed_at       = CAST('{completed_at}' AS TIMESTAMP),
                distcp_duration_seconds   = {r.get('distcp_duration_secs', 0.0)},
                distcp_is_incremental     = {str(r['is_incremental']).lower()},
                distcp_bytes_copied       = {r.get('bytes_copied', 0)},
                distcp_files_copied       = {r.get('files_copied', 0)},
                s3_total_size_bytes_before = {r.get('s3_total_size_bytes_before', 0)},
                s3_file_count_before      = {r.get('s3_file_count_before', 0)},
                s3_total_size_bytes_after = {r.get('s3_total_size_bytes_after', 0)},
                s3_file_count_after       = {r.get('s3_file_count_after', 0)},
                s3_bytes_transferred      = {r.get('s3_bytes_transferred', 0)},
                s3_files_transferred      = {r.get('s3_files_transferred', 0)},
                file_count_match = (source_file_count = {r.get('s3_files_transferred', 0)}),
                file_size_match  = (ABS(source_total_size_bytes - {r.get('s3_bytes_transferred', 0)}) / GREATEST(source_total_size_bytes, 1) < 0.01),
                overall_status            = '{overall}',
                error_message             = CASE WHEN '{r['status']}' = 'FAILED' THEN '{error_msg}' WHEN '{r['status']}' = 'COMPLETED' THEN NULL ELSE error_message END,
                updated_at                = current_timestamp()
            WHERE run_id='{run_id}'
              AND source_database='{r['source_database']}'
              AND source_table='{r['source_table']}'
              AND dest_database='{r['dest_database']}' 
        """, task_label=f"update_distcp:{r['source_table']}")

    for r in distcp_result.get('distcp_results', []):
        if r.get('status') == 'FAILED' and r.get('error'):
            err = str(r['error'])[:2000].replace("'", "''")
            execute_with_iceberg_retry(spark, f"""
                UPDATE {tracking_db}.migration_table_status
                SET distcp_status='FAILED', overall_status='FAILED', error_message='{err}', updated_at=current_timestamp()
                WHERE run_id='{run_id}' AND source_database='{r['source_database']}'
                  AND source_table='{r['source_table']}' AND dest_database='{r.get("dest_database", distcp_result["dest_database"])}' AND distcp_status IS NULL
            """, task_label=f"update_distcp:patch:{r['source_table']}")

    _distcp_processed_dbs = set(
        r.get('dest_database', distcp_result['dest_database'])
        for r in distcp_result.get('distcp_results', [])
    )
    _distcp_processed_tables = set(
        r['source_table']
        for r in distcp_result.get('distcp_results', [])
    )
    _distcp_not_in = ', '.join(f"'{t}'" for t in _distcp_processed_tables) if _distcp_processed_tables else "'__no_tables__'"
    for _pddb in _distcp_processed_dbs:
        execute_with_iceberg_retry(spark, f"""
            UPDATE {tracking_db}.migration_table_status
            SET distcp_status='FAILED', overall_status='FAILED',
                error_message=COALESCE(error_message,'S3 copy task did not process this table'),
                updated_at=current_timestamp()
            WHERE run_id='{run_id}' AND source_database='{src_db}'
              AND dest_database='{_pddb}'
              AND source_table NOT IN ({_distcp_not_in})
              AND distcp_status IS NULL AND discovery_status='COMPLETED'
        """, task_label=f"update_distcp:catchall:{_pddb}")

    return distcp_result


@task.pyspark(conn_id='spark_default')
@track_duration
def create_hive_tables(distcp_result: dict, spark, **context) -> dict:
    """Create external Hive tables via Spark. Handles incremental (repairs partitions)."""

    if not isinstance(distcp_result, dict) or 'tables' not in distcp_result:
        logger.warning(f"[create_hive_tables] Skipping invalid input: {type(distcp_result)}")
        return {}

    tables = distcp_result['tables']
    results = []
    
    created_dbs = set()
    
    for t in tables:
        tbl = t['source_table']
        dest_db = t.get('dest_database') or distcp_result['dest_database']
        
        if dest_db not in created_dbs:
            spark.sql(f"CREATE DATABASE IF NOT EXISTS {dest_db}")
            created_dbs.add(dest_db)
    
        distcp_entry = next(
            (r for r in distcp_result.get('distcp_results', [])
             if r['source_table'] == tbl and r.get('dest_database') == dest_db),
            None
        )
        
        if distcp_entry is None:
            distcp_entry = next(
                (r for r in distcp_result.get('distcp_results', [])
                 if r['source_table'] == tbl),
                None
            )
        
        distcp_status = distcp_entry['status'] if distcp_entry else 'UNKNOWN'

        if distcp_status in ('FAILED', 'SKIPPED', 'UNKNOWN'):
            results.append({
                'source_table': t['source_table'],
                'dest_database': dest_db,
                'status': 'SKIPPED',
                'error': f'DistCp status was {distcp_status}',
                'existed': False
            })
            continue

        tbl = t['source_table']
        s3_loc = t['s3_location']
        fmt = t.get('file_format', 'PARQUET')
        schema_list = t.get('schema', [])
        part_cols_str = t.get('partition_columns', '')
        is_part = t.get('is_partitioned', False)
        serde_props = t.get('serde_properties', {})
        full_name = f"{dest_db}.{tbl}"

        logger.info(f"[HiveTable] Processing {full_name} | format={fmt} | partitioned={is_part}")

        try:
            exists = False
            try:
                spark.sql(f"DESCRIBE {full_name}")
                exists = True
            except Exception:
                pass

            if exists:
                if is_part:
                    if t.get('partition_filter_active') and t.get('filtered_partitions'):
                        for part_str in t['filtered_partitions']:
                            part_kv = {}
                            for segment in part_str.split('/'):
                                if '=' in segment:
                                    k, _, v = segment.partition('=')
                                    part_kv[k.strip()] = v.strip()
                            partition_spec = ", ".join(f"{k}='{v}'" for k, v in part_kv.items())
                            part_location = f"{s3_loc}/{part_str}"
                            try:
                                spark.sql(
                                    f"ALTER TABLE {full_name} ADD IF NOT EXISTS "
                                    f"PARTITION ({partition_spec}) LOCATION '{part_location}'"
                                )
                            except Exception as add_e:
                                logger.warning(
                                    f"[HiveTable] Could not add partition {partition_spec} "
                                    f"to {full_name}: {str(add_e)[:200]}"
                                )
                    else:
                        spark.sql(f"MSCK REPAIR TABLE {full_name}")
                spark.sql(f"REFRESH TABLE {full_name}")
                logger.info(f"[HiveTable] REPAIRED (already existed): {full_name}")
                results.append({
                    'source_table': tbl,
                    'dest_database': dest_db, 
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

                row_format_clause = ""
                tbl_properties_clause = ""
                if fmt == "TEXTFILE" and serde_props:
                    field_delim = serde_props.get("field.delim") or serde_props.get("field delimiter", "")
                    escape_delim = serde_props.get("escape.delim", "")
                    null_format = serde_props.get("null.format", "")

                    if field_delim:
                        row_format_clause = f"ROW FORMAT DELIMITED FIELDS TERMINATED BY '{field_delim}'"
                        if escape_delim:
                            row_format_clause += f" ESCAPED BY '{escape_delim}'"
                    if null_format:
                        tbl_properties_clause = f"TBLPROPERTIES ('serialization.null.format'='{null_format}')"

                ddl = f"""
                    CREATE EXTERNAL TABLE IF NOT EXISTS {full_name} ({col_def})
                    {part_clause}
                    {row_format_clause}
                    STORED AS {fmt}
                    LOCATION '{s3_loc}'
                    {tbl_properties_clause}
                """
                spark.sql(ddl)

                if is_part:
                    if t.get('partition_filter_active') and t.get('filtered_partitions'):
                        for part_str in t['filtered_partitions']:
                            part_kv = {}
                            for segment in part_str.split('/'):
                                if '=' in segment:
                                    k, _, v = segment.partition('=')
                                    part_kv[k.strip()] = v.strip()
                            partition_spec = ", ".join(f"{k}='{v}'" for k, v in part_kv.items())
                            part_location = f"{s3_loc}/{part_str}"
                            try:
                                spark.sql(
                                    f"ALTER TABLE {full_name} ADD IF NOT EXISTS "
                                    f"PARTITION ({partition_spec}) LOCATION '{part_location}'"
                                )
                            except Exception as add_e:
                                logger.warning(
                                    f"[HiveTable] Could not add partition {partition_spec} "
                                    f"to {full_name}: {str(add_e)[:200]}"
                                )
                    else:
                        spark.sql(f"MSCK REPAIR TABLE {full_name}")
                spark.sql(f"REFRESH TABLE {full_name}")

                logger.info(f"[HiveTable] CREATED: {full_name} | location={s3_loc}")
                results.append({
                    'source_table': tbl,
                    'dest_database': dest_db, 
                    'status': 'COMPLETED',
                    'action': 'created',
                    'existed': False,
                    'error': None
                })
        except Exception as e:
            error_msg = f"Table creation failed for {dest_db}.{tbl}: {str(e)[:2000]}"
            results.append({
                'source_table': tbl,
                'dest_database': dest_db, 
                'status': 'FAILED',
                'action': 'error',
                'existed': False,
                'error': str(e)
            })
            logger.error(f"ERROR: {error_msg}")

    failed_tables = [r for r in results if r['status'] == 'FAILED']
    has_failures = len(failed_tables) > 0

    result_dict = {
        **distcp_result,
        'table_results': results,
        '_has_failures': has_failures,
        '_failure_summary': (
            f"Table creation failed for {len(failed_tables)}/{len(results)} table(s): "
            if has_failures else None
        )
    }

    context['ti'].xcom_push(key='return_value', value=result_dict)

    if has_failures:
        raise Exception(f"Hive table creation failed — {result_dict['_failure_summary']}. Per-table errors in tracking.")

    return result_dict


@task.pyspark(conn_id='spark_default')
def update_table_create_status(table_result: dict, spark) -> dict:
    """Update Iceberg tracking with table creation results."""

    if not isinstance(table_result, dict) or 'run_id' not in table_result:
        logger.warning(f"[update_table_create_status] Skipping invalid input: {type(table_result)}")
        return {}

    config = get_config()
    tracking_db = config['tracking_database']
    run_id = table_result['run_id']
    src_db = table_result['source_database']
    table_duration = table_result.get('_task_duration', 0.0)

    for r in table_result.get('table_results', []):
        per_table_dest_db = r.get('dest_database', table_result['dest_database'])  # ← per-row, inside loop
        overall = 'TABLE_CREATED' if r['status'] == 'COMPLETED' else ('FAILED' if r['status'] == 'FAILED' else 'SKIPPED')
        error_msg = (r.get('error', '') or '').replace("'", "''")[:2000]

        execute_with_iceberg_retry(spark, f"""
            UPDATE {tracking_db}.migration_table_status
            SET table_create_status = '{r['status']}',
                table_create_completed_at = current_timestamp(),
                table_create_duration_seconds = {table_duration},
                table_already_existed = {str(r.get('existed', False)).lower()},
                overall_status = CASE WHEN overall_status != 'FAILED' THEN '{overall}' ELSE overall_status END,
                error_message = CASE WHEN '{r['status']}' = 'FAILED' THEN '{error_msg}' ELSE error_message END,
                updated_at = current_timestamp()
            WHERE run_id = '{run_id}'
              AND source_database = '{src_db}'
              AND dest_database = '{per_table_dest_db}'
              AND source_table = '{r['source_table']}'
        """,
        task_label=f"update_table_create_status:{r['source_table']}")

    for r in table_result.get('table_results', []):
        if r.get('status') == 'FAILED' and r.get('error'):
            per_table_dest_db = r.get('dest_database', table_result['dest_database'])  # ← per-row, inside loop
            per_table_error = str(r['error'])[:2000].replace("'", "''")
            execute_with_iceberg_retry(spark, f"""
                UPDATE {tracking_db}.migration_table_status
                SET table_create_status = 'FAILED',
                    overall_status = 'FAILED',
                    error_message = '{per_table_error}',
                    updated_at = current_timestamp()
                WHERE run_id = '{run_id}'
                  AND source_database = '{src_db}'
                  AND dest_database = '{per_table_dest_db}'
                  AND source_table = '{r['source_table']}'
                  AND table_create_status IS NULL
            """,
            task_label=f"update_table_create_status:failure_patch:{r['source_table']}")

    _tbl_processed_dbs = set(
        r.get('dest_database', table_result['dest_database'])
        for r in table_result.get('table_results', [])
    )
    _tbl_processed_tables = set(
        r['source_table']
        for r in table_result.get('table_results', [])
    )
    _tbl_not_in = ', '.join(f"'{t}'" for t in _tbl_processed_tables) if _tbl_processed_tables else "'__no_tables__'"
    for _pddb in _tbl_processed_dbs:
        execute_with_iceberg_retry(spark, f"""
            UPDATE {tracking_db}.migration_table_status
            SET table_create_status = 'FAILED',
                overall_status = 'FAILED',
                error_message = COALESCE(error_message, 'Table creation task did not process this table'),
                updated_at = current_timestamp()
            WHERE run_id = '{run_id}'
              AND source_database = '{src_db}'
              AND dest_database = '{_pddb}'
              AND source_table NOT IN ({_tbl_not_in})
              AND table_create_status IS NULL
              AND discovery_status = 'COMPLETED'
        """,
        task_label=f"update_table_create_status:catchall:{_pddb}")

    return table_result


@task.pyspark(conn_id='spark_default')
@track_duration
def validate_destination_tables(source_validation: dict, spark, **context) -> dict:
    """Validate destination Hive tables: row counts, partition counts, schema comparison."""

    if not isinstance(source_validation, dict) or 'tables' not in source_validation:
        logger.warning(f"[validate_destination_tables] Skipping invalid input: {type(source_validation)}")
        return {}

    config = get_config()
    tracking_db = config['tracking_database']

    run_id = source_validation['run_id']
    src_db = source_validation['source_database']
    dest_db = source_validation['dest_database']
    tables = source_validation['tables']

    validation_results = []

    for t in tables:
        tbl = t['source_table']
        per_table_dest_db = t.get('dest_database', dest_db)
        dest_tbl = f"{per_table_dest_db}.{tbl}"

        upstream = spark.sql(f"""
            SELECT distcp_status, table_create_status, overall_status, error_message
            FROM {tracking_db}.migration_table_status
            WHERE run_id = '{run_id}'
              AND source_database = '{src_db}'
              AND dest_database = '{per_table_dest_db}' 
              AND source_table = '{tbl}'
        """).collect()

        if upstream:
            row = upstream[0]
            if row['distcp_status'] == 'FAILED' or row['table_create_status'] in ('FAILED', 'SKIPPED') or row['overall_status'] == 'FAILED':
                validation_results.append({
                    'source_table': tbl,
                    'status': 'SKIPPED',
                    'error': f"Skipped validation — upstream failure: {row['error_message']}"
                })
                continue

        logger.info(f"[Validation] Starting validation for {per_table_dest_db}.{tbl}")

        try:
            source_metrics = spark.sql(f"""
                SELECT source_row_count, source_partition_count, partition_filter
                FROM {tracking_db}.migration_table_status
                WHERE run_id = '{run_id}'
                  AND source_database = '{src_db}'
                  AND dest_database = '{per_table_dest_db}'  
                  AND source_table = '{tbl}'
            """).collect()

            if not source_metrics:
                validation_results.append({
                    'source_table': tbl,
                    'status': 'SKIPPED',
                    'error': 'Source metrics not found in tracking table'
                })
                continue

            source_row_count = source_metrics[0]['source_row_count'] or 0
            source_partition_count = source_metrics[0]['source_partition_count'] or t.get('partition_count', 0)

            partition_filter = source_metrics[0]['partition_filter'] if source_metrics else None
            if partition_filter:
                logger.info(
                    f"[Validation] {per_table_dest_db}.{tbl} — partition filter active: '{partition_filter}'. "
                    f"Comparing against filtered baseline: {source_row_count} rows, "
                    f"{source_partition_count} partitions."
                )

            # Get destination row count
            dest_row_count = spark.sql(f"SELECT COUNT(*) as c FROM {dest_tbl}").collect()[0]['c']

            # Get destination partition count
            dest_partition_count = 0
            try:
                dest_partitions_df = spark.sql(f"SHOW PARTITIONS {dest_tbl}")
                dest_partition_count = dest_partitions_df.count()
            except Exception:
                pass

            logger.info(f"[Validation] {per_table_dest_db}.{tbl} | source_rows={source_row_count} | dest_rows={dest_row_count} | source_parts={source_partition_count} | dest_parts={dest_partition_count}")

            # Schema comparison
            src_schema = t.get('schema', [])
            dest_schema_df = spark.sql(f"DESCRIBE {dest_tbl}")
            dest_schema = [
                {'name': row.col_name, 'type': row.data_type}
                for row in dest_schema_df.collect()
                if row.col_name and not row.col_name.startswith('#')
            ]

            # Compare schemas
            schema_match = True
            schema_diffs = []

            src_cols = {c['name']: c['type'] for c in src_schema}
            dest_cols = {c['name']: c['type'] for c in dest_schema}

            for col_name, col_type in src_cols.items():
                if col_name not in dest_cols:
                    schema_match = False
                    schema_diffs.append(f"Missing column: {col_name}")
                elif dest_cols[col_name] != col_type:
                    schema_match = False
                    schema_diffs.append(f"Type mismatch for {col_name}: {col_type} vs {dest_cols[col_name]}")

            for col_name in dest_cols:
                if col_name not in src_cols:
                    schema_match = False
                    schema_diffs.append(f"Extra column in dest: {col_name}")

            # Validations
            row_count_match = (source_row_count == dest_row_count)
            partition_count_match = (source_partition_count == dest_partition_count)

            match_summary = f"rows={'✓' if row_count_match else '✗'} partitions={'✓' if partition_count_match else '✗'} schema={'✓' if schema_match else '✗'}"
            logger.info(f"[Validation] DONE: {per_table_dest_db}.{tbl} | {match_summary}")
            if schema_diffs:
                logger.warning(f"[Validation] Schema diffs for {per_table_dest_db}.{tbl}: {'; '.join(schema_diffs[:5])}")

            mismatch_parts = []
            if not row_count_match:
                mismatch_parts.append(
                    f"Row count mismatch: source={source_row_count}, dest={dest_row_count}"
                )
            if not partition_count_match:
                mismatch_parts.append(
                    f"Partition count mismatch: source={source_partition_count}, dest={dest_partition_count}"
                )
            if not schema_match and schema_diffs:
                mismatch_parts.append(f"Schema differences: {'; '.join(schema_diffs[:3])}")

            mismatch_error = '; '.join(mismatch_parts) if mismatch_parts else None

            validation_results.append({
                'source_table': tbl,
                'status': 'COMPLETED',
                'source_row_count': source_row_count,
                'dest_hive_row_count': dest_row_count,
                'source_partition_count': source_partition_count,
                'dest_partition_count': dest_partition_count,
                'row_count_match': row_count_match,
                'partition_count_match': partition_count_match,
                'schema_match': schema_match,
                'schema_differences': '; '.join(schema_diffs) if schema_diffs else '',
                'error': mismatch_error
            })

        except Exception as e:
            error_msg = f"Validation failed for {dest_db}.{tbl}: {str(e)[:2000]}"
            validation_results.append({
                'source_table': tbl,
                'status': 'FAILED',
                'error': str(e)[:2000]
            })
            logger.error(f"ERROR: {error_msg}")

    failed_validations = [v for v in validation_results if v['status'] == 'FAILED']
    warned_count_checks = [
        v for v in validation_results
        if v.get('status') == 'COMPLETED' and (
            not v.get('row_count_match', True) or
            not v.get('partition_count_match', True)
        )
    ]

    if warned_count_checks:
        for v in warned_count_checks:
            warn_parts = []
            if not v.get('row_count_match', True):
                warn_parts.append(f"row count mismatch (source={v.get('source_row_count')}, dest={v.get('dest_hive_row_count')})")
            if not v.get('partition_count_match', True):
                warn_parts.append(f"partition count mismatch (source={v.get('source_partition_count')}, dest={v.get('dest_partition_count')}) — Stale partitions on source, Run MSCK")
            logger.warning(f"[Validation] WARNING for {v['source_table']}: {'; '.join(warn_parts)}")

    has_failures = len(failed_validations) > 0

    result_dict = {
        **source_validation,
        'validation_results': validation_results,
        '_has_failures': has_failures,
        '_failure_summary': (
            f"Validation failed for {len(failed_validations)}/{len(validation_results)} table(s)"
            if has_failures else None
        )
    }

    context['ti'].xcom_push(key='return_value', value=result_dict)

    if has_failures:
        raise Exception(f"Destination validation failed — {result_dict['_failure_summary']}. Per-table errors in tracking.")

    return result_dict


@task.pyspark(conn_id='spark_default')
def update_validation_status(validation_result: dict, spark) -> dict:
    """Update Iceberg tracking with validation results."""

    if not isinstance(validation_result, dict) or 'run_id' not in validation_result:
        logger.warning(f"[update_validation_status] Skipping invalid input: {type(validation_result)}")
        return {}

    config = get_config()
    tracking_db = config['tracking_database']

    run_id = validation_result['run_id']
    dest_db = validation_result['dest_database']
    src_db = validation_result.get('source_database', '')

    validation_duration = validation_result.get('_task_duration', 0.0)

    for v in validation_result.get('validation_results', []):
        if v['status'] != 'COMPLETED':
            continue

        error_msg = (v.get('error', '') or '').replace("'", "''")[:2000]
        schema_diffs = (v.get('schema_differences', '') or '').replace("'", "''")[:2000]

        is_validated = (
            v.get('row_count_match', False) and
            v.get('partition_count_match', False) and
            v.get('schema_match', False)
        )
        has_mismatch_only = (
            not is_validated and
            v['status'] == 'COMPLETED' and
            (not v.get('row_count_match', True) or not v.get('partition_count_match', True))
        )
        final_overall_status = 'VALIDATED' if is_validated else ('VALIDATED_WITH_WARNINGS' if has_mismatch_only else 'VALIDATION_FAILED')

        if v['status'] == 'FAILED':
            error_message_sql = f"'{error_msg}'"
        elif not is_validated and v.get('error'):
            mismatch_msg = str(v['error']).replace("'", "''")[:2000]
            error_message_sql = f"'{mismatch_msg}'"
        elif is_validated:
            error_message_sql = "NULL"
        else:
            error_message_sql = "error_message"

        execute_with_iceberg_retry(spark, f"""
            UPDATE {tracking_db}.migration_table_status
            SET validation_status = '{v['status']}',
                validation_completed_at = current_timestamp(),
                validation_duration_seconds = {validation_duration},
                source_row_count = {v.get('source_row_count', 0)},
                dest_hive_row_count = {v.get('dest_hive_row_count', 0)},
                source_partition_count = {v.get('source_partition_count', 0)},
                dest_partition_count = {v.get('dest_partition_count', 0)},
                row_count_match = {str(v.get('row_count_match', False)).lower()},
                partition_count_match = {str(v.get('partition_count_match', False)).lower()},
                schema_match = {str(v.get('schema_match', False)).lower()},
                schema_differences = '{schema_diffs}',
                overall_status = CASE
                    WHEN overall_status = 'FAILED' THEN overall_status
                    ELSE '{final_overall_status}'
                END,
                error_message = CASE
                    WHEN overall_status = 'FAILED' THEN error_message
                    ELSE {error_message_sql}
                END,
                updated_at = current_timestamp()
            WHERE run_id = '{run_id}'
              AND source_database = '{src_db}'
              AND dest_database = '{dest_db}'
              AND source_table = '{v['source_table']}'
        """,
        task_label=f"update_validation_status:{v['source_table']}")

    for v in validation_result.get('validation_results', []):
        if v.get('status') == 'FAILED' and v.get('error'):
            per_table_error = str(v['error'])[:2000].replace("'", "''")
            execute_with_iceberg_retry(spark, f"""
                UPDATE {tracking_db}.migration_table_status
                SET validation_status = 'FAILED',
                    overall_status = 'VALIDATION_FAILED',
                    error_message = '{per_table_error}',
                    updated_at = current_timestamp()
                WHERE run_id = '{run_id}'
                  AND source_database = '{src_db}'
                  AND dest_database = '{dest_db}'
                  AND source_table = '{v['source_table']}'
                  AND validation_status IS NULL
            """,
            task_label=f"update_validation_status:failure_patch:{v['source_table']}")

    execute_with_iceberg_retry(spark, f"""
        UPDATE {tracking_db}.migration_table_status
        SET validation_status = 'SKIPPED',
            overall_status = CASE WHEN overall_status = 'FAILED' THEN 'FAILED' ELSE 'VALIDATION_FAILED' END,
            error_message = COALESCE(error_message, 'Validation task did not process this table'),
            updated_at = current_timestamp()
        WHERE run_id = '{run_id}'
          AND source_database = '{src_db}'
          AND dest_database = '{dest_db}'
          AND table_create_status = 'COMPLETED'
          AND validation_status IS NULL
    """,
    task_label="update_validation_status:catchall")

    return validation_result


@task.pyspark(conn_id='spark_default')
def generate_html_report(run_id: str, spark) -> str:
    """Generate comprehensive HTML migration report."""
    from datetime import datetime

    config = get_config()
    tracking_db = config['tracking_database']
    report_location = config['report_output_location']

    # Get migration run info
    run_info = spark.sql(f"""
        SELECT * FROM {tracking_db}.migration_runs
        WHERE run_id = '{run_id}'
    """).collect()[0]

    # Get table status
    table_status = spark.sql(f"""
        SELECT * FROM {tracking_db}.migration_table_status
        WHERE run_id = '{run_id}'
        ORDER BY source_database, source_table
    """).collect()

    # Calculate summary stats
    total_tables = len(table_status)
    successful_tables = sum(1 for t in table_status if t.overall_status in ['VALIDATED', 'VALIDATED_WITH_WARNINGS', 'TABLE_CREATED'])
    failed_tables = sum(1 for t in table_status if 'FAILED' in (t.overall_status or ''))
    total_data_gb = sum(t.s3_total_size_bytes_after or 0 for t in table_status) / (1024**3)
    total_files = sum(t.s3_file_count_after or 0 for t in table_status)
    total_rows = sum(t.source_row_count or 0 for t in table_status)
    incremental_runs = sum(1 for t in table_status if t.distcp_is_incremental)

    # Generate HTML
    html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>MapR to S3 Migration Report - {run_id}</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background-color: white;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #2c3e50;
            border-bottom: 3px solid #3498db;
            padding-bottom: 10px;
        }}
        h2 {{
            color: #34495e;
            margin-top: 30px;
            border-bottom: 2px solid #ecf0f1;
            padding-bottom: 8px;
        }}
        .summary-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }}
        .summary-card {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }}
        .summary-card.success {{
            background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
        }}
        .summary-card.warning {{
            background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
        }}
        .summary-card.info {{
            background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
        }}
        .summary-card h3 {{
            margin: 0 0 10px 0;
            font-size: 14px;
            opacity: 0.9;
        }}
        .summary-card .value {{
            font-size: 32px;
            font-weight: bold;
            margin: 0;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
            font-size: 14px;
        }}
        th {{
            background-color: #34495e;
            color: white;
            padding: 12px;
            text-align: left;
            position: sticky;
            top: 0;
        }}
        td {{
            padding: 10px 12px;
            border-bottom: 1px solid #ecf0f1;
        }}
        tr:hover {{
            background-color: #f8f9fa;
        }}
        .status-badge {{
            padding: 4px 12px;
            border-radius: 12px;
            font-size: 12px;
            font-weight: bold;
            display: inline-block;
        }}
        .status-completed {{
            background-color: #d4edda;
            color: #155724;
        }}
        .status-failed {{
            background-color: #f8d7da;
            color: #721c24;
        }}
        .status-skipped {{
            background-color: #fff3cd;
            color: #856404;
        }}
        .status-warning {{
            background-color: #fff3cd;
            color: #856404;
        }}
        .metric {{
            font-weight: bold;
            color: #2980b9;
        }}
        .duration {{
            color: #7f8c8d;
            font-size: 12px;
        }}
        .validation-pass {{
            color: #27ae60;
            font-weight: bold;
        }}
        .validation-fail {{
            color: #e74c3c;
            font-weight: bold;
        }}
        .validation-warn {{
            color: #856404;
            background-color: #fff3cd;
            font-weight: bold;
            padding: 2px 6px;
            border-radius: 4px;
        }}
        .timestamp {{
            color: #95a5a6;
            font-size: 12px;
        }}
        .section-divider {{
            margin: 40px 0;
            border-top: 2px dashed #ecf0f1;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>MapR to S3 Migration Report</h1>

        <div class="timestamp">
            Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC<br>
            Run ID: <strong>{run_id}</strong><br>
            DAG Run: <strong>{run_info.dag_run_id}</strong>
"""

    html += f"""
        </div>

        <h2>Migration Summary</h2>
        <div class="summary-grid">
            <div class="summary-card">
                <h3>TOTAL TABLES</h3>
                <p class="value">{total_tables}</p>
            </div>
            <div class="summary-card success">
                <h3>SUCCESSFUL</h3>
                <p class="value">{successful_tables}</p>
            </div>
            <div class="summary-card warning">
                <h3>FAILED</h3>
                <p class="value">{failed_tables}</p>
            </div>
            <div class="summary-card info">
                <h3>TOTAL DATA</h3>
                <p class="value">{total_data_gb:.5f} GB</p>
            </div>
            <div class="summary-card info">
                <h3>TOTAL FILES</h3>
                <p class="value">{total_files:,}</p>
            </div>
            <div class="summary-card info">
                <h3>TOTAL ROWS</h3>
                <p class="value">{total_rows:,}</p>
            </div>
            <div class="summary-card">
                <h3>INCREMENTAL RUNS</h3>
                <p class="value">{incremental_runs}</p>
            </div>
        </div>

        <div class="section-divider"></div>

        <h2>Validation Summary</h2>
"""
    validation_summary_data = spark.sql(f"""
        SELECT
            COUNT(*) as total_tables_validated,
            SUM(CASE WHEN row_count_match = true AND partition_count_match = true AND schema_match = true THEN 1 ELSE 0 END) as tables_passed_validation,
            SUM(CASE WHEN row_count_match = false OR partition_count_match = false OR schema_match = false THEN 1 ELSE 0 END) as tables_failed_validation,
            SUM(CASE WHEN row_count_match = false THEN 1 ELSE 0 END) as total_row_count_mismatches,
            SUM(CASE WHEN partition_count_match = false THEN 1 ELSE 0 END) as total_partition_count_mismatches,
            SUM(CASE WHEN schema_match = false THEN 1 ELSE 0 END) as total_schema_mismatches
        FROM {tracking_db}.migration_table_status
        WHERE run_id = '{run_id}'
          AND validation_status = 'COMPLETED'
    """).collect()

    file_metrics_data = spark.sql(f"""
        SELECT
            SUM(CASE WHEN file_size_match = true THEN 1 ELSE 0 END)  as tables_size_match,
            SUM(CASE WHEN file_size_match = false THEN 1 ELSE 0 END) as tables_size_mismatch,
            SUM(CASE WHEN file_count_match = true THEN 1 ELSE 0 END)  as tables_file_count_match,
            SUM(CASE WHEN file_count_match = false THEN 1 ELSE 0 END) as tables_file_count_mismatch,
            SUM(source_total_size_bytes) as total_source_bytes,
            SUM(s3_total_size_bytes_after) as total_dest_bytes
        FROM {tracking_db}.migration_table_status
        WHERE run_id = '{run_id}'
          AND distcp_status = 'COMPLETED'
    """).collect()

    fm = file_metrics_data[0] if file_metrics_data else None

    if validation_summary_data and validation_summary_data[0]['total_tables_validated']:
        vs = validation_summary_data[0]

        # file metric derived values
        size_match_count      = (fm.tables_size_match      or 0) if fm else 0
        size_mismatch_count   = (fm.tables_size_mismatch   or 0) if fm else 0
        fcount_match_count    = (fm.tables_file_count_match   or 0) if fm else 0
        fcount_mismatch_count = (fm.tables_file_count_mismatch or 0) if fm else 0
        total_src_gb  = (fm.total_source_bytes or 0) / (1024**3) if fm else 0.0
        total_dest_gb = (fm.total_dest_bytes   or 0) / (1024**3) if fm else 0.0
        size_diff_pct = (
            abs(total_src_gb - total_dest_gb) / total_src_gb * 100
            if total_src_gb > 0 else 0.0
        )

        html += f"""
        <div class="summary-grid">
            <div class="summary-card info">
                <h3>TABLES VALIDATED</h3>
                <p class="value">{vs.total_tables_validated}</p>
            </div>
            <div class="summary-card success">
                <h3>PASSED VALIDATION</h3>
                <p class="value">{vs.tables_passed_validation}</p>
            </div>
            <div class="summary-card warning">
                <h3>FAILED VALIDATION</h3>
                <p class="value">{vs.tables_failed_validation}</p>
            </div>
            <div class="summary-card warning">
                <h3>ROW COUNT MISMATCHES</h3>
                <p class="value">{vs.total_row_count_mismatches}</p>
            </div>
            <div class="summary-card warning">
                <h3>PARTITION MISMATCHES</h3>
                <p class="value">{vs.total_partition_count_mismatches}</p>
            </div>
            <div class="summary-card warning">
                <h3>SCHEMA MISMATCHES</h3>
                <p class="value">{vs.total_schema_mismatches}</p>
            </div>
            <div class="summary-card {'success' if size_mismatch_count == 0 else 'warning'}">
                <h3>SIZE MATCH</h3>
                <p class="value">{size_match_count} / {size_match_count + size_mismatch_count}</p>
            </div>
            <div class="summary-card {'success' if fcount_mismatch_count == 0 else 'warning'}">
                <h3>FILE COUNT MATCH</h3>
                <p class="value">{fcount_match_count} / {fcount_match_count + fcount_mismatch_count}</p>
            </div>
            <div class="summary-card info">
                <h3>SOURCE SIZE</h3>
                <p class="value">{total_src_gb:.3f} GB</p>
            </div>
            <div class="summary-card info">
                <h3>DEST SIZE</h3>
                <p class="value">{total_dest_gb:.3f} GB</p>
            </div>
            <div class="summary-card {'success' if size_diff_pct < 1.0 else 'warning'}">
                <h3>SIZE DELTA</h3>
                <p class="value">{size_diff_pct:.2f}%</p>
            </div>
        </div>
"""
    else:
        html += """
        <p style="color: #95a5a6; font-style: italic;">No validation summary available for this run.</p>
"""

    html += """
        <div class="section-divider"></div>

        <h2>Table Migration Details</h2>
        <table>
            <thead>
                <tr>
                    <th>Database</th>
                    <th>Table</th>
                    <th>Status</th>
                    <th>Discovery</th>
                    <th>DistCp</th>
                    <th>Table Create</th>
                    <th>Validation</th>
                    <th>Format</th>
                    <th>Total Duration</th>
                </tr>
            </thead>
            <tbody>
"""

    for t in table_status:
        status = t.overall_status or ''
        if 'VALIDATED_WITH_WARNINGS' in status:
            status_class = 'status-warning'
        elif 'VALIDATED' in status or 'TABLE_CREATED' in status:
            status_class = 'status-completed'
        else:
            status_class = 'status-failed'

        discovery_dur = f"{t.discovery_duration_seconds:.1f}s" if t.discovery_duration_seconds else "N/A"
        distcp_dur = f"{t.distcp_duration_seconds:.1f}s" if t.distcp_duration_seconds else "N/A"
        distcp_detail = f"<br><small>{t.distcp_bytes_copied/(1024**2):.1f} MB, {t.distcp_files_copied:,} files</small>" if t.distcp_bytes_copied else ""
        if t.distcp_is_incremental:
            distcp_dur += " <span style='background-color: #fff3cd; padding: 2px 6px; border-radius: 4px; font-size: 10px;'>INCREMENTAL</span>"
        table_dur = f"{t.table_create_duration_seconds:.1f}s" if t.table_create_duration_seconds else "N/A"
        val_dur = f"{t.validation_duration_seconds:.1f}s" if t.validation_duration_seconds else "N/A"

        total_dur = (t.discovery_duration_seconds or 0) + (t.distcp_duration_seconds or 0) + \
                    (t.table_create_duration_seconds or 0) + (t.validation_duration_seconds or 0)

        html += f"""
                <tr>
                    <td>{t.source_database}</td>
                    <td><strong>{t.source_table}</strong></td>
                    <td><span class="status-badge {status_class}">{t.overall_status}</span></td>
                    <td class="duration">{discovery_dur}</td>
                    <td class="duration">{distcp_dur}{distcp_detail}</td>
                    <td class="duration">{table_dur}</td>
                    <td class="duration">{val_dur}</td>
                    <td>{t.file_format or 'N/A'}</td>
                    <td class="metric">{total_dur:.1f}s</td>
                </tr>
"""

    html += """
            </tbody>
        </table>

        <div class="section-divider"></div>

        <h2>Metadata Validation Results</h2>
        <table>
            <thead>
                <tr>
                    <th>Database</th>
                    <th>Table</th>
                    <th>Source Rows</th>
                    <th>Dest Hive Rows</th>
                    <th>Row Count Match</th>
                    <th>Source Partitions</th>
                    <th>Dest Partitions</th>
                    <th>Partition Match</th>
                    <th>Schema Match</th>
                </tr>
            </thead>
            <tbody>
"""

    for t in table_status:
        if not t.validation_status:
            continue

        row_match_class = 'validation-pass' if t.row_count_match else 'validation-fail'
        row_match_icon = '✓ PASS' if t.row_count_match else '✗ FAIL'

        part_match_class = 'validation-pass' if t.partition_count_match else 'validation-warn'
        part_match_icon = '✓ PASS' if t.partition_count_match else '⚠ WARN: Stale partitions on source, Run MSCK'

        schema_match_class = 'validation-pass' if t.schema_match else 'validation-fail'
        schema_match_icon = '✓ PASS' if t.schema_match else '✗ FAIL'

        html += f"""
                <tr>
                    <td>{t.source_database}</td>
                    <td><strong>{t.source_table}</strong></td>
                    <td class="metric">{(t.source_row_count or 0):,}</td>
                    <td class="metric">{(t.dest_hive_row_count or 0):,}</td>
                    <td class="{row_match_class}">{row_match_icon}</td>
                    <td class="metric">{t.source_partition_count or 0}</td>
                    <td class="metric">{t.dest_partition_count or 0}</td>
                    <td class="{part_match_class}">{part_match_icon}</td>
                    <td class="{schema_match_class}">{schema_match_icon}</td>
                </tr>
"""
    html += """
            </tbody>
        </table>

        <div class="section-divider"></div>

        <h2>Data Validation Results</h2>
        <table>
            <thead>
                <tr>
                    <th>Database</th>
                    <th>Table</th>
                    <th>MapR Size (GB)</th>
                    <th>S3 Size Before (GB)</th>
                    <th>S3 Size After (GB)</th>
                    <th>S3 Size - Transferred (GB)</th>
                    <th>Size Match</th>
                    <th>MapR Files</th>
                    <th>S3 Files Before</th>
                    <th>S3 Files After</th>
                    <th>S3 Files - Transferred</th>
                    <th>File Count Match</th>
                </tr>
            </thead>
            <tbody>
"""

    for t in table_status:
        if not t.distcp_status:
            continue

        source_total_size_gb = (t.source_total_size_bytes or 0) / (1024**3)
        s3_size_before_gb = (t.s3_total_size_bytes_before or 0) / (1024**3)
        s3_size_after_gb = (t.s3_total_size_bytes_after or 0) / (1024**3)
        s3_transferred_gb = (t.s3_bytes_transferred or 0) / (1024**3)

        size_match_class = 'validation-pass' if t.file_size_match else 'validation-fail'
        size_match_icon = '✓ PASS' if t.file_size_match else '✗ FAIL'

        count_match_class = 'validation-pass' if t.file_count_match else 'validation-fail'
        count_match_icon = '✓ PASS' if t.file_count_match else '✗ FAIL'

        html += f"""
                <tr>
                    <td>{t.source_database}</td>
                    <td><strong>{t.source_table}</strong></td>
                    <td class="metric">{source_total_size_gb:.5f}</td>
                    <td class="metric">{s3_size_before_gb:.5f}</td>
                    <td class="metric">{s3_size_after_gb:.5f}</td>
                    <td class="metric">{s3_transferred_gb:.5f}</td>
                    <td class="{size_match_class}">{size_match_icon}</td>
                    <td class="metric">{(t.source_file_count or 0):,}</td>
                    <td class="metric">{(t.s3_file_count_before or 0):,}</td>
                    <td class="metric">{(t.s3_file_count_after or 0):,}</td>
                    <td class="metric">{(t.s3_files_transferred or 0):,}</td>
                    <td class="{count_match_class}">{count_match_icon}</td>
                </tr>
"""

    html += """
            </tbody>
        </table>

        <div class="section-divider"></div>

        <h2>Performance Metrics</h2>
        <table>
            <thead>
                <tr>
                    <th>Database</th>
                    <th>Table</th>
                    <th>Data Volume</th>
                    <th>DistCp Speed</th>
                    <th>Rows/Second</th>
                    <th>End-to-End Duration</th>
                </tr>
            </thead>
            <tbody>
"""

    for t in table_status:
        data_gb = (t.s3_total_size_bytes_after or 0) / (1024**3)
        distcp_speed = (t.s3_total_size_bytes_after or 0) / (1024**2) / (t.distcp_duration_seconds or 1)

        total_dur = (t.discovery_duration_seconds or 0) + (t.distcp_duration_seconds or 0) + \
                    (t.table_create_duration_seconds or 0) + (t.validation_duration_seconds or 0)

        rows_per_sec = (t.source_row_count or 0) / (total_dur or 1)

        html += f"""
                <tr>
                    <td>{t.source_database}</td>
                    <td><strong>{t.source_table}</strong></td>
                    <td class="metric">{data_gb:.5f} GB</td>
                    <td class="metric">{distcp_speed:.5f} MB/s</td>
                    <td class="metric">{rows_per_sec:,.0f}</td>
                    <td class="metric">{total_dur:.1f}s ({total_dur/60:.1f}m)</td>
                </tr>
"""

    html += """
            </tbody>
        </table>

        <div style="margin-top: 50px; padding-top: 20px; border-top: 1px solid #ecf0f1; color: #95a5a6; font-size: 12px;">
            <p>This report was automatically generated by the MapR to S3 Migration DAG.</p>
        </div>
    </div>
</body>
</html>
"""

    # Write HTML to S3
    report_filename = f"{run_id}_report.html"
    report_path = f"{report_location}/{report_filename}"

    # Use Spark to write HTML
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jvm.java.net.URI(report_path),
        hadoop_conf
    )

    output_path = spark._jvm.org.apache.hadoop.fs.Path(report_path)
    output_stream = fs.create(output_path, True)
    output_stream.write(html.encode('utf-8'))
    output_stream.close()

    return {'report_path': report_path}


@task.pyspark(conn_id='spark_default')
def finalize_run(run_id: str, spark) -> dict:
    """Finalize migration run - update stats in Iceberg tracking."""
    config = get_config()
    tracking_db = config['tracking_database']

    stats = {'total': 0, 'successful': 0, 'failed': 0}
    final_status = 'FAILED'

    try:
        stats_result = spark.sql(f"""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN overall_status IN ('VALIDATED', 'VALIDATED_WITH_WARNINGS', 'TABLE_CREATED') THEN 1 ELSE 0 END) as successful,
                SUM(CASE WHEN overall_status IN ('FAILED', 'VALIDATION_FAILED') THEN 1 ELSE 0 END) as failed
            FROM {tracking_db}.migration_table_status
            WHERE run_id = '{run_id}'
        """).collect()

        if not stats_result or stats_result[0]['total'] == 0:
            logger.warning(f"[finalize_run] No table records found for run_id '{run_id}'. "
                           f"Upstream tasks (discover/distcp) likely failed before writing any records.")
            final_status = 'FAILED'
        else:
            stats = {
                'total': stats_result[0]['total'] or 0,
                'successful': stats_result[0]['successful'] or 0,
                'failed': stats_result[0]['failed'] or 0,
            }
            final_status = 'COMPLETED' if stats['failed'] == 0 else 'COMPLETED_WITH_FAILURES'

    except Exception as e:
        logger.error(f"[finalize_run] Failed to query migration_table_status: {str(e)}")
        final_status = 'FAILED'

    try:
        execute_with_iceberg_retry(spark, f"""
            UPDATE {tracking_db}.migration_runs
            SET status = '{final_status}',
                completed_at = current_timestamp(),
                total_tables = {stats['total']},
                successful_tables = {stats['successful']},
                failed_tables = {stats['failed']}
            WHERE run_id = '{run_id}'
        """, task_label="finalize_run:update_migration_runs")

        logger.info(f"[finalize_run] Run '{run_id}' finalized with status '{final_status}'. "
                    f"total={stats['total']}, successful={stats['successful']}, failed={stats['failed']}")

    except Exception as e:
        logger.error(f"[finalize_run] Failed to update migration_runs for run_id '{run_id}': {str(e)}")
        raise

    return {
        'run_id': run_id,
        'status': final_status,
        'total': stats['total'],
        'successful': stats['successful'],
        'failed': stats['failed']
    }


# Disabled for now as logs must remain in temp_dir for validation checks.
# Can be revisited as a future enhancement.
'''
@task
def cleanup_edge(cluster_setup: dict, run_id: str) -> dict:
    """Clean up temp files on edge node."""
    config = get_config()
    ssh = SSHHook(ssh_conn_id=config['ssh_conn_id'])
    temp_dir = cluster_setup.get('temp_dir', '')

    if temp_dir:
        try:
            with ssh.get_conn() as client:
                _, stdout, _ = client.exec_command(f"rm -rf {temp_dir}", timeout=60)
                stdout.channel.recv_exit_status()
        except:
            pass

    return {'cleaned': temp_dir}
'''


@task.pyspark(conn_id='spark_default')
def send_migration_report_email(report_result: dict, run_id: str, spark) -> dict:
    """Send HTML migration report via email using SMTP."""

    config = get_config()
    smtp_conn_id = config.get('smtp_conn_id', 'smtp_default')
    recipients_str = config.get('email_recipients', '')

    if not recipients_str:
        logger.warning("[Email] No recipients configured in 'migration_email_recipients' variable. Skipping email.")
        return {'sent': False, 'reason': 'no_recipients'}

    recipients = [r.strip() for r in recipients_str.split(',') if r.strip()]
    report_path = report_result.get('report_path', '')

    try:
        import os
        import tempfile

        from airflow.utils.email import send_email

        logger.info(f"[Email] Reading HTML report from S3: {report_path}")
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(report_path),
            hadoop_conf
        )
        s3_path_obj = spark._jvm.org.apache.hadoop.fs.Path(report_path)
        reader = spark._jvm.java.io.BufferedReader(
            spark._jvm.java.io.InputStreamReader(fs.open(s3_path_obj), "UTF-8")
        )
        lines = []
        line = reader.readLine()
        while line is not None:
            lines.append(line)
            line = reader.readLine()
        reader.close()
        html_content = "\n".join(lines)

        tmp = tempfile.NamedTemporaryFile(
            mode='w', suffix='.html',
            prefix=f'{run_id}_report_',
            delete=False
        )
        tmp.write(html_content)
        tmp.close()

        send_email(
            to=recipients,
            subject=f"Migration Report - {run_id}",
            html_content=f"<p>Please find the migration report for run <strong>{run_id}</strong> attached.</p>",
            files=[tmp.name],
            conn_id=smtp_conn_id,
        )
        os.unlink(tmp.name)
        logger.info(f"[Email] Report sent successfully to: {recipients}")
        return {'sent': True, 'recipients': recipients, 'report_path': report_path}
    except Exception as e:
        logger.error(f"[Email] Failed to send report: {str(e)}")
        raise Exception(f"Failed to send migration report email: {str(e)}") from e

# =============================================================================
# DAG 2: ICEBERG MIGRATION TASKS
# =============================================================================
@task.pyspark(conn_id='spark_default')
def init_iceberg_tracking_tables(spark) -> dict:
    """Create Iceberg tracking tables for Iceberg migration if they don't exist."""
    config = get_config()
    tracking_db = config['tracking_database']
    tracking_loc = config['tracking_location']
    spark.sql(f"""
        CREATE DATABASE IF NOT EXISTS {tracking_db} LOCATION '{tracking_loc}'
    """)
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {tracking_db}.iceberg_migration_runs (
            run_id STRING,
            dag_run_id STRING,
            excel_file_path STRING,
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
        CREATE TABLE IF NOT EXISTS {tracking_db}.iceberg_migration_table_status (
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
            migration_duration_seconds DOUBLE,
            status STRING,
            source_hive_row_count BIGINT,
            destination_iceberg_row_count BIGINT,
            row_count_match BOOLEAN,
            source_hive_partition_count INT,
            dest_iceberg_partition_count INT,
            partition_count_match BOOLEAN,
            schema_match BOOLEAN,
            schema_differences STRING,
            validation_status STRING,
            validation_completed_at TIMESTAMP,
            validation_duration_seconds DOUBLE,
            error_message STRING,
            updated_at TIMESTAMP
        )
        USING iceberg
        PARTITIONED BY (source_database)
        LOCATION '{tracking_loc}/iceberg_migration_table_status'
    """)
    return {'status': 'initialized', 'database': tracking_db}

@task.pyspark(conn_id='spark_default')
def create_iceberg_migration_run(excel_file_path: str, dag_run_id: str, spark) -> str:
    """Create migration run record."""
    import uuid
    from datetime import datetime

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
            current_timestamp(),
            NULL,
            'RUNNING',
            0, 0, 0,
            '{json.dumps(config).replace("'", "''")}'
        )
    """)

    return run_id


@task.pyspark(conn_id='spark_default')
def parse_iceberg_excel(excel_file_path: str, run_id: str, spark) -> list:
    """Read Excel config for Iceberg migration from S3, grouping rows by (database, inplace_migration, destination_iceberg_database)."""
    from io import BytesIO

    import pandas as ps

    binary_df = spark.read.format("binaryFile").load(excel_file_path)
    row = binary_df.select("content").first()
    excel_bytes = bytes(row.content)
    df = ps.read_excel(BytesIO(excel_bytes), engine='openpyxl')

    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

    grouped = {}
    for _, row in df.iterrows():
        src_db = str(row.get('database', '') or '').strip()
        if not src_db:
            continue

        inplace_val = row.get('inplace_migration', None)
        if inplace_val is None or (isinstance(inplace_val, float) and __import__('math').isnan(inplace_val)) or str(inplace_val).strip().lower() in ('', 'nan', 'f', 'false', 'no', '0'):
            inplace_migration = False
        else:
            inplace_migration = str(inplace_val).strip().upper() in ('T', 'TRUE', 'YES', '1')

        dest_ice_db_val = row.get('destination_iceberg_database', '')
        dest_ice_db = str(dest_ice_db_val).strip() if dest_ice_db_val is not None else ''
        if not dest_ice_db or dest_ice_db.lower() == 'nan':
            dest_ice_db = src_db if inplace_migration else f"{src_db}_iceberg"

        raw_cell_val = row.get('table', '')
        raw_cell = '*' if (raw_cell_val is None or (isinstance(raw_cell_val, float) and __import__('math').isnan(raw_cell_val)) or str(raw_cell_val).strip().lower() in ('', 'nan')) else str(raw_cell_val).strip() or '*'

        key = (src_db, inplace_migration, dest_ice_db)
        if key not in grouped:
            grouped[key] = {'tokens': []}

        for tok in raw_cell.split(','):
            tok = tok.strip()
            if tok:
                grouped[key]['tokens'].append(tok)

    configs = []
    for (src_db, inplace_migration, dest_ice_db), group in grouped.items():
        unique_tokens = list(dict.fromkeys(group['tokens']))
        if '*' in unique_tokens:
            unique_tokens = ['*']

        logger.info(
            f"[ParseIcebergExcel] {src_db} -> dest={dest_ice_db} | inplace={inplace_migration} | "
            f"tokens={unique_tokens[:10]}" + (" ..." if len(unique_tokens) > 10 else "")
        )

        configs.append({
            'source_database': src_db,
            'table_tokens': unique_tokens,
            'inplace_migration': inplace_migration,
            'destination_iceberg_database': dest_ice_db,
            'run_id': run_id,
        })

    logger.info(f"[ParseIcebergExcel] Total database configs emitted: {len(configs)}")
    return configs


@task.pyspark(conn_id='spark_default')
@track_duration
def discover_hive_tables(db_config: dict, spark) -> dict:
    """Discover Hive tables matching the pattern in the source database."""
    src_db = db_config['source_database']
    raw_tokens = db_config.get('table_tokens') or []
    if not raw_tokens:
        pattern_str = db_config.get('table_pattern', '*')
        raw_tokens = [t.strip() for t in pattern_str.split(',') if t.strip()] or ['*']

    def resolve_tokens(spark, db, tokens):
        resolved = []
        seen = set()
        for tok in tokens:
            if tok == '*':
                rows = spark.sql(f"SHOW TABLES IN {db}").collect()
                for r in rows:
                    t = r.tableName
                    if t not in seen:
                        seen.add(t)
                        resolved.append(t)
            elif '*' in tok:
                rows = spark.sql(f"SHOW TABLES IN {db} LIKE '{tok}'").collect()
                for r in rows:
                    t = r.tableName
                    if t not in seen:
                        seen.add(t)
                        resolved.append(t)
            else:
                if tok not in seen:
                    seen.add(tok)
                    resolved.append(tok)
        return resolved

    matched_tables = resolve_tokens(spark, src_db, raw_tokens)

    logger.info(f"[IcebergDiscover] Database '{src_db}': {len(matched_tables)} table(s) matched tokens={raw_tokens}")

    tables_metadata = []
    for tbl in matched_tables:
        logger.info(f"[IcebergDiscover] Getting location for {src_db}.{tbl}")
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
            logger.error(f"[IcebergDiscover] Failed to get location for {src_db}.{tbl}: {str(e)[:300]}")
            tables_metadata.append({
                'table': tbl,
                'location': None,
                'discovery_error': str(e)
            })

    logger.info(f"[IcebergDiscover] Completed discovery for '{src_db}': {len(tables_metadata)} table(s) ready for migration")

    return {
        **db_config,
        'discovered_tables': tables_metadata
    }


@task.pyspark(conn_id='spark_default')
@track_duration
def migrate_tables_to_iceberg(discovery: dict, dag_run_id: str, spark, **context) -> dict:
    """Migrate discovered Hive tables to Iceberg format."""
    config = get_config()
    tracking_db = config['tracking_database']

    src_db = discovery['source_database']
    dest_db = discovery['destination_iceberg_database']
    inplace = discovery['inplace_migration']
    run_id = discovery['run_id']

    if not inplace:
        spark.sql(f"CREATE DATABASE IF NOT EXISTS {dest_db}")

    results = []

    for tbl_meta in discovery.get('discovered_tables', []):
        tbl = tbl_meta['table']
        location = tbl_meta.get('location')

        logger.info(f"[IcebergMigrate] Starting migration for {src_db}.{tbl} | strategy={'INPLACE' if inplace else 'SNAPSHOT'} | dest={dest_db}.{tbl}")
        from datetime import datetime as _dt
        tbl_migrate_start = _dt.utcnow()

        try:
            hive_count = spark.sql(f"SELECT COUNT(*) as c FROM {src_db}.{tbl}").collect()[0]['c']
            src_hive_partition_count = 0
            try:
                src_partitions_df = spark.sql(f"SHOW PARTITIONS {src_db}.{tbl}")
                all_partitions = src_partitions_df.collect()

                if all_partitions:
                    non_empty_count = 0
                    table_location = tbl_meta.get('location') or ''
                    for part_row in all_partitions:
                        part_spec = part_row[0]
                        part_path = f"{table_location}/{part_spec.replace('=', '=').rstrip('/')}"
                        try:
                            from py4j.java_gateway import java_import
                            java_import(spark._jvm, "org.apache.hadoop.fs.*")
                            fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
                                spark._jvm.java.net.URI(part_path),
                                spark._jsc.hadoopConfiguration()
                            )
                            path_obj = spark._jvm.org.apache.hadoop.fs.Path(part_path)
                            if fs.exists(path_obj):
                                summary = fs.getContentSummary(path_obj)
                                if int(summary.getLength()) > 0:
                                    non_empty_count += 1
                        except Exception:
                            non_empty_count += 1
                    src_hive_partition_count = non_empty_count
                    logger.info(
                        f"[IcebergMigrate] {src_db}.{tbl} | "
                        f"strategy={'INPLACE' if inplace else 'SNAPSHOT'} | "
                        f"total_hive_partitions={len(all_partitions)} | "
                        f"non_empty_partitions={non_empty_count} "
                        f"(0-byte partitions excluded from comparison)"
                    )
            except Exception:
                pass

            if inplace:
                migration_type = "INPLACE"
                dest_table = f"{src_db}.{tbl}"
                spark.sql(f"CALL spark_catalog.system.migrate('{src_db}.{tbl}')")
            else:
                migration_type = "SNAPSHOT"
                dest_table = f"{dest_db}.{tbl}"
                try:
                    spark.sql(f"DESCRIBE {dest_table}")
                    logger.info(f"[IcebergMigrate] Destination {dest_table} already exists (prior attempt). Dropping before re-snapshot.")
                    spark.sql(f"DROP TABLE IF EXISTS {dest_table}")
                except Exception:
                    pass
                spark.sql(f"CALL spark_catalog.system.snapshot('{src_db}.{tbl}', '{dest_db}.{tbl}')")

            iceberg_count = spark.sql(f"SELECT COUNT(*) as c FROM {dest_table}").collect()[0]['c']
            dest_iceberg_partition_count = 0
            try:
                spark.catalog.refreshTable(dest_table)
                dest_iceberg_partition_count = spark.sql(f"""SELECT COUNT(*) as cnt FROM {dest_table}.partitions""").collect()[0]['cnt']
            except Exception:
                pass

            counts_match = (hive_count == iceberg_count)
            partition_match = (src_hive_partition_count == dest_iceberg_partition_count)

            logger.info(f"[IcebergMigrate] COMPLETED: {src_db}.{tbl} | hive_rows={hive_count} | iceberg_rows={iceberg_count} | rows_match={counts_match} | partitions_match={partition_match}")

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
                'hive_partition_count': src_hive_partition_count,
                'iceberg_partition_count': dest_iceberg_partition_count,
                'partition_match': partition_match,
                'error': None
            })

            spark.sql(f"""
                DELETE FROM {tracking_db}.iceberg_migration_table_status
                WHERE run_id = '{run_id}'
                  AND source_database = '{src_db}'
                  AND source_table = '{tbl}'
            """)

            tbl_migrate_duration = (_dt.utcnow() - tbl_migrate_start).total_seconds()
            spark.sql(f"""
                INSERT INTO {tracking_db}.iceberg_migration_table_status
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
                    {tbl_migrate_duration},
                    'COMPLETED',
                    {hive_count},
                    {iceberg_count},
                    {str(counts_match).lower()},
                    {src_hive_partition_count},
                    {dest_iceberg_partition_count},
                    {str(partition_match).lower()},
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    current_timestamp()
                )
            """)

        except Exception as e:
            error_msg = f"Migration to Iceberg failed for {dest_db}.{tbl}: {str(e)[:2000]}".replace("'", "''")
            tbl_fail_duration = (_dt.utcnow() - tbl_migrate_start).total_seconds()

            results.append({
                'source_table': f"{src_db}.{tbl}",
                'destination_table': f"{dest_db}.{tbl}" if not inplace else f"{src_db}.{tbl}",
                'migration_type': "INPLACE" if inplace else "SNAPSHOT",
                'status': 'FAILED',
                'error': str(e)
            })

            spark.sql(f"""
                DELETE FROM {tracking_db}.iceberg_migration_table_status
                WHERE run_id = '{run_id}'
                  AND source_database = '{src_db}'
                  AND source_table = '{tbl}'
            """)

            spark.sql(f"""
                INSERT INTO {tracking_db}.iceberg_migration_table_status
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
                    {tbl_fail_duration},
                    'FAILED',
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    NULL,
                    '{error_msg}',
                    current_timestamp()
                )
            """)
            logger.error(f"ERROR: {error_msg}")

    failed_migrations = [r for r in results if r['status'] == 'FAILED']
    has_failures = len(failed_migrations) > 0

    result_dict = {
        'run_id': run_id,
        'source_database': src_db,
        'destination_database': dest_db,
        'migration_type': 'INPLACE' if inplace else 'SNAPSHOT',
        'results': results,
        '_has_failures': has_failures,
        '_failure_summary': (
            f"Iceberg migration failed for {len(failed_migrations)}/{len(results)} table(s): "
            if has_failures else None
        )
    }

    context['ti'].xcom_push(key='return_value', value=result_dict)

    if has_failures:
        raise Exception(f"Iceberg migration failed — {result_dict['_failure_summary']}. Per-table errors in tracking.")

    return result_dict


@task.pyspark(conn_id='spark_default')
def update_migration_durations(migration_result: dict, spark) -> dict:
    """Update tracking table with migration durations from XCom."""

    if not isinstance(migration_result, dict) or 'run_id' not in migration_result:
        logger.warning(f"[update_migration_durations] Skipping invalid input: {type(migration_result)}")
        return {}

    config = get_config()
    tracking_db = config['tracking_database']

    run_id = migration_result['run_id']
    src_db = migration_result['source_database']

    # Extract duration from XCom result
    migration_duration = migration_result.get('_task_duration', 0.0)

    # Update all records for this run
    execute_with_iceberg_retry(spark, f"""
        UPDATE {tracking_db}.iceberg_migration_table_status
        SET migration_duration_seconds = {migration_duration},
            updated_at = current_timestamp()
        WHERE run_id = '{run_id}'
          AND migration_duration_seconds = 0.0
    """,
    task_label="update_migration_durations:duration_bulk")

    for r in migration_result.get('results', []):
        if r.get('status') == 'FAILED' and r.get('error'):
            per_table_error = str(r['error'])[:2000].replace("'", "''")
            tbl_name = r['source_table'].split('.')[-1]
            src_db_name = r['source_table'].split('.')[0]
            execute_with_iceberg_retry(spark, f"""
                UPDATE {tracking_db}.iceberg_migration_table_status
                SET status = 'FAILED',
                    error_message = '{per_table_error}',
                    updated_at = current_timestamp()
                WHERE run_id = '{run_id}'
                  AND source_database = '{src_db_name}'
                  AND source_table = '{tbl_name}'
                  AND status IS NULL
            """,
            task_label=f"update_migration_durations:failure_patch:{tbl_name}")

    execute_with_iceberg_retry(spark, f"""
        UPDATE {tracking_db}.iceberg_migration_table_status
        SET status = 'FAILED',
            error_message = COALESCE(error_message, 'Iceberg migration task did not process this table'),
            updated_at = current_timestamp()
        WHERE run_id = '{run_id}'
          AND source_database = '{src_db}'
          AND status IS NULL
    """,
    task_label="update_migration_durations:catchall")

    return migration_result


@task.pyspark(conn_id='spark_default')
@track_duration
def validate_iceberg_tables(migration_result: dict, spark, **context) -> dict:
    """Validate Iceberg tables: row counts, partition counts, schema comparison between source Hive and destination Iceberg."""

    if not isinstance(migration_result, dict) or 'run_id' not in migration_result:
        logger.warning(f"[validate_iceberg_tables] Skipping invalid input: {type(migration_result)}")
        return {}

    src_db = migration_result['source_database']
    dest_db = migration_result['destination_database']

    validation_results = []

    for r in migration_result.get('results', []):
        if r['status'] != 'COMPLETED':
            continue

        # Extract table name from fully qualified name
        src_tbl_full = r['source_table']
        tbl = src_tbl_full.split('.')[-1]
        dest_tbl = r['destination_table']

        logger.info(f"[IcebergValidation] Validating {src_db}.{tbl} vs {dest_tbl}")
        from datetime import datetime as _dt
        tbl_val_start = _dt.utcnow()

        try:
            # Schema comparison between source Hive and destination Iceberg
            src_hive_schema_df = spark.sql(f"DESCRIBE {src_db}.{tbl}")
            src_hive_schema = [
                {'name': row.col_name, 'type': row.data_type}
                for row in src_hive_schema_df.collect()
                if row.col_name and not row.col_name.startswith('#')
            ]

            dest_iceberg_schema_df = spark.sql(f"DESCRIBE {dest_tbl}")
            dest_iceberg_schema = [
                {'name': row.col_name, 'type': row.data_type}
                for row in dest_iceberg_schema_df.collect()
                if row.col_name and not row.col_name.startswith('#')
            ]

            # Compare schemas
            schema_match = True
            schema_diffs = []

            src_cols = {c['name']: c['type'] for c in src_hive_schema}
            dest_cols = {c['name']: c['type'] for c in dest_iceberg_schema}

            for col_name, col_type in src_cols.items():
                if col_name not in dest_cols:
                    schema_match = False
                    schema_diffs.append(f"Missing column in Iceberg: {col_name}")
                elif dest_cols[col_name] != col_type:
                    schema_match = False
                    schema_diffs.append(f"Type mismatch for {col_name}: Hive {col_type} vs Iceberg {dest_cols[col_name]}")

            for col_name in dest_cols:
                if col_name not in src_cols:
                    schema_match = False
                    schema_diffs.append(f"Extra column in Iceberg: {col_name}")

            row_ok = r.get('counts_match', False)
            part_ok = r.get('partition_match', False)
            logger.info(f"[IcebergValidation] DONE: {src_db}.{tbl} | rows={'✓' if row_ok else '✗'} partitions={'✓' if part_ok else '✗'} schema={'✓' if schema_match else '✗'}")
            if schema_diffs:
                logger.warning(f"[IcebergValidation] Schema diffs for {src_db}.{tbl}: {'; '.join(schema_diffs[:5])}")

            validation_results.append({
                'source_table': tbl,
                'destination_table': dest_tbl,
                'status': 'COMPLETED',
                'source_hive_row_count': r.get('hive_count', 0),
                'dest_iceberg_row_count': r.get('iceberg_count', 0),
                'row_count_match': r.get('counts_match', False),
                'source_hive_partition_count': r.get('hive_partition_count', 0),
                'dest_iceberg_partition_count': r.get('iceberg_partition_count', 0),
                'partition_count_match': r.get('partition_match', False),
                'schema_match': schema_match,
                'schema_differences': '; '.join(schema_diffs) if schema_diffs else '',
                'per_table_validation_duration': (_dt.utcnow() - tbl_val_start).total_seconds(),
                'error': None
            })

        except Exception as e:
            error_msg = f"Validation failed for {dest_db}.{tbl}: {str(e)[:2000]}"
            validation_results.append({
                'source_table': tbl,
                'destination_table': dest_tbl,
                'status': 'FAILED',
                'per_table_validation_duration': (_dt.utcnow() - tbl_val_start).total_seconds(),
                'error': str(e)[:2000]
            })
            logger.error(f"ERROR: {error_msg}")

    failed_validations = [v for v in validation_results if v['status'] == 'FAILED']
    mismatched = [
        v for v in validation_results
        if v.get('status') == 'COMPLETED' and (
            not v.get('row_count_match', True) or
            not v.get('partition_count_match', True)
        )
    ]
    total_failures = len(failed_validations) + len(mismatched)
    has_failures = total_failures > 0

    result_dict = {
        **migration_result,
        'validation_results': validation_results,
        '_has_failures': has_failures,
        '_failure_summary': (
            f"Iceberg validation failed for {total_failures}/{len(validation_results)} table(s)"
            if has_failures else None
        )
    }

    context['ti'].xcom_push(key='return_value', value=result_dict)

    if has_failures:
        raise Exception(f"Iceberg validation failed — {result_dict['_failure_summary']}. Per-table errors in tracking.")

    return result_dict


@task.pyspark(conn_id='spark_default')
def update_iceberg_validation_status(validation_result: dict, spark) -> dict:
    """Update Iceberg tracking with validation results."""

    if not isinstance(validation_result, dict) or 'run_id' not in validation_result:
        logger.warning(f"[update_iceberg_validation_status] Skipping invalid input: {type(validation_result)}")
        return {}

    config = get_config()
    tracking_db = config['tracking_database']

    run_id = validation_result['run_id']
    src_db = validation_result['source_database']

    # Extract duration from XCom result
    task_level_validation_duration = validation_result.get('_task_duration', 0.0)

    for v in validation_result.get('validation_results', []):
        if v['status'] != 'COMPLETED':
            continue

        schema_diffs = (v.get('schema_differences', '') or '').replace("'", "''")[:2000]

        overall_status = 'VALIDATED' if (
            v.get('row_count_match', False) and
            v.get('partition_count_match', True) and
            v.get('schema_match', False)
        ) else 'VALIDATION_FAILED'

        is_validated = (
            v.get('row_count_match', False) and
            v.get('partition_count_match', True) and
            v.get('schema_match', False)
        )

        if not is_validated and v.get('error'):
            mismatch_msg = str(v['error']).replace("'", "''")[:2000]
            error_message_sql = f"'{mismatch_msg}'"
        elif is_validated:
            error_message_sql = "NULL"
        else:
            error_message_sql = "error_message"

        execute_with_iceberg_retry(spark, f"""
            UPDATE {tracking_db}.iceberg_migration_table_status
            SET validation_status = '{v['status']}',
                validation_completed_at = current_timestamp(),
                validation_duration_seconds = {v.get('per_table_validation_duration', task_level_validation_duration)},
                source_hive_row_count = {v.get('source_hive_row_count', 0)},
                destination_iceberg_row_count = {v.get('dest_iceberg_row_count', 0)},
                row_count_match = {str(v.get('row_count_match', False)).lower()},
                source_hive_partition_count = {v.get('source_hive_partition_count', 0)},
                dest_iceberg_partition_count = {v.get('dest_iceberg_partition_count', 0)},
                partition_count_match = {str(v.get('partition_count_match', False)).lower()},
                schema_match = {str(v.get('schema_match', False)).lower()},
                schema_differences = '{schema_diffs}',
                status = CASE
                    WHEN status = 'FAILED' THEN status  -- preserve original migration failure
                    ELSE '{overall_status}'
                END,
                error_message = CASE
                    WHEN status = 'FAILED' THEN error_message  -- preserve original error
                    ELSE {error_message_sql}
                END,
                updated_at = current_timestamp()
            WHERE run_id = '{run_id}'
              AND source_database = '{src_db}'
              AND source_table = '{v['source_table']}'
        """,
        task_label=f"update_iceberg_validation_status:{v['source_table']}")

    for v in validation_result.get('validation_results', []):
        if v.get('status') == 'FAILED' and v.get('error'):
            per_table_error = str(v['error'])[:2000].replace("'", "''")
            execute_with_iceberg_retry(spark, f"""
                UPDATE {tracking_db}.iceberg_migration_table_status
                SET validation_status = 'FAILED',
                    status = 'VALIDATION_FAILED',
                    error_message = '{per_table_error}',
                    updated_at = current_timestamp()
                WHERE run_id = '{run_id}'
                  AND source_database = '{src_db}'
                  AND source_table = '{v['source_table']}'
                  AND validation_status IS NULL
            """,
            task_label=f"update_iceberg_validation_status:failure_patch:{v['source_table']}")

    execute_with_iceberg_retry(spark, f"""
        UPDATE {tracking_db}.iceberg_migration_table_status
        SET validation_status = 'SKIPPED',
            status = CASE WHEN status = 'FAILED' THEN 'FAILED' ELSE 'VALIDATION_FAILED' END,
            error_message = COALESCE(error_message, 'Iceberg validation task did not process this table'),
            updated_at = current_timestamp()
        WHERE run_id = '{run_id}'
          AND source_database = '{src_db}'
          AND status = 'COMPLETED'
          AND validation_status IS NULL
    """,
    task_label="update_iceberg_validation_status:catchall")

    return validation_result


@task.pyspark(conn_id='spark_default')
def generate_iceberg_html_report(run_id: str, spark) -> str:
    """Generate comprehensive HTML Iceberg migration report."""
    from datetime import datetime

    config = get_config()
    tracking_db = config['tracking_database']
    report_location = config['report_output_location']

    # Get migration status
    migration_status = spark.sql(f"""
        SELECT * FROM {tracking_db}.iceberg_migration_table_status
        WHERE run_id = '{run_id}'
        ORDER BY source_database, source_table
    """).collect()

    # Calculate summary stats
    total_tables = len(migration_status)
    successful_tables = sum(1 for t in migration_status if t.status in ['VALIDATED', 'COMPLETED'])
    failed_tables = sum(1 for t in migration_status if 'FAILED' in (t.status or ''))
    total_rows = sum(t.source_hive_row_count or 0 for t in migration_status)
    count_mismatches = sum(1 for t in migration_status if not t.row_count_match and t.row_count_match is not None)

    # Validation summary query
    iceberg_validation_summary = spark.sql(f"""
        SELECT
            COUNT(*) as total_tables_validated,
            SUM(CASE WHEN row_count_match = true AND partition_count_match = true AND schema_match = true THEN 1 ELSE 0 END) as tables_passed_validation,
            SUM(CASE WHEN row_count_match = false OR partition_count_match = false OR schema_match = false THEN 1 ELSE 0 END) as tables_failed_validation,
            SUM(CASE WHEN row_count_match = false THEN 1 ELSE 0 END) as total_row_count_mismatches,
            SUM(CASE WHEN partition_count_match = false THEN 1 ELSE 0 END) as total_partition_count_mismatches,
            SUM(CASE WHEN schema_match = false THEN 1 ELSE 0 END) as total_schema_mismatches
        FROM {tracking_db}.iceberg_migration_table_status
        WHERE run_id = '{run_id}'
          AND validation_status = 'COMPLETED'
    """).collect()

    # Generate HTML
    html = f"""
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Iceberg Migration Report - {run_id}</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            margin: 0;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background-color: white;
            padding: 30px;
            border-radius: 8px;
            box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #2c3e50;
            border-bottom: 3px solid #3498db;
            padding-bottom: 10px;
        }}
        h2 {{
            color: #34495e;
            margin-top: 30px;
            border-bottom: 2px solid #ecf0f1;
            padding-bottom: 8px;
        }}
        .summary-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin: 20px 0;
        }}
        .summary-card {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }}
        .summary-card.success {{
            background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
        }}
        .summary-card.warning {{
            background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
        }}
        .summary-card.info {{
            background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
        }}
        .summary-card h3 {{
            margin: 0 0 10px 0;
            font-size: 14px;
            opacity: 0.9;
        }}
        .summary-card .value {{
            font-size: 32px;
            font-weight: bold;
            margin: 0;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 20px 0;
            font-size: 14px;
        }}
        th {{
            background-color: #34495e;
            color: white;
            padding: 12px;
            text-align: left;
            position: sticky;
            top: 0;
        }}
        td {{
            padding: 10px 12px;
            border-bottom: 1px solid #ecf0f1;
        }}
        tr:hover {{
            background-color: #f8f9fa;
        }}
        .status-badge {{
            padding: 4px 12px;
            border-radius: 12px;
            font-size: 12px;
            font-weight: bold;
            display: inline-block;
        }}
        .status-completed {{
            background-color: #d4edda;
            color: #155724;
        }}
        .status-validated {{
            background-color: #c3e6cb;
            color: #155724;
        }}
        .status-failed {{
            background-color: #f8d7da;
            color: #721c24;
        }}
        .metric {{
            font-weight: bold;
            color: #2980b9;
        }}
        .duration {{
            color: #7f8c8d;
            font-size: 12px;
        }}
        .validation-pass {{
            color: #27ae60;
            font-weight: bold;
        }}
        .validation-fail {{
            color: #e74c3c;
            font-weight: bold;
        }}
        .timestamp {{
            color: #95a5a6;
            font-size: 12px;
        }}
        .section-divider {{
            margin: 40px 0;
            border-top: 2px dashed #ecf0f1;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Iceberg Migration Report</h1>

        <div class="timestamp">
            Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC<br>
            Run ID: <strong>{run_id}</strong>
        </div>

        <h2>Migration Summary</h2>
        <div class="summary-grid">
            <div class="summary-card">
                <h3>TOTAL TABLES</h3>
                <p class="value">{total_tables}</p>
            </div>
            <div class="summary-card success">
                <h3>SUCCESSFUL</h3>
                <p class="value">{successful_tables}</p>
            </div>
            <div class="summary-card warning">
                <h3>FAILED</h3>
                <p class="value">{failed_tables}</p>
            </div>
            <div class="summary-card info">
                <h3>TOTAL ROWS</h3>
                <p class="value">{total_rows:,}</p>
            </div>
            <div class="summary-card warning">
                <h3>COUNT MISMATCHES</h3>
                <p class="value">{count_mismatches}</p>
            </div>
        </div>

        <div class="section-divider"></div>

        <h2>Validation Summary</h2>

"""

    if iceberg_validation_summary and iceberg_validation_summary[0]['total_tables_validated']:
        ivs = iceberg_validation_summary[0]
        html += f"""
        <div class="summary-grid">
            <div class="summary-card info">
                <h3>TABLES VALIDATED</h3>
                <p class="value">{ivs.total_tables_validated}</p>
            </div>
            <div class="summary-card success">
                <h3>PASSED VALIDATION</h3>
                <p class="value">{ivs.tables_passed_validation}</p>
            </div>
            <div class="summary-card warning">
                <h3>FAILED VALIDATION</h3>
                <p class="value">{ivs.tables_failed_validation}</p>
            </div>
            <div class="summary-card warning">
                <h3>ROW COUNT MISMATCHES</h3>
                <p class="value">{ivs.total_row_count_mismatches}</p>
            </div>
            <div class="summary-card warning">
                <h3>PARTITION MISMATCHES</h3>
                <p class="value">{ivs.total_partition_count_mismatches}</p>
            </div>
            <div class="summary-card warning">
                <h3>SCHEMA MISMATCHES</h3>
                <p class="value">{ivs.total_schema_mismatches}</p>
            </div>
        </div>
"""
    else:
        html += """
        <p style="color: #95a5a6; font-style: italic;">No validation summary available for this run.</p>
"""

    html += """
        <div class="section-divider"></div>

        <h2>Table Migration Details</h2>
        <table>
            <thead>
                <tr>
                    <th>Source Database</th>
                    <th>Table</th>
                    <th>Migration Type</th>
                    <th>Destination</th>
                    <th>Status</th>
                    <th>Migration Duration</th>
                    <th>Validation Duration</th>
                </tr>
            </thead>
            <tbody>
"""

    for t in migration_status:
        if t.status == 'VALIDATED':
            status_class = 'status-validated'
        elif t.status == 'COMPLETED':
            status_class = 'status-completed'
        else:
            status_class = 'status-failed'

        migration_dur = f"{t.migration_duration_seconds:.1f}s" if t.migration_duration_seconds else "N/A"
        validation_dur = f"{t.validation_duration_seconds:.1f}s" if t.validation_duration_seconds else "N/A"

        html += f"""
                <tr>
                    <td>{t.source_database}</td>
                    <td><strong>{t.source_table}</strong></td>
                    <td>{t.migration_type}</td>
                    <td>{t.destination_table}</td>
                    <td><span class="status-badge {status_class}">{t.status}</span></td>
                    <td class="duration">{migration_dur}</td>
                    <td class="duration">{validation_dur}</td>
                </tr>
"""

    html += """
            </tbody>
        </table>

        <div class="section-divider"></div>

        <h2>Validation Results (Hive vs Iceberg)</h2>
        <table>
            <thead>
                <tr>
                    <th>Database</th>
                    <th>Table</th>
                    <th>Source Hive Rows</th>
                    <th>Dest Iceberg Rows</th>
                    <th>Row Count Match</th>
                    <th>Source Partitions</th>
                    <th>Dest Partitions</th>
                    <th>Partition Match</th>
                    <th>Schema Match</th>
                </tr>
            </thead>
            <tbody>
"""

    for t in migration_status:
        if t.validation_status == 'COMPLETED':
            row_match_class = 'validation-pass' if t.row_count_match else 'validation-fail'
            row_match_icon = '✓ PASS' if t.row_count_match else '✗ FAIL'
            part_match_class = 'validation-pass' if t.partition_count_match else 'validation-fail'
            part_match_icon = '✓ PASS' if t.partition_count_match else '✗ FAIL'
            schema_match_class = 'validation-pass' if t.schema_match else 'validation-fail'
            schema_match_icon = '✓ PASS' if t.schema_match else '✗ FAIL'
        else:
            row_match_class = part_match_class = schema_match_class = 'duration'
            row_match_icon = part_match_icon = schema_match_icon = 'N/A'

        html += f"""
                <tr>
                    <td>{t.source_database}</td>
                    <td><strong>{t.source_table}</strong></td>
                    <td class="metric">{(t.source_hive_row_count or 0):,}</td>
                    <td class="metric">{(t.destination_iceberg_row_count or 0):,}</td>
                    <td class="{row_match_class}">{row_match_icon}</td>
                    <td class="metric">{t.source_hive_partition_count or 0}</td>
                    <td class="metric">{t.dest_iceberg_partition_count or 0}</td>
                    <td class="{part_match_class}">{part_match_icon}</td>
                    <td class="{schema_match_class}">{schema_match_icon}</td>
                </tr>
"""

    html += """
            </tbody>
        </table>

        <div class="section-divider"></div>

        <h2>Performance Metrics</h2>
        <table>
            <thead>
                <tr>
                    <th>Database</th>
                    <th>Table</th>
                    <th>Migration Duration</th>
                    <th>Validation Duration</th>
                    <th>Total Duration</th>
                    <th>Rows Migrated</th>
                    <th>Rows/Second</th>
                </tr>
            </thead>
            <tbody>
"""

    for t in migration_status:
        migration_dur = t.migration_duration_seconds or 0
        validation_dur = t.validation_duration_seconds or 0
        total_dur = migration_dur + validation_dur

        rows_per_sec = (t.source_hive_row_count or 0) / (total_dur or 1)

        html += f"""
                <tr>
                    <td>{t.source_database}</td>
                    <td><strong>{t.source_table}</strong></td>
                    <td class="metric">{migration_dur:.1f}s</td>
                    <td class="metric">{validation_dur:.1f}s</td>
                    <td class="metric">{total_dur:.1f}s ({total_dur/60:.1f}m)</td>
                    <td class="metric">{(t.source_hive_row_count or 0):,}</td>
                    <td class="metric">{rows_per_sec:,.0f}</td>
                </tr>
"""

    html += """
            </tbody>
        </table>

        <div style="margin-top: 50px; padding-top: 20px; border-top: 1px solid #ecf0f1; color: #95a5a6; font-size: 12px;">
            <p>This report was automatically generated by the Iceberg Migration DAG.</p>
        </div>
    </div>
</body>
</html>
"""

    # Write HTML to S3
    report_filename = f"{run_id}_iceberg_report.html"
    report_path = f"{report_location}/{report_filename}"

    # Use Spark to write HTML
    hadoop_conf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
        spark._jvm.java.net.URI(report_path),
        hadoop_conf
    )

    output_path = spark._jvm.org.apache.hadoop.fs.Path(report_path)
    output_stream = fs.create(output_path, True)
    output_stream.write(html.encode('utf-8'))
    output_stream.close()

    return {'report_path': report_path}


@task.pyspark(conn_id='spark_default')
def finalize_iceberg_run(run_id: str, spark) -> dict:
    """Finalize Iceberg migration run - aggregate statistics."""
    config = get_config()
    tracking_db = config['tracking_database']

    stats = {'total': 0, 'successful': 0, 'failed': 0, 'skipped': 0, 'count_mismatches': 0}
    final_status = 'FAILED'
    overall_migration_type = 'UNKNOWN'

    try:
        stats_result = spark.sql(f"""
            SELECT
                COUNT(*) as total,
                SUM(CASE WHEN status IN ('VALIDATED', 'COMPLETED') THEN 1 ELSE 0 END) as successful,
                SUM(CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END) as failed,
                SUM(CASE WHEN status = 'SKIPPED' THEN 1 ELSE 0 END) as skipped,
                SUM(CASE WHEN row_count_match = false THEN 1 ELSE 0 END) as count_mismatches
            FROM {tracking_db}.iceberg_migration_table_status
            WHERE run_id = '{run_id}'
        """).collect()

        if not stats_result or stats_result[0]['total'] == 0:
            logger.warning(f"[finalize_iceberg_run] No table records found for run_id '{run_id}'.")
            final_status = 'FAILED'
        else:
            stats = {
                'total': stats_result[0]['total'] or 0,
                'successful': stats_result[0]['successful'] or 0,
                'failed': stats_result[0]['failed'] or 0,
                'skipped': stats_result[0]['skipped'] or 0,
                'count_mismatches': stats_result[0]['count_mismatches'] or 0,
            }
            final_status = 'COMPLETED' if stats['failed'] == 0 else 'COMPLETED_WITH_FAILURES'

    except Exception as e:
        logger.error(f"[finalize_iceberg_run] Failed to query iceberg_migration_table_status: {str(e)}")
        final_status = 'FAILED'

    try:
        migration_type_result = spark.sql(f"""
            SELECT migration_type, COUNT(*) as cnt
            FROM {tracking_db}.iceberg_migration_table_status
            WHERE run_id = '{run_id}'
            GROUP BY migration_type
            ORDER BY cnt DESC
            LIMIT 1
        """).collect()
        overall_migration_type = migration_type_result[0]['migration_type'] if migration_type_result else 'UNKNOWN'
    except Exception as e:
        logger.warning(f"[finalize_iceberg_run] Could not determine migration_type: {str(e)}")

    try:
        execute_with_iceberg_retry(spark, f"""
            UPDATE {tracking_db}.iceberg_migration_runs
            SET status = '{final_status}',
                completed_at = current_timestamp(),
                migration_type = '{overall_migration_type}',
                total_tables = {stats['total']},
                successful_tables = {stats['successful']},
                failed_tables = {stats['failed']}
            WHERE run_id = '{run_id}'
        """, task_label="finalize_iceberg_run:update_iceberg_migration_runs")

        logger.info(f"[finalize_iceberg_run] Run '{run_id}' finalized with status '{final_status}'. "
                    f"total={stats['total']}, successful={stats['successful']}, failed={stats['failed']}")

    except Exception as e:
        logger.error(f"[finalize_iceberg_run] Failed to update iceberg_migration_runs: {str(e)}")
        raise

    return {
        'run_id': run_id,
        'status': final_status,
        'total': stats['total'],
        'successful': stats['successful'],
        'failed': stats['failed'],
        'skipped': stats['skipped'],
        'count_mismatches': stats['count_mismatches']
    }



@task.pyspark(conn_id='spark_default')
def send_iceberg_report_email(report_result: dict, run_id: str, spark) -> dict:
    """Send HTML Iceberg migration report via email using SMTP."""
    import os
    import tempfile

    from airflow.utils.email import send_email

    config = get_config()
    smtp_conn_id = config.get('smtp_conn_id', 'smtp_default')
    recipients_str = config.get('email_recipients', '')

    if not recipients_str:
        logger.warning("[Email] No recipients configured. Skipping email.")
        return {'sent': False, 'reason': 'no_recipients'}

    recipients = [r.strip() for r in recipients_str.split(',') if r.strip()]
    report_path = report_result.get('report_path', '')

    try:
        logger.info(f"[Email] Reading Iceberg HTML report from S3: {report_path}")
        hadoop_conf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(
            spark._jvm.java.net.URI(report_path),
            hadoop_conf
        )
        s3_path_obj = spark._jvm.org.apache.hadoop.fs.Path(report_path)
        reader = spark._jvm.java.io.BufferedReader(
            spark._jvm.java.io.InputStreamReader(fs.open(s3_path_obj), "UTF-8")
        )
        lines = []
        line = reader.readLine()
        while line is not None:
            lines.append(line)
            line = reader.readLine()
        reader.close()
        html_content = "\n".join(lines)

        tmp = tempfile.NamedTemporaryFile(
            mode='w', suffix='.html',
            prefix=f'{run_id}_iceberg_report_',
            delete=False
        )
        tmp.write(html_content)
        tmp.close()

        send_email(
            to=recipients,
            subject=f"Iceberg Migration Report - {run_id}",
            html_content=f"<p>Please find the Iceberg migration report for run <strong>{run_id}</strong> attached.</p>",
            files=[tmp.name],
            conn_id=smtp_conn_id,
        )
        os.unlink(tmp.name)
        logger.info(f"[Email] Iceberg report sent to: {recipients}")
        return {'sent': True, 'recipients': recipients, 'report_path': report_path}
    except Exception as e:
        logger.error(f"[Email] Failed to send Iceberg report: {str(e)}")
        raise Exception(f"Failed to send Iceberg report email: {str(e)}") from e

# =============================================================================
# DAG 3: FOLDER-ONLY DATA COPY TASKS  (unchanged from v3)
# =============================================================================

@task
def validate_prerequisites_folder_copy() -> dict:
    checks = {'pyspark_available': False, 's3_accessible': False}; errors = []
    logger.info("=" * 60); logger.info("[FolderCopy] PRE-DAG VALIDATION (Test Env)"); logger.info("=" * 60)
    try:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("fc_validation").getOrCreate(); spark.range(1).collect()
        checks['pyspark_available'] = True; logger.info("PySpark: PASSED"); spark.stop()
    except Exception as e: errors.append(f"PySpark: {str(e)}"); logger.error(f"PySpark: FAILED — {str(e)}")
    try:
        config = get_config()
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.appName("fc_s3_test").getOrCreate()
        test_path = config['default_s3_bucket']; hconf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jvm.java.net.URI(test_path), hconf)
        fs.exists(spark._jvm.org.apache.hadoop.fs.Path(test_path))
        checks['s3_accessible'] = True; logger.info("S3 Access: PASSED"); spark.stop()
    except Exception as e: errors.append(f"S3: {str(e)}"); logger.error(f"S3: FAILED — {str(e)}")
    if errors: raise Exception(f"Pre-DAG validation failed:\n" + "\n".join(f"  - {e}" for e in errors))
    logger.info("ALL PRE-DAG CHECKS PASSED"); return checks


@task.pyspark(conn_id='spark_default')
def init_folder_copy_tracking_tables(spark) -> dict:
    config = get_config(); tracking_db = config['tracking_database']; tracking_loc = config['tracking_location']
    spark.sql(f"CREATE DATABASE IF NOT EXISTS {tracking_db} LOCATION '{tracking_loc}'")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {tracking_db}.data_copy_runs (
        run_id STRING, excel_file_path STRING, started_at TIMESTAMP, completed_at TIMESTAMP, status STRING,
        total_folders INT, successful_folders INT, failed_folders INT, error_message STRING, created_at TIMESTAMP
    ) USING iceberg LOCATION '{tracking_loc}/data_copy_runs'""")
    spark.sql(f"""CREATE TABLE IF NOT EXISTS {tracking_db}.data_copy_status (
        run_id STRING, source_path STRING, dest_bucket STRING, dest_path STRING,
        status STRING, started_at TIMESTAMP, completed_at TIMESTAMP,
        source_file_count BIGINT, source_size_bytes BIGINT, dest_file_count BIGINT, dest_size_bytes BIGINT,
        files_copied BIGINT, bytes_copied BIGINT, is_incremental BOOLEAN, file_count_match BOOLEAN,
        size_match BOOLEAN, error_message STRING, updated_at TIMESTAMP
    ) USING iceberg LOCATION '{tracking_loc}/data_copy_status'""")
    return {'status': 'initialized', 'database': tracking_db}


@task.pyspark(conn_id='spark_default')
def create_data_copy_run(excel_file_path: str, spark) -> str:
    import uuid; from datetime import datetime
    config = get_config(); tracking_db = config['tracking_database']
    run_id = f"folder_run_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:8]}"
    spark.sql(f"""INSERT INTO {tracking_db}.data_copy_runs (run_id, excel_file_path, started_at, completed_at, status, total_folders, successful_folders, failed_folders, error_message, created_at)
        VALUES ('{run_id}', '{excel_file_path}', current_timestamp(), NULL, 'RUNNING', NULL, NULL, NULL, NULL, current_timestamp())""")
    return run_id


@task.pyspark(conn_id='spark_default')
def parse_folder_copy_excel(excel_file_path: str, run_id: str, spark) -> list:
    import os; from io import BytesIO; import pandas as ps
    def _norm(raw):
        val = raw.strip()
        if val.startswith('s3n://'): val = 's3a://' + val[6:]
        elif val.startswith('s3://'): val = 's3a://' + val[5:]
        elif not val.startswith('s3a://'): val = f"s3a://{val}"
        return val
    binary_df = spark.read.format("binaryFile").load(excel_file_path)
    row = binary_df.select("content").first()
    df = ps.read_excel(BytesIO(bytes(row.content)), engine='openpyxl')
    df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')
    configs = []
    for _, row in df.iterrows():
        source_path = str(row.get('source_path', '') or '').strip()
        if not source_path: continue
        raw_bucket = str(row.get('target_bucket', '') or '').strip()
        if not raw_bucket: logger.warning(f"[FolderCopy] Skipping — missing target_bucket for {source_path!r}"); continue
        dest_bucket = _norm(raw_bucket)
        raw_dest_folder = str(row.get('dest_folder', '') or '').strip()
        dest_folder = raw_dest_folder if raw_dest_folder else os.path.basename(source_path.rstrip('/'))
        configs.append({'run_id': run_id, 'source_path': source_path, 'dest_bucket': dest_bucket, 'dest_folder': dest_folder})
    if not configs: raise ValueError("[FolderCopy] No valid rows in Excel")
    return configs


@task.pyspark(conn_id='spark_default')
def run_folder_s3_copy(folder_config: dict, spark) -> dict:
    from datetime import datetime as _dt; from py4j.java_gateway import java_import
    run_id = folder_config['run_id']; source_path = folder_config['source_path']
    dest_bucket = folder_config['dest_bucket']; dest_folder = folder_config['dest_folder']
    s3_dest = f"{dest_bucket}/{dest_folder}"; started_str = _dt.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    try:
        java_import(spark._jvm, "org.apache.hadoop.fs.*"); hconf = spark._jsc.hadoopConfiguration()
        src_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jvm.java.net.URI(source_path), hconf)
        dst_fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jvm.java.net.URI(s3_dest), hconf)
        src_path = spark._jvm.org.apache.hadoop.fs.Path(source_path); dst_path = spark._jvm.org.apache.hadoop.fs.Path(s3_dest)
        is_incr = dst_fs.exists(dst_path)
        size_before = 0; files_before = 0
        if is_incr:
            try: cs = dst_fs.getContentSummary(dst_path); size_before = int(cs.getLength()); files_before = int(cs.getFileCount())
            except Exception: pass
        src_size = 0; src_files = 0
        try: cs = src_fs.getContentSummary(src_path); src_size = int(cs.getLength()); src_files = int(cs.getFileCount())
        except Exception: pass
        success = spark._jvm.org.apache.hadoop.fs.FileUtil.copy(src_fs, src_path, dst_fs, dst_path, False, True, hconf)
        if not success: raise Exception("FileUtil.copy returned false")
        size_after = 0; files_after = 0
        try: cs = dst_fs.getContentSummary(dst_path); size_after = int(cs.getLength()); files_after = int(cs.getFileCount())
        except Exception: pass
        return {'run_id': run_id, 'source_path': source_path, 'dest_bucket': dest_bucket, 'dest_path': dest_folder, 'status': 'COMPLETED',
                'started_at': started_str, 'completed_at': _dt.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                'source_file_count': src_files, 'source_size_bytes': src_size, 'dest_file_count': files_after, 'dest_size_bytes': size_after,
                'files_copied': max(files_after - files_before, 0), 'bytes_copied': max(size_after - size_before, 0),
                'is_incremental': is_incr, 'file_count_match': src_files == files_after,
                'size_match': abs(src_size - size_after) <= max(1, int(src_size * 0.01)), 'error': None}
    except Exception as e:
        error_msg = str(e)[:2000]; logger.error(f"[FolderCopy] ERROR {source_path} -> {s3_dest}: {error_msg}")
        return {'run_id': run_id, 'source_path': source_path, 'dest_bucket': dest_bucket, 'dest_path': dest_folder, 'status': 'FAILED',
                'started_at': started_str, 'completed_at': _dt.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
                'source_file_count': 0, 'source_size_bytes': 0, 'dest_file_count': 0, 'dest_size_bytes': 0,
                'files_copied': 0, 'bytes_copied': 0, 'is_incremental': False, 'file_count_match': False, 'size_match': False, 'error': error_msg}


@task.pyspark(conn_id='spark_default')
def record_data_copy_status(distcp_result: dict, spark) -> dict:
    config = get_config(); tracking_db = config['tracking_database']
    run_id = distcp_result['run_id']; src = distcp_result['source_path'].replace("'","''")
    db = distcp_result['dest_bucket'].replace("'","''"); dp = distcp_result['dest_path'].replace("'","''")
    err = (distcp_result.get('error') or '').replace("'","''")[:2000]
    execute_with_iceberg_retry(spark, f"""
        INSERT INTO {tracking_db}.data_copy_status (
            run_id, source_path, dest_bucket, dest_path, status, started_at, completed_at,
            source_file_count, source_size_bytes, dest_file_count, dest_size_bytes,
            files_copied, bytes_copied, is_incremental, file_count_match, size_match, error_message, updated_at
        ) VALUES (
            '{run_id}','{src}','{db}','{dp}','{distcp_result['status']}',
            CAST('{distcp_result.get('started_at','')}' AS TIMESTAMP), CAST('{distcp_result.get('completed_at','')}' AS TIMESTAMP),
            {distcp_result.get('source_file_count',0)},{distcp_result.get('source_size_bytes',0)},
            {distcp_result.get('dest_file_count',0)},{distcp_result.get('dest_size_bytes',0)},
            {distcp_result.get('files_copied',0)},{distcp_result.get('bytes_copied',0)},
            {str(distcp_result.get('is_incremental',False)).lower()},
            {str(distcp_result.get('file_count_match',False)).lower()},
            {str(distcp_result.get('size_match',False)).lower()},
            '{err}', current_timestamp())
    """, task_label=f"record_data_copy:{src}")
    return distcp_result


@task.pyspark(conn_id='spark_default')
def validate_data_copy(copy_status: dict, spark) -> dict:
    from py4j.java_gateway import java_import
    source_path = copy_status['source_path']; dest_bucket = copy_status['dest_bucket']
    dest_path = copy_status['dest_path']; s3_dest = f"{dest_bucket}/{dest_path}"
    if copy_status.get('status') == 'FAILED':
        logger.warning(f"[FolderValidate] Skipping — copy FAILED for {source_path}")
        raise Exception(f"Validation skipped — upstream copy FAILED for {source_path}")
    dest_file_count = 0; dest_size_bytes = 0; validation_error = None; dest_exists = False
    try:
        java_import(spark._jvm, "org.apache.hadoop.fs.*"); hconf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jvm.java.net.URI(s3_dest), hconf)
        dst_path = spark._jvm.org.apache.hadoop.fs.Path(s3_dest); dest_exists = fs.exists(dst_path)
        if dest_exists:
            cs = fs.getContentSummary(dst_path); dest_file_count = int(cs.getFileCount()); dest_size_bytes = int(cs.getLength())
        else:
            validation_error = f"S3 destination does not exist: {s3_dest}"
    except Exception as e:
        validation_error = f"Validation error: {str(e)[:1000]}"; logger.error(f"[FolderValidate] {validation_error}")
    src_file_count = copy_status.get('source_file_count', 0) or 0; src_size_bytes = copy_status.get('source_size_bytes', 0) or 0
    file_count_match = (dest_file_count == src_file_count); size_match = abs(src_size_bytes - dest_size_bytes) <= max(1, int(src_size_bytes * 0.01))
    validation_status = 'VALIDATED' if (dest_exists and file_count_match and size_match and not validation_error) else 'VALIDATION_FAILED'
    result = {**copy_status, 'dest_file_count': dest_file_count, 'dest_size_bytes': dest_size_bytes,
              'file_count_match': file_count_match, 'size_match': size_match, 'validation_status': validation_status, 'validation_error': validation_error}
    if validation_status != 'VALIDATED':
        raise Exception(f"Validation {validation_status} for {source_path}: {validation_error or 'mismatch'}")
    return result


@task.pyspark(conn_id='spark_default')
def update_data_copy_validation(validation_result: dict, spark) -> dict:
    config = get_config(); tracking_db = config['tracking_database']
    run_id = validation_result['run_id']; src = validation_result['source_path'].replace("'","''")
    db = validation_result['dest_bucket'].replace("'","''"); dp = validation_result['dest_path'].replace("'","''")
    vs = validation_result.get('validation_status', 'VALIDATION_FAILED')
    val_err = (validation_result.get('validation_error') or '').replace("'","''")[:2000]
    execute_with_iceberg_retry(spark, f"""
        UPDATE {tracking_db}.data_copy_status
        SET status='{vs}', dest_file_count={validation_result.get('dest_file_count',0) or 0},
            dest_size_bytes={validation_result.get('dest_size_bytes',0) or 0},
            file_count_match={str(validation_result.get('file_count_match',False)).lower()},
            size_match={str(validation_result.get('size_match',False)).lower()},
            error_message=CASE WHEN '{val_err}'!='' THEN '{val_err}' ELSE error_message END,
            updated_at=current_timestamp()
        WHERE run_id='{run_id}' AND source_path='{src}' AND dest_bucket='{db}' AND dest_path='{dp}'
    """, task_label=f"update_data_copy_val:{src}")
    return validation_result


@task.pyspark(conn_id='spark_default')
def finalize_data_copy_run(run_id: str, spark) -> dict:
    config = get_config(); tracking_db = config['tracking_database']
    stats = spark.sql(f"""SELECT COUNT(*) AS total, SUM(CASE WHEN status='VALIDATED' THEN 1 ELSE 0 END) AS successful, SUM(CASE WHEN status IN ('FAILED','VALIDATION_FAILED','VALIDATION_SKIPPED') THEN 1 ELSE 0 END) AS failed FROM {tracking_db}.data_copy_status WHERE run_id='{run_id}'""").collect()
    total = int(stats[0]['total'] or 0) if stats else 0; successful = int(stats[0]['successful'] or 0) if stats else 0; failed = int(stats[0]['failed'] or 0) if stats else 0
    overall = 'COMPLETED' if failed == 0 else 'COMPLETED_WITH_ERRORS'
    execute_with_iceberg_retry(spark, f"""UPDATE {tracking_db}.data_copy_runs SET status='{overall}', completed_at=current_timestamp(), total_folders={total}, successful_folders={successful}, failed_folders={failed} WHERE run_id='{run_id}'""", task_label="finalize_data_copy_run")
    return {'run_id': run_id, 'status': overall, 'total_folders': total, 'successful_folders': successful, 'failed_folders': failed}


@task.pyspark(conn_id='spark_default')
def generate_data_copy_html_report(finalize_result: dict, run_id: str, spark) -> dict:
    from datetime import datetime
    config = get_config(); tracking_db = config['tracking_database']; report_location = config['report_output_location']
    run_info = spark.sql(f"SELECT * FROM {tracking_db}.data_copy_runs WHERE run_id='{run_id}'").collect()
    run_row = run_info[0] if run_info else None
    folders = spark.sql(f"SELECT * FROM {tracking_db}.data_copy_status WHERE run_id='{run_id}' ORDER BY source_path").collect()
    total = len(folders); validated = sum(1 for f in folders if f.status == 'VALIDATED')
    failed = sum(1 for f in folders if (f.status or '') in ('FAILED','VALIDATION_FAILED','VALIDATION_SKIPPED'))
    total_gb = sum(f.dest_size_bytes or 0 for f in folders) / (1024**3)
    html = f"""<!DOCTYPE html><html><head><meta charset="UTF-8"><title>Folder Copy Report (v4) - {run_id}</title>
<style>body{{font-family:'Segoe UI',sans-serif;margin:0;padding:20px;background:#f5f5f5}}.container{{max-width:1400px;margin:0 auto;background:#fff;padding:30px;border-radius:8px}}
h1{{color:#2c3e50;border-bottom:3px solid #3498db;padding-bottom:10px}}h2{{color:#34495e;margin-top:28px}}
.grid{{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:16px;margin:20px 0}}.card{{color:#fff;padding:18px;border-radius:8px}}
.c1{{background:linear-gradient(135deg,#667eea,#764ba2)}}.c2{{background:linear-gradient(135deg,#11998e,#38ef7d)}}.c3{{background:linear-gradient(135deg,#f093fb,#f5576c)}}.c4{{background:linear-gradient(135deg,#4facfe,#00f2fe)}}
.card h3{{margin:0 0 8px;font-size:12px;opacity:.9}}.card .val{{font-size:26px;font-weight:bold;margin:0}}
table{{width:100%;border-collapse:collapse;margin:16px 0;font-size:13px}}th{{background:#34495e;color:#fff;padding:9px}}td{{padding:8px 10px;border-bottom:1px solid #ecf0f1}}tr:hover{{background:#f8f9fa}}
.badge{{padding:3px 9px;border-radius:9px;font-size:11px;font-weight:bold}}.ok{{background:#d4edda;color:#155724}}.fail{{background:#f8d7da;color:#721c24}}
.pass{{color:#27ae60;font-weight:bold}}.fail-txt{{color:#e74c3c;font-weight:bold}}.dim{{color:#7f8c8d;font-size:12px}}.metric{{font-weight:bold;color:#2980b9}}</style></head>
<body><div class="container">
<h1>Folder Data Copy Report <small style="font-size:14px;color:#95a5a6">(Test Env v4)</small></h1>
<div class="dim">Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')} UTC | Run: <strong>{run_id}</strong> | Status: <strong>{run_row.status if run_row else 'UNKNOWN'}</strong></div>
<h2>Summary</h2><div class="grid">
<div class="card c1"><h3>TOTAL FOLDERS</h3><p class="val">{total}</p></div>
<div class="card c2"><h3>VALIDATED</h3><p class="val">{validated}</p></div>
<div class="card c3"><h3>FAILED</h3><p class="val">{failed}</p></div>
<div class="card c4"><h3>TOTAL DATA</h3><p class="val">{total_gb:.3f} GB</p></div></div>
<h2>Copy Details</h2><table><thead><tr><th>Source Path</th><th>Destination</th><th>Status</th><th>Src Files</th><th>Dst Files</th><th>File Match</th><th>Src (GB)</th><th>Dst (GB)</th><th>Size Match</th><th>Incr</th><th>Error</th></tr></thead><tbody>"""
    for f in folders:
        s = f.status or ''; bcls = 'ok' if 'VALIDATED' in s else 'fail'
        sg = (f.source_size_bytes or 0) / (1024**3); dg = (f.dest_size_bytes or 0) / (1024**3)
        fmc = 'pass' if f.file_count_match else 'fail-txt'; smc = 'pass' if f.size_match else 'fail-txt'
        err = (f.error_message or '')[:120]
        html += f"<tr><td>{f.source_path}</td><td>{f.dest_bucket}/{f.dest_path}</td><td><span class='badge {bcls}'>{s}</span></td><td class='metric'>{(f.source_file_count or 0):,}</td><td class='metric'>{(f.dest_file_count or 0):,}</td><td class='{fmc}'>{'✓' if f.file_count_match else '✗'}</td><td class='metric'>{sg:.4f}</td><td class='metric'>{dg:.4f}</td><td class='{smc}'>{'✓' if f.size_match else '✗'}</td><td>{'✓' if f.is_incremental else ''}</td><td class='dim'>{err}</td></tr>\n"
    html += f"</tbody></table><div style='margin-top:40px;color:#95a5a6;font-size:12px;border-top:1px solid #ecf0f1;padding-top:16px'>Auto-generated by es_folder_only_data_copy_v4.</div></div></body></html>"
    report_path = f"{report_location}/{run_id}_data_copy_report.html"
    hconf = spark._jsc.hadoopConfiguration()
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jvm.java.net.URI(report_path), hconf)
    out = spark._jvm.org.apache.hadoop.fs.Path(report_path); s = fs.create(out, True); s.write(html.encode('utf-8')); s.close()
    return {'report_path': report_path}


@task.pyspark(conn_id='spark_default')
def send_data_copy_report_email(report_result: dict, run_id: str, spark) -> dict:
    config = get_config(); recipients_str = config.get('email_recipients', '')
    if not recipients_str: return {'sent': False, 'reason': 'no_recipients'}
    import os, tempfile; from airflow.utils.email import send_email
    recipients = [r.strip() for r in recipients_str.split(',') if r.strip()]; report_path = report_result.get('report_path', '')
    try:
        hconf = spark._jsc.hadoopConfiguration()
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jvm.java.net.URI(report_path), hconf)
        reader = spark._jvm.java.io.BufferedReader(spark._jvm.java.io.InputStreamReader(fs.open(spark._jvm.org.apache.hadoop.fs.Path(report_path)), "UTF-8"))
        lines = []; line = reader.readLine()
        while line is not None: lines.append(line); line = reader.readLine()
        reader.close()
        tmp = tempfile.NamedTemporaryFile(mode='w', suffix='.html', prefix=f'{run_id}_folder_report_', delete=False); tmp.write("\n".join(lines)); tmp.close()
        send_email(to=recipients, subject=f"Folder Copy Report - {run_id}", html_content=f"<p>Report for <strong>{run_id}</strong> attached.</p>", files=[tmp.name], conn_id=config.get('smtp_conn_id','smtp_default'))
        os.unlink(tmp.name); return {'sent': True, 'recipients': recipients}
    except Exception as e: logger.error(f"[Email] {str(e)}"); raise


# =============================================================================
# DAG 1 DEFINITION: S3-TO-S3 MIGRATION (v4)
# =============================================================================

with DAG(
    dag_id='es_mapr_to_s3_migration_v4',
    default_args=DEFAULT_ARGS,
    description='Migrate Hive tables S3→S3 – Test Env v4 (full partition_filter parity)',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=5,
    tags=['migration', 'test', 's3', 'hive', 'v4'],
    params={
        'excel_file_path': Param(
            default='s3a://rapid-pdc-default-5b7a5u7/migration_inputs_es_v4/migration.xlsx',
            type='string',
            description='S3 path to migration.xlsx'
        ),
        'setup_test_data': Param(
            default=True,
            type='boolean',
            description='Run setup_test_datasets first. Set False if source data already exists.'
        ),
    },
    render_template_as_native_obj=True,
) as dag_mapr_to_s3:

    t_setup   = setup_test_datasets()
    t_validate = validate_prerequisites(run_id="{{ run_id }}")
    t_init    = init_tracking_tables()
    t_run_id  = create_migration_run(excel_file_path="{{ params.excel_file_path }}", dag_run_id="{{ run_id }}")
    t_excel   = parse_excel(excel_file_path="{{ params.excel_file_path }}", run_id=t_run_id)
    t_cluster = cluster_login_setup(run_id=t_run_id)

    t_discover       = discover_tables_via_spark.expand(db_config=t_excel)
    t_record         = record_discovered_tables.expand(discovery=t_discover)
    t_record.operator.trigger_rule = 'all_done'
    t_distcp         = run_s3_copy.expand(discovery=t_record)
    t_distcp.operator.trigger_rule = 'all_done'
    t_distcp_status  = update_distcp_status.expand(distcp_result=t_distcp)
    t_distcp_status.operator.trigger_rule = 'all_done'
    t_tables         = create_hive_tables.expand(distcp_result=t_distcp_status)
    t_tables.operator.trigger_rule = 'all_done'
    t_tbl_status     = update_table_create_status.expand(table_result=t_tables)
    t_tbl_status.operator.trigger_rule = 'all_done'
    t_dest_validation = validate_destination_tables.expand(source_validation=t_tbl_status)
    t_dest_validation.operator.max_active_tis_per_dagrun = 2
    t_dest_validation.operator.trigger_rule = 'all_done'
    t_val_status     = update_validation_status.expand(validation_result=t_dest_validation)
    t_val_status.operator.trigger_rule = 'all_done'
    t_report  = generate_html_report(run_id=t_run_id)
    t_report.operator.trigger_rule = 'all_done'
    t_email   = send_migration_report_email(run_id=t_run_id, report_result=t_report)
    t_email.operator.trigger_rule = 'all_done'
    t_final   = finalize_run(run_id=t_run_id)
    t_final.operator.trigger_rule = 'all_done'

    t_setup >> t_validate >> t_init >> t_run_id >> t_excel >> t_cluster >> t_discover
    t_discover >> t_record >> t_distcp >> t_distcp_status >> t_tables >> t_tbl_status
    t_tbl_status >> t_dest_validation >> t_val_status
    t_val_status >> t_report >> t_email >> t_final

# =============================================================================
# DAG 2 DEFINITION: ICEBERG MIGRATION (v4)
# =============================================================================

with DAG(
    dag_id='es_iceberg_migration_v4',
    default_args=DEFAULT_ARGS,
    description='Migrate Hive tables to Iceberg – Test Env v4',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=5,
    tags=['migration', 'test', 'iceberg', 'hive', 'v4'],
    params={
        'excel_file_path': Param(
            default='s3a://rapid-pdc-default-5b7a5u7/migration_inputs_es_v4/iceberg_migration.xlsx',
            type='string',
            description='S3 path to iceberg_migration.xlsx'
        )
    },
    render_template_as_native_obj=True,
) as dag_iceberg:

    t_ice_init    = init_iceberg_tracking_tables()
    t_ice_run_id  = create_iceberg_migration_run(excel_file_path="{{ params.excel_file_path }}", dag_run_id="{{ run_id }}")
    t_ice_excel   = parse_iceberg_excel(excel_file_path="{{ params.excel_file_path }}", run_id=t_ice_run_id)
    t_ice_discover = discover_hive_tables.expand(db_config=t_ice_excel)
    t_ice_migrate  = migrate_tables_to_iceberg.partial(dag_run_id="{{ run_id }}").expand(discovery=t_ice_discover)
    t_ice_migrate.operator.trigger_rule = 'all_done'
    t_ice_durations = update_migration_durations.expand(migration_result=t_ice_migrate)
    t_ice_durations.operator.trigger_rule = 'all_done'
    t_ice_validate  = validate_iceberg_tables.expand(migration_result=t_ice_durations)
    t_ice_validate.operator.max_active_tis_per_dagrun = 2
    t_ice_validate.operator.trigger_rule = 'all_done'
    t_ice_val_status = update_iceberg_validation_status.expand(validation_result=t_ice_validate)
    t_ice_val_status.operator.trigger_rule = 'all_done'
    t_ice_report = generate_iceberg_html_report(run_id=t_ice_run_id)
    t_ice_report.operator.trigger_rule = 'all_done'
    t_ice_email  = send_iceberg_report_email(run_id=t_ice_run_id, report_result=t_ice_report)
    t_ice_email.operator.trigger_rule = 'all_done'
    t_ice_final  = finalize_iceberg_run(run_id=t_ice_run_id)
    t_ice_final.operator.trigger_rule = 'all_done'

    (t_ice_init >> t_ice_run_id >> t_ice_excel >> t_ice_discover >> t_ice_migrate
     >> t_ice_durations >> t_ice_validate >> t_ice_val_status >> t_ice_report
     >> t_ice_email >> t_ice_final)

# =============================================================================
# DAG 3 DEFINITION: FOLDER-ONLY DATA COPY (v4)
# =============================================================================

with DAG(
    dag_id='es_folder_only_data_copy_v4',
    default_args=DEFAULT_ARGS,
    description='Copy S3 folders to S3 – Test Env v4',
    schedule=None,
    start_date=datetime(2025, 1, 1),
    catchup=False,
    max_active_runs=3,
    tags=['migration', 'test', 's3', 'folder-copy', 'v4'],
    params={
        'excel_file_path': Param(
            default='s3a://<YOUR_BUCKET>/migration_inputs/folder_copy.xlsx',
            type='string',
            description='S3 path to folder_copy.xlsx (columns: source_path | target_bucket | dest_folder)'
        )
    },
    render_template_as_native_obj=True,
) as dag_folder_copy:

    t_fc_prereq  = validate_prerequisites_folder_copy()
    t_fc_init    = init_folder_copy_tracking_tables()
    t_fc_run_id  = create_data_copy_run(excel_file_path="{{ params.excel_file_path }}")
    t_fc_cluster = cluster_login_setup(run_id=t_fc_run_id)
    t_fc_excel   = parse_folder_copy_excel(excel_file_path="{{ params.excel_file_path }}", run_id=t_fc_run_id)

    t_fc_copy = run_folder_s3_copy.override(
        trigger_rule='all_done', max_active_tis_per_dagrun=3
    ).expand(folder_config=t_fc_excel)
    t_fc_record = record_data_copy_status.override(trigger_rule='all_done').expand(distcp_result=t_fc_copy)
    t_fc_validate = validate_data_copy.override(trigger_rule='all_done').expand(copy_status=t_fc_record)
    t_fc_val_status = update_data_copy_validation.override(trigger_rule='all_done').expand(validation_result=t_fc_validate)
    t_fc_final = finalize_data_copy_run.override(trigger_rule='all_done')(run_id=t_fc_run_id)
    t_fc_report = generate_data_copy_html_report.override(trigger_rule='all_done')(run_id=t_fc_run_id, finalize_result=t_fc_final)
    t_fc_email  = send_data_copy_report_email.override(trigger_rule='all_done')(report_result=t_fc_report, run_id=t_fc_run_id)

    (t_fc_prereq >> t_fc_init >> t_fc_run_id >> t_fc_excel >> t_fc_cluster
     >> t_fc_copy >> t_fc_record >> t_fc_validate >> t_fc_val_status
     >> t_fc_final >> t_fc_report >> t_fc_email)