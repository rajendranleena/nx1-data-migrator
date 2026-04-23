"""
Shared pytest fixtures for migration DAG tests.
All external dependencies (SSH, Spark, Airflow Variables, SMTP) are mocked.
"""

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_MODULE_DIR = Path(__file__).resolve().parent.parent

MOCK_VARIABLES = {
    'cluster_ssh_conn_id':          'cluster_edge_ssh',
    'migration_default_s3_bucket':  's3a://test-bucket',
    's3_endpoint':                  '',
    's3_access_key':                'AKIATEST',
    's3_secret_key':                'testsecret',
    'migration_distcp_mappers':     '10',
    'migration_distcp_bandwidth':   '50',
    'migration_spark_conn_id':      'spark_default',
    'migration_tracking_database':  'migration_tracking',
    'migration_tracking_location':  's3a://test-bucket/tracking',
    'migration_report_location':    's3a://test-bucket/reports',
    'auth_method':                  'none',
    'mapr_user':                    '',
    'mapr_ticketfile_location':     '/tmp/maprticket',
    'kinit_principal':              '',
    'kinit_keytab':                 '',
    'kinit_password':               '',
    'cluster_edge_temp_path':       '/tmp/migration',
    's3_listing_tool':              'hadoop',
    'migration_smtp_conn_id':       'smtp_default',
    'migration_email_recipients':   'user@example.com',
    's3_source_endpoint':  '',
    's3_source_access_key': 'AKIASRC',
    's3_source_secret_key': 'srcsecret',
    's3_dest_endpoint':    '',
    's3_dest_access_key':  'AKIADST',
    's3_dest_secret_key':  'dstsecret',
}


# ---------------------------------------------------------------------------
# Airflow / PySpark stub builders
# ---------------------------------------------------------------------------

class _FakeParam:
    def __init__(self, default=None, **kwargs):
        self._default = default

    def __str__(self):
        return str(self._default) if self._default is not None else ''


class _FakeDAG:
    """Minimal DAG stub so the module can be imported outside Airflow."""
    _active = None

    def __init__(self, dag_id, **kwargs):
        self.dag_id = dag_id
        self.params = kwargs.get('params', {})

    def __enter__(self):
        _FakeDAG._active = self
        return self

    def __exit__(self, *args):
        _FakeDAG._active = None
        return False


def _make_airflow_stubs():
    variable_mock = MagicMock()
    variable_mock.get.side_effect = (
        lambda key, default_var=None, **kw: MOCK_VARIABLES.get(key, default_var)
    )

    def _wrap_task_func(f):
        def _make_xcom(label):
            xcom = MagicMock(name=f"XComArg<{label}>")
            xcom.operator = MagicMock()
            xcom.override = lambda **kw: xcom
            return xcom

        def task_callable(*args, **kwargs):
            return _make_xcom(f.__name__)

        def _expand(**kwargs):
            return _make_xcom(f"expand:{f.__name__}")

        def _partial(**kwargs):
            obj = MagicMock()
            obj.expand = _expand
            return obj

        task_callable.function = f
        task_callable.__name__ = f.__name__
        task_callable.__qualname__ = f.__qualname__
        task_callable.override = lambda **kw: task_callable
        task_callable.expand = _expand
        task_callable.partial = _partial
        return task_callable

    def passthrough_task(func=None, **kwargs):
        if func is not None:
            return _wrap_task_func(func)
        return lambda f: _wrap_task_func(f)

    passthrough_task.pyspark = lambda **kw: passthrough_task

    airflow_decorators = MagicMock()
    airflow_decorators.task = passthrough_task

    airflow_models = MagicMock()
    airflow_models.Variable = variable_mock
    airflow_models.param = MagicMock(Param=_FakeParam)

    pyspark_sql_utils = MagicMock()
    pyspark_sql_utils.AnalysisException = type('AnalysisException', (Exception,), {})

    stubs = {
        "airflow":                              MagicMock(DAG=_FakeDAG),
        "airflow.DAG":                          _FakeDAG,
        "airflow.decorators":                   airflow_decorators,
        "airflow.decorators.task":              passthrough_task,
        "airflow.models":                       airflow_models,
        "airflow.models.Variable":              variable_mock,
        "airflow.models.param":                 MagicMock(Param=_FakeParam),
        "airflow.models.param.Param":           _FakeParam,
        "airflow.providers":                    MagicMock(),
        "airflow.providers.ssh":                MagicMock(),
        "airflow.providers.ssh.hooks":          MagicMock(),
        "airflow.providers.ssh.hooks.ssh":      MagicMock(),
        "airflow.utils":                        MagicMock(),
        "airflow.utils.email":                  MagicMock(),
        "airflow.utils.trigger_rule":           MagicMock(TriggerRule=MagicMock(ALL_DONE="all_done")),
        "dotenv":                               MagicMock(),
        "pyspark":                              MagicMock(),
        "pyspark.sql":                          MagicMock(),
        "pyspark.sql.utils":                    pyspark_sql_utils,
        "py4j":                                 MagicMock(),
        "py4j.java_gateway":                    MagicMock(),
    }
    return stubs, variable_mock


# ---------------------------------------------------------------------------
# Session-level stub installation
# ---------------------------------------------------------------------------
_SESSION_STUBS, _ = _make_airflow_stubs()
_saved_modules: dict = {}

_str_dir = str(_MODULE_DIR)
if _str_dir not in sys.path:
    sys.path.insert(0, _str_dir)

for _name, _fake in _SESSION_STUBS.items():
    _saved_modules[_name] = sys.modules.get(_name)
    sys.modules[_name] = _fake

@pytest.fixture(scope="session", autouse=True)
def _install_airflow_stubs():
    yield
    for name, original in _saved_modules.items():
        if original is None:
            sys.modules.pop(name, None)
        else:
            sys.modules[name] = original
    sys.modules.pop("migration_dag_mapr_to_s3", None)
    sys.modules.pop("migration_dag_iceberg", None)
    sys.modules.pop("migration_dag_folder_copy", None)
    sys.modules.pop("migration_dag_metadata", None)
    sys.modules.pop("utils.migrations.iceberg_to_iceberg", None)
    sys.modules.pop("utils.migrations.iceberg_rewrite", None)
    sys.modules.pop("utils.migrations.partition_utils", None)
    sys.modules.pop("utils.migrations.shared", None)
    sys.modules.pop("utils.migrations", None)
    sys.modules.pop("utils", None)
    if _str_dir in sys.path:
        sys.path.remove(_str_dir)


# ---------------------------------------------------------------------------
# Core mock fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def mock_spark():
    spark = MagicMock(name='SparkSession')
    default_df = MagicMock()
    default_df.collect.return_value = []
    default_df.count.return_value = 0
    default_df.select.return_value = default_df
    spark.sql.return_value = default_df

    spark._jsc.hadoopConfiguration.return_value = MagicMock()
    jvm = MagicMock()
    spark._jvm = jvm
    jvm.java.net.URI.side_effect = lambda uri: uri
    jvm.org.apache.hadoop.fs.Path.side_effect = lambda p: p

    fs_mock = MagicMock()
    jvm.org.apache.hadoop.fs.FileSystem.get.return_value = fs_mock
    fs_mock.exists.return_value = True
    content_summary = MagicMock()
    content_summary.getLength.return_value = 1024 * 1024 * 10
    content_summary.getFileCount.return_value = 5
    fs_mock.getContentSummary.return_value = content_summary
    fs_mock.create.return_value = MagicMock()

    spark.catalog = MagicMock()
    spark.catalog.refreshTable.return_value = None
    spark.read = MagicMock()
    return spark


@pytest.fixture
def mock_ssh_hook():
    """Patch SSHHook at both the provider path and DAG module-level references.

    Yields (hook_instance, client, stdout_mock, stderr_mock).
    """
    with patch('airflow.providers.ssh.hooks.ssh.SSHHook') as MockSSH, \
         patch('migration_dag_mapr_to_s3.SSHHook', MockSSH), \
         patch('migration_dag_folder_copy.SSHHook', MockSSH):
        hook_instance = MagicMock()
        MockSSH.return_value = hook_instance

        client = MagicMock()
        hook_instance.get_conn.return_value.__enter__ = MagicMock(return_value=client)
        hook_instance.get_conn.return_value.__exit__ = MagicMock(return_value=False)

        stdout_mock = MagicMock()
        stdout_mock.channel.recv_exit_status.return_value = 0
        stdout_mock.read.return_value = b'SSH_TEST_OK\nCLUSTER_LOGIN_SUCCESS\nTEMP_DIR=/tmp/migration/run_test\n'
        stderr_mock = MagicMock()
        stderr_mock.read.return_value = b''
        client.exec_command.return_value = (MagicMock(), stdout_mock, stderr_mock)

        sftp = MagicMock()
        client.open_sftp.return_value = sftp
        sftp.file.return_value.__enter__ = MagicMock(return_value=MagicMock())
        sftp.file.return_value.__exit__ = MagicMock(return_value=False)

        yield hook_instance, client, stdout_mock, stderr_mock


@pytest.fixture
def mock_iceberg_retry():
    """Patch execute_with_iceberg_retry across all DAG modules."""
    with patch('migration_dag_mapr_to_s3.execute_with_iceberg_retry') as retry, \
         patch('migration_dag_iceberg.execute_with_iceberg_retry', retry), \
         patch('migration_dag_folder_copy.execute_with_iceberg_retry', retry), \
         patch('migration_dag_metadata.execute_with_iceberg_retry', retry):
        yield retry


# ---------------------------------------------------------------------------
# DAG 1 sample data
# ---------------------------------------------------------------------------
@pytest.fixture
def sample_run_id():
    return 'run_20250101_120000_abcd1234'

@pytest.fixture
def sample_table_metadata():
    return [{
        'source_database': 'sales_data', 'source_table': 'transactions',
        'dest_database': 'sales_data_s3', 'dest_bucket': 's3a://test-bucket',
        'source_location': 'maprfs:///data/sales_data/transactions',
        's3_location': 's3a://test-bucket/sales_data_s3/transactions',
        'file_format': 'PARQUET',
        'schema': [
            {'name': 'id', 'type': 'bigint'},
            {'name': 'amount', 'type': 'double'},
            {'name': 'dt', 'type': 'string'},
        ],
        'partitions': ['dt=2024-01-01', 'dt=2024-01-02'],
        'partition_columns': 'dt',
        'partition_count': 2,
        'row_count': 1000,
        'is_partitioned': True,
        'unregistered_partitions': False,
        'table_type': 'EXTERNAL',
        'source_total_size_bytes': 10 * 1024 * 1024,
        'source_file_count': 5,
        'serde_properties': {},
        # --- fields added by this branch ---
        'partition_filter': None,
        'filtered_partitions': ['dt=2024-01-01', 'dt=2024-01-02'],
        'partition_filter_active': False,
        'filtered_row_count': 1000,
        'filtered_source_size_bytes': 10 * 1024 * 1024,
        'filtered_file_count': 5,
        'full_table_row_count': 1000,
        'full_table_partition_count': 2,
    }]

@pytest.fixture
def sample_discovery(sample_run_id, sample_table_metadata):
    return {
        'run_id': sample_run_id, 'source_database': 'sales_data',
        'dest_database': 'sales_data_s3', 'dest_bucket': 's3a://test-bucket',
        'tables': sample_table_metadata, '_task_duration': 12.5,
    }

@pytest.fixture
def sample_distcp_result(sample_discovery):
    return {
        **sample_discovery,
        'distcp_results': [{
            'source_database': 'sales_data', 'source_table': 'transactions',
            'dest_database': 'sales_data_s3',
            'status': 'COMPLETED', 'distcp_started_at': '2025-01-01 12:00:00',
            'distcp_completed_at': '2025-01-01 12:05:00', 'distcp_duration_secs': 300.0,
            'is_incremental': False, 'bytes_copied': 10 * 1024 * 1024, 'files_copied': 5,
            's3_total_size_bytes_before': 0, 's3_file_count_before': 0,
            's3_total_size_bytes_after': 10 * 1024 * 1024, 's3_file_count_after': 5,
            's3_bytes_transferred': 10 * 1024 * 1024, 's3_files_transferred': 5,
            'partition_filter_active': False, 'partitions_requested': None,
            'error': None,
        }],
        '_task_duration': 305.0,
    }

@pytest.fixture
def sample_table_result(sample_distcp_result):
    return {
        **sample_distcp_result,
        'table_results': [{
            'source_table': 'transactions', 'dest_database': 'sales_data_s3',
            'status': 'COMPLETED', 'action': 'created', 'existed': False, 'error': None,
        }],
        '_task_duration': 8.0,
    }

@pytest.fixture
def sample_validation_result(sample_table_result):
    return {
        **sample_table_result,
        'validation_results': [{
            'source_table': 'transactions', 'status': 'COMPLETED',
            'source_row_count': 1000, 'dest_hive_row_count': 1000,
            'source_partition_count': 2, 'dest_partition_count': 2,
            'row_count_match': True, 'partition_count_match': True,
            'schema_match': True, 'schema_differences': '', 'error': None,
        }],
        '_task_duration': 5.0,
    }

# ---------------------------------------------------------------------------
# DAG 2 (Iceberg) sample data
# ---------------------------------------------------------------------------
@pytest.fixture
def sample_iceberg_run_id():
    return 'iceberg_run_20250101_120000_abcd1234'

@pytest.fixture
def sample_iceberg_db_config(sample_iceberg_run_id):
    return {
        'source_database': 'sales_data_s3', 'table_pattern': '*',
        'inplace_migration': False, 'destination_iceberg_database': 'sales_data_s3_iceberg',
        'run_id': sample_iceberg_run_id,
    }

@pytest.fixture
def sample_iceberg_discovery(sample_iceberg_db_config):
    return {
        **sample_iceberg_db_config,
        'discovered_tables': [
            {'table': 'transactions', 'location': 's3a://test-bucket/sales_data_s3/transactions'},
            {'table': 'orders', 'location': 's3a://test-bucket/sales_data_s3/orders'},
        ],
        '_task_duration': 3.5,
    }

@pytest.fixture
def sample_iceberg_migration_result(sample_iceberg_run_id):
    return {
        'run_id': sample_iceberg_run_id, 'source_database': 'sales_data_s3',
        'destination_database': 'sales_data_s3_iceberg', 'migration_type': 'SNAPSHOT',
        'results': [{
            'source_table': 'sales_data_s3.transactions',
            'destination_table': 'sales_data_s3_iceberg.transactions',
            'migration_type': 'SNAPSHOT', 'status': 'COMPLETED',
            'hive_count': 1000, 'iceberg_count': 1000, 'counts_match': True,
            'hive_partition_count': 2, 'iceberg_partition_count': 2,
            'partition_match': True, 'error': None,
        }],
        '_task_duration': 45.0,
    }

# ---------------------------------------------------------------------------
# DAG 3 (Folder copy) sample data
# ---------------------------------------------------------------------------
@pytest.fixture
def sample_folder_run_id():
    return 'folder_run_20250101_120000_abcd1234'

@pytest.fixture
def sample_folder_config(sample_folder_run_id):
    return {
        'run_id': sample_folder_run_id,
        'source_path': '/data/sales/raw',
        'dest_bucket': 's3a://test-bucket',
        'dest_folder': 'raw',
        'dest_endpoint': '',
    }


@pytest.fixture
def sample_folder_distcp_result(sample_folder_run_id):
    return {
        'run_id': sample_folder_run_id,
        'source_path': '/data/sales/raw',
        'dest_bucket': 's3a://test-bucket',
        'dest_path': 'raw',
        'dest_endpoint': '',
        'status': 'COMPLETED',
        'started_at': '2025-01-01 12:00:00',
        'completed_at': '2025-01-01 12:05:00',
        'source_file_count': 20,
        'source_size_bytes': 50 * 1024 * 1024,
        'dest_file_count': 20,
        'dest_size_bytes': 50 * 1024 * 1024,
        'files_copied': 20,
        'bytes_copied': 50 * 1024 * 1024,
        'is_incremental': False,
        'file_count_match': True,
        'size_match': True,
        'error': None,
    }


@pytest.fixture
def sample_folder_validation_result(sample_folder_distcp_result):
    return {**sample_folder_distcp_result, 'validation_status': 'VALIDATED', 'validation_error': None}

@pytest.fixture
def sample_folder_finalize_result(sample_folder_run_id):
    return {
        'run_id': sample_folder_run_id, 'status': 'COMPLETED',
        'total_folders': 2, 'successful_folders': 2, 'failed_folders': 0,
    }

# ---------------------------------------------------------------------------
# DAG 4 sample data
# ---------------------------------------------------------------------------
@pytest.fixture
def sample_s3_run_id():
    return 's3_run_20250101_120000_abcd1234'


@pytest.fixture
def sample_s3_db_config(sample_s3_run_id):
    return {
        'source_database':  'sales_data',
        'dest_database':    'sales_data',
        'dest_s3_prefix':   's3a://dest-bucket/data',
        'table_tokens':     ['transactions'],
        'run_id':           sample_s3_run_id,
        'migration_type':   'iceberg_to_iceberg',
    }


@pytest.fixture
def sample_s3_table_metadata():
    return [{
        'source_database':        'sales_data',
        'source_table':           'transactions',
        'dest_database':          'sales_data',
        'source_location':        's3a://dest-bucket/data/sales_data/transactions',
        'dest_location':          's3a://dest-bucket/data/sales_data/transactions',
        'file_format':            'PARQUET',
        'table_type':             'ICEBERG',
        'schema':                 [{'name': 'id', 'type': 'BIGINT'}, {'name': 'amount', 'type': 'DOUBLE'}],
        'partition_columns':      'dt',
        'partition_spec_detail':  [{'source_column': 'dt', 'transform': 'identity', 'name': 'dt', 'param': None}],
        'partitions':             [],
        'partition_count':        2,
        'is_partitioned':         True,
        'source_row_count':       1000,
        'source_file_count':      5,
        'source_total_size_bytes': 10 * 1024 * 1024,
        'format_version':         '2',
    }]


@pytest.fixture
def sample_s3_discovery(sample_s3_run_id, sample_s3_table_metadata):
    return {
        'run_id':           sample_s3_run_id,
        'source_database':  'sales_data',
        'dest_database':    'sales_data',
        'dest_s3_prefix':   's3a://dest-bucket/data',
        'migration_type':   'iceberg_to_iceberg',
        'tables':           sample_s3_table_metadata,
        '_task_duration':   4.2,
    }


@pytest.fixture
def sample_s3_presence_result(sample_s3_discovery):
    return {
        **sample_s3_discovery,
        'presence_results': [{
            'source_database': 'sales_data',
            'source_table':    'transactions',
            'status':          'CONFIRMED',
            'file_count':      5,
            'size_bytes':      10 * 1024 * 1024,
            'error':           None,
        }],
    }


@pytest.fixture
def sample_s3_table_result(sample_s3_presence_result):
    return {
        **sample_s3_presence_result,
        'table_results': [{
            'source_table': 'transactions',
            'status':       'COMPLETED',
            'existed':      False,
            'error':        None,
        }],
        '_task_duration': 3.1,
        '_has_failures':  False,
    }


@pytest.fixture
def sample_s3_validation_result(sample_s3_table_result):
    return {
        **sample_s3_table_result,
        'validation_results': [{
            'source_table':          'transactions',
            'status':                'COMPLETED',
            'source_row_count':      1000,
            'dest_hive_row_count':   1000,
            'source_partition_count': 2,
            'dest_partition_count':  2,
            'row_count_match':       True,
            'partition_count_match': True,
            'schema_match':          True,
            'schema_differences':    '',
            'error':                 None,
        }],
        '_task_duration': 2.8,
        '_has_failures':  False,
    }
