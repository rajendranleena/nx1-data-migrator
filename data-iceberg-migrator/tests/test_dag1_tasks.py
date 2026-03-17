"""
DAG 1 Task Tests (Part 1): validate_prerequisites, init_tracking_tables,
create_migration_run, parse_excel, cluster_login_setup
"""

import json
import pytest
from unittest.mock import MagicMock, patch, call
from io import BytesIO


def _import_module():
    import migration_dags_combined as m  
    return m


# ---------------------------------------------------------------------------
# validate_prerequisites
# ---------------------------------------------------------------------------
class TestValidatePrerequisites:

    def test_all_checks_pass(self, mock_ssh_hook):
        m = _import_module()
        MockSSH, hook, client, stdout_mock, _ = mock_ssh_hook

        # Four exec_command calls all succeed
        def make_stdout(output_bytes):
            s = MagicMock()
            s.channel.recv_exit_status.return_value = 0
            s.read.return_value = output_bytes
            return s

        responses = [
            (MagicMock(), make_stdout(b'SSH_TEST_OK'), MagicMock()),
            (MagicMock(), make_stdout(b'PySpark 3.4.0'), MagicMock()),
            (MagicMock(), make_stdout(b'Hive 3.1.0'), MagicMock()),
            (MagicMock(), make_stdout(b'Hadoop 3.3.0\nHADOOP_FS_OK'), MagicMock()),
        ]
        for r in responses:
            r[2].read.return_value = b''
        client.exec_command.side_effect = responses

        with patch('migration_dags_combined.SSHHook', MockSSH):
            result = m.validate_prerequisites.function(run_id='test_run')

        assert result['ssh_connectivity'] is True
        assert result['pyspark_available'] is True
        assert result['hive_available'] is True
        assert result['hadoop_fs_available'] is True
        assert result['errors'] == []

    def test_ssh_failure_raises(self, mock_ssh_hook):
        m = _import_module()
        MockSSH, hook, client, _, _ = mock_ssh_hook
        hook.get_conn.side_effect = Exception("Connection refused")

        with patch('migration_dags_combined.SSHHook', MockSSH), \
             pytest.raises(Exception, match="Pre-DAG validation failed"):
            m.validate_prerequisites.function(run_id='test_run')

    def test_pyspark_missing_raises(self, mock_ssh_hook):
        m = _import_module()
        MockSSH, hook, client, _, _ = mock_ssh_hook

        def make_out(code, text):
            s = MagicMock()
            s.channel.recv_exit_status.return_value = code
            s.read.return_value = text
            e = MagicMock()
            e.read.return_value = b''
            return MagicMock(), s, e

        client.exec_command.side_effect = [
            make_out(0, b'SSH_TEST_OK'),
            make_out(1, b'pyspark: command not found'),    # pyspark fails
            make_out(0, b'Hive 3.1.0'),
            make_out(0, b'HADOOP_FS_OK'),
        ]

        with patch('migration_dags_combined.SSHHook', MockSSH), \
             pytest.raises(Exception, match="Pre-DAG validation failed"):
            m.validate_prerequisites.function(run_id='test_run')

    def test_hive_missing_raises(self, mock_ssh_hook):
        m = _import_module()
        MockSSH, hook, client, _, _ = mock_ssh_hook

        def make_out(code, text):
            s = MagicMock()
            s.channel.recv_exit_status.return_value = code
            s.read.return_value = text
            e = MagicMock()
            e.read.return_value = b''
            return MagicMock(), s, e

        client.exec_command.side_effect = [
            make_out(0, b'SSH_TEST_OK'),
            make_out(0, b'pyspark 3.4.0'),
            make_out(1, b'hive: not found'),              # hive fails
            make_out(0, b'HADOOP_FS_OK'),
        ]

        with patch('migration_dags_combined.SSHHook', MockSSH), \
             pytest.raises(Exception, match="Pre-DAG validation failed"):
            m.validate_prerequisites.function(run_id='test_run')


# ---------------------------------------------------------------------------
# init_tracking_tables
# ---------------------------------------------------------------------------
class TestInitTrackingTables:

    def test_creates_database_and_tables(self, mock_spark):
        m = _import_module()
        result = m.init_tracking_tables.function(spark=mock_spark)

        assert result['status'] == 'initialized'
        assert result['database'] == 'migration_tracking'
        # Should have called CREATE DATABASE + 3 table CREATEs
        assert mock_spark.sql.call_count >= 4

    def test_sql_contains_iceberg(self, mock_spark):
        m = _import_module()
        m.init_tracking_tables.function(spark=mock_spark)
        all_calls = [str(c) for c in mock_spark.sql.call_args_list]
        assert any('iceberg' in c.lower() for c in all_calls)

    def test_creates_migration_runs_table(self, mock_spark):
        m = _import_module()
        m.init_tracking_tables.function(spark=mock_spark)
        all_sql = ' '.join(str(c) for c in mock_spark.sql.call_args_list).lower()
        assert 'migration_runs' in all_sql

    def test_creates_table_status_table(self, mock_spark):
        m = _import_module()
        m.init_tracking_tables.function(spark=mock_spark)
        all_sql = ' '.join(str(c) for c in mock_spark.sql.call_args_list).lower()
        assert 'migration_table_status' in all_sql

    def test_creates_validation_results_table(self, mock_spark):
        m = _import_module()
        m.init_tracking_tables.function(spark=mock_spark)
        all_sql = ' '.join(str(c) for c in mock_spark.sql.call_args_list).lower()
        assert 'validation_results' in all_sql


# ---------------------------------------------------------------------------
# create_migration_run
# ---------------------------------------------------------------------------
class TestCreateMigrationRun:

    def test_returns_valid_run_id(self, mock_spark):
        m = _import_module()
        run_id = m.create_migration_run.function(
            excel_file_path='s3a://bucket/file.xlsx',
            dag_run_id='dag_run_123',
            spark=mock_spark,
        )
        assert run_id.startswith('run_')
        assert len(run_id) > 10

    def test_run_id_contains_timestamp(self, mock_spark):
        m = _import_module()
        run_id = m.create_migration_run.function(
            excel_file_path='s3a://bucket/file.xlsx',
            dag_run_id='dag_run_123',
            spark=mock_spark,
        )
        # Format: run_YYYYMMDD_HHMMSS_<uuid>
        parts = run_id.split('_')
        assert len(parts) >= 4

    def test_inserts_into_tracking_table(self, mock_spark):
        m = _import_module()
        m.create_migration_run.function(
            excel_file_path='s3a://bucket/file.xlsx',
            dag_run_id='dag_run_123',
            spark=mock_spark,
        )
        all_sql = ' '.join(str(c) for c in mock_spark.sql.call_args_list).lower()
        assert 'insert into' in all_sql
        assert 'migration_runs' in all_sql

    def test_status_is_running(self, mock_spark):
        m = _import_module()
        m.create_migration_run.function(
            excel_file_path='s3a://bucket/file.xlsx',
            dag_run_id='dag_run_123',
            spark=mock_spark,
        )
        insert_calls = [str(c) for c in mock_spark.sql.call_args_list if 'INSERT' in str(c)]
        assert any('RUNNING' in c for c in insert_calls)

    def test_unique_run_ids_per_call(self, mock_spark):
        m = _import_module()
        id1 = m.create_migration_run.function('s3a://b/f.xlsx', 'dr1', spark=mock_spark)
        id2 = m.create_migration_run.function('s3a://b/f.xlsx', 'dr2', spark=mock_spark)
        assert id1 != id2


# ---------------------------------------------------------------------------
# parse_excel
# ---------------------------------------------------------------------------
class TestParseExcel:

    def _make_excel_bytes(self, rows):
        """Create a real in-memory Excel file using openpyxl."""
        import openpyxl
        wb = openpyxl.Workbook()
        ws = wb.active
        if rows:
            ws.append(list(rows[0].keys()))
            for row in rows:
                ws.append(list(row.values()))
        buf = BytesIO()
        wb.save(buf)
        buf.seek(0)
        return buf.getvalue()

    def test_basic_parse_single_row(self, mock_spark):
        m = _import_module()
        excel_bytes = self._make_excel_bytes([
            {'database': 'sales', 'table': '*', 'dest database': 'sales_s3', 'bucket': 's3a://mybucket'},
        ])
        content_row = MagicMock()
        content_row.content = excel_bytes
        df_mock = MagicMock()
        df_mock.select.return_value.first.return_value = content_row
        mock_spark.read.format.return_value.load.return_value = df_mock

        result = m.parse_excel.function(
            excel_file_path='s3a://bucket/file.xlsx',
            run_id='run_test',
            spark=mock_spark,
        )
        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]['source_database'] == 'sales'
        assert result[0]['dest_database'] == 'sales_s3'

    def test_empty_table_defaults_to_wildcard(self, mock_spark):
        m = _import_module()
        excel_bytes = self._make_excel_bytes([
            {'database': 'mydb', 'table': None, 'dest database': None, 'bucket': None},
        ])
        content_row = MagicMock()
        content_row.content = excel_bytes
        df_mock = MagicMock()
        df_mock.select.return_value.first.return_value = content_row
        mock_spark.read.format.return_value.load.return_value = df_mock

        result = m.parse_excel.function('s3a://bucket/f.xlsx', 'run_test', spark=mock_spark)
        assert result[0]['table_tokens'] == ['*']

    def test_comma_separated_tables_tokenized(self, mock_spark):
        m = _import_module()
        excel_bytes = self._make_excel_bytes([
            {'database': 'db1', 'table': 'tbl_a,tbl_b,tbl_c', 'dest database': '', 'bucket': ''},
        ])
        content_row = MagicMock()
        content_row.content = excel_bytes
        df_mock = MagicMock()
        df_mock.select.return_value.first.return_value = content_row
        mock_spark.read.format.return_value.load.return_value = df_mock

        result = m.parse_excel.function('s3a://bucket/f.xlsx', 'run_test', spark=mock_spark)
        assert set(result[0]['table_tokens']) == {'tbl_a', 'tbl_b', 'tbl_c'}

    def test_s3_bucket_normalized(self, mock_spark):
        m = _import_module()
        excel_bytes = self._make_excel_bytes([
            {'database': 'mydb', 'table': '*', 'dest database': '', 'bucket': 's3://plain-bucket'},
        ])
        content_row = MagicMock()
        content_row.content = excel_bytes
        df_mock = MagicMock()
        df_mock.select.return_value.first.return_value = content_row
        mock_spark.read.format.return_value.load.return_value = df_mock

        result = m.parse_excel.function('s3a://bucket/f.xlsx', 'run_test', spark=mock_spark)
        assert result[0]['dest_bucket'].startswith('s3a://')

    def test_dest_database_defaults_to_source(self, mock_spark):
        m = _import_module()
        excel_bytes = self._make_excel_bytes([
            {'database': 'sourcedb', 'table': '*', 'dest database': None, 'bucket': None},
        ])
        content_row = MagicMock()
        content_row.content = excel_bytes
        df_mock = MagicMock()
        df_mock.select.return_value.first.return_value = content_row
        mock_spark.read.format.return_value.load.return_value = df_mock

        result = m.parse_excel.function('s3a://bucket/f.xlsx', 'run_test', spark=mock_spark)
        assert result[0]['dest_database'] == 'sourcedb'

    def test_wildcard_overrides_other_tokens(self, mock_spark):
        m = _import_module()
        excel_bytes = self._make_excel_bytes([
            {'database': 'db', 'table': 'tbl_a', 'dest database': '', 'bucket': ''},
            {'database': 'db', 'table': '*', 'dest database': '', 'bucket': ''},
        ])
        content_row = MagicMock()
        content_row.content = excel_bytes
        df_mock = MagicMock()
        df_mock.select.return_value.first.return_value = content_row
        mock_spark.read.format.return_value.load.return_value = df_mock

        result = m.parse_excel.function('s3a://bucket/f.xlsx', 'run_test', spark=mock_spark)
        # When * is present, tokens should collapse to ['*']
        assert result[0]['table_tokens'] == ['*']

    def test_run_id_embedded_in_each_config(self, mock_spark):
        m = _import_module()
        excel_bytes = self._make_excel_bytes([
            {'database': 'db1', 'table': '*', 'dest database': '', 'bucket': ''},
        ])
        content_row = MagicMock()
        content_row.content = excel_bytes
        df_mock = MagicMock()
        df_mock.select.return_value.first.return_value = content_row
        mock_spark.read.format.return_value.load.return_value = df_mock

        result = m.parse_excel.function('s3a://bucket/f.xlsx', 'run_xyz', spark=mock_spark)
        assert result[0]['run_id'] == 'run_xyz'


# ---------------------------------------------------------------------------
# cluster_login_setup
# ---------------------------------------------------------------------------
class TestClusterLoginSetup:

    def test_success_returns_temp_dir(self, mock_ssh_hook):
        m = _import_module()
        MockSSH, hook, client, stdout_mock, stderr_mock = mock_ssh_hook
        stdout_mock.read.return_value = b'CLUSTER_LOGIN_SUCCESS\nTEMP_DIR=/tmp/migration/run_test'
        stderr_mock.read.return_value = b''

        with patch('migration_dags_combined.SSHHook', MockSSH):
            result = m.cluster_login_setup.function(run_id='run_test')

        assert 'temp_dir' in result
        assert result['run_id'] == 'run_test'

    def test_failure_raises_exception(self, mock_ssh_hook):
        m = _import_module()
        MockSSH, hook, client, stdout_mock, stderr_mock = mock_ssh_hook
        stdout_mock.channel.recv_exit_status.return_value = 1
        stdout_mock.read.return_value = b'ERROR: authentication failed'
        stderr_mock.read.return_value = b'auth error'

        with patch('migration_dags_combined.SSHHook', MockSSH), \
             pytest.raises(Exception):
            m.cluster_login_setup.function(run_id='run_test')

    def test_missing_success_marker_raises(self, mock_ssh_hook):
        m = _import_module()
        MockSSH, hook, client, stdout_mock, stderr_mock = mock_ssh_hook
        stdout_mock.channel.recv_exit_status.return_value = 0
        stdout_mock.read.return_value = b'Something happened but no marker'
        stderr_mock.read.return_value = b''

        with patch('migration_dags_combined.SSHHook', MockSSH), \
             pytest.raises(Exception, match="success marker not found"):
            m.cluster_login_setup.function(run_id='run_test')

    def test_auth_none_script_content(self, mock_ssh_hook):
        """auth_method=none should not include kinit or maprlogin."""
        m = _import_module()
        MockSSH, hook, client, stdout_mock, stderr_mock = mock_ssh_hook
        stdout_mock.read.return_value = b'CLUSTER_LOGIN_SUCCESS\nTEMP_DIR=/tmp/test'

        with patch('migration_dags_combined.SSHHook', MockSSH):
            m.cluster_login_setup.function(run_id='run_test')

        script_executed = client.exec_command.call_args[0][0]
        assert 'auth_method=none' in script_executed or 'No authentication' in script_executed

# ---------------------------------------------------------------------------
# record_discovered_tables
# ---------------------------------------------------------------------------
class TestRecordDiscoveredTables:

    def test_inserts_new_table_record(self, mock_spark, sample_discovery):
        m = _import_module()
        # First call = COUNT → 0 (new record)
        count_row = MagicMock()
        count_row.__getitem__ = lambda self, k: 0
        count_df = MagicMock()
        count_df.collect.return_value = [count_row]
        mock_spark.sql.return_value = count_df

        with patch('migration_dags_combined.execute_with_iceberg_retry') as mock_retry:
            result = m.record_discovered_tables.function(discovery=sample_discovery, spark=mock_spark)

        # Verify INSERT was requested via retry
        insert_calls = [str(c) for c in mock_retry.call_args_list]
        assert any('INSERT INTO' in c for c in insert_calls)

    def test_updates_existing_table_record(self, mock_spark, sample_discovery):
        m = _import_module()
        count_row = MagicMock()
        count_row.__getitem__ = lambda self, k: 1  # existing record
        count_df = MagicMock()
        count_df.collect.return_value = [count_row]
        mock_spark.sql.return_value = count_df

        with patch('migration_dags_combined.execute_with_iceberg_retry') as mock_retry:
            result = m.record_discovered_tables.function(discovery=sample_discovery, spark=mock_spark)

        update_calls = [str(c) for c in mock_retry.call_args_list]
        assert any('UPDATE' in c for c in update_calls)

    def test_skips_invalid_input(self, mock_spark):
        m = _import_module()
        result = m.record_discovered_tables.function(discovery={'no_tables': True}, spark=mock_spark)
        assert result == {}

    def test_returns_original_discovery(self, mock_spark, sample_discovery):
        m = _import_module()
        count_row = MagicMock()
        count_row.__getitem__ = lambda self, k: 0
        count_df = MagicMock()
        count_df.collect.return_value = [count_row]
        mock_spark.sql.return_value = count_df

        with patch('migration_dags_combined.execute_with_iceberg_retry'):
            result = m.record_discovered_tables.function(discovery=sample_discovery, spark=mock_spark)

        assert result['run_id'] == sample_discovery['run_id']
        assert result['source_database'] == sample_discovery['source_database']


# ---------------------------------------------------------------------------
# run_distcp_ssh
# ---------------------------------------------------------------------------
class TestRunDistcpSsh:

    def _make_distcp_stdout(self, success=True, incremental=False):
        stdout = MagicMock()
        stdout.channel.recv_exit_status.return_value = 0 if success else 1
        output = (
            f"INCREMENTAL={'true' if incremental else 'false'}\n"
            "S3_FILE_COUNT_BEFORE=0\n"
            "S3_TOTAL_SIZE_BEFORE=0\n"
            "DISTCP_EXIT_CODE=0\n"
            "BYTES_COPIED=10485760\n"
            "FILES_COPIED=5\n"
            "S3_FILE_COUNT_AFTER=5\n"
            "S3_TOTAL_SIZE_AFTER=10485760\n"
            "S3_FILES_TRANSFERRED=5\n"
            "S3_BYTES_TRANSFERRED=10485760\n"
        )
        stdout.read.return_value = output.encode()
        return stdout

    def test_successful_copy(self, mock_ssh_hook, sample_discovery):
        m = _import_module()
        MockSSH, hook, client, stdout_mock, stderr_mock = mock_ssh_hook
        client.exec_command.return_value = (
            MagicMock(), self._make_distcp_stdout(success=True), MagicMock()
        )
        client.exec_command.return_value[2].read.return_value = b''

        cluster_setup = {'temp_dir': '/tmp/migration/run_test', 'run_id': 'run_test'}

        with patch('migration_dags_combined.SSHHook', MockSSH):
            result = m.run_distcp_ssh.function.__wrapped__(
                discovery=sample_discovery,
                cluster_setup=cluster_setup,
                **{'ti': MagicMock()}
            )

        assert result['distcp_results'][0]['status'] == 'COMPLETED'
        assert result['distcp_results'][0]['bytes_copied'] == 10485760

    def test_incremental_detected(self, mock_ssh_hook, sample_discovery):
        m = _import_module()
        MockSSH, hook, client, _, _ = mock_ssh_hook
        client.exec_command.return_value = (
            MagicMock(), self._make_distcp_stdout(success=True, incremental=True), MagicMock()
        )
        client.exec_command.return_value[2].read.return_value = b''
        cluster_setup = {'temp_dir': '/tmp/migration/run_test', 'run_id': 'run_test'}

        with patch('migration_dags_combined.SSHHook', MockSSH):
            result = m.run_distcp_ssh.function.__wrapped__(
                discovery=sample_discovery,
                cluster_setup=cluster_setup,
                **{'ti': MagicMock()}
            )
        assert result['distcp_results'][0]['is_incremental'] is True

    def test_distcp_failure_raises(self, mock_ssh_hook, sample_discovery):
        m = _import_module()
        MockSSH, hook, client, _, _ = mock_ssh_hook
        fail_stdout = MagicMock()
        fail_stdout.channel.recv_exit_status.return_value = 1
        fail_stdout.read.return_value = b'DISTCP_EXIT_CODE=1\n'
        fail_stderr = MagicMock()
        fail_stderr.read.return_value = b'DistCp failed: connection timeout'
        client.exec_command.return_value = (MagicMock(), fail_stdout, fail_stderr)

        cluster_setup = {'temp_dir': '/tmp/migration/run_test', 'run_id': 'run_test'}

        with patch('migration_dags_combined.SSHHook', MockSSH), \
             pytest.raises(Exception, match="DistCp failed"):
            m.run_distcp_ssh.function.__wrapped__(
                discovery=sample_discovery,
                cluster_setup=cluster_setup,
                **{'ti': MagicMock()}
            )

    def test_skips_invalid_input(self, mock_ssh_hook):
        m = _import_module()
        MockSSH, hook, client, _, _ = mock_ssh_hook
        cluster_setup = {'temp_dir': '/tmp/test', 'run_id': 'run_test'}

        with patch('migration_dags_combined.SSHHook', MockSSH):
            result = m.run_distcp_ssh.function.__wrapped__(
                discovery={'bad': 'input'},
                cluster_setup=cluster_setup,
                **{'ti': MagicMock()}
            )
        assert result == {}


# ---------------------------------------------------------------------------
# update_distcp_status
# ---------------------------------------------------------------------------
class TestUpdateDistcpStatus:

    def test_updates_completed_table(self, mock_spark, sample_distcp_result):
        m = _import_module()
        with patch('migration_dags_combined.execute_with_iceberg_retry') as mock_retry:
            result = m.update_distcp_status.function(distcp_result=sample_distcp_result, spark=mock_spark)

        update_calls = [str(c) for c in mock_retry.call_args_list if 'UPDATE' in str(c)]
        assert len(update_calls) > 0

    def test_sets_overall_status_copied(self, mock_spark, sample_distcp_result):
        m = _import_module()
        with patch('migration_dags_combined.execute_with_iceberg_retry') as mock_retry:
            m.update_distcp_status.function(distcp_result=sample_distcp_result, spark=mock_spark)

        update_calls = [str(c) for c in mock_retry.call_args_list]
        assert any('COPIED' in c for c in update_calls)

    def test_handles_failed_distcp_result(self, mock_spark, sample_distcp_result):
        m = _import_module()
        sample_distcp_result['distcp_results'][0]['status'] = 'FAILED'
        sample_distcp_result['distcp_results'][0]['error'] = 'Network error'

        with patch('migration_dags_combined.execute_with_iceberg_retry') as mock_retry:
            m.update_distcp_status.function(distcp_result=sample_distcp_result, spark=mock_spark)

        update_calls = [str(c) for c in mock_retry.call_args_list]
        assert any('FAILED' in c for c in update_calls)

    def test_skips_invalid_input(self, mock_spark):
        m = _import_module()
        result = m.update_distcp_status.function(distcp_result={}, spark=mock_spark)
        assert result == {}

    def test_returns_original_result(self, mock_spark, sample_distcp_result):
        m = _import_module()
        with patch('migration_dags_combined.execute_with_iceberg_retry'):
            result = m.update_distcp_status.function(distcp_result=sample_distcp_result, spark=mock_spark)
        assert result['run_id'] == sample_distcp_result['run_id']


# ---------------------------------------------------------------------------
# create_hive_tables
# ---------------------------------------------------------------------------
class TestCreateHiveTables:

    def test_creates_new_table(self, mock_spark, sample_distcp_result):
        m = _import_module()
        # Table doesn't exist → DESCRIBE raises
        mock_spark.sql.side_effect = [
            None,                            # CREATE DATABASE
            Exception("Table not found"),   # DESCRIBE (table doesn't exist)
            None,                            # CREATE EXTERNAL TABLE
            None,                            # MSCK REPAIR TABLE
            None,                            # REFRESH TABLE
        ]

        result = m.create_hive_tables.function.__wrapped__(
            distcp_result=sample_distcp_result,
            spark=mock_spark,
            **{'ti': MagicMock()}
        )
        assert result['table_results'][0]['status'] == 'COMPLETED'
        assert result['table_results'][0]['existed'] is False

    def test_repairs_existing_table(self, mock_spark, sample_distcp_result):
        m = _import_module()
        desc_df = MagicMock()
        desc_df.collect.return_value = []
        mock_spark.sql.return_value = desc_df  # DESCRIBE succeeds → table exists

        result = m.create_hive_tables.function.__wrapped__(
            distcp_result=sample_distcp_result,
            spark=mock_spark,
            **{'ti': MagicMock()}
        )
        assert result['table_results'][0]['existed'] is True

    def test_skips_table_on_distcp_failure(self, mock_spark, sample_distcp_result):
        m = _import_module()
        sample_distcp_result['distcp_results'][0]['status'] = 'FAILED'
        desc_df = MagicMock()
        mock_spark.sql.return_value = desc_df

        result = m.create_hive_tables.function.__wrapped__(
            distcp_result=sample_distcp_result,
            spark=mock_spark,
            **{'ti': MagicMock()}
        )
        assert result['table_results'][0]['status'] == 'SKIPPED'

    def test_handles_invalid_input(self, mock_spark):
        m = _import_module()
        result = m.create_hive_tables.function.__wrapped__(
            distcp_result={'no_tables': True},
            spark=mock_spark,
            **{'ti': MagicMock()}
        )
        assert result == {}


# ---------------------------------------------------------------------------
# update_table_create_status
# ---------------------------------------------------------------------------
class TestUpdateTableCreateStatus:

    def test_updates_tracking_for_completed(self, mock_spark, sample_table_result):
        m = _import_module()
        with patch('migration_dags_combined.execute_with_iceberg_retry') as mock_retry:
            m.update_table_create_status.function(table_result=sample_table_result, spark=mock_spark)

        update_calls = [str(c) for c in mock_retry.call_args_list if 'UPDATE' in str(c)]
        assert len(update_calls) > 0

    def test_sets_status_table_created(self, mock_spark, sample_table_result):
        m = _import_module()
        with patch('migration_dags_combined.execute_with_iceberg_retry') as mock_retry:
            m.update_table_create_status.function(table_result=sample_table_result, spark=mock_spark)

        update_calls = [str(c) for c in mock_retry.call_args_list]
        assert any('TABLE_CREATED' in c for c in update_calls)

    def test_skips_invalid_input(self, mock_spark):
        m = _import_module()
        result = m.update_table_create_status.function(table_result={}, spark=mock_spark)
        assert result == {}


# ---------------------------------------------------------------------------
# validate_destination_tables
# ---------------------------------------------------------------------------
class TestValidateDestinationTables:

    def _make_tracking_row(self, distcp_status='COMPLETED', create_status='COMPLETED'):
        row = MagicMock()
        row.__getitem__ = lambda self, k: {
            'distcp_status': distcp_status,
            'table_create_status': create_status,
            'overall_status': 'TABLE_CREATED',
            'error_message': None,
        }[k]
        return row

    def test_validation_passes_matching_counts(self, mock_spark, sample_table_result):
        m = _import_module()

        def sql_router(sql):
            df = MagicMock()
            sql_lower = sql.strip().lower()
            if 'distcp_status' in sql_lower:
                r = self._make_tracking_row()
                df.collect.return_value = [r]
            elif 'source_row_count' in sql_lower:
                r = MagicMock()
                r.__getitem__ = lambda self, k: {'source_row_count': 1000, 'source_partition_count': 2}[k]
                df.collect.return_value = [r]
            elif 'count(*)' in sql_lower and 'as c' in sql_lower:
                r = MagicMock()
                r.__getitem__ = lambda self, k: 1000
                df.collect.return_value = [r]
            elif 'show partitions' in sql_lower:
                df.count.return_value = 2
                df.collect.return_value = [MagicMock(), MagicMock()]
            elif 'describe' in sql_lower:
                rows = [
                    MagicMock(col_name='id', data_type='bigint'),
                    MagicMock(col_name='amount', data_type='double'),
                    MagicMock(col_name='dt', data_type='string'),
                ]
                df.collect.return_value = rows
            else:
                df.collect.return_value = []
            return df

        mock_spark.sql.side_effect = sql_router

        result = m.validate_destination_tables.function.__wrapped__(
            source_validation=sample_table_result,
            spark=mock_spark,
            **{'ti': MagicMock()}
        )
        vr = result['validation_results'][0]
        assert vr['status'] == 'COMPLETED'
        assert vr['row_count_match'] is True
        assert vr['partition_count_match'] is True
        assert vr['schema_match'] is True

    def test_validation_fails_row_count_mismatch(self, mock_spark, sample_table_result):
        m = _import_module()

        def sql_router(sql):
            df = MagicMock()
            sql_lower = sql.strip().lower()
            if 'distcp_status' in sql_lower:
                df.collect.return_value = [self._make_tracking_row()]
            elif 'source_row_count' in sql_lower:
                r = MagicMock()
                r.__getitem__ = lambda self, k: {'source_row_count': 1000, 'source_partition_count': 2}[k]
                df.collect.return_value = [r]
            elif 'count(*)' in sql_lower and 'as c' in sql_lower:
                r = MagicMock()
                r.__getitem__ = lambda self, k: 500  # Mismatch!
                df.collect.return_value = [r]
            elif 'show partitions' in sql_lower:
                df.count.return_value = 2
            elif 'describe' in sql_lower:
                df.collect.return_value = []
            else:
                df.collect.return_value = []
            return df

        mock_spark.sql.side_effect = sql_router

        result = m.validate_destination_tables.function.__wrapped__(
            source_validation=sample_table_result,
            spark=mock_spark,
            **{'ti': MagicMock()}
        )
        assert result['validation_results'][0]['row_count_match'] is False

    def test_skips_table_on_upstream_failure(self, mock_spark, sample_table_result):
        m = _import_module()
        failed_row = self._make_tracking_row(distcp_status='FAILED')
        df = MagicMock()
        df.collect.return_value = [failed_row]
        mock_spark.sql.return_value = df

        result = m.validate_destination_tables.function.__wrapped__(
            source_validation=sample_table_result,
            spark=mock_spark,
            **{'ti': MagicMock()}
        )
        assert result['validation_results'][0]['status'] == 'SKIPPED'

    def test_skips_invalid_input(self, mock_spark):
        m = _import_module()
        result = m.validate_destination_tables.function.__wrapped__(
            source_validation={'no_tables': True},
            spark=mock_spark,
            **{'ti': MagicMock()}
        )
        assert result == {}


# ---------------------------------------------------------------------------
# update_validation_status
# ---------------------------------------------------------------------------
class TestUpdateValidationStatus:

    def test_sets_validated_status(self, mock_spark, sample_validation_result):
        m = _import_module()
        # file metrics query
        metrics_row = MagicMock()
        metrics_row.__getitem__ = lambda self, k: 0
        metrics_df = MagicMock()
        metrics_df.collect.return_value = [metrics_row]
        mock_spark.sql.return_value = metrics_df

        with patch('migration_dags_combined.execute_with_iceberg_retry') as mock_retry:
            m.update_validation_status.function(validation_result=sample_validation_result, spark=mock_spark)

        update_calls = [str(c) for c in mock_retry.call_args_list]
        assert any('VALIDATED' in c for c in update_calls)

    def test_sets_validation_failed_on_mismatch(self, mock_spark, sample_validation_result):
        m = _import_module()
        sample_validation_result['validation_results'][0]['row_count_match'] = False

        metrics_row = MagicMock()
        metrics_row.__getitem__ = lambda self, k: 0
        metrics_df = MagicMock()
        metrics_df.collect.return_value = [metrics_row]
        mock_spark.sql.return_value = metrics_df

        with patch('migration_dags_combined.execute_with_iceberg_retry') as mock_retry:
            m.update_validation_status.function(validation_result=sample_validation_result, spark=mock_spark)

        update_calls = [str(c) for c in mock_retry.call_args_list]
        assert any('VALIDATION_FAILED' in c for c in update_calls)

    def test_skips_invalid_input(self, mock_spark):
        m = _import_module()
        result = m.update_validation_status.function(validation_result={}, spark=mock_spark)
        assert result == {}


# ---------------------------------------------------------------------------
# generate_html_report
# ---------------------------------------------------------------------------
class TestGenerateHtmlReport:

    def _setup_spark_for_report(self, mock_spark, sample_run_id):
        """Configure mock spark to return realistic data for HTML report generation."""
        # run_info row
        run_row = MagicMock()
        run_row.dag_run_id = 'dag_run_test'

        # table_status rows
        tbl_row = MagicMock()
        tbl_row.source_database = 'sales_data'
        tbl_row.source_table = 'transactions'
        tbl_row.overall_status = 'VALIDATED'
        tbl_row.discovery_duration_seconds = 12.0
        tbl_row.distcp_duration_seconds = 300.0
        tbl_row.distcp_bytes_copied = 10 * 1024 * 1024
        tbl_row.distcp_files_copied = 5
        tbl_row.distcp_is_incremental = False
        tbl_row.table_create_duration_seconds = 8.0
        tbl_row.validation_duration_seconds = 5.0
        tbl_row.validation_status = 'COMPLETED'
        tbl_row.row_count_match = True
        tbl_row.partition_count_match = True
        tbl_row.schema_match = True
        tbl_row.source_row_count = 1000
        tbl_row.dest_hive_row_count = 1000
        tbl_row.source_partition_count = 2
        tbl_row.dest_partition_count = 2
        tbl_row.source_total_size_bytes = 10 * 1024 * 1024
        tbl_row.s3_total_size_bytes_before = 0
        tbl_row.s3_total_size_bytes_after = 10 * 1024 * 1024
        tbl_row.s3_bytes_transferred = 10 * 1024 * 1024
        tbl_row.file_size_match = True
        tbl_row.source_file_count = 5
        tbl_row.s3_file_count_before = 0
        tbl_row.s3_file_count_after = 5
        tbl_row.s3_files_transferred = 5
        tbl_row.file_count_match = True
        tbl_row.distcp_status = 'COMPLETED'

        # validation summary row
        vs_row = MagicMock()
        vs_row.__getitem__ = lambda self, k: {'total_tables_validated': 1}[k] if k == 'total_tables_validated' else 1
        vs_row.total_tables_validated = 1
        vs_row.tables_passed_validation = 1
        vs_row.tables_failed_validation = 0
        vs_row.total_row_count_mismatches = 0
        vs_row.total_partition_count_mismatches = 0
        vs_row.total_schema_mismatches = 0

        call_count = [0]

        def sql_router(sql):
            df = MagicMock()
            sql_lower = sql.lower()
            if 'migration_runs' in sql_lower and 'where' in sql_lower:
                df.collect.return_value = [run_row]
            elif 'migration_table_status' in sql_lower and 'order by' in sql_lower:
                df.collect.return_value = [tbl_row]
            elif 'sum(case when row_count_match' in sql_lower:
                df.collect.return_value = [vs_row]
            else:
                df.collect.return_value = []
            return df

        mock_spark.sql.side_effect = sql_router

    def test_generates_html_with_run_id(self, mock_spark, sample_run_id):
        m = _import_module()
        self._setup_spark_for_report(mock_spark, sample_run_id)

        result = m.generate_html_report.function(run_id=sample_run_id, spark=mock_spark)
        assert 'report_path' in result
        assert sample_run_id in result['report_path']

    def test_report_written_to_s3(self, mock_spark, sample_run_id):
        m = _import_module()
        self._setup_spark_for_report(mock_spark, sample_run_id)

        m.generate_html_report.function(run_id=sample_run_id, spark=mock_spark)

        # Verify the output stream was written to
        fs_mock = mock_spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value
        assert fs_mock.create.called
        out_stream = fs_mock.create.return_value
        assert out_stream.write.called

    def test_report_path_contains_html_extension(self, mock_spark, sample_run_id):
        m = _import_module()
        self._setup_spark_for_report(mock_spark, sample_run_id)

        result = m.generate_html_report.function(run_id=sample_run_id, spark=mock_spark)
        assert result['report_path'].endswith('.html')


# ---------------------------------------------------------------------------
# send_migration_report_email
# ---------------------------------------------------------------------------
class TestSendMigrationReportEmail:

    def test_skips_when_no_recipients(self, mock_spark, sample_run_id):
        m = _import_module()
        with patch('airflow.models.Variable.get', return_value=''):
            # Reload to pick up empty recipients
            result = m.send_migration_report_email.function(
                report_result={'report_path': 's3a://bucket/report.html'},
                run_id=sample_run_id,
                spark=mock_spark,
            )
        assert result['sent'] is False
        assert result['reason'] == 'no_recipients'

    def test_sends_email_to_configured_recipients(self, mock_spark, sample_run_id):
        m = _import_module()

        # Mock S3 read
        reader_mock = MagicMock()
        reader_mock.readLine.side_effect = ['<html>test</html>', None]
        mock_spark._jvm.java.io.BufferedReader.return_value = reader_mock

        with patch('airflow.utils.email.send_email') as mock_send, \
             patch('builtins.open', create=True), \
             patch('tempfile.NamedTemporaryFile') as mock_tmp:
            mock_tmp_instance = MagicMock()
            mock_tmp_instance.name = '/tmp/report.html'
            mock_tmp.return_value.__enter__ = MagicMock(return_value=mock_tmp_instance)
            mock_tmp.return_value.__exit__ = MagicMock(return_value=False)

            with patch('os.unlink'):
                result = m.send_migration_report_email.function(
                    report_result={'report_path': 's3a://bucket/report.html'},
                    run_id=sample_run_id,
                    spark=mock_spark,
                )

        assert result['sent'] is True
        assert 'recipients' in result


# ---------------------------------------------------------------------------
# finalize_run
# ---------------------------------------------------------------------------
class TestFinalizeRun:

    def test_updates_migration_runs_table(self, mock_spark, sample_run_id):
        m = _import_module()
        stats_row = MagicMock()
        stats_row.__getitem__ = lambda self, k: 5
        df = MagicMock()
        df.collect.return_value = [stats_row]
        mock_spark.sql.return_value = df

        m.finalize_run.function(run_id=sample_run_id, spark=mock_spark)
        sql_calls = [str(c) for c in mock_spark.sql.call_args_list]
        assert any('COMPLETED' in c for c in sql_calls)
        assert any('migration_runs' in c.lower() for c in sql_calls)

