"""
DAG 3 Task Tests: init_folder_copy_tracking_tables, create_data_copy_run,
parse_folder_copy_excel, run_folder_distcp_ssh, record_data_copy_status,
validate_data_copy, update_data_copy_validation, finalize_data_copy_run,
generate_data_copy_html_report, send_data_copy_report_email
"""

import json
import pytest
from io import BytesIO
from unittest.mock import MagicMock, patch, call


def _import_module():
    import migration_dags_combined as m
    return m


# ---------------------------------------------------------------------------
# Helper: build a minimal in-memory Excel file with given rows
# ---------------------------------------------------------------------------
def _make_excel_bytes(rows: list[dict]) -> bytes:
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


# ---------------------------------------------------------------------------
# Helper: build a mock Spark whose binaryFile read returns given bytes
# ---------------------------------------------------------------------------
def _spark_with_excel(mock_spark, excel_bytes: bytes):
    content_row = MagicMock()
    content_row.content = excel_bytes
    binary_df = MagicMock()
    binary_df.select.return_value.first.return_value = content_row
    mock_spark.read.format.return_value.load.return_value = binary_df
    return mock_spark


# ---------------------------------------------------------------------------
# Helper: build a mock SSH command result
# ---------------------------------------------------------------------------
def _make_ssh_stdout(exit_code: int, output: bytes):
    s = MagicMock()
    s.channel.recv_exit_status.return_value = exit_code
    s.read.return_value = output
    e = MagicMock()
    e.read.return_value = b''
    return MagicMock(), s, e


# ===========================================================================
# init_folder_copy_tracking_tables
# ===========================================================================
class TestInitFolderCopyTrackingTables:

    def test_returns_initialized_status(self, mock_spark):
        m = _import_module()
        result = m.init_folder_copy_tracking_tables.function(spark=mock_spark)
        assert result['status'] == 'initialized'
        assert result['database'] == 'migration_tracking'

    def test_creates_data_copy_runs_table(self, mock_spark):
        m = _import_module()
        m.init_folder_copy_tracking_tables.function(spark=mock_spark)
        sql_calls = ' '.join(str(c) for c in mock_spark.sql.call_args_list).lower()
        assert 'data_copy_runs' in sql_calls

    def test_creates_data_copy_status_table(self, mock_spark):
        m = _import_module()
        m.init_folder_copy_tracking_tables.function(spark=mock_spark)
        sql_calls = ' '.join(str(c) for c in mock_spark.sql.call_args_list).lower()
        assert 'data_copy_status' in sql_calls

    def test_creates_database_first(self, mock_spark):
        m = _import_module()
        m.init_folder_copy_tracking_tables.function(spark=mock_spark)
        first_call = str(mock_spark.sql.call_args_list[0]).lower()
        assert 'create database' in first_call

    def test_uses_iceberg_format(self, mock_spark):
        m = _import_module()
        m.init_folder_copy_tracking_tables.function(spark=mock_spark)
        sql_calls = ' '.join(str(c) for c in mock_spark.sql.call_args_list).lower()
        assert 'using iceberg' in sql_calls

    def test_sql_called_at_least_three_times(self, mock_spark):
        m = _import_module()
        m.init_folder_copy_tracking_tables.function(spark=mock_spark)
        assert mock_spark.sql.call_count >= 3


# ===========================================================================
# create_data_copy_run
# ===========================================================================
class TestCreateDataCopyRun:

    def test_returns_folder_run_id_prefix(self, mock_spark):
        m = _import_module()
        run_id = m.create_data_copy_run.function(
            excel_file_path='s3a://bucket/folder_copy.xlsx',
            spark=mock_spark,
        )
        assert run_id.startswith('folder_run_')

    def test_inserts_running_status(self, mock_spark):
        m = _import_module()
        m.create_data_copy_run.function(
            excel_file_path='s3a://bucket/folder_copy.xlsx',
            spark=mock_spark,
        )
        sql_calls = ' '.join(str(c) for c in mock_spark.sql.call_args_list)
        assert 'RUNNING' in sql_calls

    def test_inserts_excel_path(self, mock_spark):
        m = _import_module()
        m.create_data_copy_run.function(
            excel_file_path='s3a://bucket/folder_copy.xlsx',
            spark=mock_spark,
        )
        sql_calls = ' '.join(str(c) for c in mock_spark.sql.call_args_list)
        assert 'folder_copy.xlsx' in sql_calls

    def test_unique_run_ids_each_call(self, mock_spark):
        m = _import_module()
        id1 = m.create_data_copy_run.function('s3a://b/f.xlsx', spark=mock_spark)
        id2 = m.create_data_copy_run.function('s3a://b/f.xlsx', spark=mock_spark)
        assert id1 != id2

    def test_run_id_is_string(self, mock_spark):
        m = _import_module()
        run_id = m.create_data_copy_run.function('s3a://b/f.xlsx', spark=mock_spark)
        assert isinstance(run_id, str)
        assert len(run_id) > 0


# ===========================================================================
# parse_folder_copy_excel
# ===========================================================================
class TestParseFolderCopyExcel:

    def test_basic_row_parsing(self, mock_spark, sample_folder_run_id):
        m = _import_module()
        excel_bytes = _make_excel_bytes([
            {'source_path': '/data/sales', 'target_bucket': 'my-bucket', 'dest_folder': 'sales_copy'},
        ])
        spark = _spark_with_excel(mock_spark, excel_bytes)
        configs = m.parse_folder_copy_excel.function(
            excel_file_path='s3a://bucket/fc.xlsx',
            run_id=sample_folder_run_id,
            spark=spark,
        )
        assert len(configs) == 1
        assert configs[0]['source_path'] == '/data/sales'
        assert configs[0]['dest_bucket'] == 's3a://my-bucket'
        assert configs[0]['dest_folder'] == 'sales_copy'
        assert configs[0]['run_id'] == sample_folder_run_id

    def test_normalizes_s3_bucket_prefix(self, mock_spark, sample_folder_run_id):
        m = _import_module()
        for raw, expected in [
            ('s3://bucket', 's3a://bucket'),
            ('s3n://bucket', 's3a://bucket'),
            ('bucket',       's3a://bucket'),
            ('s3a://bucket', 's3a://bucket'),
        ]:
            excel_bytes = _make_excel_bytes([
                {'source_path': '/data/x', 'target_bucket': raw, 'dest_folder': 'out'},
            ])
            spark = _spark_with_excel(mock_spark, excel_bytes)
            configs = m.parse_folder_copy_excel.function(
                's3a://b/f.xlsx', sample_folder_run_id, spark=spark,
            )
            assert configs[0]['dest_bucket'] == expected, f"failed for raw={raw!r}"

    def test_dest_folder_defaults_to_basename_of_source(self, mock_spark, sample_folder_run_id):
        m = _import_module()
        excel_bytes = _make_excel_bytes([
            {'source_path': '/data/warehouse/events', 'target_bucket': 's3a://bkt'},
        ])
        spark = _spark_with_excel(mock_spark, excel_bytes)
        configs = m.parse_folder_copy_excel.function(
            's3a://b/f.xlsx', sample_folder_run_id, spark=spark,
        )
        assert configs[0]['dest_folder'] == 'events'

    def test_skips_rows_with_empty_source_path(self, mock_spark, sample_folder_run_id):
        import pandas as pd
        m = _import_module()
        fake_df = pd.DataFrame([
            {'source_path': None,         'target_bucket': 's3a://bkt', 'dest_folder': 'x'},
            {'source_path': '/data/good', 'target_bucket': 's3a://bkt', 'dest_folder': 'good'},
        ])
        excel_bytes = _make_excel_bytes([{'source_path': '', 'target_bucket': '', 'dest_folder': ''}])
        spark = _spark_with_excel(mock_spark, excel_bytes)
        with patch('pandas.read_excel', return_value=fake_df):
            configs = m.parse_folder_copy_excel.function(
                's3a://b/f.xlsx', sample_folder_run_id, spark=spark,
            )
        assert len(configs) == 1
        assert configs[0]['source_path'] == '/data/good'

    def test_skips_rows_with_missing_target_bucket(self, mock_spark, sample_folder_run_id):
        m = _import_module()
        fake_df = pd.DataFrame([
            {'source_path': '/data/no_bucket',  'target_bucket': None,        'dest_folder': 'x'},
            {'source_path': '/data/with_bucket', 'target_bucket': 's3a://bkt', 'dest_folder': 'y'},
        ])
        excel_bytes = _make_excel_bytes([{'source_path': '', 'target_bucket': '', 'dest_folder': ''}])
        spark = _spark_with_excel(mock_spark, excel_bytes)
        with patch('pandas.read_excel', return_value=fake_df):
            configs = m.parse_folder_copy_excel.function(
                's3a://b/f.xlsx', sample_folder_run_id, spark=spark,
            )
        assert len(configs) == 1
        assert configs[0]['source_path'] == '/data/with_bucket'

    def test_raises_when_no_valid_rows(self, mock_spark, sample_folder_run_id):
        m = _import_module()
        excel_bytes = _make_excel_bytes([
            {'source_path': '', 'target_bucket': '', 'dest_folder': ''},
        ])
        spark = _spark_with_excel(mock_spark, excel_bytes)
        with pytest.raises(ValueError, match="No valid rows"):
            m.parse_folder_copy_excel.function(
                's3a://b/f.xlsx', sample_folder_run_id, spark=spark,
            )

    def test_multiple_rows_all_parsed(self, mock_spark, sample_folder_run_id):
        m = _import_module()
        excel_bytes = _make_excel_bytes([
            {'source_path': '/data/a', 'target_bucket': 's3a://bkt', 'dest_folder': 'a'},
            {'source_path': '/data/b', 'target_bucket': 's3a://bkt', 'dest_folder': 'b'},
            {'source_path': '/data/c', 'target_bucket': 's3a://bkt', 'dest_folder': 'c'},
        ])
        spark = _spark_with_excel(mock_spark, excel_bytes)
        configs = m.parse_folder_copy_excel.function(
            's3a://b/f.xlsx', sample_folder_run_id, spark=spark,
        )
        assert len(configs) == 3

    def test_trailing_slash_stripped_from_source_path_basename(self, mock_spark, sample_folder_run_id):
        m = _import_module()
        excel_bytes = _make_excel_bytes([
            {'source_path': '/data/mydir/', 'target_bucket': 's3a://bkt'},
        ])
        spark = _spark_with_excel(mock_spark, excel_bytes)
        configs = m.parse_folder_copy_excel.function(
            's3a://b/f.xlsx', sample_folder_run_id, spark=spark,
        )
        assert configs[0]['dest_folder'] == 'mydir'


# ===========================================================================
# run_folder_distcp_ssh
# ===========================================================================
class TestRunFolderDistcpSsh:

    def _make_success_output(self, incr=False, src_files=20, src_size=52428800,
                              s3_before_files=0, s3_before_size=0,
                              s3_after_files=20, s3_after_size=52428800):
        lines = [
            f"INCREMENTAL={'true' if incr else 'false'}",
            f"SRC_FILE_COUNT={src_files}",
            f"SRC_TOTAL_SIZE={src_size}",
            f"S3_FILE_COUNT_BEFORE={s3_before_files}",
            f"S3_TOTAL_SIZE_BEFORE={s3_before_size}",
            "DISTCP_EXIT_CODE=0",
            f"S3_FILE_COUNT_AFTER={s3_after_files}",
            f"S3_TOTAL_SIZE_AFTER={s3_after_size}",
        ]
        return '\n'.join(lines).encode()

    def test_successful_copy_returns_completed_status(self, mock_ssh_hook, sample_folder_config):
        m = _import_module()
        MockSSH, hook, client, stdout_mock, _ = mock_ssh_hook
        stdout_mock.read.return_value = self._make_success_output()
        stdout_mock.channel.recv_exit_status.return_value = 0

        with patch('migration_dags_combined.SSHHook', MockSSH):
            result = m.run_folder_distcp_ssh.function(folder_config=sample_folder_config)

        assert result['status'] == 'COMPLETED'
        assert result['error'] is None
        assert result['run_id'] == sample_folder_config['run_id']

    def test_returns_correct_source_metrics(self, mock_ssh_hook, sample_folder_config):
        m = _import_module()
        MockSSH, hook, client, stdout_mock, _ = mock_ssh_hook
        stdout_mock.read.return_value = self._make_success_output(src_files=42, src_size=1_000_000)
        stdout_mock.channel.recv_exit_status.return_value = 0

        with patch('migration_dags_combined.SSHHook', MockSSH):
            result = m.run_folder_distcp_ssh.function(folder_config=sample_folder_config)

        assert result['source_file_count'] == 42
        assert result['source_size_bytes'] == 1_000_000

    def test_file_count_match_flag_set_correctly(self, mock_ssh_hook, sample_folder_config):
        m = _import_module()
        MockSSH, hook, client, stdout_mock, _ = mock_ssh_hook
        stdout_mock.read.return_value = self._make_success_output(src_files=20, s3_after_files=20)
        stdout_mock.channel.recv_exit_status.return_value = 0

        with patch('migration_dags_combined.SSHHook', MockSSH):
            result = m.run_folder_distcp_ssh.function(folder_config=sample_folder_config)

        assert result['file_count_match'] is True

    def test_file_count_mismatch_flag_set_false(self, mock_ssh_hook, sample_folder_config):
        m = _import_module()
        MockSSH, hook, client, stdout_mock, _ = mock_ssh_hook
        stdout_mock.read.return_value = self._make_success_output(src_files=20, s3_after_files=18)
        stdout_mock.channel.recv_exit_status.return_value = 0

        with patch('migration_dags_combined.SSHHook', MockSSH):
            result = m.run_folder_distcp_ssh.function(folder_config=sample_folder_config)

        assert result['file_count_match'] is False

    def test_incremental_flag_detected(self, mock_ssh_hook, sample_folder_config):
        m = _import_module()
        MockSSH, hook, client, stdout_mock, _ = mock_ssh_hook
        stdout_mock.read.return_value = self._make_success_output(incr=True)
        stdout_mock.channel.recv_exit_status.return_value = 0

        with patch('migration_dags_combined.SSHHook', MockSSH):
            result = m.run_folder_distcp_ssh.function(folder_config=sample_folder_config)

        assert result['is_incremental'] is True

    def test_distcp_failure_returns_failed_status(self, mock_ssh_hook, sample_folder_config):
        m = _import_module()
        MockSSH, hook, client, _, _ = mock_ssh_hook
        stdout_fail = MagicMock()
        stdout_fail.channel.recv_exit_status.return_value = 1
        stdout_fail.read.return_value = b'DISTCP_EXIT_CODE=1\nsome error\n'
        stderr_fail = MagicMock()
        stderr_fail.read.return_value = b'DistCp failed'
        client.exec_command.return_value = (MagicMock(), stdout_fail, stderr_fail)

        with patch('migration_dags_combined.SSHHook', MockSSH):
            result = m.run_folder_distcp_ssh.function(folder_config=sample_folder_config)

        assert result['status'] == 'FAILED'
        assert result['error'] is not None

    def test_ssh_exception_returns_failed_status(self, mock_ssh_hook, sample_folder_config):
        m = _import_module()
        MockSSH, hook, client, _, _ = mock_ssh_hook
        hook.get_conn.side_effect = Exception("SSH timeout")

        with patch('migration_dags_combined.SSHHook', MockSSH):
            result = m.run_folder_distcp_ssh.function(folder_config=sample_folder_config)

        assert result['status'] == 'FAILED'
        assert 'SSH timeout' in result['error']

    def test_result_contains_dest_info(self, mock_ssh_hook, sample_folder_config):
        m = _import_module()
        MockSSH, hook, client, stdout_mock, _ = mock_ssh_hook
        stdout_mock.read.return_value = self._make_success_output()
        stdout_mock.channel.recv_exit_status.return_value = 0

        with patch('migration_dags_combined.SSHHook', MockSSH):
            result = m.run_folder_distcp_ssh.function(folder_config=sample_folder_config)

        assert result['dest_bucket'] == sample_folder_config['dest_bucket']
        assert result['dest_path'] == sample_folder_config['dest_folder']
        assert result['source_path'] == sample_folder_config['source_path']

    def test_bytes_and_files_copied_computed(self, mock_ssh_hook, sample_folder_config):
        m = _import_module()
        MockSSH, hook, client, stdout_mock, _ = mock_ssh_hook
        stdout_mock.read.return_value = self._make_success_output(
            s3_before_files=0, s3_before_size=0,
            s3_after_files=20, s3_after_size=1_000_000,
        )
        stdout_mock.channel.recv_exit_status.return_value = 0

        with patch('migration_dags_combined.SSHHook', MockSSH):
            result = m.run_folder_distcp_ssh.function(folder_config=sample_folder_config)

        assert result['files_copied'] == 20
        assert result['bytes_copied'] == 1_000_000

    def test_started_and_completed_at_present(self, mock_ssh_hook, sample_folder_config):
        m = _import_module()
        MockSSH, hook, client, stdout_mock, _ = mock_ssh_hook
        stdout_mock.read.return_value = self._make_success_output()
        stdout_mock.channel.recv_exit_status.return_value = 0

        with patch('migration_dags_combined.SSHHook', MockSSH):
            result = m.run_folder_distcp_ssh.function(folder_config=sample_folder_config)

        assert 'started_at' in result
        assert 'completed_at' in result


# ===========================================================================
# record_data_copy_status
# ===========================================================================
class TestRecordDataCopyStatus:

    def test_inserts_completed_status(self, mock_spark, sample_folder_distcp_result):
        m = _import_module()
        with patch('migration_dags_combined.execute_with_iceberg_retry') as mock_retry:
            m.record_data_copy_status.function(
                distcp_result=sample_folder_distcp_result,
                spark=mock_spark,
            )
        mock_retry.assert_called_once()
        insert_sql = mock_retry.call_args[0][1]
        assert 'COMPLETED' in insert_sql

    def test_inserts_source_path(self, mock_spark, sample_folder_distcp_result):
        m = _import_module()
        with patch('migration_dags_combined.execute_with_iceberg_retry') as mock_retry:
            m.record_data_copy_status.function(
                distcp_result=sample_folder_distcp_result,
                spark=mock_spark,
            )
        insert_sql = mock_retry.call_args[0][1]
        assert '/data/sales/raw' in insert_sql

    def test_inserts_dest_bucket(self, mock_spark, sample_folder_distcp_result):
        m = _import_module()
        with patch('migration_dags_combined.execute_with_iceberg_retry') as mock_retry:
            m.record_data_copy_status.function(
                distcp_result=sample_folder_distcp_result,
                spark=mock_spark,
            )
        insert_sql = mock_retry.call_args[0][1]
        assert 's3a://test-bucket' in insert_sql

    def test_returns_distcp_result_unchanged(self, mock_spark, sample_folder_distcp_result):
        m = _import_module()
        with patch('migration_dags_combined.execute_with_iceberg_retry'):
            result = m.record_data_copy_status.function(
                distcp_result=sample_folder_distcp_result,
                spark=mock_spark,
            )
        assert result['source_path'] == sample_folder_distcp_result['source_path']
        assert result['status'] == 'COMPLETED'

    def test_inserts_failed_status_when_copy_failed(self, mock_spark, sample_folder_run_id):
        m = _import_module()
        failed_result = {
            'run_id': sample_folder_run_id,
            'source_path': '/data/bad',
            'dest_bucket': 's3a://test-bucket',
            'dest_path': 'bad',
            'status': 'FAILED',
            'started_at': '2025-01-01 12:00:00',
            'completed_at': '2025-01-01 12:01:00',
            'source_file_count': 0, 'source_size_bytes': 0,
            'dest_file_count': 0, 'dest_size_bytes': 0,
            'files_copied': 0, 'bytes_copied': 0,
            'is_incremental': False,
            'file_count_match': False, 'size_match': False,
            'error': 'DistCp exit code 1',
        }
        with patch('migration_dags_combined.execute_with_iceberg_retry') as mock_retry:
            m.record_data_copy_status.function(
                distcp_result=failed_result, spark=mock_spark,
            )
        insert_sql = mock_retry.call_args[0][1]
        assert 'FAILED' in insert_sql


# ===========================================================================
# validate_data_copy
# ===========================================================================
class TestValidateDataCopy:

    def test_validated_when_dest_exists_and_counts_match(
        self, mock_ssh_hook, sample_folder_distcp_result
    ):
        m = _import_module()
        MockSSH, hook, client, stdout_mock, _ = mock_ssh_hook
        stdout_mock.read.return_value = (
            b"DEST_EXISTS=true\n"
            b"DEST_FILE_COUNT=20\n"
            b"DEST_TOTAL_SIZE=52428800\n"
        )
        stdout_mock.channel.recv_exit_status.return_value = 0

        with patch('migration_dags_combined.SSHHook', MockSSH):
            result = m.validate_data_copy.function(
                copy_status=sample_folder_distcp_result
            )

        assert result['validation_status'] == 'VALIDATED'
        assert result['file_count_match'] is True
        assert result['size_match'] is True

    def test_validation_failed_when_dest_does_not_exist(
        self, mock_ssh_hook, sample_folder_distcp_result
    ):
        m = _import_module()
        MockSSH, hook, client, stdout_mock, _ = mock_ssh_hook
        stdout_mock.read.return_value = b"DEST_EXISTS=false\nDEST_FILE_COUNT=0\nDEST_TOTAL_SIZE=0\n"
        stdout_mock.channel.recv_exit_status.return_value = 0

        with patch('migration_dags_combined.SSHHook', MockSSH):
            result = m.validate_data_copy.function(
                copy_status=sample_folder_distcp_result
            )

        assert result['validation_status'] == 'VALIDATION_FAILED'
        assert result['validation_error'] is not None

    def test_validation_failed_when_file_count_mismatch(
        self, mock_ssh_hook, sample_folder_distcp_result
    ):
        m = _import_module()
        MockSSH, hook, client, stdout_mock, _ = mock_ssh_hook
        # source had 20 files but only 15 in dest
        stdout_mock.read.return_value = (
            b"DEST_EXISTS=true\n"
            b"DEST_FILE_COUNT=15\n"
            b"DEST_TOTAL_SIZE=52428800\n"
        )
        stdout_mock.channel.recv_exit_status.return_value = 0

        with patch('migration_dags_combined.SSHHook', MockSSH):
            result = m.validate_data_copy.function(
                copy_status=sample_folder_distcp_result
            )

        assert result['file_count_match'] is False
        assert result['validation_status'] == 'VALIDATION_FAILED'

    def test_skips_validation_when_copy_failed(
        self, mock_ssh_hook, sample_folder_distcp_result
    ):
        m = _import_module()
        MockSSH, _, _, _, _ = mock_ssh_hook
        failed_copy = {**sample_folder_distcp_result, 'status': 'FAILED'}

        with patch('migration_dags_combined.SSHHook', MockSSH):
            result = m.validate_data_copy.function(copy_status=failed_copy)

        assert result['validation_status'] == 'VALIDATION_SKIPPED'

    def test_ssh_error_marks_validation_failed(
        self, mock_ssh_hook, sample_folder_distcp_result
    ):
        m = _import_module()
        MockSSH, hook, _, _, _ = mock_ssh_hook
        hook.get_conn.side_effect = Exception("Network error")

        with patch('migration_dags_combined.SSHHook', MockSSH):
            result = m.validate_data_copy.function(
                copy_status=sample_folder_distcp_result
            )

        assert result['validation_status'] == 'VALIDATION_FAILED'
        assert 'Network error' in result['validation_error']

    def test_result_preserves_original_copy_fields(
        self, mock_ssh_hook, sample_folder_distcp_result
    ):
        m = _import_module()
        MockSSH, hook, client, stdout_mock, _ = mock_ssh_hook
        stdout_mock.read.return_value = (
            b"DEST_EXISTS=true\n"
            b"DEST_FILE_COUNT=20\n"
            b"DEST_TOTAL_SIZE=52428800\n"
        )
        stdout_mock.channel.recv_exit_status.return_value = 0

        with patch('migration_dags_combined.SSHHook', MockSSH):
            result = m.validate_data_copy.function(
                copy_status=sample_folder_distcp_result
            )

        assert result['run_id'] == sample_folder_distcp_result['run_id']
        assert result['source_path'] == sample_folder_distcp_result['source_path']


# ===========================================================================
# update_data_copy_validation
# ===========================================================================
class TestUpdateDataCopyValidation:

    def test_updates_tracking_with_validated_status(
        self, mock_spark, sample_folder_validation_result
    ):
        m = _import_module()
        with patch('migration_dags_combined.execute_with_iceberg_retry') as mock_retry:
            m.update_data_copy_validation.function(
                validation_result=sample_folder_validation_result,
                spark=mock_spark,
            )
        update_sql = mock_retry.call_args[0][1]
        assert 'VALIDATED' in update_sql

    def test_updates_correct_run_and_source(
        self, mock_spark, sample_folder_validation_result
    ):
        m = _import_module()
        with patch('migration_dags_combined.execute_with_iceberg_retry') as mock_retry:
            m.update_data_copy_validation.function(
                validation_result=sample_folder_validation_result,
                spark=mock_spark,
            )
        update_sql = mock_retry.call_args[0][1]
        assert sample_folder_validation_result['run_id'] in update_sql
        assert '/data/sales/raw' in update_sql

    def test_returns_validation_result(
        self, mock_spark, sample_folder_validation_result
    ):
        m = _import_module()
        with patch('migration_dags_combined.execute_with_iceberg_retry'):
            result = m.update_data_copy_validation.function(
                validation_result=sample_folder_validation_result,
                spark=mock_spark,
            )
        assert result['validation_status'] == 'VALIDATED'

    def test_updates_with_validation_failed_status(
        self, mock_spark, sample_folder_distcp_result
    ):
        m = _import_module()
        failed_val = {
            **sample_folder_distcp_result,
            'validation_status': 'VALIDATION_FAILED',
            'dest_file_count': 5,
            'dest_size_bytes': 1024,
            'file_count_match': False,
            'size_match': False,
            'validation_error': 'File count mismatch',
        }
        with patch('migration_dags_combined.execute_with_iceberg_retry') as mock_retry:
            m.update_data_copy_validation.function(
                validation_result=failed_val, spark=mock_spark,
            )
        update_sql = mock_retry.call_args[0][1]
        assert 'VALIDATION_FAILED' in update_sql

    def test_includes_error_message_in_update(
        self, mock_spark, sample_folder_distcp_result
    ):
        m = _import_module()
        val_with_error = {
            **sample_folder_distcp_result,
            'validation_status': 'VALIDATION_FAILED',
            'dest_file_count': 0,
            'dest_size_bytes': 0,
            'file_count_match': False,
            'size_match': False,
            'validation_error': 'Destination missing',
        }
        with patch('migration_dags_combined.execute_with_iceberg_retry') as mock_retry:
            m.update_data_copy_validation.function(
                validation_result=val_with_error, spark=mock_spark,
            )
        update_sql = mock_retry.call_args[0][1]
        assert 'Destination missing' in update_sql


# ===========================================================================
# finalize_data_copy_run
# ===========================================================================
class TestFinalizeDataCopyRun:

    def _make_stats_row(self, total=2, successful=2, failed=0):
        row = MagicMock()
        row.__getitem__ = lambda self, key: {
            'total_folders': total,
            'successful_folders': successful,
            'failed_folders': failed,
        }[key]
        return row

    def test_returns_completed_when_no_failures(
        self, mock_spark, sample_folder_run_id, sample_folder_validation_result
    ):
        m = _import_module()
        mock_spark.sql.return_value.collect.return_value = [
            self._make_stats_row(total=2, successful=2, failed=0)
        ]
        with patch('migration_dags_combined.execute_with_iceberg_retry'):
            result = m.finalize_data_copy_run.function(
                run_id=sample_folder_run_id,
                validation_results=[sample_folder_validation_result],
                spark=mock_spark,
            )
        assert result['status'] == 'COMPLETED'
        assert result['total_folders'] == 2
        assert result['successful_folders'] == 2
        assert result['failed_folders'] == 0

    def test_returns_completed_with_errors_when_failures(
        self, mock_spark, sample_folder_run_id, sample_folder_validation_result
    ):
        m = _import_module()
        mock_spark.sql.return_value.collect.return_value = [
            self._make_stats_row(total=3, successful=2, failed=1)
        ]
        with patch('migration_dags_combined.execute_with_iceberg_retry'):
            result = m.finalize_data_copy_run.function(
                run_id=sample_folder_run_id,
                validation_results=[sample_folder_validation_result],
                spark=mock_spark,
            )
        assert result['status'] == 'COMPLETED_WITH_ERRORS'
        assert result['failed_folders'] == 1

    def test_updates_data_copy_runs_record(
        self, mock_spark, sample_folder_run_id, sample_folder_validation_result
    ):
        m = _import_module()
        mock_spark.sql.return_value.collect.return_value = [
            self._make_stats_row()
        ]
        with patch('migration_dags_combined.execute_with_iceberg_retry') as mock_retry:
            m.finalize_data_copy_run.function(
                run_id=sample_folder_run_id,
                validation_results=[sample_folder_validation_result],
                spark=mock_spark,
            )
        update_sql = mock_retry.call_args[0][1]
        assert 'data_copy_runs' in update_sql
        assert sample_folder_run_id in update_sql

    def test_handles_empty_stats_gracefully(
        self, mock_spark, sample_folder_run_id, sample_folder_validation_result
    ):
        m = _import_module()
        mock_spark.sql.return_value.collect.return_value = []
        with patch('migration_dags_combined.execute_with_iceberg_retry'):
            result = m.finalize_data_copy_run.function(
                run_id=sample_folder_run_id,
                validation_results=[sample_folder_validation_result],
                spark=mock_spark,
            )
        assert result['total_folders'] == 0

    def test_run_id_in_result(
        self, mock_spark, sample_folder_run_id, sample_folder_validation_result
    ):
        m = _import_module()
        mock_spark.sql.return_value.collect.return_value = [
            self._make_stats_row()
        ]
        with patch('migration_dags_combined.execute_with_iceberg_retry'):
            result = m.finalize_data_copy_run.function(
                run_id=sample_folder_run_id,
                validation_results=[sample_folder_validation_result],
                spark=mock_spark,
            )
        assert result['run_id'] == sample_folder_run_id


# ===========================================================================
# generate_data_copy_html_report
# ===========================================================================
class TestGenerateDataCopyHtmlReport:

    def _setup_spark_for_report(self, mock_spark, run_id, folders=None):
        """Set up mock_spark.sql return values for the report generator."""
        if folders is None:
            folders = [self._make_folder_row(run_id)]

        run_row = MagicMock()
        run_row.run_id = run_id
        run_row.status = 'COMPLETED'
        run_row.excel_file_path = 's3a://bucket/fc.xlsx'
        run_row.started_at = '2025-01-01 12:00:00'
        run_row.completed_at = '2025-01-01 12:10:00'

        call_results = [
            MagicMock(collect=MagicMock(return_value=[run_row])),  
            MagicMock(collect=MagicMock(return_value=folders)),    
        ]

        mock_spark.sql.side_effect = call_results
        return mock_spark

    def _make_folder_row(self, run_id, status='VALIDATED'):
        row = MagicMock()
        row.run_id = run_id
        row.source_path = '/data/sales/raw'
        row.dest_bucket = 's3a://test-bucket'
        row.dest_path = 'raw'
        row.status = status
        row.is_incremental = False
        row.source_file_count = 20
        row.dest_file_count = 20
        row.file_count_match = True
        row.source_size_bytes = 52428800
        row.dest_size_bytes = 52428800
        row.size_match = True
        row.bytes_copied = 52428800
        row.files_copied = 20
        row.error_message = None
        return row

    def test_returns_report_path_dict(
        self, mock_spark, sample_folder_run_id, sample_folder_finalize_result
    ):
        m = _import_module()
        spark = self._setup_spark_for_report(mock_spark, sample_folder_run_id)
        result = m.generate_data_copy_html_report.function(
            run_id=sample_folder_run_id,
            finalize_result=sample_folder_finalize_result,
            spark=spark,
        )
        assert 'report_path' in result
        assert sample_folder_run_id in result['report_path']
        assert result['report_path'].endswith('.html')

    def test_report_contains_run_id(
        self, mock_spark, sample_folder_run_id, sample_folder_finalize_result
    ):
        m = _import_module()
        spark = self._setup_spark_for_report(mock_spark, sample_folder_run_id)
        result = m.generate_data_copy_html_report.function(
            run_id=sample_folder_run_id,
            finalize_result=sample_folder_finalize_result,
            spark=spark,
        )
        assert sample_folder_run_id in result.get('html_content', '')

    def test_report_contains_source_path(
        self, mock_spark, sample_folder_run_id, sample_folder_finalize_result
    ):
        m = _import_module()
        spark = self._setup_spark_for_report(mock_spark, sample_folder_run_id)
        result = m.generate_data_copy_html_report.function(
            run_id=sample_folder_run_id,
            finalize_result=sample_folder_finalize_result,
            spark=spark,
        )
        assert '/data/sales/raw' in result.get('html_content', '')

    def test_html_written_to_s3(
        self, mock_spark, sample_folder_run_id, sample_folder_finalize_result
    ):
        m = _import_module()
        spark = self._setup_spark_for_report(mock_spark, sample_folder_run_id)
        m.generate_data_copy_html_report.function(
            run_id=sample_folder_run_id,
            finalize_result=sample_folder_finalize_result,
            spark=spark,
        )
        fs_mock = spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value
        assert fs_mock.create.called

    def test_html_contains_validated_status(
        self, mock_spark, sample_folder_run_id, sample_folder_finalize_result
    ):
        m = _import_module()
        spark = self._setup_spark_for_report(mock_spark, sample_folder_run_id)
        result = m.generate_data_copy_html_report.function(
            run_id=sample_folder_run_id,
            finalize_result=sample_folder_finalize_result,
            spark=spark,
        )
        assert 'VALIDATED' in result.get('html_content', '')

    def test_report_path_contains_data_copy_suffix(
        self, mock_spark, sample_folder_run_id, sample_folder_finalize_result
    ):
        m = _import_module()
        spark = self._setup_spark_for_report(mock_spark, sample_folder_run_id)
        result = m.generate_data_copy_html_report.function(
            run_id=sample_folder_run_id,
            finalize_result=sample_folder_finalize_result,
            spark=spark,
        )
        assert 'data_copy_report' in result['report_path']


# ===========================================================================
# send_data_copy_report_email
# ===========================================================================
class TestSendDataCopyReportEmail:

    def test_skips_email_when_no_recipients_configured(
        self, mock_spark, sample_folder_run_id
    ):
        m = _import_module()
        with patch('migration_dags_combined.get_config') as mock_cfg:
            mock_cfg.return_value = {
                'smtp_conn_id': 'smtp_default',
                'email_recipients': '',
            }
            result = m.send_data_copy_report_email.function(
                report_result={'report_path': 's3a://b/report.html', 'html_content': '<html/>'},
                run_id=sample_folder_run_id,
                spark=mock_spark,
            )
        assert result['sent'] is False
        assert result['reason'] == 'no_recipients'

    def test_sends_email_when_recipients_configured(
        self, mock_spark, sample_folder_run_id
    ):
        m = _import_module()
        with patch('migration_dags_combined.get_config') as mock_cfg, \
             patch('smtplib.SMTP') as mock_smtp:
            mock_cfg.return_value = {
                'smtp_conn_id': 'smtp_default',
                'email_recipients': 'user@example.com',
            }
            smtp_instance = MagicMock()
            mock_smtp.return_value.__enter__ = MagicMock(return_value=smtp_instance)
            mock_smtp.return_value.__exit__ = MagicMock(return_value=False)

            conn_mock = MagicMock()
            conn_mock.host = 'smtp.example.com'
            conn_mock.port = 587
            conn_mock.login = 'user@example.com'
            conn_mock.password = 'secret'

            with patch('migration_dags_combined.BaseHook') as MockBaseHook:
                MockBaseHook.get_connection.return_value = conn_mock
                result = m.send_data_copy_report_email.function(
                    report_result={'report_path': 's3a://b/r.html', 'html_content': '<html/>'},
                    run_id=sample_folder_run_id,
                    spark=mock_spark,
                )

        assert result['sent'] is True
        assert 'user@example.com' in result['recipients']

    def test_result_includes_report_path(
        self, mock_spark, sample_folder_run_id
    ):
        m = _import_module()
        with patch('migration_dags_combined.get_config') as mock_cfg:
            mock_cfg.return_value = {'smtp_conn_id': 'smtp_default', 'email_recipients': ''}
            result = m.send_data_copy_report_email.function(
                report_result={'report_path': 's3a://b/r.html', 'html_content': ''},
                run_id=sample_folder_run_id,
                spark=mock_spark,
            )
        assert result['report_path'] == 's3a://b/r.html'