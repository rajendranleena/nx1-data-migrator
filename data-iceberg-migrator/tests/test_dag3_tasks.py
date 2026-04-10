"""DAG 3 Task Tests: folder_only_data_copy pipeline."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import migration_dag_folder_copy as m
import pandas as pd
import pytest

from .helpers import make_excel_bytes, mock_ssh_stdout, setup_spark_excel


class TestValidatePrerequisitesFolderCopy:

    def test_all_checks_pass(self, mock_ssh_hook):
        hook, client, stdout_mock, stderr_mock = mock_ssh_hook
        responses = [
            (MagicMock(), mock_ssh_stdout(0, b'SSH_TEST_OK'), MagicMock()),
            (MagicMock(), mock_ssh_stdout(0, b'DISTCP_OK'), MagicMock()),
            (MagicMock(), mock_ssh_stdout(0, b'HADOOP_FS_OK'), MagicMock()),
        ]
        for r in responses:
            r[2].read.return_value = b''
        client.exec_command.side_effect = responses

        result = m.validate_prerequisites_folder_copy.function()
        assert result['ssh_connectivity'] is True
        assert result['hadoop_distcp_available'] is True
        assert result['hadoop_fs_available'] is True


class TestInitFolderCopyTrackingTables:

    def test_creates_database_and_tables(self, mock_spark):
        result = m.init_folder_copy_tracking_tables.function(spark=mock_spark)
        assert result == {'status': 'initialized', 'database': 'migration_tracking'}
        sql = ' '.join(str(c) for c in mock_spark.sql.call_args_list).lower()
        assert 'data_copy_runs' in sql
        assert 'data_copy_status' in sql
        assert 'using iceberg' in sql


class TestCreateDataCopyRun:

    def test_creates_run_with_running_status(self, mock_spark):
        run_id = m.create_data_copy_run.function(
            excel_file_path='s3a://bucket/fc.xlsx', spark=mock_spark,
        )
        assert run_id.startswith('folder_run_')
        assert 'RUNNING' in ' '.join(str(c) for c in mock_spark.sql.call_args_list)


class TestParseFolderCopyExcel:

    def test_basic_row_parsing(self, mock_spark, sample_folder_run_id):
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'source_path': '/data/sales', 'target_bucket': 'my-bucket', 'dest_folder': 'sales_copy'},
        ]))
        configs = m.parse_folder_copy_excel.function('s3a://b/fc.xlsx', sample_folder_run_id, spark=mock_spark)
        assert len(configs) == 1
        assert configs[0]['source_path'] == '/data/sales'
        assert configs[0]['dest_bucket'] == 's3a://my-bucket'
        assert configs[0]['dest_folder'] == 'sales_copy'

    @pytest.mark.parametrize("raw_bucket,expected", [
        ('s3://b', 's3a://b'),
        ('s3n://b', 's3a://b'),
        ('b', 's3a://b'),
        ('s3a://b', 's3a://b'),
    ])
    def test_normalizes_s3_bucket_prefix(self, mock_spark, sample_folder_run_id, raw_bucket, expected):
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'source_path': '/x', 'target_bucket': raw_bucket, 'dest_folder': 'out'},
        ]))
        result = m.parse_folder_copy_excel.function('s3a://b/f.xlsx', sample_folder_run_id, spark=mock_spark)
        assert result[0]['dest_bucket'] == expected

    def test_skips_rows_with_missing_target_bucket(self, mock_spark, sample_folder_run_id):
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'source_path': '', 'target_bucket': '', 'dest_folder': ''},
        ]))
        fake_df = pd.DataFrame([
            {'source_path': '/data/no_bucket',   'target_bucket': None,        'dest_folder': 'x'},
            {'source_path': '/data/with_bucket', 'target_bucket': 's3a://bkt', 'dest_folder': 'y'},
        ])
        with patch('pandas.read_excel', return_value=fake_df):
            configs = m.parse_folder_copy_excel.function(
                's3a://b/f.xlsx', sample_folder_run_id, spark=mock_spark,
            )
        assert len(configs) == 1
        assert configs[0]['source_path'] == '/data/with_bucket'

    def test_raises_when_no_valid_rows(self, mock_spark, sample_folder_run_id):
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'source_path': '', 'target_bucket': '', 'dest_folder': ''},
        ]))
        with pytest.raises(ValueError, match="No valid rows"):
            m.parse_folder_copy_excel.function(
                's3a://b/f.xlsx', sample_folder_run_id, spark=mock_spark,
            )

    def test_multiple_rows_all_parsed(self, mock_spark, sample_folder_run_id):
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'source_path': '/data/a', 'target_bucket': 's3a://bkt', 'dest_folder': 'a'},
            {'source_path': '/data/b', 'target_bucket': 's3a://bkt', 'dest_folder': 'b'},
            {'source_path': '/data/c', 'target_bucket': 's3a://bkt', 'dest_folder': 'c'},
        ]))
        configs = m.parse_folder_copy_excel.function('s3a://b/f.xlsx', sample_folder_run_id, spark=mock_spark)
        assert len(configs) == 3

    def test_trailing_slash_stripped_from_source_path_basename(self, mock_spark, sample_folder_run_id):
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'source_path': '/data/mydir/', 'target_bucket': 's3a://bkt', 'dest_folder': 'mydir'},
        ]))
        configs = m.parse_folder_copy_excel.function('s3a://b/f.xlsx', sample_folder_run_id, spark=mock_spark)
        assert configs[0]['dest_folder'] == 'mydir'

    @pytest.mark.parametrize("empty_val", [None, float('nan')])
    def test_empty_dest_folder_defaults_to_source_basename(self, mock_spark, sample_folder_run_id, empty_val):
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'source_path': '/mapr/cluster1/data/raw/sales', 'target_bucket': 's3a://bkt', 'dest_folder': empty_val},
        ]))
        configs = m.parse_folder_copy_excel.function('s3a://b/f.xlsx', sample_folder_run_id, spark=mock_spark)
        assert configs[0]['dest_folder'] == 'sales'

    def test_dest_folder_with_only_whitespace_defaults_to_source_basename(self, mock_spark, sample_folder_run_id):
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'source_path': '/data/raw/reports/', 'target_bucket': 's3a://bkt', 'dest_folder': '   '},
        ]))
        configs = m.parse_folder_copy_excel.function('s3a://b/f.xlsx', sample_folder_run_id, spark=mock_spark)
        assert configs[0]['dest_folder'] == 'reports'

    def test_leading_slash_stripped_from_dest_folder(self, mock_spark, sample_folder_run_id):
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'source_path': '/data/sales', 'target_bucket': 's3a://bkt', 'dest_folder': '/temp'},
        ]))
        configs = m.parse_folder_copy_excel.function('s3a://b/f.xlsx', sample_folder_run_id, spark=mock_spark)
        assert configs[0]['dest_folder'] == 'temp'

    def test_endpoint_emitted_when_present(self, mock_spark, sample_folder_run_id):
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'source_path': '/data/sales', 'target_bucket': 's3a://bkt', 'dest_folder': 'sales',
            'endpoint': 'https://s3.tenant-a.example.com'},
        ]))
        configs = m.parse_folder_copy_excel.function('s3a://b/f.xlsx', sample_folder_run_id, spark=mock_spark)
        assert configs[0]['dest_endpoint'] == 'https://s3.tenant-a.example.com'

    def test_endpoint_defaults_to_empty_when_absent(self, mock_spark, sample_folder_run_id):
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'source_path': '/data/sales', 'target_bucket': 's3a://bkt', 'dest_folder': 'sales'},
        ]))
        configs = m.parse_folder_copy_excel.function('s3a://b/f.xlsx', sample_folder_run_id, spark=mock_spark)
        assert configs[0]['dest_endpoint'] == ''

    def test_each_row_independent_regardless_of_matching_bucket(self, mock_spark, sample_folder_run_id):
        """Two rows with same bucket but different endpoints must both appear as separate jobs."""
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'source_path': '/data/a', 'target_bucket': 's3a://data-lake',
            'endpoint': 'https://s3.tenant-a.example.com'},
            {'source_path': '/data/b', 'target_bucket': 's3a://data-lake',
            'endpoint': 'https://s3.tenant-b.example.com'},
        ]))
        configs = m.parse_folder_copy_excel.function('s3a://b/f.xlsx', sample_folder_run_id, spark=mock_spark)
        assert len(configs) == 2
        endpoints = {c['dest_endpoint'] for c in configs}
        assert endpoints == {'https://s3.tenant-a.example.com', 'https://s3.tenant-b.example.com'}


# ===========================================================================
# run_folder_distcp_ssh
# ===========================================================================
class TestRunFolderDistcpSsh:

    def _success_output(self, incr=False, src_files=20, src_size=52428800,
                         s3_after_files=20, s3_after_size=52428800):
        return '\n'.join([
            f"INCREMENTAL={'true' if incr else 'false'}",
            f"SRC_FILE_COUNT={src_files}", f"SRC_TOTAL_SIZE={src_size}",
            "S3_FILE_COUNT_BEFORE=0", "S3_TOTAL_SIZE_BEFORE=0", "DISTCP_EXIT_CODE=0",
            f"S3_FILE_COUNT_AFTER={s3_after_files}", f"S3_TOTAL_SIZE_AFTER={s3_after_size}",
        ]).encode()

    def test_successful_copy(self, mock_ssh_hook, sample_folder_config):
        hook, client, stdout_mock, _ = mock_ssh_hook
        stdout_mock.read.return_value = self._success_output(
            incr=True, src_files=42, src_size=1_000_000,
            s3_after_files=42, s3_after_size=1_000_000,
        )
        stdout_mock.channel.recv_exit_status.return_value = 0

        result = m.run_folder_distcp_ssh.function(folder_config=sample_folder_config)
        assert result['status'] == 'COMPLETED'
        assert result['is_incremental'] is True
        assert result['source_file_count'] == 42
        assert result['file_count_match'] is True

    def test_file_count_mismatch(self, mock_ssh_hook, sample_folder_config):
        hook, client, stdout_mock, _ = mock_ssh_hook
        stdout_mock.read.return_value = self._success_output(src_files=20, s3_after_files=18)
        stdout_mock.channel.recv_exit_status.return_value = 0

        result = m.run_folder_distcp_ssh.function(folder_config=sample_folder_config)
        assert result['file_count_match'] is False

    def test_distcp_failure_pushes_xcom_and_raises(self, mock_ssh_hook, sample_folder_config):
        hook, client, _, _ = mock_ssh_hook
        fail_stderr = MagicMock()
        fail_stderr.read.return_value = b'DistCp failed'
        client.exec_command.return_value = (
            MagicMock(), mock_ssh_stdout(1, b'DISTCP_EXIT_CODE=1\n'), fail_stderr,
        )
        ti = MagicMock()

        with pytest.raises(Exception, match="DistCp failed"):
            m.run_folder_distcp_ssh.function(folder_config=sample_folder_config, ti=ti)
        assert ti.xcom_push.call_args[1]['value']['status'] == 'FAILED'

    def test_ssh_exception_pushes_xcom_and_raises(self, mock_ssh_hook, sample_folder_config):
        hook, _, _, _ = mock_ssh_hook
        hook.get_conn.side_effect = Exception("SSH timeout")
        ti = MagicMock()

        with pytest.raises(Exception, match="SSH timeout"):
            m.run_folder_distcp_ssh.function(folder_config=sample_folder_config, ti=ti)
        assert 'SSH timeout' in ti.xcom_push.call_args[1]['value']['error']


class TestRecordDataCopyStatus:

    def test_inserts_completed(self, mock_spark, sample_folder_distcp_result, mock_iceberg_retry):
        m.record_data_copy_status.function(distcp_result=sample_folder_distcp_result, spark=mock_spark)
        sql = mock_iceberg_retry.call_args[0][1]
        assert 'COMPLETED' in sql
        assert '/data/sales/raw' in sql

    def test_inserts_failed(self, mock_spark, sample_folder_distcp_result, mock_iceberg_retry):
        failed = {
            **sample_folder_distcp_result,
            'status': 'FAILED', 'error': 'exit 1',
            'source_path': '/bad', 'dest_path': 'x',
        }
        m.record_data_copy_status.function(distcp_result=failed, spark=mock_spark)
        assert 'FAILED' in mock_iceberg_retry.call_args[0][1]


class TestValidateDataCopy:

    def test_validated_when_counts_match(self, mock_ssh_hook, sample_folder_distcp_result):
        hook, client, stdout_mock, _ = mock_ssh_hook
        stdout_mock.read.return_value = b"DEST_EXISTS=true\nDEST_FILE_COUNT=20\nDEST_TOTAL_SIZE=52428800\n"
        stdout_mock.channel.recv_exit_status.return_value = 0

        result = m.validate_data_copy.function(copy_status=sample_folder_distcp_result)
        assert result['validation_status'] == 'VALIDATED'
        assert result['file_count_match'] is True

    def test_failed_when_dest_missing(self, mock_ssh_hook, sample_folder_distcp_result):
        hook, client, stdout_mock, _ = mock_ssh_hook
        stdout_mock.read.return_value = b"DEST_EXISTS=false\nDEST_FILE_COUNT=0\nDEST_TOTAL_SIZE=0\n"
        stdout_mock.channel.recv_exit_status.return_value = 0
        ti = MagicMock()

        with pytest.raises(Exception, match="VALIDATION_FAILED"):
            m.validate_data_copy.function(copy_status=sample_folder_distcp_result, ti=ti)
        assert ti.xcom_push.call_args[1]['value']['validation_status'] == 'VALIDATION_FAILED'

    def test_failed_on_file_count_mismatch(self, mock_ssh_hook, sample_folder_distcp_result):
        hook, client, stdout_mock, _ = mock_ssh_hook
        stdout_mock.read.return_value = b"DEST_EXISTS=true\nDEST_FILE_COUNT=15\nDEST_TOTAL_SIZE=52428800\n"
        stdout_mock.channel.recv_exit_status.return_value = 0
        ti = MagicMock()

        with pytest.raises(Exception, match="file count or size mismatch"):
            m.validate_data_copy.function(copy_status=sample_folder_distcp_result, ti=ti)
        assert ti.xcom_push.call_args[1]['value']['file_count_match'] is False


class TestUpdateDataCopyValidation:

    def test_updates_validated(self, mock_spark, sample_folder_validation_result, mock_iceberg_retry):
        m.update_data_copy_validation.function(
            validation_result=sample_folder_validation_result, spark=mock_spark,
        )
        assert 'VALIDATED' in mock_iceberg_retry.call_args[0][1]

    def test_updates_failed_with_error(self, mock_spark, sample_folder_distcp_result, mock_iceberg_retry):
        failed = {
            **sample_folder_distcp_result,
            'validation_status': 'VALIDATION_FAILED',
            'dest_file_count': 0, 'dest_size_bytes': 0,
            'file_count_match': False, 'size_match': False,
            'validation_error': 'Destination missing',
        }
        m.update_data_copy_validation.function(validation_result=failed, spark=mock_spark)
        sql = mock_iceberg_retry.call_args[0][1]
        assert 'VALIDATION_FAILED' in sql
        assert 'Destination missing' in sql


class TestFinalizeDataCopyRun:

    def _make_stats_row(self, total=2, successful=2, failed=0):
        row = MagicMock()
        row.__getitem__ = lambda self, k: {
            'total_folders': total, 'successful_folders': successful, 'failed_folders': failed,
        }[k]
        return row

    def test_completed_when_no_failures(self, mock_spark, sample_folder_run_id, mock_iceberg_retry):
        mock_spark.sql.return_value.collect.return_value = [self._make_stats_row(2, 2, 0)]
        result = m.finalize_data_copy_run.function(run_id=sample_folder_run_id, spark=mock_spark)
        assert result['status'] == 'COMPLETED'
        assert 'data_copy_runs' in mock_iceberg_retry.call_args[0][1]

    def test_completed_with_errors_when_failures(self, mock_spark, sample_folder_run_id, mock_iceberg_retry):
        mock_spark.sql.return_value.collect.return_value = [self._make_stats_row(3, 2, 1)]
        result = m.finalize_data_copy_run.function(run_id=sample_folder_run_id, spark=mock_spark)
        assert result['status'] == 'COMPLETED_WITH_ERRORS'
        assert result['failed_folders'] == 1


class TestGenerateDataCopyHtmlReport:

    def test_generates_report_and_writes_to_s3(self, mock_spark, sample_folder_run_id, sample_folder_finalize_result):
        run_row = SimpleNamespace(
            run_id=sample_folder_run_id, status='COMPLETED',
            excel_file_path='s3a://bucket/fc.xlsx',
            started_at='2025-01-01 12:00:00', completed_at='2025-01-01 12:10:00',
        )
        folder_row = SimpleNamespace(
            run_id=sample_folder_run_id, source_path='/data/sales/raw',
            dest_bucket='s3a://test-bucket', dest_path='raw',
            status='VALIDATED', is_incremental=False,
            source_file_count=20, dest_file_count=20, file_count_match=True,
            source_size_bytes=52428800, dest_size_bytes=52428800, size_match=True,
            bytes_copied=52428800, files_copied=20, error_message=None,
        )
        mock_spark.sql.side_effect = [
            MagicMock(collect=MagicMock(return_value=[run_row])),
            MagicMock(collect=MagicMock(return_value=[folder_row])),
        ]
        result = m.generate_data_copy_html_report.function(
            run_id=sample_folder_run_id,
            finalize_result=sample_folder_finalize_result,
            spark=mock_spark,
        )
        assert result['report_path'].endswith('.html')
        assert 'data_copy_report' in result['report_path']


class TestSendDataCopyReportEmail:

    def test_skips_when_no_recipients(self, mock_spark, sample_folder_run_id):
        with patch('migration_dag_folder_copy.get_config') as cfg:
            cfg.return_value = {'smtp_conn_id': 'smtp_default', 'email_recipients': ''}
            result = m.send_data_copy_report_email.function(
                report_result={'report_path': 's3a://b/r.html', 'html_content': '<html/>'},
                run_id=sample_folder_run_id, spark=mock_spark,
            )
        assert result == {'sent': False, 'reason': 'no_recipients'}
