"""DAG 2 Task Tests: iceberg_migration pipeline."""

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import migration_dag_iceberg as m
import pytest

from .helpers import make_excel_bytes, setup_spark_excel


class TestInitIcebergTrackingTables:

    def test_creates_database_and_tables(self, mock_spark):
        result = m.init_iceberg_tracking_tables.function(spark=mock_spark)
        assert result == {'status': 'initialized', 'database': 'migration_tracking'}
        sql = ' '.join(str(c) for c in mock_spark.sql.call_args_list).lower()
        assert 'iceberg_migration_runs' in sql
        assert 'iceberg_migration_table_status' in sql


class TestCreateIcebergMigrationRun:

    def test_creates_run_with_running_status(self, mock_spark):
        run_id = m.create_iceberg_migration_run.function(
            excel_file_path='s3a://bucket/ice.xlsx',
            dag_run_id='dag_test',
            spark=mock_spark,
        )
        assert run_id.startswith('iceberg_run_')
        assert 'RUNNING' in ' '.join(str(c) for c in mock_spark.sql.call_args_list)


class TestParseIcebergExcel:

    def test_snapshot_migration_by_default(self, mock_spark, sample_iceberg_run_id):
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'database': 'sales_s3', 'table': '*', 'inplace_migration': 'F', 'destination_iceberg_database': ''},
        ]))
        result = m.parse_iceberg_excel.function('s3a://b/f.xlsx', sample_iceberg_run_id, spark=mock_spark)
        assert result[0]['inplace_migration'] is False
        assert result[0]['destination_iceberg_database'] == 'sales_s3_iceberg'

    def test_inplace_uses_same_database(self, mock_spark, sample_iceberg_run_id):
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'database': 'sales_s3', 'table': '*', 'inplace_migration': 'T', 'destination_iceberg_database': ''},
        ]))
        result = m.parse_iceberg_excel.function('s3a://b/f.xlsx', sample_iceberg_run_id, spark=mock_spark)
        assert result[0]['destination_iceberg_database'] == 'sales_s3'

    def test_custom_dest_database(self, mock_spark, sample_iceberg_run_id):
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'database': 'sales_s3', 'table': '*', 'inplace_migration': 'F', 'destination_iceberg_database': 'custom_ice_db'},
        ]))
        result = m.parse_iceberg_excel.function('s3a://b/f.xlsx', sample_iceberg_run_id, spark=mock_spark)
        assert result[0]['destination_iceberg_database'] == 'custom_ice_db'


class TestDiscoverHiveTables:

    def _location_router(self, tables):
        def sql_router(sql):
            df = MagicMock()
            if 'show tables' in sql.lower():
                df.collect.return_value = [MagicMock(tableName=t) for t in tables]
            elif 'describe formatted' in sql.lower():
                loc = MagicMock()
                loc.col_name = 'Location'
                loc.data_type = 's3a://bucket/db/tbl'
                df.collect.return_value = [loc]
            return df
        return sql_router

    def test_discovers_all_tables_with_wildcard(self, mock_spark, sample_iceberg_db_config):
        mock_spark.sql.side_effect = self._location_router(['transactions', 'orders'])
        result = m.discover_hive_tables.function.__wrapped__(
            db_config=sample_iceberg_db_config, spark=mock_spark,
        )
        assert len(result['discovered_tables']) == 2

    def test_discovers_filtered_tables_by_pattern(self, mock_spark, sample_iceberg_db_config):
        import fnmatch
        import re
        sample_iceberg_db_config['table_pattern'] = 'trans*'

        def sql_router(sql):
            df = MagicMock()
            if 'show tables' in sql.lower():
                all_t = ['transactions', 'orders', 'trans_history']
                like = re.search(r"like '([^']+)'", sql.lower())
                matched = [t for t in all_t if fnmatch.fnmatch(t, like.group(1).replace('%', '*'))] if like else all_t
                df.collect.return_value = [MagicMock(tableName=n) for n in matched]
            elif 'describe formatted' in sql.lower():
                loc = MagicMock()
                loc.col_name = 'Location'
                loc.data_type = 's3a://bucket/db/tbl'
                df.collect.return_value = [loc]
            return df

        mock_spark.sql.side_effect = sql_router
        result = m.discover_hive_tables.function.__wrapped__(
            db_config=sample_iceberg_db_config, spark=mock_spark,
        )
        names = [t['table'] for t in result['discovered_tables']]
        assert 'transactions' in names
        assert 'orders' not in names


class TestMigrateTablesToIceberg:

    def _default_router(self, count=1000):
        def router(sql):
            sl = sql.lower()
            df = MagicMock()
            if 'count(*)' in sl:
                row = MagicMock()
                row.__getitem__ = lambda self, k: count
                df.collect.return_value = [row]
            elif 'show partitions' in sl:
                df.collect.return_value = []
            elif 'describe formatted' in sl:
                loc = MagicMock()
                loc.col_name = 'Location'
                loc.data_type = 's3a://bucket/t'
                df.collect.return_value = [loc]
            else:
                df.collect.return_value = []
                df.count.return_value = 0
            return df
        return router

    def test_snapshot_migration(self, mock_spark, sample_iceberg_discovery):
        mock_spark.sql.side_effect = self._default_router()
        result = m.migrate_tables_to_iceberg.function.__wrapped__(
            discovery=sample_iceberg_discovery, dag_run_id='dag_test',
            spark=mock_spark, ti=MagicMock(),
        )
        assert result['results'][0]['status'] == 'COMPLETED'

    def test_inplace_migration(self, mock_spark, sample_iceberg_discovery):
        sample_iceberg_discovery['inplace_migration'] = True
        sample_iceberg_discovery['destination_iceberg_database'] = 'sales_data_s3'
        mock_spark.sql.side_effect = self._default_router(500)
        result = m.migrate_tables_to_iceberg.function.__wrapped__(
            discovery=sample_iceberg_discovery, dag_run_id='dag_test',
            spark=mock_spark, ti=MagicMock(),
        )
        assert any(r['migration_type'] == 'INPLACE' for r in result['results'])

    def test_failure_raises(self, mock_spark, sample_iceberg_discovery):
        def router(sql):
            sl = sql.lower()
            df = MagicMock()
            if 'count(*)' in sl:
                row = MagicMock()
                row.__getitem__ = lambda self, k: 1000
                df.collect.return_value = [row]
            elif 'show partitions' in sl:
                df.collect.return_value = []
            elif 'system.snapshot' in sl or 'system.migrate' in sl:
                raise Exception("Snapshot procedure failed")
            else:
                df.collect.return_value = []
            return df

        mock_spark.sql.side_effect = router
        with pytest.raises(Exception, match="Iceberg migration failed"):
            m.migrate_tables_to_iceberg.function.__wrapped__(
                discovery=sample_iceberg_discovery, dag_run_id='dag_test',
                spark=mock_spark, ti=MagicMock(),
            )


class TestUpdateMigrationDurations:

    def test_updates_duration_in_tracking(self, mock_spark, sample_iceberg_migration_result, mock_iceberg_retry):
        m.update_migration_durations.function(
            migration_result=sample_iceberg_migration_result, spark=mock_spark,
        )
        assert mock_iceberg_retry.called


class TestValidateIcebergTables:

    def test_matching_schema(self, mock_spark, sample_iceberg_migration_result):
        def router(sql):
            df = MagicMock()
            if 'describe' in sql.lower():
                df.collect.return_value = [MagicMock(col_name='id', data_type='bigint')]
            else:
                df.collect.return_value = []
            return df

        mock_spark.sql.side_effect = router
        result = m.validate_iceberg_tables.function.__wrapped__(
            migration_result=sample_iceberg_migration_result, spark=mock_spark, ti=MagicMock(),
        )
        assert result['validation_results'][0]['schema_match'] is True

    def test_schema_mismatch(self, mock_spark, sample_iceberg_migration_result):
        call_tracker = [0]

        def router(sql):
            df = MagicMock()
            if 'describe' in sql.lower():
                call_tracker[0] += 1
                dtype = 'bigint' if call_tracker[0] % 2 == 1 else 'int'
                df.collect.return_value = [MagicMock(col_name='id', data_type=dtype)]
            else:
                df.collect.return_value = []
            return df

        mock_spark.sql.side_effect = router
        result = m.validate_iceberg_tables.function.__wrapped__(
            migration_result=sample_iceberg_migration_result, spark=mock_spark, ti=MagicMock(),
        )
        assert result['validation_results'][0]['schema_match'] is False


class TestUpdateIcebergValidationStatus:

    def _make_validation_result(self, sample, row_count_match=True):
        return {
            **sample,
            'validation_results': [{
                'source_table': 'transactions',
                'destination_table': 'sales_data_s3_iceberg.transactions',
                'status': 'COMPLETED', 'source_hive_row_count': 1000,
                'dest_iceberg_row_count': 1000, 'row_count_match': row_count_match,
                'source_hive_partition_count': 2, 'dest_iceberg_partition_count': 2,
                'partition_count_match': True, 'schema_match': True,
                'schema_differences': '', 'per_table_validation_duration': 4.5,
                'error': None,
            }],
            '_task_duration': 4.5,
        }

    def test_sets_validated_on_match(self, mock_spark, sample_iceberg_migration_result, mock_iceberg_retry):
        vr = self._make_validation_result(sample_iceberg_migration_result)
        m.update_iceberg_validation_status.function(validation_result=vr, spark=mock_spark)
        assert any('VALIDATED' in str(c) for c in mock_iceberg_retry.call_args_list)

    def test_sets_validation_failed_on_mismatch(self, mock_spark, sample_iceberg_migration_result, mock_iceberg_retry):
        vr = self._make_validation_result(sample_iceberg_migration_result, row_count_match=False)
        m.update_iceberg_validation_status.function(validation_result=vr, spark=mock_spark)
        assert any('VALIDATION_FAILED' in str(c) for c in mock_iceberg_retry.call_args_list)


class TestGenerateIcebergHtmlReport:

    def test_generates_report_and_writes_to_s3(self, mock_spark, sample_iceberg_run_id):
        tbl_row = SimpleNamespace(
            source_database='sales_s3', source_table='transactions',
            migration_type='SNAPSHOT', destination_table='sales_s3_iceberg.transactions',
            status='VALIDATED', migration_duration_seconds=45.0,
            validation_duration_seconds=4.5, validation_status='COMPLETED',
            row_count_match=True, partition_count_match=True, schema_match=True,
            source_hive_row_count=1000, destination_iceberg_row_count=1000,
            source_hive_partition_count=2, dest_iceberg_partition_count=2,
        )
        ivs_row = MagicMock()
        ivs_row.__getitem__ = lambda self, k: 1 if k == 'total_tables_validated' else 0
        ivs_row.total_tables_validated = 1
        ivs_row.tables_passed_validation = 1
        ivs_row.tables_failed_validation = 0
        ivs_row.total_row_count_mismatches = 0
        ivs_row.total_partition_count_mismatches = 0
        ivs_row.total_schema_mismatches = 0

        def router(sql):
            df = MagicMock()
            if 'order by' in sql.lower():
                df.collect.return_value = [tbl_row]
            elif 'sum(case when' in sql.lower():
                df.collect.return_value = [ivs_row]
            else:
                df.collect.return_value = []
            return df

        mock_spark.sql.side_effect = router
        result = m.generate_iceberg_html_report.function(run_id=sample_iceberg_run_id, spark=mock_spark)
        assert result['report_path'].endswith('.html')
        assert sample_iceberg_run_id in result['report_path']


class TestSendIcebergReportEmail:

    def test_skips_when_no_recipients(self, mock_spark, sample_iceberg_run_id):
        with patch('airflow.models.Variable.get', return_value=''):
            result = m.send_iceberg_report_email.function(
                report_result={'report_path': 's3a://bucket/report.html'},
                run_id=sample_iceberg_run_id, spark=mock_spark,
            )
        assert result['sent'] is False

    def test_sends_when_recipients_configured(self, mock_spark, sample_iceberg_run_id):
        reader_mock = MagicMock()
        reader_mock.readLine.side_effect = ['<html>iceberg report</html>', None]
        mock_spark._jvm.java.io.BufferedReader.return_value = reader_mock

        with patch('airflow.utils.email.send_email'), \
             patch('tempfile.NamedTemporaryFile') as mock_tmp, \
             patch('os.unlink'):
            tmp_inst = MagicMock()
            tmp_inst.name = '/tmp/ice_report.html'
            mock_tmp.return_value = tmp_inst
            result = m.send_iceberg_report_email.function(
                report_result={'report_path': 's3a://bucket/report.html'},
                run_id=sample_iceberg_run_id, spark=mock_spark,
            )
        assert result['sent'] is True


class TestFinalizeIcebergRun:

    def test_returns_completed(self, mock_spark, sample_iceberg_run_id):
        stats = MagicMock()
        stats.__getitem__ = lambda self, k: {
            'total': 2, 'successful': 2, 'failed': 0, 'skipped': 0, 'count_mismatches': 0,
        }[k]
        type_row = MagicMock()
        type_row.__getitem__ = lambda self, k: 'SNAPSHOT'

        def router(sql):
            df = MagicMock()
            if 'count_mismatches' in sql.lower():
                df.collect.return_value = [stats]
            elif 'migration_type' in sql.lower():
                df.collect.return_value = [type_row]
            else:
                df.collect.return_value = []
            return df

        mock_spark.sql.side_effect = router
        result = m.finalize_iceberg_run.function(run_id=sample_iceberg_run_id, spark=mock_spark)
        assert result['status'] == 'COMPLETED'
