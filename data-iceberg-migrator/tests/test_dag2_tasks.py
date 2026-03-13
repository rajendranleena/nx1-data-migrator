"""
DAG 2 Task Tests: init_iceberg_tracking_tables, create_iceberg_migration_run,
parse_iceberg_excel, discover_hive_tables, migrate_tables_to_iceberg,
update_migration_durations, validate_iceberg_tables,
update_iceberg_validation_status, generate_iceberg_html_report,
send_iceberg_report_email, finalize_iceberg_run
"""

import json
import pytest
from io import BytesIO
from unittest.mock import MagicMock, patch


def _import_module():
    import migration_dags_combined as m
    return m


# ---------------------------------------------------------------------------
# init_iceberg_tracking_tables
# ---------------------------------------------------------------------------
class TestInitIcebergTrackingTables:

    def test_creates_database_and_tables(self, mock_spark):
        m = _import_module()
        result = m.init_iceberg_tracking_tables.function(spark=mock_spark)
        assert result['status'] == 'initialized'
        assert result['database'] == 'migration_tracking'
        assert mock_spark.sql.call_count >= 3

    def test_creates_iceberg_runs_table(self, mock_spark):
        m = _import_module()
        m.init_iceberg_tracking_tables.function(spark=mock_spark)
        sql_calls = ' '.join(str(c) for c in mock_spark.sql.call_args_list).lower()
        assert 'iceberg_migration_runs' in sql_calls

    def test_creates_table_status_table(self, mock_spark):
        m = _import_module()
        m.init_iceberg_tracking_tables.function(spark=mock_spark)
        sql_calls = ' '.join(str(c) for c in mock_spark.sql.call_args_list).lower()
        assert 'iceberg_migration_table_status' in sql_calls


# ---------------------------------------------------------------------------
# create_iceberg_migration_run
# ---------------------------------------------------------------------------
class TestCreateIcebergMigrationRun:

    def test_returns_iceberg_run_id(self, mock_spark):
        m = _import_module()
        run_id = m.create_iceberg_migration_run.function(
            excel_file_path='s3a://bucket/ice.xlsx',
            dag_run_id='dag_test',
            spark=mock_spark,
        )
        assert run_id.startswith('iceberg_run_')

    def test_inserts_running_status(self, mock_spark):
        m = _import_module()
        m.create_iceberg_migration_run.function(
            excel_file_path='s3a://bucket/ice.xlsx',
            dag_run_id='dag_test',
            spark=mock_spark,
        )
        sql_calls = ' '.join(str(c) for c in mock_spark.sql.call_args_list)
        assert 'RUNNING' in sql_calls

    def test_unique_run_ids(self, mock_spark):
        m = _import_module()
        id1 = m.create_iceberg_migration_run.function('s3a://b/f.xlsx', 'dr1', spark=mock_spark)
        id2 = m.create_iceberg_migration_run.function('s3a://b/f.xlsx', 'dr2', spark=mock_spark)
        assert id1 != id2


# ---------------------------------------------------------------------------
# parse_iceberg_excel
# ---------------------------------------------------------------------------
class TestParseIcebergExcel:

    def _make_excel_bytes(self, rows):
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

    def test_snapshot_migration_by_default(self, mock_spark, sample_iceberg_run_id):
        m = _import_module()
        excel_bytes = self._make_excel_bytes([
            {'database': 'sales_s3', 'table': '*', 'inplace_migration': 'F', 'destination_iceberg_database': ''},
        ])
        content_row = MagicMock()
        content_row.content = excel_bytes
        df_mock = MagicMock()
        df_mock.select.return_value.first.return_value = content_row
        mock_spark.read.format.return_value.load.return_value = df_mock

        result = m.parse_iceberg_excel.function('s3a://b/f.xlsx', sample_iceberg_run_id, spark=mock_spark)
        assert result[0]['inplace_migration'] is False
        assert result[0]['destination_iceberg_database'] == 'sales_s3_iceberg'

    def test_inplace_migration_flag_true(self, mock_spark, sample_iceberg_run_id):
        m = _import_module()
        excel_bytes = self._make_excel_bytes([
            {'database': 'sales_s3', 'table': '*', 'inplace_migration': 'T', 'destination_iceberg_database': ''},
        ])
        content_row = MagicMock()
        content_row.content = excel_bytes
        df_mock = MagicMock()
        df_mock.select.return_value.first.return_value = content_row
        mock_spark.read.format.return_value.load.return_value = df_mock

        result = m.parse_iceberg_excel.function('s3a://b/f.xlsx', sample_iceberg_run_id, spark=mock_spark)
        assert result[0]['inplace_migration'] is True
        assert result[0]['destination_iceberg_database'] == 'sales_s3'  # same db for inplace

    def test_custom_dest_database(self, mock_spark, sample_iceberg_run_id):
        m = _import_module()
        excel_bytes = self._make_excel_bytes([
            {'database': 'sales_s3', 'table': '*', 'inplace_migration': 'F', 'destination_iceberg_database': 'custom_ice_db'},
        ])
        content_row = MagicMock()
        content_row.content = excel_bytes
        df_mock = MagicMock()
        df_mock.select.return_value.first.return_value = content_row
        mock_spark.read.format.return_value.load.return_value = df_mock

        result = m.parse_iceberg_excel.function('s3a://b/f.xlsx', sample_iceberg_run_id, spark=mock_spark)
        assert result[0]['destination_iceberg_database'] == 'custom_ice_db'

    def test_run_id_in_config(self, mock_spark, sample_iceberg_run_id):
        m = _import_module()
        excel_bytes = self._make_excel_bytes([
            {'database': 'db1', 'table': '*', 'inplace_migration': 'F', 'destination_iceberg_database': ''},
        ])
        content_row = MagicMock()
        content_row.content = excel_bytes
        df_mock = MagicMock()
        df_mock.select.return_value.first.return_value = content_row
        mock_spark.read.format.return_value.load.return_value = df_mock

        result = m.parse_iceberg_excel.function('s3a://b/f.xlsx', sample_iceberg_run_id, spark=mock_spark)
        assert result[0]['run_id'] == sample_iceberg_run_id


# ---------------------------------------------------------------------------
# discover_hive_tables
# ---------------------------------------------------------------------------
class TestDiscoverHiveTables:

    def test_discovers_all_tables_with_wildcard(self, mock_spark, sample_iceberg_db_config):
        m = _import_module()

        def sql_router(sql):
            df = MagicMock()
            sql_lower = sql.lower()
            if 'show tables' in sql_lower:
                r1, r2 = MagicMock(), MagicMock()
                r1.tableName = 'transactions'
                r2.tableName = 'orders'
                df.collect.return_value = [r1, r2]
            elif 'describe formatted' in sql_lower:
                loc_row = MagicMock()
                loc_row.col_name = 'Location'
                loc_row.data_type = 's3a://test-bucket/db/table'
                df.collect.return_value = [loc_row]
            return df

        mock_spark.sql.side_effect = sql_router

        result = m.discover_hive_tables.function.__wrapped__(
            db_config=sample_iceberg_db_config, spark=mock_spark
        )
        assert len(result['discovered_tables']) == 2
        assert result['discovered_tables'][0]['table'] == 'transactions'

    def test_discovers_filtered_tables_by_pattern(self, mock_spark, sample_iceberg_db_config):
        m = _import_module()
        sample_iceberg_db_config['table_pattern'] = 'trans*'

        def sql_router(sql):
            import fnmatch, re
            df = MagicMock()
            sql_lower = sql.lower()
            if 'show tables' in sql_lower:
                all_tables = ['transactions', 'orders', 'trans_history']
                like_match = re.search(r"like '([^']+)'", sql_lower)
                if like_match:
                    pattern = like_match.group(1).replace('%', '*')
                    matched = [t for t in all_tables if fnmatch.fnmatch(t, pattern)]
                else:
                    matched = all_tables
                rows = []
                for name in matched:
                    r = MagicMock()
                    r.tableName = name
                    rows.append(r)
                df.collect.return_value = rows
            elif 'describe formatted' in sql_lower:
                loc_row = MagicMock()
                loc_row.col_name = 'Location'
                loc_row.data_type = 's3a://test-bucket/db/table'
                df.collect.return_value = [loc_row]
            return df

        mock_spark.sql.side_effect = sql_router

        result = m.discover_hive_tables.function.__wrapped__(
            db_config=sample_iceberg_db_config, spark=mock_spark
        )
        table_names = [t['table'] for t in result['discovered_tables']]
        assert 'transactions' in table_names
        assert 'trans_history' in table_names
        assert 'orders' not in table_names

    def test_handles_location_fetch_error_gracefully(self, mock_spark, sample_iceberg_db_config):
        m = _import_module()

        def sql_router(sql):
            df = MagicMock()
            sql_lower = sql.lower()
            if 'show tables' in sql_lower:
                r = MagicMock()
                r.tableName = 'broken_table'
                df.collect.return_value = [r]
            elif 'describe formatted' in sql_lower:
                raise Exception("Table metadata not found")
            return df

        mock_spark.sql.side_effect = sql_router

        result = m.discover_hive_tables.function.__wrapped__(
            db_config=sample_iceberg_db_config, spark=mock_spark
        )
        # Should still include the table with discovery_error
        assert len(result['discovered_tables']) == 1
        assert 'discovery_error' in result['discovered_tables'][0]

    def test_preserves_db_config_in_result(self, mock_spark, sample_iceberg_db_config):
        m = _import_module()
        df = MagicMock()
        df.collect.return_value = []
        mock_spark.sql.return_value = df

        result = m.discover_hive_tables.function.__wrapped__(
            db_config=sample_iceberg_db_config, spark=mock_spark
        )
        assert result['source_database'] == sample_iceberg_db_config['source_database']
        assert result['run_id'] == sample_iceberg_db_config['run_id']


# ---------------------------------------------------------------------------
# migrate_tables_to_iceberg
# ---------------------------------------------------------------------------
class TestMigratesTablesToIceberg:

    def _make_count_df(self, count):
        df = MagicMock()
        row = MagicMock()
        row.__getitem__ = lambda self, k: count
        df.collect.return_value = [row]
        return df

    def test_snapshot_migration_success(self, mock_spark, sample_iceberg_discovery):
        m = _import_module()
        call_tracker = [0]

        def sql_router(sql):
            sql_lower = sql.lower()
            if 'count(*)' in sql_lower:
                return self._make_count_df(1000)
            elif 'show partitions' in sql_lower:
                df = MagicMock()
                df.collect.return_value = []
                return df
            elif 'describe formatted' in sql_lower:
                loc_row = MagicMock()
                loc_row.col_name = 'Location'
                loc_row.data_type = 's3a://test-bucket/ice/transactions'
                df = MagicMock()
                df.collect.return_value = [loc_row]
                return df
            else:
                df = MagicMock()
                df.collect.return_value = []
                df.count.return_value = 0
                return df

        mock_spark.sql.side_effect = sql_router

        result = m.migrate_tables_to_iceberg.function.__wrapped__(
            discovery=sample_iceberg_discovery,
            dag_run_id='dag_test',
            spark=mock_spark,
            **{'ti': MagicMock()}
        )

        assert result['results'][0]['status'] == 'COMPLETED'
        assert result['results'][0]['hive_count'] == 1000

    def test_inplace_migration_success(self, mock_spark, sample_iceberg_discovery):
        m = _import_module()
        sample_iceberg_discovery['inplace_migration'] = True
        sample_iceberg_discovery['destination_iceberg_database'] = 'sales_data_s3'

        def sql_router(sql):
            sql_lower = sql.lower()
            if 'count(*)' in sql_lower:
                return self._make_count_df(500)
            elif 'show partitions' in sql_lower:
                df = MagicMock()
                df.collect.return_value = []
                return df
            elif 'describe formatted' in sql_lower:
                loc_row = MagicMock()
                loc_row.col_name = 'Location'
                loc_row.data_type = 's3a://test-bucket/db/table'
                df = MagicMock()
                df.collect.return_value = [loc_row]
                return df
            else:
                df = MagicMock()
                df.collect.return_value = []
                df.count.return_value = 0
                return df

        mock_spark.sql.side_effect = sql_router

        result = m.migrate_tables_to_iceberg.function.__wrapped__(
            discovery=sample_iceberg_discovery,
            dag_run_id='dag_test',
            spark=mock_spark,
            **{'ti': MagicMock()}
        )
        assert any(r['migration_type'] == 'INPLACE' for r in result['results'])

    def test_migration_failure_recorded(self, mock_spark, sample_iceberg_discovery):
        m = _import_module()

        def sql_router(sql):
            sql_lower = sql.lower()
            if 'count(*)' in sql_lower:
                return self._make_count_df(1000)
            elif 'show partitions' in sql_lower:
                df = MagicMock()
                df.collect.return_value = []
                return df
            elif 'system.snapshot' in sql_lower or 'system.migrate' in sql_lower:
                raise Exception("Snapshot procedure failed")
            else:
                df = MagicMock()
                df.collect.return_value = []
                return df

        mock_spark.sql.side_effect = sql_router

        with pytest.raises(Exception, match="Iceberg migration failed"):
            m.migrate_tables_to_iceberg.function.__wrapped__(
                discovery=sample_iceberg_discovery,
                dag_run_id='dag_test',
                spark=mock_spark,
                **{'ti': MagicMock()}
            )


# ---------------------------------------------------------------------------
# update_migration_durations
# ---------------------------------------------------------------------------
class TestUpdateMigrationDurations:

    def test_updates_duration_in_tracking(self, mock_spark, sample_iceberg_migration_result):
        m = _import_module()
        with patch('migration_dags_combined.execute_with_iceberg_retry') as mock_retry:
            m.update_migration_durations.function(
                migration_result=sample_iceberg_migration_result,
                spark=mock_spark,
            )
        assert mock_retry.called

    def test_skips_invalid_input(self, mock_spark):
        m = _import_module()
        result = m.update_migration_durations.function(migration_result={}, spark=mock_spark)
        assert result == {}


# ---------------------------------------------------------------------------
# validate_iceberg_tables
# ---------------------------------------------------------------------------
class TestValidateIcebergTables:

    def test_validates_matching_counts(self, mock_spark, sample_iceberg_migration_result):
        m = _import_module()

        def sql_router(sql):
            sql_lower = sql.lower()
            df = MagicMock()
            if 'describe' in sql_lower:
                rows = [
                    MagicMock(col_name='id', data_type='bigint'),
                    MagicMock(col_name='amount', data_type='double'),
                ]
                df.collect.return_value = rows
            else:
                df.collect.return_value = []
            return df

        mock_spark.sql.side_effect = sql_router

        result = m.validate_iceberg_tables.function.__wrapped__(
            migration_result=sample_iceberg_migration_result,
            spark=mock_spark,
            **{'ti': MagicMock()}
        )
        assert result['validation_results'][0]['status'] == 'COMPLETED'
        assert result['validation_results'][0]['row_count_match'] is True

    def test_detects_schema_mismatch(self, mock_spark, sample_iceberg_migration_result):
        m = _import_module()

        call_tracker = [0]

        def sql_router(sql):
            sql_lower = sql.lower()
            df = MagicMock()
            if 'describe' in sql_lower:
                call_tracker[0] += 1
                if call_tracker[0] % 2 == 1:  # source schema
                    rows = [MagicMock(col_name='id', data_type='bigint')]
                else:  # dest schema - different type
                    rows = [MagicMock(col_name='id', data_type='int')]
                df.collect.return_value = rows
            else:
                df.collect.return_value = []
            return df

        mock_spark.sql.side_effect = sql_router

        result = m.validate_iceberg_tables.function.__wrapped__(
            migration_result=sample_iceberg_migration_result,
            spark=mock_spark,
            **{'ti': MagicMock()}
        )
        assert result['validation_results'][0]['schema_match'] is False

    def test_skips_invalid_input(self, mock_spark):
        m = _import_module()
        result = m.validate_iceberg_tables.function.__wrapped__(
            migration_result={},
            spark=mock_spark,
            **{'ti': MagicMock()}
        )
        assert result == {}

    def test_skips_failed_migration_results(self, mock_spark, sample_iceberg_migration_result):
        m = _import_module()
        sample_iceberg_migration_result['results'][0]['status'] = 'FAILED'

        result = m.validate_iceberg_tables.function.__wrapped__(
            migration_result=sample_iceberg_migration_result,
            spark=mock_spark,
            **{'ti': MagicMock()}
        )
        assert result['validation_results'] == []


# ---------------------------------------------------------------------------
# update_iceberg_validation_status
# ---------------------------------------------------------------------------
class TestUpdateIcebergValidationStatus:

    def _make_validation_result(self, sample_iceberg_migration_result):
        return {
            **sample_iceberg_migration_result,
            'validation_results': [
                {
                    'source_table': 'transactions',
                    'destination_table': 'sales_data_s3_iceberg.transactions',
                    'status': 'COMPLETED',
                    'source_hive_row_count': 1000,
                    'dest_iceberg_row_count': 1000,
                    'row_count_match': True,
                    'source_hive_partition_count': 2,
                    'dest_iceberg_partition_count': 2,
                    'partition_count_match': True,
                    'schema_match': True,
                    'schema_differences': '',
                    'per_table_validation_duration': 4.5,
                    'error': None,
                }
            ],
            '_task_duration': 4.5,
        }

    def test_sets_validated_status(self, mock_spark, sample_iceberg_migration_result):
        m = _import_module()
        vr = self._make_validation_result(sample_iceberg_migration_result)

        with patch('migration_dags_combined.execute_with_iceberg_retry') as mock_retry:
            m.update_iceberg_validation_status.function(validation_result=vr, spark=mock_spark)

        update_calls = [str(c) for c in mock_retry.call_args_list]
        assert any('VALIDATED' in c for c in update_calls)

    def test_sets_validation_failed_on_mismatch(self, mock_spark, sample_iceberg_migration_result):
        m = _import_module()
        vr = self._make_validation_result(sample_iceberg_migration_result)
        vr['validation_results'][0]['row_count_match'] = False

        with patch('migration_dags_combined.execute_with_iceberg_retry') as mock_retry:
            m.update_iceberg_validation_status.function(validation_result=vr, spark=mock_spark)

        update_calls = [str(c) for c in mock_retry.call_args_list]
        assert any('VALIDATION_FAILED' in c for c in update_calls)

    def test_skips_invalid_input(self, mock_spark):
        m = _import_module()
        result = m.update_iceberg_validation_status.function(validation_result={}, spark=mock_spark)
        assert result == {}


# ---------------------------------------------------------------------------
# generate_iceberg_html_report
# ---------------------------------------------------------------------------
class TestGenerateIcebergHtmlReport:

    def _setup_spark(self, mock_spark):
        tbl_row = MagicMock()
        tbl_row.source_database = 'sales_s3'
        tbl_row.source_table = 'transactions'
        tbl_row.migration_type = 'SNAPSHOT'
        tbl_row.destination_table = 'sales_s3_iceberg.transactions'
        tbl_row.status = 'VALIDATED'
        tbl_row.migration_duration_seconds = 45.0
        tbl_row.validation_duration_seconds = 4.5
        tbl_row.validation_status = 'COMPLETED'
        tbl_row.row_count_match = True
        tbl_row.partition_count_match = True
        tbl_row.schema_match = True
        tbl_row.source_hive_row_count = 1000
        tbl_row.destination_iceberg_row_count = 1000
        tbl_row.source_hive_partition_count = 2
        tbl_row.dest_iceberg_partition_count = 2

        # validation summary
        ivs_row = MagicMock()
        ivs_row.total_tables_validated = 1
        ivs_row.tables_passed_validation = 1
        ivs_row.tables_failed_validation = 0
        ivs_row.total_row_count_mismatches = 0
        ivs_row.total_partition_count_mismatches = 0
        ivs_row.total_schema_mismatches = 0
        ivs_row.__getitem__ = lambda self, k: 1 if k == 'total_tables_validated' else 0

        def sql_router(sql):
            df = MagicMock()
            sql_lower = sql.lower()
            if 'order by' in sql_lower:
                df.collect.return_value = [tbl_row]
            elif 'sum(case when row_count_match' in sql_lower:
                df.collect.return_value = [ivs_row]
            else:
                df.collect.return_value = []
            return df

        mock_spark.sql.side_effect = sql_router

    def test_generates_report_path(self, mock_spark, sample_iceberg_run_id):
        m = _import_module()
        self._setup_spark(mock_spark)
        result = m.generate_iceberg_html_report.function(run_id=sample_iceberg_run_id, spark=mock_spark)
        assert 'report_path' in result
        assert sample_iceberg_run_id in result['report_path']
        assert result['report_path'].endswith('.html')

    def test_writes_to_s3(self, mock_spark, sample_iceberg_run_id):
        m = _import_module()
        self._setup_spark(mock_spark)
        m.generate_iceberg_html_report.function(run_id=sample_iceberg_run_id, spark=mock_spark)
        fs_mock = mock_spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value
        assert fs_mock.create.called


# ---------------------------------------------------------------------------
# send_iceberg_report_email
# ---------------------------------------------------------------------------
class TestSendIcebergReportEmail:

    def test_skips_when_no_recipients(self, mock_spark, sample_iceberg_run_id):
        m = _import_module()
        with patch('airflow.models.Variable.get', return_value=''):
            result = m.send_iceberg_report_email.function(
                report_result={'report_path': 's3a://bucket/report.html'},
                run_id=sample_iceberg_run_id,
                spark=mock_spark,
            )
        assert result['sent'] is False

    def test_sends_when_recipients_configured(self, mock_spark, sample_iceberg_run_id):
        m = _import_module()
        reader_mock = MagicMock()
        reader_mock.readLine.side_effect = ['<html>iceberg report</html>', None]
        mock_spark._jvm.java.io.BufferedReader.return_value = reader_mock

        with patch('airflow.utils.email.send_email') as mock_send, \
             patch('tempfile.NamedTemporaryFile') as mock_tmp, \
             patch('os.unlink'):
            tmp_inst = MagicMock()
            tmp_inst.name = '/tmp/ice_report.html'
            mock_tmp.return_value.__enter__ = MagicMock(return_value=tmp_inst)
            mock_tmp.return_value.__exit__ = MagicMock(return_value=False)

            result = m.send_iceberg_report_email.function(
                report_result={'report_path': 's3a://bucket/report.html'},
                run_id=sample_iceberg_run_id,
                spark=mock_spark,
            )

        assert result['sent'] is True


# ---------------------------------------------------------------------------
# finalize_iceberg_run
# ---------------------------------------------------------------------------
class TestFinalizeIcebergRun:

    def test_returns_completed_status(self, mock_spark, sample_iceberg_run_id):
        m = _import_module()
        stats_row = MagicMock()
        stats_row.__getitem__ = lambda self, k: {
            'total': 2, 'successful': 2, 'failed': 0, 'skipped': 0, 'count_mismatches': 0
        }[k]

        type_row = MagicMock()
        type_row.__getitem__ = lambda self, k: 'SNAPSHOT'

        call_count = [0]

        def sql_router(sql):
            df = MagicMock()
            call_count[0] += 1
            if 'count(*)' in sql.lower() and 'count_mismatches' in sql.lower():
                df.collect.return_value = [stats_row]
            elif 'migration_type' in sql.lower():
                df.collect.return_value = [type_row]
            else:
                df.collect.return_value = []
            return df

        mock_spark.sql.side_effect = sql_router

        result = m.finalize_iceberg_run.function(run_id=sample_iceberg_run_id, spark=mock_spark)
        assert result['status'] == 'COMPLETED'

    def test_updates_runs_table(self, mock_spark, sample_iceberg_run_id):
        m = _import_module()
        stats_row = MagicMock()
        stats_row.__getitem__ = lambda self, k: 1

        type_row = MagicMock()
        type_row.__getitem__ = lambda self, k: 'SNAPSHOT'

        def sql_router(sql):
            df = MagicMock()
            if 'count_mismatches' in sql.lower():
                df.collect.return_value = [stats_row]
            elif 'migration_type' in sql.lower():
                df.collect.return_value = [type_row]
            else:
                df.collect.return_value = []
            return df

        mock_spark.sql.side_effect = sql_router

        m.finalize_iceberg_run.function(run_id=sample_iceberg_run_id, spark=mock_spark)
        sql_calls = [str(c) for c in mock_spark.sql.call_args_list]
        assert any('COMPLETED' in c for c in sql_calls)
        assert any('iceberg_migration_runs' in c.lower() for c in sql_calls)
