"""DAG 4 Task Tests: s3_to_s3_metadata_migration pipeline."""

from unittest.mock import MagicMock, patch

import migration_dag_metadata as m
import pytest

from .helpers import make_excel_bytes, setup_spark_excel

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

def _make_tracking_row(**overrides):
    """Build a MagicMock that mimics a tracking-table row with sensible defaults."""
    defaults = dict(
        table_create_status='COMPLETED',
        data_presence_status='CONFIRMED',
        overall_status='TABLE_CREATED',
        error_message=None,
        source_row_count=1000,
        source_partition_count=2,
        validation_status=None,
    )
    defaults.update(overrides)
    row = MagicMock()
    row.__getitem__.side_effect = lambda k: defaults[k]
    for k, v in defaults.items():
        setattr(row, k, v)
    return row


# ─────────────────────────────────────────────────────────────────────────────
# TestInitS3TrackingTables
# ─────────────────────────────────────────────────────────────────────────────

class TestInitS3TrackingTables:

    def test_creates_database_and_tables(self, mock_spark):
        result = m.init_s3_tracking_tables.function(spark=mock_spark)
        assert result == {'status': 'initialized', 'database': 'migration_tracking'}
        sql = ' '.join(str(c) for c in mock_spark.sql.call_args_list).lower()
        assert 's3_migration_runs' in sql
        assert 's3_migration_table_status' in sql
        assert 'using iceberg' in sql

    def test_creates_database_first(self, mock_spark):
        m.init_s3_tracking_tables.function(spark=mock_spark)
        first_call = str(mock_spark.sql.call_args_list[0]).lower()
        assert 'create database' in first_call


# ─────────────────────────────────────────────────────────────────────────────
# TestCreateS3MigrationRun
# ─────────────────────────────────────────────────────────────────────────────

class TestCreateS3MigrationRun:

    def test_returns_run_id_with_prefix(self, mock_spark):
        run_id = m.create_s3_migration_run.function(
            excel_file_path='s3a://bucket/s3_meta.xlsx',
            dag_run_id='dag_run_123',
            spark=mock_spark,
        )
        assert run_id.startswith('s3_run_')
        assert len(run_id) > 10

    def test_inserts_running_status(self, mock_spark):
        m.create_s3_migration_run.function(
            excel_file_path='s3a://bucket/s3_meta.xlsx',
            dag_run_id='dag_run_123',
            spark=mock_spark,
        )
        all_sql = ' '.join(str(c) for c in mock_spark.sql.call_args_list)
        assert 'INSERT INTO' in all_sql
        assert 'RUNNING' in all_sql


# ─────────────────────────────────────────────────────────────────────────────
# TestParseS3Excel
# ─────────────────────────────────────────────────────────────────────────────

class TestParseS3Excel:

    def test_basic_row_parsing(self, mock_spark, sample_s3_run_id):
        setup_spark_excel(mock_spark, make_excel_bytes([{
            'database': 'sales', 'table': '*',
            'dest_database': 'sales_dest',
            'dest_bucket': 's3a://dest-bkt',
            'source_s3_prefix': '', 'dest_s3_prefix': '',
        }]))
        result = m.parse_s3_excel.function('s3a://b/f.xlsx', sample_s3_run_id, spark=mock_spark)
        assert len(result) == 1
        assert result[0]['source_database'] == 'sales'
        assert result[0]['dest_database'] == 'sales_dest'
        assert result[0]['run_id'] == sample_s3_run_id

    @pytest.mark.parametrize("raw_bucket,expected", [
        ('s3://b',   's3a://b'),
        ('s3n://b',  's3a://b'),
        ('b',        's3a://b'),
        ('s3a://b',  's3a://b'),
    ])
    def test_normalizes_s3_prefix(self, mock_spark, sample_s3_run_id, raw_bucket, expected):
        setup_spark_excel(mock_spark, make_excel_bytes([{
            'database': 'db', 'table': '*', 'dest_database': '',
            'dest_bucket': raw_bucket, 'source_s3_prefix': '', 'dest_s3_prefix': '',
        }]))
        result = m.parse_s3_excel.function('s3a://b/f.xlsx', sample_s3_run_id, spark=mock_spark)
        assert result[0]['dest_bucket'] == expected

    def test_wildcard_collapses_specific_tokens(self, mock_spark, sample_s3_run_id):
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'database': 'db', 'table': 'tbl_a', 'dest_database': '', 'dest_bucket': 's3a://b', 'source_s3_prefix': '', 'dest_s3_prefix': ''},
            {'database': 'db', 'table': '*',     'dest_database': '', 'dest_bucket': 's3a://b', 'source_s3_prefix': '', 'dest_s3_prefix': ''},
        ]))
        result = m.parse_s3_excel.function('s3a://b/f.xlsx', sample_s3_run_id, spark=mock_spark)
        assert result[0]['table_tokens'] == ['*']

    def test_comma_separated_tables_tokenized(self, mock_spark, sample_s3_run_id):
        setup_spark_excel(mock_spark, make_excel_bytes([{
            'database': 'db', 'table': 'tbl_a,tbl_b,tbl_c',
            'dest_database': '', 'dest_bucket': 's3a://b',
            'source_s3_prefix': '', 'dest_s3_prefix': '',
        }]))
        result = m.parse_s3_excel.function('s3a://b/f.xlsx', sample_s3_run_id, spark=mock_spark)
        assert set(result[0]['table_tokens']) == {'tbl_a', 'tbl_b', 'tbl_c'}

    def test_empty_rows_skipped(self, mock_spark, sample_s3_run_id):
        setup_spark_excel(mock_spark, make_excel_bytes([
            {'database': '',      'table': '*', 'dest_database': '', 'dest_bucket': '',   'source_s3_prefix': '', 'dest_s3_prefix': ''},
            {'database': 'mydb', 'table': '*', 'dest_database': '', 'dest_bucket': 's3a://b', 'source_s3_prefix': '', 'dest_s3_prefix': ''},
        ]))
        result = m.parse_s3_excel.function('s3a://b/f.xlsx', sample_s3_run_id, spark=mock_spark)
        assert len(result) == 1
        assert result[0]['source_database'] == 'mydb'

    def test_prefix_pair_recorded(self, mock_spark, sample_s3_run_id):
        setup_spark_excel(mock_spark, make_excel_bytes([{
            'database': 'db', 'table': '*', 'dest_database': 'db2',
            'dest_bucket': '',
            'source_s3_prefix': 's3a://src/data',
            'dest_s3_prefix':   's3a://dst/data',
        }]))
        result = m.parse_s3_excel.function('s3a://b/f.xlsx', sample_s3_run_id, spark=mock_spark)
        assert result[0]['source_s3_prefix'] == 's3a://src/data'
        assert result[0]['dest_s3_prefix'] == 's3a://dst/data'


# ─────────────────────────────────────────────────────────────────────────────
# TestDiscoverSourceHiveTables
# ─────────────────────────────────────────────────────────────────────────────

class TestDiscoverSourceHiveTables:

    def _make_sql_router(self, tables, file_format='PARQUET', partitions=None, row_count=500):
        """Return a spark.sql side_effect that answers all queries discover makes."""
        if partitions is None:
            partitions = []

        def router(sql):
            sl = sql.lower().strip()
            df = MagicMock()
            if 'show tables' in sl:
                df.collect.return_value = [MagicMock(tableName=t) for t in tables]
            elif 'describe formatted' in sl:
                rows = []
                # location
                loc_row = MagicMock()
                loc_row.col_name = 'Location'
                loc_row.data_type = 's3a://src/db/tbl'
                rows.append(loc_row)
                # table type
                type_row = MagicMock()
                type_row.col_name = 'Type'
                type_row.data_type = 'EXTERNAL_TABLE'
                rows.append(type_row)
                # input format
                fmt_map = {'PARQUET': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
                           'ORC': 'org.apache.hadoop.hive.ql.io.orc.OrcInputFormat'}
                fmt_row = MagicMock()
                fmt_row.col_name = 'InputFormat:'
                fmt_row.data_type = fmt_map.get(file_format, fmt_map['PARQUET'])
                rows.append(fmt_row)
                if partitions:
                    ph = MagicMock()
                    ph.col_name = '# Partition Information'
                    ph.data_type = ''
                    rows.append(ph)
                    ch = MagicMock()
                    ch.col_name = '# col_name'
                    ch.data_type = ''
                    rows.append(ch)
                    pc = MagicMock()
                    pc.col_name = 'dt'
                    pc.data_type = 'string'
                    rows.append(pc)
                df.collect.return_value = rows
            elif 'show partitions' in sl:
                df.collect.return_value = [MagicMock(partition=p) for p in partitions]
            elif 'select count' in sl:
                count_row = MagicMock()
                count_row.__getitem__ = lambda self, k: row_count
                df.collect.return_value = [count_row]
            elif 'describe ' in sl:
                col = MagicMock()
                col.col_name = 'id'
                col.data_type = 'bigint'
                df.collect.return_value = [col]
            else:
                df.collect.return_value = []
            return df
        return router

    def test_discovers_all_tables_with_wildcard(self, mock_spark, sample_s3_db_config):
        mock_spark.sql.side_effect = self._make_sql_router(['transactions', 'orders'])
        sample_s3_db_config['table_tokens'] = ['*']
        result = m.discover_source_hive_tables.function.__wrapped__(
            db_config=sample_s3_db_config, spark=mock_spark,
        )
        assert result['source_database'] == 'sales_data'
        assert len(result['tables']) == 2

    def test_discovers_specific_table_by_token(self, mock_spark, sample_s3_db_config):
        mock_spark.sql.side_effect = self._make_sql_router(['transactions', 'orders'])
        sample_s3_db_config['table_tokens'] = ['transactions']
        result = m.discover_source_hive_tables.function.__wrapped__(
            db_config=sample_s3_db_config, spark=mock_spark,
        )
        assert len(result['tables']) == 1
        assert result['tables'][0]['source_table'] == 'transactions'

    def test_detects_orc_format(self, mock_spark, sample_s3_db_config):
        mock_spark.sql.side_effect = self._make_sql_router(['transactions'], file_format='ORC')
        result = m.discover_source_hive_tables.function.__wrapped__(
            db_config=sample_s3_db_config, spark=mock_spark,
        )
        assert result['tables'][0]['file_format'] == 'ORC'

    def test_detects_partitioned_table(self, mock_spark, sample_s3_db_config):
        mock_spark.sql.side_effect = self._make_sql_router(
            ['transactions'], partitions=['dt=2024-01-01', 'dt=2024-01-02']
        )
        result = m.discover_source_hive_tables.function.__wrapped__(
            db_config=sample_s3_db_config, spark=mock_spark,
        )
        tbl = result['tables'][0]
        assert tbl['is_partitioned'] is True
        assert tbl['partition_count'] == 2

    def test_includes_source_row_count(self, mock_spark, sample_s3_db_config):
        mock_spark.sql.side_effect = self._make_sql_router(['transactions'], row_count=999)
        result = m.discover_source_hive_tables.function.__wrapped__(
            db_config=sample_s3_db_config, spark=mock_spark,
        )
        assert result['tables'][0]['source_row_count'] == 999

    def test_compute_dest_path_via_prefix(self, mock_spark, sample_s3_db_config):
        """When source and dest prefixes are configured, dest_location should be prefix-derived."""
        sample_s3_db_config['source_s3_prefix'] = 's3a://src/data'
        sample_s3_db_config['dest_s3_prefix'] = 's3a://dst/data'

        def router(sql):
            sl = sql.lower()
            df = MagicMock()
            if 'show tables' in sl:
                df.collect.return_value = [MagicMock(tableName='transactions')]
            elif 'describe formatted' in sl:
                loc = MagicMock()
                loc.col_name = 'Location'
                loc.data_type = 's3a://src/data/sales_data/transactions'
                df.collect.return_value = [loc]
            elif 'show partitions' in sl:
                df.collect.return_value = []
            elif 'select count' in sl:
                r = MagicMock()
                r.__getitem__ = lambda self, k: 0
                df.collect.return_value = [r]
            else:
                df.collect.return_value = []
            return df

        mock_spark.sql.side_effect = router
        result = m.discover_source_hive_tables.function.__wrapped__(
            db_config=sample_s3_db_config, spark=mock_spark,
        )
        assert result['tables'][0]['dest_location'].startswith('s3a://dst/data')


# ─────────────────────────────────────────────────────────────────────────────
# TestRecordS3DiscoveredTables
# ─────────────────────────────────────────────────────────────────────────────

class TestRecordS3DiscoveredTables:

    def test_inserts_new_rows_when_none_exist(self, mock_spark, mock_iceberg_retry, sample_s3_discovery):
        cnt_row = MagicMock()
        cnt_row.__getitem__ = lambda self, k: 0
        mock_spark.sql.return_value.collect.return_value = [cnt_row]

        result = m.record_s3_discovered_tables.function(
            discovery=sample_s3_discovery, spark=mock_spark,
        )
        assert result is sample_s3_discovery
        assert mock_iceberg_retry.called
        insert_calls = [str(c) for c in mock_iceberg_retry.call_args_list if 'INSERT' in str(c)]
        assert len(insert_calls) > 0

    def test_updates_existing_rows(self, mock_spark, mock_iceberg_retry, sample_s3_discovery):
        cnt_row = MagicMock()
        cnt_row.__getitem__ = lambda self, k: 1
        mock_spark.sql.return_value.collect.return_value = [cnt_row]

        m.record_s3_discovered_tables.function(
            discovery=sample_s3_discovery, spark=mock_spark,
        )
        update_calls = [str(c) for c in mock_iceberg_retry.call_args_list if 'UPDATE' in str(c)]
        assert len(update_calls) > 0

    def test_skips_invalid_input(self, mock_spark, mock_iceberg_retry):
        result = m.record_s3_discovered_tables.function(discovery={}, spark=mock_spark)
        assert result == {}
        mock_iceberg_retry.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# TestValidateDataPresence
# ─────────────────────────────────────────────────────────────────────────────

class TestValidateDataPresence:

    def test_confirms_data_when_files_exist(self, mock_spark, sample_s3_discovery):
        # mock_spark._jvm FS is already set up with fileCount=5 in conftest
        result = m.validate_data_presence.function(
            discovery=sample_s3_discovery, spark=mock_spark,
        )
        pr = result['presence_results']
        assert len(pr) == 1
        assert pr[0]['status'] == 'CONFIRMED'
        assert pr[0]['file_count'] == 5

    def test_marks_missing_when_path_does_not_exist(self, mock_spark, sample_s3_discovery):
        fs_mock = mock_spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value
        fs_mock.exists.return_value = False

        result = m.validate_data_presence.function(
            discovery=sample_s3_discovery, spark=mock_spark,
        )
        assert result['presence_results'][0]['status'] == 'MISSING'

    def test_marks_missing_when_zero_files(self, mock_spark, sample_s3_discovery):
        fs_mock = mock_spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value
        fs_mock.exists.return_value = True
        summary = MagicMock()
        summary.getFileCount.return_value = 0
        summary.getLength.return_value = 0
        fs_mock.getContentSummary.return_value = summary

        result = m.validate_data_presence.function(
            discovery=sample_s3_discovery, spark=mock_spark,
        )
        assert result['presence_results'][0]['status'] == 'MISSING'

    def test_raises_on_fs_exception(self, mock_spark, sample_s3_discovery):
        mock_spark._jvm.org.apache.hadoop.fs.FileSystem.get.side_effect = Exception("S3 error")
        with pytest.raises(Exception, match="Data presence check FAILED"):
            m.validate_data_presence.function(
                discovery=sample_s3_discovery, spark=mock_spark, ti=MagicMock(),
            )

    def test_skips_invalid_input(self, mock_spark):
        result = m.validate_data_presence.function(discovery={}, spark=mock_spark)
        assert result == {}


# ─────────────────────────────────────────────────────────────────────────────
# TestUpdateDataPresenceStatus
# ─────────────────────────────────────────────────────────────────────────────

class TestUpdateDataPresenceStatus:

    def test_updates_confirmed_status(self, mock_spark, mock_iceberg_retry, sample_s3_presence_result):
        m.update_data_presence_status.function(
            presence_result=sample_s3_presence_result, spark=mock_spark,
        )
        calls_str = ' '.join(str(c) for c in mock_iceberg_retry.call_args_list)
        assert 'CONFIRMED' in calls_str
        assert 'DATA_CONFIRMED' in calls_str

    def test_skips_invalid_input(self, mock_spark, mock_iceberg_retry):
        result = m.update_data_presence_status.function(presence_result={}, spark=mock_spark)
        assert result == {}
        mock_iceberg_retry.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# TestCreateDestHiveTables
# ─────────────────────────────────────────────────────────────────────────────

class TestCreateDestHiveTables:

    def test_creates_new_table_when_not_existing(self, mock_spark, mock_iceberg_retry, sample_s3_presence_result):
        def sql_router(sql):
            sl = sql.lower()
            df = MagicMock()
            if sl.strip().startswith('describe ') and 'formatted' not in sl:
                raise Exception("Table not found")
            df.collect.return_value = []
            return df
        mock_spark.sql.side_effect = sql_router

        result = m.create_dest_hive_tables.function.__wrapped__(
            presence_result=sample_s3_presence_result, spark=mock_spark, ti=MagicMock(),
        )
        assert result['table_results'][0]['status'] == 'COMPLETED'
        assert result['table_results'][0]['existed'] is False

    def test_repairs_existing_table(self, mock_spark, mock_iceberg_retry, sample_s3_presence_result):
        mock_spark.sql.return_value.collect.return_value = []  # DESCRIBE doesn't raise

        result = m.create_dest_hive_tables.function.__wrapped__(
            presence_result=sample_s3_presence_result, spark=mock_spark, ti=MagicMock(),
        )
        assert result['table_results'][0]['status'] == 'COMPLETED'
        assert result['table_results'][0]['existed'] is True
        repair_calls = [c for c in mock_spark.sql.call_args_list if 'MSCK REPAIR' in str(c)]
        assert len(repair_calls) > 0

    def test_skips_table_with_missing_data(self, mock_spark, mock_iceberg_retry, sample_s3_presence_result):
        sample_s3_presence_result['presence_results'][0]['status'] = 'MISSING'

        result = m.create_dest_hive_tables.function.__wrapped__(
            presence_result=sample_s3_presence_result, spark=mock_spark, ti=MagicMock(),
        )
        assert result['table_results'][0]['status'] == 'SKIPPED'

    def test_raises_on_ddl_failure(self, mock_spark, mock_iceberg_retry, sample_s3_presence_result):
        def sql_router(sql):
            sl = sql.lower()
            df = MagicMock()
            if sl.strip().startswith('describe ') and 'formatted' not in sl:
                raise Exception("Table not found")
            if 'create external table' in sl:
                raise Exception("DDL permission denied")
            df.collect.return_value = []
            return df
        mock_spark.sql.side_effect = sql_router

        with pytest.raises(Exception, match="Hive table creation failed"):
            m.create_dest_hive_tables.function.__wrapped__(
                presence_result=sample_s3_presence_result, spark=mock_spark, ti=MagicMock(),
            )

    def test_skips_invalid_input(self, mock_spark, mock_iceberg_retry):
        result = m.create_dest_hive_tables.function.__wrapped__(
            presence_result={}, spark=mock_spark, ti=MagicMock(),
        )
        assert result == {}


# ─────────────────────────────────────────────────────────────────────────────
# TestUpdateS3TableCreateStatus
# ─────────────────────────────────────────────────────────────────────────────

class TestUpdateS3TableCreateStatus:

    def test_updates_completed_status(self, mock_spark, mock_iceberg_retry, sample_s3_table_result):
        m.update_s3_table_create_status.function(
            table_result=sample_s3_table_result, spark=mock_spark,
        )
        calls_str = ' '.join(str(c) for c in mock_iceberg_retry.call_args_list)
        assert 'COMPLETED' in calls_str
        assert 'TABLE_CREATED' in calls_str

    def test_skips_invalid_input(self, mock_spark, mock_iceberg_retry):
        result = m.update_s3_table_create_status.function(table_result={}, spark=mock_spark)
        assert result == {}
        mock_iceberg_retry.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# TestValidateS3DestinationTables
# ─────────────────────────────────────────────────────────────────────────────

class TestValidateS3DestinationTables:

    def _make_sql_router(self, dest_row_count=1000, dest_part_count=2,
                          table_create_status='COMPLETED', data_presence_status='CONFIRMED',
                          schema_cols=None):
        if schema_cols is None:
            schema_cols = [('id', 'bigint'), ('amount', 'double')]

        def router(sql):
            sl = sql.lower().strip()
            df = MagicMock()
            if 'table_create_status' in sl or 'data_presence_status' in sl:
                row = _make_tracking_row(
                    table_create_status=table_create_status,
                    data_presence_status=data_presence_status,
                )
                df.collect.return_value = [row]
            elif 'source_row_count' in sl:
                r = MagicMock()
                r.__getitem__.side_effect = lambda k: {'source_row_count': 1000, 'source_partition_count': 2}[k]
                df.collect.return_value = [r]
            elif 'count(*)' in sl:
                r = MagicMock()
                r.__getitem__ = lambda self, k: dest_row_count
                df.collect.return_value = [r]
            elif 'show partitions' in sl:
                df.count.return_value = dest_part_count
                df.collect.return_value = [MagicMock() for _ in range(dest_part_count)]
            elif sl.startswith('describe '):
                df.collect.return_value = [
                    MagicMock(col_name=name, data_type=dtype) for name, dtype in schema_cols
                ]
            else:
                df.collect.return_value = []
            return df
        return router

    def test_validates_matching_table(self, mock_spark, mock_iceberg_retry, sample_s3_table_result):
        mock_spark.sql.side_effect = self._make_sql_router()
        result = m.validate_s3_destination_tables.function.__wrapped__(
            table_result=sample_s3_table_result, spark=mock_spark, ti=MagicMock(),
        )
        vr = result['validation_results'][0]
        assert vr['status'] == 'COMPLETED'
        assert vr['row_count_match'] is True
        assert vr['schema_match'] is True

    def test_detects_row_count_mismatch(self, mock_spark, mock_iceberg_retry, sample_s3_table_result):
        mock_spark.sql.side_effect = self._make_sql_router(dest_row_count=500)
        result = m.validate_s3_destination_tables.function.__wrapped__(
            table_result=sample_s3_table_result, spark=mock_spark, ti=MagicMock(),
        )
        assert result['validation_results'][0]['row_count_match'] is False

    def test_detects_schema_mismatch(self, mock_spark, mock_iceberg_retry, sample_s3_table_result):
        mock_spark.sql.side_effect = self._make_sql_router(schema_cols=[('id', 'string')])
        result = m.validate_s3_destination_tables.function.__wrapped__(
            table_result=sample_s3_table_result, spark=mock_spark, ti=MagicMock(),
        )
        assert result['validation_results'][0]['schema_match'] is False

    def test_skips_table_with_failed_create(self, mock_spark, mock_iceberg_retry, sample_s3_table_result):
        mock_spark.sql.side_effect = self._make_sql_router(table_create_status='FAILED')
        result = m.validate_s3_destination_tables.function.__wrapped__(
            table_result=sample_s3_table_result, spark=mock_spark, ti=MagicMock(),
        )
        assert result['validation_results'][0]['status'] == 'SKIPPED'

    def test_skips_invalid_input(self, mock_spark, mock_iceberg_retry):
        result = m.validate_s3_destination_tables.function.__wrapped__(
            table_result={}, spark=mock_spark, ti=MagicMock(),
        )
        assert result == {}


# ─────────────────────────────────────────────────────────────────────────────
# TestUpdateS3ValidationStatus
# ─────────────────────────────────────────────────────────────────────────────

class TestUpdateS3ValidationStatus:

    def test_marks_validated_on_full_match(self, mock_spark, mock_iceberg_retry, sample_s3_validation_result):
        m.update_s3_validation_status.function(
            validation_result=sample_s3_validation_result, spark=mock_spark,
        )
        calls_str = ' '.join(str(c) for c in mock_iceberg_retry.call_args_list)
        assert 'VALIDATED' in calls_str

    def test_marks_validation_failed_on_mismatch(self, mock_spark, mock_iceberg_retry, sample_s3_validation_result):
        sample_s3_validation_result['validation_results'][0]['row_count_match'] = False
        sample_s3_validation_result['validation_results'][0]['error'] = 'Row count mismatch'
        m.update_s3_validation_status.function(
            validation_result=sample_s3_validation_result, spark=mock_spark,
        )
        calls_str = ' '.join(str(c) for c in mock_iceberg_retry.call_args_list)
        assert 'VALIDATION_FAILED' in calls_str

    def test_skips_invalid_input(self, mock_spark, mock_iceberg_retry):
        result = m.update_s3_validation_status.function(validation_result={}, spark=mock_spark)
        assert result == {}
        mock_iceberg_retry.assert_not_called()


# ─────────────────────────────────────────────────────────────────────────────
# TestGenerateS3HtmlReport
# ─────────────────────────────────────────────────────────────────────────────

class TestGenerateS3HtmlReport:

    def _setup_sql(self, mock_spark, run_id, overall_status='VALIDATED'):
        run_row = MagicMock()
        run_row.dag_run_id = 'test_dag_run'

        ts_row = MagicMock()
        ts_row.source_database = 'sales_data'
        ts_row.source_table = 'transactions'
        ts_row.overall_status = overall_status
        ts_row.data_presence_status = 'CONFIRMED'
        ts_row.dest_s3_location = 's3a://dest/sales_data/transactions'
        ts_row.data_presence_file_count = 5
        ts_row.data_presence_size_bytes = 10 * 1024 * 1024
        ts_row.file_format = 'PARQUET'
        ts_row.is_partitioned = True
        ts_row.partition_count = 2
        ts_row.table_already_existed = False
        ts_row.discovery_duration_seconds = 1.5
        ts_row.table_create_duration_seconds = 2.0
        ts_row.validation_duration_seconds = 1.0
        ts_row.source_row_count = 1000
        ts_row.dest_hive_row_count = 1000
        ts_row.source_partition_count = 2
        ts_row.dest_partition_count = 2
        ts_row.row_count_match = True
        ts_row.partition_count_match = True
        ts_row.schema_match = True
        ts_row.validation_status = 'COMPLETED'

        def sql_router(sql):
            df = MagicMock()
            sl = sql.lower()
            if 's3_migration_runs' in sl:
                df.collect.return_value = [run_row]
            elif 's3_migration_table_status' in sl:
                df.collect.return_value = [ts_row]
            else:
                df.collect.return_value = []
            return df

        mock_spark.sql.side_effect = sql_router

    def test_writes_html_report_to_s3(self, mock_spark, sample_s3_run_id):
        self._setup_sql(mock_spark, sample_s3_run_id)
        result = m.generate_s3_html_report.function(run_id=sample_s3_run_id, spark=mock_spark)

        assert 'report_path' in result
        assert sample_s3_run_id in result['report_path']
        assert result['report_path'].endswith('_s3_report.html')

    def test_html_written_to_filesystem(self, mock_spark, sample_s3_run_id):
        self._setup_sql(mock_spark, sample_s3_run_id)
        m.generate_s3_html_report.function(run_id=sample_s3_run_id, spark=mock_spark)

        fs_mock = mock_spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value
        assert fs_mock.create.called
        stream = fs_mock.create.return_value
        assert stream.write.called
        written_bytes = stream.write.call_args[0][0]
        html = written_bytes.decode('utf-8')
        assert sample_s3_run_id in html
        assert 'S3' in html


# ─────────────────────────────────────────────────────────────────────────────
# TestSendS3ReportEmail
# ─────────────────────────────────────────────────────────────────────────────

class TestSendS3ReportEmail:

    def test_skips_when_no_recipients_configured(self, mock_spark, sample_s3_run_id):
        with patch('migration_dag_metadata.get_config') as mock_cfg:
            mock_cfg.return_value = {
                'smtp_conn_id': 'smtp_default',
                'email_recipients': '',
            }
            result = m.send_s3_report_email.function(
                report_result={'report_path': 's3a://r/report.html'},
                run_id=sample_s3_run_id,
                spark=mock_spark,
            )
        assert result['sent'] is False
        assert result['reason'] == 'no_recipients'

    def test_sends_email_with_recipients(self, mock_spark, sample_s3_run_id):
        # Set up Hadoop FS read for the report HTML
        reader_mock = MagicMock()
        lines = iter(['<html>', '</html>', None])
        reader_mock.readLine.side_effect = lambda: next(lines)
        mock_spark._jvm.java.io.BufferedReader.return_value = reader_mock

        send_email_mock = MagicMock()
        with patch('migration_dag_metadata.get_config') as mock_cfg, \
             patch('airflow.utils.email.send_email', send_email_mock):
            mock_cfg.return_value = {
                'smtp_conn_id': 'smtp_default',
                'email_recipients': 'a@b.com,c@d.com',
            }
            result = m.send_s3_report_email.function(
                report_result={'report_path': 's3a://r/report.html'},
                run_id=sample_s3_run_id,
                spark=mock_spark,
            )
        assert result['sent'] is True
        assert len(result['recipients']) == 2


# ─────────────────────────────────────────────────────────────────────────────
# TestFinalizeS3Run
# ─────────────────────────────────────────────────────────────────────────────

class TestFinalizeS3Run:

    def _stats_router(self, total=5, successful=5, failed=0, missing=0):
        def router(sql):
            df = MagicMock()
            row = {'total': total, 'successful': successful, 'failed': failed, 'missing': missing}
            mock_row = MagicMock()
            mock_row.__getitem__.side_effect = lambda k: row[k]
            df.collect.return_value = [mock_row]
            return df
        return router

    def test_marks_completed_when_all_validated(self, mock_spark, mock_iceberg_retry, sample_s3_run_id):
        mock_spark.sql.side_effect = self._stats_router(5, 5, 0, 0)
        result = m.finalize_s3_run.function(run_id=sample_s3_run_id, spark=mock_spark)
        assert result['status'] == 'COMPLETED'
        assert result['successful'] == 5

    def test_marks_completed_with_failures_when_some_failed(self, mock_spark, mock_iceberg_retry, sample_s3_run_id):
        mock_spark.sql.side_effect = self._stats_router(5, 3, 2, 0)
        result = m.finalize_s3_run.function(run_id=sample_s3_run_id, spark=mock_spark)
        assert result['status'] == 'COMPLETED_WITH_FAILURES'

    def test_marks_completed_with_missing_when_data_absent(self, mock_spark, mock_iceberg_retry, sample_s3_run_id):
        mock_spark.sql.side_effect = self._stats_router(5, 3, 0, 2)
        result = m.finalize_s3_run.function(run_id=sample_s3_run_id, spark=mock_spark)
        assert result['status'] == 'COMPLETED_WITH_MISSING'

    def test_updates_run_record(self, mock_spark, mock_iceberg_retry, sample_s3_run_id):
        mock_spark.sql.side_effect = self._stats_router()
        m.finalize_s3_run.function(run_id=sample_s3_run_id, spark=mock_spark)
        calls_str = ' '.join(str(c) for c in mock_iceberg_retry.call_args_list)
        assert 's3_migration_runs' in calls_str.lower()
        assert sample_s3_run_id in calls_str
