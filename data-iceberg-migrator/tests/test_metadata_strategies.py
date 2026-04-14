"""Tests for metadata migration strategy functions."""

import json
from unittest.mock import MagicMock

# ─────────────────────────────────────────────────────────────────────────────
# Helpers
# ─────────────────────────────────────────────────────────────────────────────

SAMPLE_ICEBERG_METADATA = {
    'format-version': 2,
    'table-uuid': 'test-uuid-1234',
    'location': 's3a://bucket/warehouse/orders',
    'current-schema-id': 0,
    'schemas': [{
        'schema-id': 0,
        'type': 'struct',
        'fields': [
            {'id': 1, 'name': 'id', 'required': False, 'type': 'long'},
            {'id': 2, 'name': 'amount', 'required': False, 'type': 'double'},
            {'id': 3, 'name': 'dt', 'required': False, 'type': 'date'},
        ],
    }],
    'default-spec-id': 0,
    'partition-specs': [{
        'spec-id': 0,
        'fields': [
            {'source-id': 3, 'field-id': 1000, 'name': 'dt', 'transform': 'identity'},
        ],
    }],
    'current-snapshot-id': 100,
    'snapshots': [{
        'snapshot-id': 100,
        'timestamp-ms': 1700000000000,
        'summary': {
            'operation': 'append',
            'total-records': '5000',
            'total-data-files': '10',
        },
        'manifest-list': 's3a://bucket/warehouse/orders/metadata/snap-100.avro',
    }],
    'properties': {
        'write.format.default': 'parquet',
    },
}


def _mock_fs_for_iceberg(spark, metadata_dict, has_version_hint=True):
    """Set up mock Hadoop FS to serve Iceberg metadata files."""
    fs_mock = spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value

    def mock_exists(path):
        path_str = str(path)
        if 'version-hint.text' in path_str:
            return has_version_hint
        return True

    fs_mock.exists.side_effect = mock_exists

    # Mock reading version-hint.text
    hint_reader = MagicMock()
    hint_reader.readLine.return_value = '1'

    # Mock reading metadata.json
    meta_json = json.dumps(metadata_dict)
    meta_lines = meta_json.split('\n')
    meta_reader = MagicMock()
    meta_reader.readLine.side_effect = meta_lines + [None]

    call_count = {'n': 0}
    def mock_buffered_reader(input_stream_reader):
        call_count['n'] += 1
        if call_count['n'] == 1 and has_version_hint:
            return hint_reader
        return meta_reader

    spark._jvm.java.io.BufferedReader.side_effect = mock_buffered_reader

    # FS stats
    summary = MagicMock()
    summary.getFileCount.return_value = 10
    summary.getLength.return_value = 50 * 1024 * 1024
    fs_mock.getContentSummary.return_value = summary

    # listStatus fallback (when no version-hint.text)
    if not has_version_hint:
        file_status = MagicMock()
        file_path = MagicMock()
        file_path.getName.return_value = 'v1.metadata.json'
        file_path.toString.return_value = 's3a://bucket/warehouse/orders/metadata/v1.metadata.json'
        file_status.getPath.return_value = file_path
        fs_mock.listStatus.return_value = [file_status]


# ─────────────────────────────────────────────────────────────────────────────
# Test iceberg_to_iceberg parse_excel_rows
# ─────────────────────────────────────────────────────────────────────────────

class TestIcebergParseExcelRows:

    def test_basic_parsing(self):
        import pandas as pd
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import parse_excel_rows

        df = pd.DataFrame([{
            'database': 'warehouse',
            'table': 'orders',
            'dest_database': 'analytics',
            'dest_bucket': 's3a://dest-bkt',
            'source_s3_prefix': '',
            'dest_s3_prefix': '',
            'source_table_path': 's3a://bucket/warehouse/orders',
        }])
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

        config = {'default_s3_bucket': 's3a://default-bucket'}
        result = parse_excel_rows(df, config, 'run_123')

        assert len(result) == 1
        assert result[0]['source_database'] == 'warehouse'
        assert result[0]['dest_database'] == 'analytics'
        assert len(result[0]['table_entries']) == 1
        assert result[0]['table_entries'][0]['table_name'] == 'orders'
        assert result[0]['table_entries'][0]['source_table_path'] == 's3a://bucket/warehouse/orders'
        assert result[0]['run_id'] == 'run_123'

    def test_skips_rows_without_source_table_path(self):
        import pandas as pd
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import parse_excel_rows

        df = pd.DataFrame([{
            'database': 'warehouse',
            'table': 'orders',
            'dest_database': 'analytics',
            'dest_bucket': 's3a://dest-bkt',
            'source_s3_prefix': '',
            'dest_s3_prefix': '',
            'source_table_path': '',
        }])
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

        config = {'default_s3_bucket': 's3a://default-bucket'}
        result = parse_excel_rows(df, config, 'run_123')
        assert len(result) == 0

    def test_groups_by_dest_database(self):
        import pandas as pd
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import parse_excel_rows

        df = pd.DataFrame([
            {'database': 'wh', 'table': 'orders', 'dest_database': 'analytics',
             'dest_bucket': 's3a://bkt', 'source_s3_prefix': '', 'dest_s3_prefix': '',
             'source_table_path': 's3a://bucket/wh/orders'},
            {'database': 'wh', 'table': 'users', 'dest_database': 'analytics',
             'dest_bucket': 's3a://bkt', 'source_s3_prefix': '', 'dest_s3_prefix': '',
             'source_table_path': 's3a://bucket/wh/users'},
        ])
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

        config = {'default_s3_bucket': 's3a://default-bucket'}
        result = parse_excel_rows(df, config, 'run_123')

        assert len(result) == 1
        assert len(result[0]['table_entries']) == 2

    def test_normalizes_s3_paths(self):
        import pandas as pd
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import parse_excel_rows

        df = pd.DataFrame([{
            'database': 'wh', 'table': 'orders', 'dest_database': 'analytics',
            'dest_bucket': 's3://bkt', 'source_s3_prefix': '', 'dest_s3_prefix': '',
            'source_table_path': 's3n://bucket/wh/orders',
        }])
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

        config = {'default_s3_bucket': 's3a://default-bucket'}
        result = parse_excel_rows(df, config, 'run_123')

        assert result[0]['dest_bucket'] == 's3a://bkt'
        assert result[0]['table_entries'][0]['source_table_path'] == 's3a://bucket/wh/orders'

    def test_skips_row_with_both_dest_bucket_and_prefix_pair(self):
        import pandas as pd
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import parse_excel_rows

        df = pd.DataFrame([{
            'database': 'wh', 'table': 'orders', 'dest_database': 'analytics',
            'dest_bucket': 's3a://explicit-bkt',
            'source_s3_prefix': 's3a://src/data',
            'dest_s3_prefix': 's3a://dst/data',
            'source_table_path': 's3a://src/data/wh/orders',
        }])
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

        config = {'default_s3_bucket': 's3a://default-bucket'}
        result = parse_excel_rows(df, config, 'run_123')
        assert len(result) == 0


# ─────────────────────────────────────────────────────────────────────────────
# Test iceberg_to_iceberg discover_tables
# ─────────────────────────────────────────────────────────────────────────────

class TestIcebergDiscoverTables:

    def test_discovers_table_from_metadata(self, mock_spark):
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import discover_tables
        from utils.migrations.shared import get_config

        _mock_fs_for_iceberg(mock_spark, SAMPLE_ICEBERG_METADATA)

        db_config = {
            'source_database': 'warehouse',
            'dest_database': 'analytics',
            'dest_bucket': 's3a://dest-bkt',
            'source_s3_prefix': '',
            'dest_s3_prefix': '',
            'table_entries': [
                {'table_name': 'orders', 'source_table_path': 's3a://bucket/warehouse/orders'},
            ],
            'run_id': 'run_123',
        }
        config = get_config()
        result = discover_tables(db_config, mock_spark, config)

        assert len(result) == 1
        tbl = result[0]
        assert tbl['source_table'] == 'orders'
        assert tbl['source_database'] == 'warehouse'
        assert tbl['dest_database'] == 'analytics'
        assert tbl['source_location'] == 's3a://bucket/warehouse/orders'
        assert tbl['source_row_count'] == 5000
        assert tbl['is_partitioned'] is True
        assert 'dt' in tbl['partition_columns']

    def test_extracts_schema_correctly(self, mock_spark):
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import discover_tables
        from utils.migrations.shared import get_config

        _mock_fs_for_iceberg(mock_spark, SAMPLE_ICEBERG_METADATA)

        db_config = {
            'source_database': 'wh', 'dest_database': 'dest',
            'dest_bucket': 's3a://bkt', 'source_s3_prefix': '', 'dest_s3_prefix': '',
            'table_entries': [{'table_name': 'orders', 'source_table_path': 's3a://bucket/wh/orders'}],
            'run_id': 'run_123',
        }
        result = discover_tables(db_config, mock_spark, get_config())

        schema = result[0]['schema']
        names = [c['name'] for c in schema]
        assert 'id' in names
        assert 'amount' in names
        assert 'dt' in names
        id_col = next(c for c in schema if c['name'] == 'id')
        assert id_col['type'] == 'BIGINT'

    def test_handles_missing_version_hint(self, mock_spark):
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import discover_tables
        from utils.migrations.shared import get_config

        _mock_fs_for_iceberg(mock_spark, SAMPLE_ICEBERG_METADATA, has_version_hint=False)

        db_config = {
            'source_database': 'wh', 'dest_database': 'dest',
            'dest_bucket': 's3a://bkt', 'source_s3_prefix': '', 'dest_s3_prefix': '',
            'table_entries': [{'table_name': 'orders', 'source_table_path': 's3a://bucket/wh/orders'}],
            'run_id': 'run_123',
        }
        result = discover_tables(db_config, mock_spark, get_config())
        assert len(result) == 1
        assert 'error' not in result[0]

    def test_records_error_on_metadata_failure(self, mock_spark):
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import discover_tables
        from utils.migrations.shared import get_config

        fs_mock = mock_spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value
        fs_mock.exists.side_effect = Exception("S3 connection refused")

        db_config = {
            'source_database': 'wh', 'dest_database': 'dest',
            'dest_bucket': 's3a://bkt', 'source_s3_prefix': '', 'dest_s3_prefix': '',
            'table_entries': [{'table_name': 'orders', 'source_table_path': 's3a://bucket/wh/orders'}],
            'run_id': 'run_123',
        }
        result = discover_tables(db_config, mock_spark, get_config())
        assert len(result) == 1
        assert 'error' in result[0]
        assert result[0]['source_table'] == 'orders'

    def test_uses_prefix_remapping_for_dest_path(self, mock_spark):
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import discover_tables
        from utils.migrations.shared import get_config

        _mock_fs_for_iceberg(mock_spark, SAMPLE_ICEBERG_METADATA)

        db_config = {
            'source_database': 'wh', 'dest_database': 'dest',
            'dest_bucket': 's3a://dest-bkt',
            'source_s3_prefix': 's3a://bucket/warehouse',
            'dest_s3_prefix': 's3a://dest-bkt/warehouse',
            'table_entries': [{'table_name': 'orders', 'source_table_path': 's3a://bucket/warehouse/orders'}],
            'run_id': 'run_123',
        }
        result = discover_tables(db_config, mock_spark, get_config())
        assert result[0]['dest_location'] == 's3a://dest-bkt/warehouse/orders'

    def test_data_stays_in_place_without_prefix(self, mock_spark):
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import discover_tables
        from utils.migrations.shared import get_config

        _mock_fs_for_iceberg(mock_spark, SAMPLE_ICEBERG_METADATA)

        db_config = {
            'source_database': 'wh', 'dest_database': 'dest',
            'dest_bucket': 's3a://dest-bkt',
            'source_s3_prefix': '', 'dest_s3_prefix': '',
            'table_entries': [{'table_name': 'orders', 'source_table_path': 's3a://bucket/warehouse/orders'}],
            'run_id': 'run_123',
        }
        result = discover_tables(db_config, mock_spark, get_config())
        assert result[0]['dest_location'] == 's3a://bucket/warehouse/orders'


# ─────────────────────────────────────────────────────────────────────────────
# Test iceberg_to_iceberg create_dest_table
# ─────────────────────────────────────────────────────────────────────────────

class TestIcebergCreateDestTable:

    def test_registers_new_table(self, mock_spark):
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import create_dest_table
        from utils.migrations.shared import get_config

        def sql_router(sql):
            sl = sql.lower().strip()
            if sl.startswith('describe '):
                raise Exception("Table not found")
            return MagicMock()
        mock_spark.sql.side_effect = sql_router

        table_info = {
            'source_table': 'orders',
            'dest_location': 's3a://bucket/warehouse/orders',
            'file_format': 'PARQUET',
        }
        result = create_dest_table(table_info, 'analytics', mock_spark, get_config())

        assert result['status'] == 'COMPLETED'
        assert result['existed'] is False
        calls_str = ' '.join(str(c) for c in mock_spark.sql.call_args_list)
        assert 'register_table' in calls_str
        assert 'analytics.orders' in calls_str

    def test_refreshes_existing_table(self, mock_spark):
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import create_dest_table
        from utils.migrations.shared import get_config

        mock_spark.sql.return_value = MagicMock()  # DESCRIBE succeeds

        table_info = {
            'source_table': 'orders',
            'dest_location': 's3a://bucket/warehouse/orders',
            'file_format': 'PARQUET',
        }
        result = create_dest_table(table_info, 'analytics', mock_spark, get_config())

        assert result['status'] == 'COMPLETED'
        assert result['existed'] is True
        calls_str = ' '.join(str(c) for c in mock_spark.sql.call_args_list)
        assert 'REFRESH TABLE' in calls_str

    def test_returns_error_on_failure(self, mock_spark):
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import create_dest_table
        from utils.migrations.shared import get_config

        def sql_router(sql):
            sl = sql.lower().strip()
            if sl.startswith('describe '):
                raise Exception("Table not found")
            if 'register_table' in sl:
                raise Exception("Permission denied")
            return MagicMock()
        mock_spark.sql.side_effect = sql_router

        table_info = {
            'source_table': 'orders',
            'dest_location': 's3a://bucket/warehouse/orders',
            'file_format': 'PARQUET',
        }
        result = create_dest_table(table_info, 'analytics', mock_spark, get_config())

        assert result['status'] == 'FAILED'
        assert 'Permission denied' in result['error']


# ─────────────────────────────────────────────────────────────────────────────
# Test Iceberg type mapping
# ─────────────────────────────────────────────────────────────────────────────

class TestIcebergTypeMapping:

    def test_primitive_types(self):
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import _map_iceberg_type

        assert _map_iceberg_type('boolean') == 'BOOLEAN'
        assert _map_iceberg_type('int') == 'INT'
        assert _map_iceberg_type('long') == 'BIGINT'
        assert _map_iceberg_type('float') == 'FLOAT'
        assert _map_iceberg_type('double') == 'DOUBLE'
        assert _map_iceberg_type('string') == 'STRING'
        assert _map_iceberg_type('binary') == 'BINARY'
        assert _map_iceberg_type('date') == 'DATE'
        assert _map_iceberg_type('timestamp') == 'TIMESTAMP'
        assert _map_iceberg_type('timestamptz') == 'TIMESTAMP'
        assert _map_iceberg_type('uuid') == 'STRING'

    def test_decimal_type(self):
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import _map_iceberg_type

        assert _map_iceberg_type('decimal(10,2)') == 'DECIMAL(10,2)'

    def test_fixed_type(self):
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import _map_iceberg_type

        assert _map_iceberg_type('fixed[16]') == 'BINARY'

    def test_nested_type_maps_to_string(self):
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import _map_iceberg_type

        assert _map_iceberg_type({'type': 'struct', 'fields': []}) == 'STRING'


# ─────────────────────────────────────────────────────────────────────────────
# Test Iceberg metadata extraction helpers (pure functions, no mocking)
# ─────────────────────────────────────────────────────────────────────────────

class TestExtractSchema:

    def test_extracts_fields_from_current_schema(self):
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import _extract_schema

        metadata = {
            'current-schema-id': 1,
            'schemas': [
                {'schema-id': 0, 'fields': [{'name': 'old', 'type': 'string'}]},
                {'schema-id': 1, 'fields': [
                    {'name': 'id', 'type': 'long'},
                    {'name': 'name', 'type': 'string'},
                ]},
            ],
        }
        result = _extract_schema(metadata)
        assert result == [
            {'name': 'id', 'type': 'BIGINT'},
            {'name': 'name', 'type': 'STRING'},
        ]

    def test_falls_back_to_last_schema_if_id_not_found(self):
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import _extract_schema

        metadata = {
            'current-schema-id': 99,
            'schemas': [
                {'schema-id': 0, 'fields': [{'name': 'a', 'type': 'int'}]},
            ],
        }
        result = _extract_schema(metadata)
        assert result == [{'name': 'a', 'type': 'INT'}]

    def test_returns_empty_list_when_no_schemas(self):
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import _extract_schema

        assert _extract_schema({'schemas': []}) == []
        assert _extract_schema({}) == []


class TestExtractPartitionSpec:

    def test_extracts_partition_columns(self):
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import _extract_partition_spec

        metadata = {
            'current-schema-id': 0,
            'schemas': [{'schema-id': 0, 'fields': [
                {'id': 1, 'name': 'id', 'type': 'long'},
                {'id': 2, 'name': 'dt', 'type': 'date'},
            ]}],
            'default-spec-id': 0,
            'partition-specs': [{'spec-id': 0, 'fields': [
                {'source-id': 2, 'field-id': 1000, 'name': 'dt', 'transform': 'identity'},
            ]}],
        }
        cols, is_part = _extract_partition_spec(metadata)
        assert cols == ['dt']
        assert is_part is True

    def test_unpartitioned_table(self):
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import _extract_partition_spec

        metadata = {
            'current-schema-id': 0,
            'schemas': [{'schema-id': 0, 'fields': [{'id': 1, 'name': 'id', 'type': 'long'}]}],
            'default-spec-id': 0,
            'partition-specs': [{'spec-id': 0, 'fields': []}],
        }
        cols, is_part = _extract_partition_spec(metadata)
        assert cols == []
        assert is_part is False

    def test_no_partition_specs_at_all(self):
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import _extract_partition_spec

        cols, is_part = _extract_partition_spec({})
        assert cols == []
        assert is_part is False


class TestExtractRowCount:

    def test_extracts_count_from_current_snapshot(self):
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import _extract_row_count

        metadata = {
            'current-snapshot-id': 42,
            'snapshots': [
                {'snapshot-id': 41, 'summary': {'total-records': '100'}},
                {'snapshot-id': 42, 'summary': {'total-records': '5000'}},
            ],
        }
        assert _extract_row_count(metadata) == 5000

    def test_returns_zero_when_no_snapshots(self):
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import _extract_row_count

        assert _extract_row_count({}) == 0
        assert _extract_row_count({'current-snapshot-id': None}) == 0

    def test_returns_zero_when_snapshot_id_not_found(self):
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import _extract_row_count

        metadata = {
            'current-snapshot-id': 99,
            'snapshots': [{'snapshot-id': 1, 'summary': {'total-records': '100'}}],
        }
        assert _extract_row_count(metadata) == 0
