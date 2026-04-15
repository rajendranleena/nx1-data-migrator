"""Tests for metadata migration strategy functions."""

import json
from unittest.mock import MagicMock

import pytest

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

    hint_reader = MagicMock()
    hint_reader.readLine.return_value = '1'

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

    summary = MagicMock()
    summary.getFileCount.return_value = 10
    summary.getLength.return_value = 50 * 1024 * 1024
    fs_mock.getContentSummary.return_value = summary

    if not has_version_hint:
        file_status = MagicMock()
        file_path = MagicMock()
        file_path.getName.return_value = 'v1.metadata.json'
        file_path.toString.return_value = 's3a://bucket/warehouse/orders/metadata/v1.metadata.json'
        file_status.getPath.return_value = file_path
        fs_mock.listStatus.return_value = [file_status]


def _mock_fs_for_table_listing(spark, table_names, dirs_without_metadata=None):
    """Set up mock Hadoop FS to return a list of table directories.

    table_names: directories that have a metadata/ subfolder (Iceberg tables).
    dirs_without_metadata: directories that lack metadata/ (should be filtered out).
    """
    dirs_without_metadata = dirs_without_metadata or []
    fs_mock = spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value

    iceberg_set = set(table_names)
    def mock_exists(path):
        path_str = str(path)
        if '/metadata' in path_str:
            return any(f"/{name}/metadata" in path_str for name in iceberg_set)
        return True
    fs_mock.exists.side_effect = mock_exists

    status_entries = []
    for name in list(table_names) + list(dirs_without_metadata):
        entry = MagicMock()
        entry.isDirectory.return_value = True
        path_mock = MagicMock()
        path_mock.getName.return_value = name
        entry.getPath.return_value = path_mock
        status_entries.append(entry)

    fs_mock.listStatus.return_value = status_entries


# ─────────────────────────────────────────────────────────────────────────────
# Test parse_excel_rows
# ─────────────────────────────────────────────────────────────────────────────

class TestIcebergParseExcelRows:

    def test_basic_parsing_and_s3_normalization(self):
        import pandas as pd
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import parse_excel_rows

        df = pd.DataFrame([{
            'database': 'analytics',
            'table': 'orders',
            'dest_s3_prefix': 's3n://dest-bucket/warehouse',
        }])
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

        result = parse_excel_rows(df, {}, 'run_123')

        assert len(result) == 1
        assert result[0]['source_database'] == 'analytics'
        assert result[0]['dest_database'] == 'analytics'
        assert result[0]['dest_s3_prefix'] == 's3a://dest-bucket/warehouse'
        assert result[0]['table_tokens'] == ['orders']
        assert result[0]['run_id'] == 'run_123'

    def test_skips_rows_without_dest_s3_prefix(self):
        import pandas as pd
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import parse_excel_rows

        df = pd.DataFrame([{
            'database': 'analytics',
            'table': 'orders',
            'dest_s3_prefix': '',
        }])
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

        result = parse_excel_rows(df, {}, 'run_123')
        assert len(result) == 0

    def test_comma_separated_tables(self):
        import pandas as pd
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import parse_excel_rows

        df = pd.DataFrame([{
            'database': 'analytics',
            'table': 'orders, users, events',
            'dest_s3_prefix': 's3a://dest/wh',
        }])
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

        result = parse_excel_rows(df, {}, 'run_123')

        assert result[0]['table_tokens'] == ['orders', 'users', 'events']

    def test_wildcard_collapses_other_tokens(self):
        import pandas as pd
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import parse_excel_rows

        df = pd.DataFrame([
            {'database': 'analytics', 'table': 'orders',
             'dest_s3_prefix': 's3a://dest/wh'},
            {'database': 'analytics', 'table': '*',
             'dest_s3_prefix': 's3a://dest/wh'},
        ])
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

        result = parse_excel_rows(df, {}, 'run_123')

        assert len(result) == 1
        assert result[0]['table_tokens'] == ['*']

    def test_groups_by_database_and_prefix(self):
        import pandas as pd
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import parse_excel_rows

        df = pd.DataFrame([
            {'database': 'analytics', 'table': 'orders',
             'dest_s3_prefix': 's3a://dest/wh'},
            {'database': 'reporting', 'table': 'summary',
             'dest_s3_prefix': 's3a://dest/wh'},
        ])
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

        result = parse_excel_rows(df, {}, 'run_123')

        assert len(result) == 2
        dbs = {r['dest_database'] for r in result}
        assert dbs == {'analytics', 'reporting'}

    def test_defaults_to_wildcard_when_table_empty(self):
        import pandas as pd
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import parse_excel_rows

        df = pd.DataFrame([{
            'database': 'analytics', 'table': '',
            'dest_s3_prefix': 's3a://dest/wh',
        }])
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

        result = parse_excel_rows(df, {}, 'run_123')

        assert result[0]['table_tokens'] == ['*']

    def test_merges_tokens_from_same_group(self):
        import pandas as pd
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import parse_excel_rows

        df = pd.DataFrame([
            {'database': 'analytics', 'table': 'orders',
             'dest_s3_prefix': 's3a://dest/wh'},
            {'database': 'analytics', 'table': 'users',
             'dest_s3_prefix': 's3a://dest/wh'},
        ])
        df.columns = df.columns.str.strip().str.lower().str.replace(' ', '_')

        result = parse_excel_rows(df, {}, 'run_123')

        assert len(result) == 1
        assert result[0]['table_tokens'] == ['orders', 'users']


# ─────────────────────────────────────────────────────────────────────────────
# Test discover_tables
# ─────────────────────────────────────────────────────────────────────────────

class TestIcebergDiscoverTables:

    def test_filters_out_dirs_without_metadata(self, mock_spark):
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import _list_iceberg_tables

        _mock_fs_for_table_listing(
            mock_spark, ['orders'], dirs_without_metadata=['tmp_staging', 'logs']
        )

        result = _list_iceberg_tables(mock_spark, 's3a://dest-bucket/warehouse/analytics')

        assert result == ['orders']

    def test_discovers_from_metadata_json_when_not_in_hms(self, mock_spark):
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import discover_tables
        from utils.migrations.shared import get_config

        _mock_fs_for_table_listing(mock_spark, ['orders'])
        _mock_fs_for_iceberg(mock_spark, SAMPLE_ICEBERG_METADATA)

        def sql_router(sql):
            sl = sql.lower().strip()
            if sl.startswith('describe '):
                raise Exception("Table not found")
            return MagicMock()
        mock_spark.sql.side_effect = sql_router

        db_config = {
            'source_database': 'analytics',
            'dest_database': 'analytics',
            'dest_s3_prefix': 's3a://dest-bucket/warehouse',
            'table_tokens': ['orders'],
            'run_id': 'run_123',
        }
        result = discover_tables(db_config, mock_spark, get_config())

        assert len(result) == 1
        tbl = result[0]
        assert tbl['source_table'] == 'orders'
        assert tbl['dest_location'] == 's3a://dest-bucket/warehouse/analytics/orders'
        assert tbl['source_row_count'] == 5000
        assert tbl['is_partitioned'] is True

    def test_queries_hms_when_table_exists(self, mock_spark):
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import discover_tables
        from utils.migrations.shared import get_config

        _mock_fs_for_table_listing(mock_spark, ['orders'])

        # Mock HMS responses
        describe_row = MagicMock()
        describe_row.col_name = 'id'
        describe_row.data_type = 'bigint'

        count_row = MagicMock()
        count_row.__getitem__ = lambda self, key: 10000 if key == 'c' else None

        partition_row = MagicMock()

        desc_formatted_loc = MagicMock()
        desc_formatted_loc.col_name = 'Location'
        desc_formatted_loc.data_type = 's3a://dest-bucket/warehouse/analytics/orders'

        def sql_router(sql):
            sl = sql.lower().strip()
            df = MagicMock()
            if sl.startswith('describe formatted'):
                df.collect.return_value = [desc_formatted_loc]
            elif sl.startswith('describe '):
                df.collect.return_value = [describe_row]
            elif sl.startswith('select count'):
                df.collect.return_value = [count_row]
            elif '.partitions' in sl:
                df.count.return_value = 3
                df.columns = ['dt']
            return df
        mock_spark.sql.side_effect = sql_router

        db_config = {
            'source_database': 'analytics',
            'dest_database': 'analytics',
            'dest_s3_prefix': 's3a://dest-bucket/warehouse',
            'table_tokens': ['orders'],
            'run_id': 'run_123',
        }
        result = discover_tables(db_config, mock_spark, get_config())

        assert len(result) == 1
        tbl = result[0]
        assert tbl['source_row_count'] == 10000
        assert tbl['partition_count'] == 3
        assert tbl['partition_spec_detail'] == [
            {'source_column': 'dt', 'transform': 'identity', 'name': 'dt', 'param': None},
        ]

    def test_records_error_on_failure(self, mock_spark):
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import discover_tables
        from utils.migrations.shared import get_config

        _mock_fs_for_table_listing(mock_spark, ['orders'])

        # After table listing succeeds, break FS for metadata reads
        fs_mock = mock_spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value
        call_count = {'n': 0}

        def exists_then_fail(path):
            call_count['n'] += 1
            path_str = str(path)
            if 'version-hint' in path_str:
                raise Exception("S3 connection refused")
            return True
        fs_mock.exists.side_effect = exists_then_fail

        def sql_router(sql):
            raise Exception("Table not found")
        mock_spark.sql.side_effect = sql_router

        db_config = {
            'source_database': 'analytics',
            'dest_database': 'analytics',
            'dest_s3_prefix': 's3a://dest/warehouse',
            'table_tokens': ['orders'],
            'run_id': 'run_123',
        }
        result = discover_tables(db_config, mock_spark, get_config())
        assert len(result) == 1
        assert 'error' in result[0]


# ─────────────────────────────────────────────────────────────────────────────
# Test create_dest_table
# ─────────────────────────────────────────────────────────────────────────────

class TestIcebergCreateDestTable:

    def _make_table_info(self, **overrides):
        base = {
            'source_table': 'orders',
            'dest_location': 's3a://dest-bucket/warehouse/analytics/orders',
            'file_format': 'PARQUET',
            'format_version': '2',
            'schema': [
                {'name': 'id', 'type': 'BIGINT'},
                {'name': 'amount', 'type': 'DOUBLE'},
                {'name': 'dt', 'type': 'DATE'},
            ],
            'partition_spec_detail': [
                {'source_column': 'dt', 'transform': 'identity', 'name': 'dt', 'param': None},
            ],
        }
        base.update(overrides)
        return base

    def test_creates_new_table_with_add_files(self, mock_spark):
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import create_dest_table
        from utils.migrations.shared import get_config

        call_log = []
        def sql_router(sql):
            call_log.append(sql)
            sl = sql.lower().strip()
            if sl.startswith('describe '):
                raise Exception("Table not found")
            return MagicMock()
        mock_spark.sql.side_effect = sql_router

        result = create_dest_table(self._make_table_info(), 'analytics', mock_spark, get_config())

        assert result['status'] == 'COMPLETED'
        assert result['existed'] is False
        calls_str = ' '.join(call_log)
        assert 'CREATE TABLE' in calls_str
        assert 'add_files' in calls_str

    def test_incremental_add_files_when_at_dest(self, mock_spark):
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import create_dest_table
        from utils.migrations.shared import get_config

        call_log = []
        loc_row = MagicMock()
        loc_row.col_name = 'Location'
        loc_row.data_type = 's3a://dest-bucket/warehouse/analytics/orders'

        def sql_router(sql):
            call_log.append(sql)
            sl = sql.lower().strip()
            df = MagicMock()
            if sl.startswith('describe formatted'):
                df.collect.return_value = [loc_row]
            return df
        mock_spark.sql.side_effect = sql_router

        result = create_dest_table(self._make_table_info(), 'analytics', mock_spark, get_config())

        assert result['status'] == 'COMPLETED'
        assert result['existed'] is True
        calls_str = ' '.join(call_log)
        assert 'add_files' in calls_str
        assert 'DROP TABLE' not in calls_str
        assert 'CREATE TABLE' not in calls_str

    def test_drops_table_at_different_location(self, mock_spark):
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import create_dest_table
        from utils.migrations.shared import get_config

        call_log = []
        loc_row = MagicMock()
        loc_row.col_name = 'Location'
        loc_row.data_type = 's3a://old-bucket/somewhere/orders'

        def sql_router(sql):
            call_log.append(sql)
            sl = sql.lower().strip()
            df = MagicMock()
            if sl.startswith('describe formatted'):
                df.collect.return_value = [loc_row]
            return df
        mock_spark.sql.side_effect = sql_router

        result = create_dest_table(self._make_table_info(), 'analytics', mock_spark, get_config())

        assert result['status'] == 'COMPLETED'
        calls_str = ' '.join(call_log)
        assert 'DROP TABLE' in calls_str
        assert 'CREATE TABLE' in calls_str
        assert 'add_files' in calls_str

    def test_returns_error_on_add_files_failure(self, mock_spark):
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import create_dest_table
        from utils.migrations.shared import get_config

        def sql_router(sql):
            sl = sql.lower().strip()
            if sl.startswith('describe '):
                raise Exception("Table not found")
            if 'add_files' in sl:
                raise Exception("No files found at path")
            return MagicMock()
        mock_spark.sql.side_effect = sql_router

        result = create_dest_table(self._make_table_info(), 'analytics', mock_spark, get_config())

        assert result['status'] == 'FAILED'
        assert 'No files found' in result['error']

    def test_unpartitioned_table(self, mock_spark):
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import create_dest_table
        from utils.migrations.shared import get_config

        call_log = []
        def sql_router(sql):
            call_log.append(sql)
            sl = sql.lower().strip()
            if sl.startswith('describe '):
                raise Exception("Table not found")
            return MagicMock()
        mock_spark.sql.side_effect = sql_router

        table_info = self._make_table_info(partition_spec_detail=[])
        result = create_dest_table(table_info, 'analytics', mock_spark, get_config())

        assert result['status'] == 'COMPLETED'
        create_sql = next(s for s in call_log if 'CREATE TABLE' in s)
        assert 'PARTITIONED BY' not in create_sql


# ─────────────────────────────────────────────────────────────────────────────
# Test type mapping (branching logic only — the lookup table is self-evident)
# ─────────────────────────────────────────────────────────────────────────────

class TestIcebergTypeMapping:

    def test_special_type_branches(self):
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import _map_iceberg_type

        assert _map_iceberg_type('decimal(10,2)') == 'DECIMAL(10,2)'
        assert _map_iceberg_type('fixed[16]') == 'BINARY'
        assert _map_iceberg_type({'type': 'struct', 'fields': []}) == 'STRING'


# ─────────────────────────────────────────────────────────────────────────────
# Test metadata extraction from metadata.json
# ─────────────────────────────────────────────────────────────────────────────

class TestMetadataExtraction:

    def test_extracts_schema_partitions_and_row_count(self):
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import (
            _extract_schema, _extract_partition_spec, _extract_row_count,
        )

        schema = _extract_schema(SAMPLE_ICEBERG_METADATA)
        assert schema == [
            {'name': 'id', 'type': 'BIGINT'},
            {'name': 'amount', 'type': 'DOUBLE'},
            {'name': 'dt', 'type': 'DATE'},
        ]

        spec, is_partitioned = _extract_partition_spec(SAMPLE_ICEBERG_METADATA)
        assert is_partitioned is True
        assert len(spec) == 1
        assert spec[0]['source_column'] == 'dt'
        assert spec[0]['transform'] == 'identity'

        assert _extract_row_count(SAMPLE_ICEBERG_METADATA) == 5000

    def test_schema_selection_by_id(self):
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
        assert len(result) == 2
        assert result[0]['name'] == 'id'


# ─────────────────────────────────────────────────────────────────────────────
# Test DDL builders
# ─────────────────────────────────────────────────────────────────────────────

class TestDDLBuilders:

    def test_column_defs_and_partition_clause(self):
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import (
            _build_column_defs, _build_iceberg_partition_clause,
        )

        schema = [
            {'name': 'id', 'type': 'BIGINT'},
            {'name': 'amount', 'type': 'DOUBLE'},
            {'name': 'dt', 'type': 'DATE'},
        ]
        assert _build_column_defs(schema) == '`id` BIGINT, `amount` DOUBLE, `dt` DATE'

        spec = [{'source_column': 'dt', 'transform': 'identity', 'name': 'dt', 'param': None}]
        assert _build_iceberg_partition_clause(spec) == '`dt`'

    @pytest.mark.parametrize('spec, expected', [
        ([{'source_column': 'id', 'transform': 'bucket', 'name': 'id_bucket', 'param': 16}],
         'bucket(16, `id`)'),
        ([{'source_column': 'ts', 'transform': 'year', 'name': 'ts_year', 'param': None}],
         'years(`ts`)'),
        ([{'source_column': 'val', 'transform': 'truncate', 'name': 'val_trunc', 'param': 10}],
         'truncate(10, `val`)'),
    ])
    def test_partition_transforms(self, spec, expected):
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import _build_iceberg_partition_clause

        assert _build_iceberg_partition_clause(spec) == expected


# ─────────────────────────────────────────────────────────────────────────────
# Test token matching
# ─────────────────────────────────────────────────────────────────────────────

class TestMatchTokens:

    @pytest.mark.parametrize('tables, tokens, expected', [
        (['a', 'b', 'c'], ['*'], ['a', 'b', 'c']),
        (['orders', 'users', 'events'], ['orders'], ['orders']),
        (['orders', 'order_items', 'users'], ['order*'], ['orders', 'order_items']),
        (['orders'], ['orders', 'orders'], ['orders']),
    ])
    def test_matching(self, tables, tokens, expected):
        from utils.migrations.metadata_strategies.iceberg_to_iceberg import _match_tokens

        assert _match_tokens(tables, tokens) == expected
