"""
Tests for shared utility functions:
  - get_config()
  - track_duration decorator
  - execute_with_iceberg_retry()
"""

import time
from unittest.mock import MagicMock, patch

import pytest
import utils.migrations.shared as m


class TestGetConfig:

    def test_returns_expected_values_from_variables(self):
        cfg = m.get_config()
        assert cfg['ssh_conn_id'] == 'cluster_edge_ssh'
        assert cfg['tracking_database'] == 'migration_tracking'
        assert cfg['default_s3_bucket'] == 's3a://test-bucket'
        assert cfg['distcp_mappers'] == '10'
        for key in ['s3_endpoint', 's3_access_key', 's3_secret_key',
                     'spark_conn_id', 'tracking_location', 'report_output_location',
                     'auth_method', 'smtp_conn_id', 'email_recipients']:
            assert key in cfg, f"Missing key: {key}"

    def test_falls_back_to_default_when_variable_missing(self):
        with patch('airflow.models.Variable.get', return_value='fallback'):
            cfg = m.get_config()
        assert cfg['ssh_conn_id'] == 'fallback'


class TestTrackDuration:

    def test_adds_duration_and_preserves_result(self):
        @m.track_duration
        def sample_task(x, y=10):
            time.sleep(0.02)
            return {'status': 'COMPLETED', 'sum': x + y}

        result = sample_task(5, y=3)
        assert result['status'] == 'COMPLETED'
        assert result['sum'] == 8
        assert isinstance(result['_task_duration'], float)
        assert result['_task_duration'] >= 0.01


class TestExecuteWithIcebergRetry:

    def test_succeeds_immediately_or_after_retries(self, mock_spark):
        # Immediate success
        m.execute_with_iceberg_retry(mock_spark, "SELECT 1")
        mock_spark.sql.assert_called_once_with("SELECT 1")

        # Success after retries
        mock_spark.sql.reset_mock()
        mock_spark.sql.side_effect = [Exception("conflict"), Exception("conflict"), None]
        with patch('time.sleep'):
            m.execute_with_iceberg_retry(mock_spark, "MERGE INTO t USING s", max_retries=3)
        assert mock_spark.sql.call_count == 3

    def test_raises_after_exhausting_default_six_retries(self, mock_spark):
        mock_spark.sql.side_effect = Exception("persistent error")
        with patch('time.sleep'), pytest.raises(Exception, match="persistent error"):
            m.execute_with_iceberg_retry(mock_spark, "BAD SQL")
        assert mock_spark.sql.call_count == 6


# ---------------------------------------------------------------------------
# build_s3_opts
# ---------------------------------------------------------------------------
class TestBuildS3Opts:
    """Unit tests for the build_s3_opts credential-builder helper."""

    def _cfg(self, endpoint='', access_key='GLOBALAK', secret_key='GLOBALSK'):
        return {'s3_endpoint': endpoint, 's3_access_key': access_key, 's3_secret_key': secret_key}

    # ------------------------------------------------------------------
    # Case 2: no dest_endpoint — original unscoped global config behaviour
    # ------------------------------------------------------------------

    def test_global_creds_emitted_unscoped(self):
        opts = m.build_s3_opts('s3a://data-lake', self._cfg())
        assert 'fs.s3a.access.key=GLOBALAK' in opts
        assert 'fs.s3a.secret.key=GLOBALSK' in opts
        assert 'fs.s3a.bucket.' not in opts

    def test_global_endpoint_emitted_unscoped(self):
        opts = m.build_s3_opts('s3a://data-lake', self._cfg(endpoint='https://s3.default.example.com'))
        assert 'fs.s3a.endpoint=https://s3.default.example.com' in opts
        assert 'fs.s3a.bucket.' not in opts

    def test_case2_same_output_regardless_of_s3_url_prefix(self):
        """Case 2 emits unscoped props — bucket name in URL does not matter."""
        for url in ('s3://bucket-x', 's3n://bucket-x', 's3a://bucket-x', ''):
            opts = m.build_s3_opts(url, self._cfg())
            assert 'fs.s3a.access.key=GLOBALAK' in opts
            assert 'fs.s3a.secret.key=GLOBALSK' in opts
            assert 'fs.s3a.bucket.' not in opts

    def test_empty_global_creds_produce_empty_string(self):
        opts = m.build_s3_opts('s3a://data-lake', self._cfg(access_key='', secret_key=''))
        assert opts == ''

    def test_no_endpoint_no_creds_produces_empty_string(self):
        opts = m.build_s3_opts('', self._cfg(access_key='', secret_key=''))
        assert opts == ''

    # ------------------------------------------------------------------
    # Case 1: dest_endpoint provided — endpoint-hostname credential lookup
    # ------------------------------------------------------------------

    def test_endpoint_used_directly(self):
        ep = 'https://s3.tenant-a.example.com'
        with patch('airflow.models.Variable.get', return_value=''):
            opts = m.build_s3_opts('s3a://data-lake', self._cfg(), dest_endpoint=ep)
        assert f'fs.s3a.bucket.data-lake.endpoint={ep}' in opts

    def test_endpoint_creds_looked_up_by_hostname(self):
        ep = 'https://s3.tenant-a.example.com'
        def fake_var(key, default_var=''):
            return {'s3.tenant-a.example.com_access_key': 'TENANTAAK',
                    's3.tenant-a.example.com_secret_key': 'TENANTASK'}.get(key, default_var)
        with patch('airflow.models.Variable.get', side_effect=fake_var):
            opts = m.build_s3_opts('s3a://data-lake', self._cfg(), dest_endpoint=ep)
        assert 'fs.s3a.bucket.data-lake.access.key=TENANTAAK' in opts
        assert 'fs.s3a.bucket.data-lake.secret.key=TENANTASK' in opts

    def test_endpoint_creds_fall_back_to_global_when_variable_absent(self):
        ep = 'https://s3.tenant-a.example.com'
        with patch('airflow.models.Variable.get', return_value=''):
            opts = m.build_s3_opts('s3a://data-lake', self._cfg(), dest_endpoint=ep)
        assert 'fs.s3a.bucket.data-lake.access.key=GLOBALAK' in opts
        assert 'fs.s3a.bucket.data-lake.secret.key=GLOBALSK' in opts

    def test_two_different_endpoints_same_bucket_produce_different_opts(self):
        ep_a = 'https://s3.tenant-a.example.com'
        ep_b = 'https://s3.tenant-b.example.com'
        def fake_var(key, default_var=''):
            mapping = {
                's3.tenant-a.example.com_access_key': 'AK_A',
                's3.tenant-b.example.com_access_key': 'AK_B',
            }
            return mapping.get(key, default_var)
        with patch('airflow.models.Variable.get', side_effect=fake_var):
            opts_a = m.build_s3_opts('s3a://data-lake', self._cfg(), dest_endpoint=ep_a)
            opts_b = m.build_s3_opts('s3a://data-lake', self._cfg(), dest_endpoint=ep_b)
        assert f'endpoint={ep_a}' in opts_a
        assert f'endpoint={ep_b}' in opts_b
        assert 'AK_A' in opts_a
        assert 'AK_B' in opts_b
        assert opts_a != opts_b

    def test_global_creds_not_used_when_endpoint_hostname_variable_present(self):
        """Endpoint-scoped Variable must win over global config key."""
        ep = 'https://s3.tenant-x.example.com'
        def fake_var(key, default_var=''):
            if key == 's3.tenant-x.example.com_access_key':
                return 'TENANT_X_AK'
            if key == 's3.tenant-x.example.com_secret_key':
                return 'TENANT_X_SK'
            return default_var
        with patch('airflow.models.Variable.get', side_effect=fake_var):
            opts = m.build_s3_opts('s3a://data-lake', self._cfg(), dest_endpoint=ep)
        assert 'TENANT_X_AK' in opts
        assert 'GLOBALAK' not in opts


# ---------------------------------------------------------------------------
# configure_spark_s3
# ---------------------------------------------------------------------------
class TestConfigureSparkS3:

    def test_sets_source_and_dest_credentials(self, mock_spark):
        config = {
            's3_source_endpoint': 'https://src.example.com',
            's3_source_access_key': 'SRC_AK',
            's3_source_secret_key': 'SRC_SK',
            's3_dest_endpoint': 'https://dst.example.com',
            's3_dest_access_key': 'DST_AK',
            's3_dest_secret_key': 'DST_SK',
        }
        m.configure_spark_s3(mock_spark, config)
        mock_spark.conf.set.assert_any_call('fs.s3a.endpoint', 'https://src.example.com')
        mock_spark.conf.set.assert_any_call('fs.s3a.access.key', 'SRC_AK')
        mock_spark.conf.set.assert_any_call('fs.s3a.secret.key', 'SRC_SK')
        assert config['_dest_endpoint'] == 'https://dst.example.com'
        assert config['_dest_access_key'] == 'DST_AK'

    def test_falls_back_to_global_keys(self, mock_spark):
        config = {
            's3_endpoint': 'https://global.example.com',
            's3_access_key': 'GLOBAL_AK',
            's3_secret_key': 'GLOBAL_SK',
        }
        m.configure_spark_s3(mock_spark, config)
        mock_spark.conf.set.assert_any_call('fs.s3a.endpoint', 'https://global.example.com')
        assert config['_src_endpoint'] == 'https://global.example.com'
        assert config['_dest_endpoint'] == 'https://global.example.com'

    def test_skips_empty_values(self, mock_spark):
        config = {}
        m.configure_spark_s3(mock_spark, config)
        mock_spark.conf.set.assert_not_called()


# ---------------------------------------------------------------------------
# apply_bucket_credentials
# ---------------------------------------------------------------------------
class TestApplyBucketCredentials:

    def test_sets_per_bucket_credentials(self, mock_spark):
        m.apply_bucket_credentials(mock_spark, 's3a://my-bucket/path', 'https://ep.com', 'AK', 'SK')
        mock_spark.conf.set.assert_any_call('fs.s3a.bucket.my-bucket.endpoint', 'https://ep.com')
        mock_spark.conf.set.assert_any_call('fs.s3a.bucket.my-bucket.access.key', 'AK')
        mock_spark.conf.set.assert_any_call('fs.s3a.bucket.my-bucket.secret.key', 'SK')

    def test_skips_non_s3a_url(self, mock_spark):
        m.apply_bucket_credentials(mock_spark, '/local/path', 'https://ep.com', 'AK', 'SK')
        mock_spark.conf.set.assert_not_called()

    def test_skips_when_no_credentials_or_endpoint(self, mock_spark):
        m.apply_bucket_credentials(mock_spark, 's3a://bucket/path', '', '', '')
        mock_spark.conf.set.assert_not_called()


# ---------------------------------------------------------------------------
# compute_dest_path
# ---------------------------------------------------------------------------
class TestComputeDestPath:

    def test_uses_prefix_mapping_when_matched(self):
        result = m.compute_dest_path(
            source_location='s3a://src-bucket/data/db/tbl',
            dest_database='dest_db',
            table_name='tbl',
            dest_bucket='s3a://dest-bucket',
            source_s3_prefix='s3a://src-bucket/data',
            dest_s3_prefix='s3a://dest-bucket/data',
        )
        assert result == 's3a://dest-bucket/data/db/tbl'

    def test_falls_back_to_bucket_db_table(self):
        result = m.compute_dest_path(
            source_location='s3a://src-bucket/data/db/tbl',
            dest_database='dest_db',
            table_name='tbl',
            dest_bucket='s3a://dest-bucket',
            source_s3_prefix='',
            dest_s3_prefix='',
        )
        assert result == 's3a://dest-bucket/dest_db/tbl'

    def test_raises_when_source_doesnt_match_prefix(self):
        with pytest.raises(ValueError, match="does not start with source_s3_prefix"):
            m.compute_dest_path(
                source_location='s3a://other-bucket/data/db/tbl',
                dest_database='dest_db',
                table_name='tbl',
                dest_bucket='s3a://dest-bucket',
                source_s3_prefix='s3a://src-bucket/data',
                dest_s3_prefix='s3a://dest-bucket/data',
            )

# ---------------------------------------------------------------------------
# cell_str
# ---------------------------------------------------------------------------
class TestCellStr:

    def test_returns_stripped_string(self):
        assert m.cell_str('  hello  ') == 'hello'

    def test_returns_default_for_none(self):
        assert m.cell_str(None) == ''
        assert m.cell_str(None, default='N/A') == 'N/A'

    def test_returns_default_for_nan(self):
        assert m.cell_str(float('nan')) == ''
        assert m.cell_str(float('nan'), default='EMPTY') == 'EMPTY'

    def test_returns_default_for_blank_string(self):
        assert m.cell_str('   ') == ''
        assert m.cell_str('   ', default='EMPTY') == 'EMPTY'

    def test_converts_int_and_float(self):
        assert m.cell_str(42) == '42'
        assert m.cell_str(3.14) == '3.14'


# ---------------------------------------------------------------------------
# normalize_s3
# ---------------------------------------------------------------------------
class TestNormalizeS3:

    def test_s3n_prefix_converted(self):
        assert m.normalize_s3('s3n://bucket/key') == 's3a://bucket/key'

    def test_s3_prefix_converted(self):
        assert m.normalize_s3('s3://bucket/key') == 's3a://bucket/key'

    def test_s3a_prefix_unchanged(self):
        assert m.normalize_s3('s3a://bucket/key') == 's3a://bucket/key'

    def test_no_prefix_gets_s3a_prepended(self):
        assert m.normalize_s3('bucket/key') == 's3a://bucket/key'

    def test_empty_string_returned_as_is(self):
        assert m.normalize_s3('') == ''

    def test_none_returned_as_is(self):
        assert m.normalize_s3(None) is None


# ---------------------------------------------------------------------------
# validate_bucket_endpoint_pairs — boto3-independent paths
# ---------------------------------------------------------------------------
class TestValidateBucketEndpointPairs:

    def _make_grouped(self, bucket='s3a://data-lake', endpoint='https://s3.example.com', src_db='sales'):
        return {(src_db, 'dest_db', bucket, endpoint, None): {'tokens': []}}

    def test_skips_silently_when_boto3_unavailable(self):
        import builtins
        real_import = builtins.__import__
        def mock_import(name, *args, **kwargs):
            if name == 'boto3':
                raise ImportError('no boto3')
            return real_import(name, *args, **kwargs)
        with patch('builtins.__import__', side_effect=mock_import):
            m.validate_bucket_endpoint_pairs(self._make_grouped(), {})  # must not raise

    def test_rows_with_no_endpoint_are_skipped(self):
        grouped = {('db', 'dest_db', 's3a://bucket', '', None): {'tokens': []}}
        import builtins
        real_import = builtins.__import__
        def mock_import(name, *args, **kwargs):
            if name == 'boto3':
                raise ImportError('no boto3')
            return real_import(name, *args, **kwargs)
        with patch('builtins.__import__', side_effect=mock_import):
            m.validate_bucket_endpoint_pairs(grouped, {})  # no endpoint → skipped, no raise

    def _inject_boto3(self, s3_client_mock, fake_client_error_cls):
        import sys
        import types

        boto3_mod = types.ModuleType('boto3')
        boto3_mod.client = MagicMock(return_value=s3_client_mock)

        botocore_mod = types.ModuleType('botocore')
        botocore_exc_mod = types.ModuleType('botocore.exceptions')
        botocore_exc_mod.ClientError = fake_client_error_cls
        botocore_mod.exceptions = botocore_exc_mod

        return patch.dict(sys.modules, {
            'boto3': boto3_mod,
            'botocore': botocore_mod,
            'botocore.exceptions': botocore_exc_mod,
        })

    def test_deduplicates_same_bucket_endpoint_pair(self):
        """Same (bucket, endpoint) pair appearing in two rows is only validated once."""
        s3_client_mock = MagicMock()
        s3_client_mock.head_bucket.return_value = {}

        class _CE(Exception):
            pass

        grouped = {
            ('db1', 'dest1', 's3a://bucket', 'https://ep.com', None): {},
            ('db2', 'dest2', 's3a://bucket', 'https://ep.com', None): {},
        }
        with self._inject_boto3(s3_client_mock, _CE), \
             patch('airflow.models.Variable.get', return_value=''):
            m.validate_bucket_endpoint_pairs(grouped, {})

        assert s3_client_mock.head_bucket.call_count == 1

    def test_raises_on_missing_bucket(self):
        """404/NoSuchBucket causes a validation Exception listing the failure."""

        class FakeClientError(Exception):
            def __init__(self, *a, **kw):
                self.response = {'Error': {'Code': 'NoSuchBucket'}}

        s3_client_mock = MagicMock()
        s3_client_mock.head_bucket.side_effect = FakeClientError()

        with self._inject_boto3(s3_client_mock, FakeClientError), \
             patch('airflow.models.Variable.get', return_value=''), \
             pytest.raises(Exception, match='validation failed'):
            m.validate_bucket_endpoint_pairs(self._make_grouped(), {})

    def test_403_access_denied_does_not_raise(self):
        """403 means bucket exists but creds lack full access — should warn and continue."""

        class FakeClientError(Exception):
            def __init__(self, *a, **kw):
                self.response = {'Error': {'Code': '403'}}

        s3_client_mock = MagicMock()
        s3_client_mock.head_bucket.side_effect = FakeClientError()

        with self._inject_boto3(s3_client_mock, FakeClientError), \
             patch('airflow.models.Variable.get', return_value=''):
            m.validate_bucket_endpoint_pairs(self._make_grouped(), {})

    def test_general_connection_error_raises(self):
        """Any non-ClientError (e.g. network timeout) is collected and raised."""
        class _CE(Exception):
            pass

        s3_client_mock = MagicMock()
        s3_client_mock.head_bucket.side_effect = ConnectionError('timeout')

        with self._inject_boto3(s3_client_mock, _CE), \
             patch('airflow.models.Variable.get', return_value=''), \
             pytest.raises(Exception, match='validation failed'):
            m.validate_bucket_endpoint_pairs(self._make_grouped(), {})
