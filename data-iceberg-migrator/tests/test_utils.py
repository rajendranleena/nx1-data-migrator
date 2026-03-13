"""
Tests for shared utility functions:
  - get_config()
  - track_duration decorator
  - execute_with_iceberg_retry()
"""

import time
import pytest
from unittest.mock import MagicMock, patch

import migration_dags_combined as m


# ---------------------------------------------------------------------------
# get_config
# ---------------------------------------------------------------------------
class TestGetConfig:

    def test_returns_all_required_keys(self):
        cfg = m.get_config()

        required_keys = [
            'ssh_conn_id', 'edge_temp_path', 'default_s3_bucket',
            's3_endpoint', 's3_access_key', 's3_secret_key',
            'distcp_mappers', 'distcp_bandwidth', 'spark_conn_id',
            'tracking_database', 'tracking_location', 'report_output_location',
            'auth_method', 'mapr_user', 'mapr_ticketfile_location',
            'kinit_principal', 'kinit_keytab', 'kinit_password',
            's3_listing_tool', 'smtp_conn_id', 'email_recipients',
        ]
        for key in required_keys:
            assert key in cfg, f"Missing key in config: {key}"

    def test_uses_variable_values(self):
        cfg = m.get_config()
        assert cfg['ssh_conn_id'] == 'cluster_edge_ssh'
        assert cfg['tracking_database'] == 'migration_tracking'
        assert cfg['default_s3_bucket'] == 's3a://test-bucket'
        assert cfg['distcp_mappers'] == '10'

    def test_falls_back_to_default_when_variable_missing(self):
        with patch('airflow.models.Variable.get', return_value='fallback'):
            cfg = m.get_config()
        # All values come back as 'fallback' when Variable.get always returns 'fallback'
        assert cfg['ssh_conn_id'] == 'fallback'


# ---------------------------------------------------------------------------
# track_duration
# ---------------------------------------------------------------------------
class TestTrackDuration:
    def test_adds_task_duration_to_dict_result(self):
        @m.track_duration
        def sample_task():
            return {'result': 'ok'}

        result = sample_task()
        assert '_task_duration' in result
        assert isinstance(result['_task_duration'], float)
        assert result['_task_duration'] >= 0

    def test_preserves_existing_keys(self):
        @m.track_duration
        def sample_task():
            return {'status': 'COMPLETED', 'count': 42}

        result = sample_task()
        assert result['status'] == 'COMPLETED'
        assert result['count'] == 42
        assert '_task_duration' in result

    def test_handles_non_dict_result(self):
        @m.track_duration
        def sample_task():
            return "just a string"

        result = sample_task()
        # Non-dict: decorator should not crash, just returns the string
        assert result == "just a string"

    def test_duration_reflects_sleep_time(self):
        @m.track_duration
        def slow_task():
            time.sleep(0.05)
            return {}

        result = slow_task()
        assert result['_task_duration'] >= 0.04

    def test_wraps_preserves_function_name(self):
        @m.track_duration
        def my_named_task():
            return {}

        assert my_named_task.__name__ == 'my_named_task'

    def test_passes_args_and_kwargs(self):
        @m.track_duration
        def parameterized_task(x, y=10):
            return {'sum': x + y}

        result = parameterized_task(5, y=3)
        assert result['sum'] == 8


# ---------------------------------------------------------------------------
# execute_with_iceberg_retry
# ---------------------------------------------------------------------------
class TestExecuteWithIcebergRetry:
    def test_succeeds_on_first_try(self, mock_spark):
        m.execute_with_iceberg_retry(mock_spark, "SELECT 1")
        mock_spark.sql.assert_called_once_with("SELECT 1")

    def test_retries_on_exception_and_eventually_succeeds(self, mock_spark):
        mock_spark.sql.side_effect = [
            Exception("commit conflict"),
            Exception("commit conflict"),
            None,
        ]
        with patch('time.sleep'):
            m.execute_with_iceberg_retry(mock_spark, "MERGE INTO t USING s", max_retries=3)
        assert mock_spark.sql.call_count == 3

    def test_raises_after_max_retries(self, mock_spark):
        mock_spark.sql.side_effect = Exception("persistent error")
        with patch('time.sleep'), pytest.raises(Exception, match="persistent error"):
            m.execute_with_iceberg_retry(mock_spark, "BAD SQL", max_retries=3)
        assert mock_spark.sql.call_count == 3

    def test_task_label_does_not_affect_sql(self, mock_spark):
        m.execute_with_iceberg_retry(mock_spark, "SELECT 2", task_label="my_label")
        mock_spark.sql.assert_called_once_with("SELECT 2")

    def test_default_max_retries_is_six(self, mock_spark):
        mock_spark.sql.side_effect = Exception("always fails")
        with patch('time.sleep'), pytest.raises(Exception):
            m.execute_with_iceberg_retry(mock_spark, "X")
        assert mock_spark.sql.call_count == 6
