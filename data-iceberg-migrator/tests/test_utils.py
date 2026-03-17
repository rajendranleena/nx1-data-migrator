"""
Tests for shared utility functions:
  - get_config()
  - track_duration decorator
  - execute_with_iceberg_retry()
"""

import time
from unittest.mock import patch

import migration_dags_combined as m
import pytest


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
