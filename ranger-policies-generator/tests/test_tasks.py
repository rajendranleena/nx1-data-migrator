from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

_CONFIG = {
    "ranger_url": "http://ranger:6080",
    "ranger_username": "admin",
    "ranger_password": "admin",
    "service_name": "nx1-unifiedsql",
    "keycloak_url": "https://keycloak.local",
    "keycloak_realm": "master",
    "keycloak_client_id": "admin-cli",
    "keycloak_client_secret": "secret",
    "keycloak_verify_ssl": False,
    "keycloak_cacert": "",
    "smtp_conn_id": "smtp_default",
    "email_recipients": "",
    "spark_conn_id": "spark_default",
    "tracking_database": "policy_tracking",
    "tracking_location": "s3a://data-lake/policy_tracking",
    "report_output_location": "s3a://data-lake/policy_reports",
}


def _unwrap(dag_module, name):
    return getattr(dag_module, name).__wrapped__


def _parsed_data_one_policy():
    return {
        "policies": {
            "iceberg.db1.t1": {
                "type": "table", "catalog": "iceberg", "schema": "db1",
                "table": "t1", "column": "*",
                "roles": [{
                    "role": "role_analyst", "permissions": ["read"],
                    "groups": ["grp1"], "users": ["user1"], "rowfilter": "",
                }],
                "label": None,
            }
        },
        "role_principals": {"role_analyst": {"groups": ["grp1"], "users": ["user1"]}},
        "skipped_rows": [],
    }


class TestBuildReportHtml:
    """Verify that user-controlled data is HTML-escaped to prevent XSS."""

    def _make_run_info(self, **overrides):
        defaults = dict(
            run_id="run_001", dag_run_id="manual__2026",
            excel_file_path="s3a://bucket/file.xlsx",
            started_at="2026-03-10 10:00:00", completed_at="2026-03-10 10:05:00",
            status="COMPLETED", total_policies_parsed=1, total_role_mappings_parsed=1,
            groups_created=0, groups_existing=0, policies_created=1, policies_updated=0,
            policies_failed=0, roles_created=0, roles_existing=0, mappings_created=0,
            mappings_existing=0, failed_operations=0,
        )
        defaults.update(overrides)
        return SimpleNamespace(**defaults)

    def test_xss_in_policy_name_is_escaped(self, dag_module):
        payload = "<script>alert(1)</script>"
        run_info = self._make_run_info()
        policy_status = SimpleNamespace(
            policy_name=payload, users=[], groups=[], permissions=[],
            rowfilter="", status="CREATED", error_message="",
            created_at="", updated_at="",
        )
        html = dag_module.build_report_html(run_info, [], [policy_status], [], "run_001")
        assert payload not in html
        assert "&lt;script&gt;" in html

    def test_xss_in_rowfilter_is_escaped(self, dag_module):
        payload = "<img src=x onerror=alert(1)>"
        run_info = self._make_run_info()
        policy_status = SimpleNamespace(
            policy_name="p1", users=[], groups=[], permissions=[],
            rowfilter=payload, status="CREATED", error_message="",
            created_at="", updated_at="",
        )
        html = dag_module.build_report_html(run_info, [], [policy_status], [], "run_001")
        assert payload not in html
        assert "&lt;img" in html

    def test_xss_in_error_message_is_escaped(self, dag_module):
        payload = "<b>bold</b>"
        run_info = self._make_run_info()
        obj = SimpleNamespace(
            object_type="policy", object_name="iceberg.db1.t1",
            policy_name="iceberg.db1.t1", policy_id="1",
            status="FAILED", error_message=payload, attempt=1,
            started_at="", completed_at="",
        )
        html = dag_module.build_report_html(run_info, [obj], [], [], "run_001")
        assert payload not in html
        assert "&lt;b&gt;" in html

    def test_xss_in_skipped_row_reason_is_escaped(self, dag_module):
        payload = "<script>steal(document.cookie)</script>"
        run_info = self._make_run_info()
        skipped = [{"row_index": 0, "role": "r1", "database": "", "url": "", "reason": payload}]
        html = dag_module.build_report_html(run_info, [], [], skipped, "run_001")
        assert payload not in html
        assert "&lt;script&gt;" in html

    def test_xss_in_run_id_is_escaped(self, dag_module):
        payload = "<script>x</script>"
        run_info = self._make_run_info()
        html = dag_module.build_report_html(run_info, [], [], [], payload)
        assert payload not in html
        assert "&lt;script&gt;" in html

    def test_xss_in_excel_path_is_escaped(self, dag_module):
        run_info = self._make_run_info(excel_file_path="s3a://bucket/<evil>.xlsx")
        html = dag_module.build_report_html(run_info, [], [], [], "run_001")
        assert "<evil>" not in html
        assert "&lt;evil&gt;" in html


class TestBuildInitialPolicyStatuses:
    def test_conflicting_rowfilters_raises(self, dag_module):
        fn = _unwrap(dag_module, "build_initial_policy_statuses")
        parsed = {
            "policies": {
                "iceberg.db1.t1": {
                    "type": "table",
                    "roles": [
                        {"role": "r1", "permissions": ["read"], "groups": [], "users": [], "rowfilter": "a=1"},
                        {"role": "r2", "permissions": ["read"], "groups": [], "users": [], "rowfilter": "b=2"},
                    ],
                }
            },
            "role_principals": {},
            "skipped_rows": [],
        }
        with pytest.raises(ValueError, match="Conflicting rowfilters"):
            fn(parsed, "run_conflict")


class TestBuildFinalPolicyStatuses:
    def _ranger_result(self, status="CREATED", policy_id="42"):
        return {
            "summary": {
                "groups": {"created": [], "existing": []},
                "policies": {
                    "created": [{"policy_name": "iceberg.db1.t1", "policy_id": policy_id}],
                    "updated": [], "failed": [],
                },
            },
            "policy_principals": {
                "applied_roles": {"iceberg.db1.t1": ["role_analyst"]},
                "excluded_roles": {},
            },
            "statuses": [{
                "run_id": "run_001", "object_type": "policy",
                "object_name": "iceberg.db1.t1", "policy_id": policy_id,
                "policy_name": "iceberg.db1.t1", "status": status,
                "error_message": "", "attempt": 1,
                "started_at": datetime.now(timezone.utc),
                "completed_at": datetime.now(timezone.utc),
            }],
        }

    def test_exclusion_notes_added(self, dag_module):
        fn = _unwrap(dag_module, "build_final_policy_statuses")
        ranger = self._ranger_result()
        ranger["policy_principals"]["excluded_roles"] = {"iceberg.db1.t1": ["role_excluded"]}
        result = fn(_parsed_data_one_policy(), "run_001", ranger)
        assert "role_excluded" in result[0]["error_message"]


class TestWriteSkippedRows:
    def test_insert_sql(self, dag_module):
        fn = _unwrap(dag_module, "write_skipped_rows")
        spark = MagicMock()
        skipped = [
            {"row_index": 0, "role": "r1", "database": "db1", "url": "", "reason": "bad"},
        ]
        with patch.object(dag_module, "get_config", return_value=_CONFIG):
            fn("run_001", skipped, spark)

        sql = spark.sql.call_args[0][0]
        assert "policy_tracking.tracking_ranger_policy_skipped_rows" in sql
        assert "'run_001'" in sql
        assert "'r1'" in sql
        assert "'db1'" in sql

    def test_escapes_single_quotes(self, dag_module):
        fn = _unwrap(dag_module, "write_skipped_rows")
        spark = MagicMock()
        skipped = [{"row_index": 0, "role": "it's", "database": "db", "url": "", "reason": "has'quote"}]
        with patch.object(dag_module, "get_config", return_value=_CONFIG):
            fn("run_001", skipped, spark)

        sql = spark.sql.call_args[0][0]
        assert "it''s" in sql
        assert "has''quote" in sql

    def test_null_row_index(self, dag_module):
        fn = _unwrap(dag_module, "write_skipped_rows")
        spark = MagicMock()
        skipped = [{"row_index": None, "role": "r1", "database": "db1", "url": "", "reason": "bad"}]
        with patch.object(dag_module, "get_config", return_value=_CONFIG):
            fn("run_001", skipped, spark)

        sql = spark.sql.call_args[0][0]
        assert "NULL" in sql

    def test_noop_on_empty_input(self, dag_module):
        fn = _unwrap(dag_module, "write_skipped_rows")
        spark = MagicMock()
        with patch.object(dag_module, "get_config", return_value=_CONFIG):
            count = fn("run_001", [], spark)

        assert count == 0
        spark.sql.assert_not_called()

    def test_continues_after_spark_error(self, dag_module):
        fn = _unwrap(dag_module, "write_skipped_rows")
        spark = MagicMock()
        spark.sql.side_effect = [Exception("connection lost"), None]
        skipped = [
            {"row_index": 0, "role": "r1", "database": "db1", "url": "", "reason": "bad"},
            {"row_index": 1, "role": "r2", "database": "db2", "url": "", "reason": "worse"},
        ]
        with patch.object(dag_module, "get_config", return_value=_CONFIG):
            count = fn("run_001", skipped, spark)

        assert count == 1
        assert spark.sql.call_count == 2


class TestCreateRangerGroupsAndPolicies:
    def _setup_mocks(self, sync_return=None):
        import sys
        ranger_utils_mock = sys.modules["ranger_utils"]
        manager = MagicMock()
        ranger_utils_mock.RangerPolicyManager.return_value = manager
        manager.ensure_groups_exist.return_value = {"role_analyst": True, "grp1": False}
        manager.sync_policies_from_dict.return_value = sync_return or {
            "created": [{"policy_name": "iceberg.db1.t1", "policy_id": "42"}],
            "updated": [], "failed": [],
        }
        return manager

    def _keycloak_result(self):
        return {
            "summary": {
                "created_mappings": [
                    {"role": "role_analyst", "principal": "grp1", "type": "group"},
                    {"role": "role_analyst", "principal": "user1", "type": "user"},
                ],
                "existing_mappings": [],
            },
            "statuses": [],
        }

    def test_group_creation_statuses(self, dag_module):
        fn = _unwrap(dag_module, "create_ranger_groups_and_policies")
        manager = self._setup_mocks()

        with patch.object(dag_module, "get_config", return_value=_CONFIG):
            result = fn(_parsed_data_one_policy(), "run_001", self._keycloak_result())

        called_roles = set(manager.ensure_groups_exist.call_args[0][0])
        assert "role_analyst" in called_roles

        group_statuses = [s for s in result["statuses"] if s["object_type"] == "ranger_group"]
        created_groups = [s["object_name"] for s in group_statuses if s["status"] == "CREATED"]
        existing_groups = [s["object_name"] for s in group_statuses if s["status"] == "ALREADY_EXISTS"]
        assert "role_analyst" in created_groups
        assert "grp1" in existing_groups

        assert "iceberg.db1.t1" in result["summary"]["policies"]["created"]

    def test_failed_policies_tracked(self, dag_module):
        fn = _unwrap(dag_module, "create_ranger_groups_and_policies")
        self._setup_mocks(sync_return={
            "created": [], "updated": [],
            "failed": [{"name": "iceberg.db1.t1", "error": "conflict"}],
        })

        with patch.object(dag_module, "get_config", return_value=_CONFIG):
            result = fn(_parsed_data_one_policy(), "run_001", self._keycloak_result())

        failed = result["summary"]["policies"]["failed"]
        assert any(f["name"] == "iceberg.db1.t1" and f["error"] == "conflict" for f in failed)

        failed_statuses = [s for s in result["statuses"]
                          if s["object_type"] == "policy" and s["status"] == "FAILED"]
        assert any(s["object_name"] == "iceberg.db1.t1" and s["error_message"] == "conflict"
                   for s in failed_statuses)


class TestCreateKeycloakRoles:
    def test_syncs_roles_and_builds_statuses(self, dag_module):
        fn = _unwrap(dag_module, "create_keycloak_roles")
        import sys
        ranger_utils_mock = sys.modules["ranger_utils"]
        manager = MagicMock()
        ranger_utils_mock.KeycloakRoleManager.return_value = manager
        ranger_utils_mock.KeycloakRoleManager.side_effect = None
        manager.sync_roles_and_principals.return_value = {
            "created_roles": ["role_analyst"], "existing_roles": [],
            "created_groups": [], "existing_groups": [],
            "created_mappings": [{"role": "role_analyst", "principal": "grp1", "type": "group"}],
            "existing_mappings": [], "failed": [],
        }

        with patch.object(dag_module, "get_config", return_value=_CONFIG):
            result = fn(_parsed_data_one_policy(), "run_001", {"status": "healthy"})

        synced = manager.sync_roles_and_principals.call_args[0][0]
        assert "role_analyst" in synced

        role_statuses = [s for s in result["statuses"] if s["object_type"] == "role"]
        assert any(s["object_name"] == "role_analyst" and s["status"] == "CREATED" for s in role_statuses)

        mapping_statuses = [s for s in result["statuses"] if s["object_type"] == "mapping"]
        assert any("role_analyst->grp1 (group)" in s["object_name"] for s in mapping_statuses)

    def test_connection_error_returns_failure(self, dag_module):
        fn = _unwrap(dag_module, "create_keycloak_roles")
        import sys
        ranger_utils_mock = sys.modules["ranger_utils"]
        ranger_utils_mock.KeycloakRoleManager.side_effect = ConnectionError("timeout")

        with patch.object(dag_module, "get_config", return_value=_CONFIG):
            result = fn(_parsed_data_one_policy(), "run_001", {"status": "healthy"})

        assert result["connection_error"] is True
        ranger_utils_mock.KeycloakRoleManager.side_effect = None

    def test_attempt_num_from_context_on_failure(self, dag_module):
        """When get_current_context is available, attempt_num should flow into failure statuses."""
        fn = _unwrap(dag_module, "create_keycloak_roles")
        import sys
        ranger_utils_mock = sys.modules["ranger_utils"]
        ranger_utils_mock.KeycloakRoleManager.side_effect = ConnectionError("timeout")

        ctx_mock = MagicMock()
        ctx_mock.__getitem__.side_effect = lambda k: SimpleNamespace(try_number=3) if k == "ti" else None

        operators_python_mock = MagicMock()
        operators_python_mock.get_current_context = MagicMock(return_value=ctx_mock)

        with patch.object(dag_module, "get_config", return_value=_CONFIG), \
             patch.dict(sys.modules, {
                 "airflow.operators": MagicMock(),
                 "airflow.operators.python": operators_python_mock,
             }):
            result = fn(_parsed_data_one_policy(), "run_001", {"status": "healthy"})

        assert result["connection_error"] is True
        # All failure statuses should carry attempt=3
        assert all(s["attempt"] == 3 for s in result["statuses"])
        ranger_utils_mock.KeycloakRoleManager.side_effect = None

    def test_attempt_num_defaults_to_1_without_context(self, dag_module):
        """When get_current_context raises, attempt_num defaults to 1 gracefully."""
        fn = _unwrap(dag_module, "create_keycloak_roles")
        import sys
        ranger_utils_mock = sys.modules["ranger_utils"]
        ranger_utils_mock.KeycloakRoleManager.side_effect = ConnectionError("timeout")

        with patch.object(dag_module, "get_config", return_value=_CONFIG), \
             patch("airflow.operators.python.get_current_context", side_effect=RuntimeError("no context")):
            result = fn(_parsed_data_one_policy(), "run_001", {"status": "healthy"})

        assert result["connection_error"] is True
        assert all(s["attempt"] == 1 for s in result["statuses"])
        ranger_utils_mock.KeycloakRoleManager.side_effect = None


class TestFinalizePolicyRun:
    def _make_run_metrics(self):
        return {
            "groups_created": 1, "groups_existing": 0,
            "policies_created": 1, "policies_updated": 0, "policies_failed": 0,
            "roles_created": 1, "roles_existing": 0,
            "mappings_created": 1, "mappings_existing": 0,
            "total_objects": 2, "successful_objects": 2, "failed_objects": 0,
            "overall_status": "COMPLETED",
            "total_policies_parsed": 1,
            "total_role_mappings_parsed": 1,
        }

    def test_metrics_flow_into_update_sql(self, dag_module):
        fn = _unwrap(dag_module, "finalize_policy_run")
        spark = MagicMock()
        run_metrics = self._make_run_metrics()

        with patch.object(dag_module, "get_config", return_value=_CONFIG):
            fn("run_001", "dag_run_1", "s3a://b/f.xlsx", run_metrics, spark)

        update_sql = spark.sql.call_args_list[0][0][0]
        assert "policy_tracking.tracking_ranger_policy_runs" in update_sql
        assert "status = 'COMPLETED'" in update_sql
        assert "run_id = 'run_001'" in update_sql
        assert "groups_created = 1" in update_sql
        assert "policies_created = 1" in update_sql

    def test_leftover_running_statuses_marked_failed(self, dag_module):
        fn = _unwrap(dag_module, "finalize_policy_run")
        spark = MagicMock()
        run_metrics = self._make_run_metrics()

        with patch.object(dag_module, "get_config", return_value=_CONFIG):
            fn("run_001", "dag_run_1", "s3a://b/f.xlsx", run_metrics, spark)

        fallback_sql = spark.sql.call_args_list[1][0][0]
        assert "status = 'FAILED'" in fallback_sql
        assert "status = 'RUNNING'" in fallback_sql
        assert "run_id = 'run_001'" in fallback_sql

    def test_returns_run_summary(self, dag_module):
        fn = _unwrap(dag_module, "finalize_policy_run")
        spark = MagicMock()
        run_metrics = self._make_run_metrics()

        with patch.object(dag_module, "get_config", return_value=_CONFIG):
            result = fn("run_001", "dag_run_1", "s3a://b/f.xlsx", run_metrics, spark)

        assert result["run_id"] == "run_001"
        assert result["status"] == "COMPLETED"
        assert result["failed_objects"] == 0
        assert result["successful_objects"] >= 1


class TestGeneratePolicyReport:
    def _mock_spark(self):
        spark = MagicMock()
        run_info = SimpleNamespace(
            run_id="run_001", dag_run_id="manual__2026-03-10",
            excel_file_path="s3a://bucket/file.xlsx",
            started_at="2026-03-10 10:00:00", completed_at="2026-03-10 10:05:00",
            status="COMPLETED", total_policies_parsed=1, total_role_mappings_parsed=1,
            groups_created=1, groups_existing=0, policies_created=1, policies_updated=0,
            policies_failed=0, roles_created=1, roles_existing=0, mappings_created=1,
            mappings_existing=0, failed_operations=0,
        )
        object_row = SimpleNamespace(
            object_type="policy", object_name="iceberg.db1.t1",
            policy_name="iceberg.db1.t1", policy_id="42", status="CREATED",
            error_message="", attempt=1,
            started_at="2026-03-10 10:01:00", completed_at="2026-03-10 10:01:01",
        )
        policy_status = SimpleNamespace(
            policy_name="iceberg.db1.t1", users=["user1"], groups=["role_analyst"],
            permissions=["read"], rowfilter="", status="CREATED", error_message="",
            created_at="2026-03-10 10:01:00", updated_at="2026-03-10 10:01:01",
        )
        skipped_row = {
            "row_index": 0, "role": "bad_role", "database": "", "url": "", "reason": "invalid",
        }
        spark.sql.return_value.collect.side_effect = [
            [run_info], [object_row], [policy_status], [skipped_row],
        ]
        spark._jsc = MagicMock()
        spark._jvm = MagicMock()
        fs_mock = MagicMock()
        spark._jvm.org.apache.hadoop.fs.FileSystem.get.return_value = fs_mock
        output_stream = MagicMock()
        fs_mock.create.return_value = output_stream
        return spark, output_stream

    def test_report_queries_and_output_path(self, dag_module):
        fn = _unwrap(dag_module, "generate_policy_report")
        spark, output_stream = self._mock_spark()

        with patch.object(dag_module, "get_config", return_value=_CONFIG):
            result = fn("run_001", spark)

        sql_calls = [call[0][0] for call in spark.sql.call_args_list]
        tables_queried = [
            "tracking_ranger_policy_runs",
            "tracking_ranger_policy_object_status",
            "tracking_ranger_policy_status",
            "tracking_ranger_policy_skipped_rows",
        ]
        for table in tables_queried:
            assert any(table in sql for sql in sql_calls), f"{table} not queried"
        assert all("run_001" in sql for sql in sql_calls)

        assert result["report_path"].startswith("s3a://data-lake/policy_reports/")
        assert "run_001" in result["report_path"]
        assert "html_content" not in result, "html_content must not be stored in XCom"

        written_bytes = output_stream.write.call_args[0][0]
        assert len(written_bytes) > 0

    def test_missing_run_raises(self, dag_module):
        fn = _unwrap(dag_module, "generate_policy_report")
        spark = MagicMock()
        spark.sql.return_value.collect.return_value = []

        with patch.object(dag_module, "get_config", return_value=_CONFIG), pytest.raises(ValueError, match="No run found"):
            fn("run_missing", spark)


class TestSendPolicyReportEmail:
    def test_sends_when_recipients_configured(self, dag_module):
        fn = _unwrap(dag_module, "send_policy_report_email")
        import sys
        email_mock = sys.modules["airflow.utils.email"]
        email_mock.send_email = MagicMock()

        report = {"report_path": "s3a://bucket/report.html"}
        config = {**_CONFIG, "email_recipients": "a@b.com, c@d.com"}

        boto3_mock = MagicMock()
        boto3_mock.client.return_value.get_object.return_value = {
            'Body': MagicMock(read=MagicMock(return_value=b"<html>report</html>"))
        }
        with patch.object(dag_module, "get_config", return_value=config), \
             patch.dict(sys.modules, {"boto3": boto3_mock}):
            result = fn(report, "run_001")

        assert result["sent"] is True

    def test_skips_when_no_recipients(self, dag_module):
        fn = _unwrap(dag_module, "send_policy_report_email")
        report = {"report_path": "s3a://bucket/r.html"}

        with patch.object(dag_module, "get_config", return_value=_CONFIG):
            result = fn(report, "run_001")

        assert result["sent"] is False


class TestWritePolicyStatuses:
    def test_merge_sql(self, dag_module):
        fn = _unwrap(dag_module, "write_policy_statuses")
        spark = MagicMock()
        now = datetime.now(timezone.utc)
        statuses = [{
            "run_id": "run_001", "policy_id": "p1", "policy_name": "iceberg.db1.t1",
            "users": ["alice", "bob"], "groups": ["analysts"], "permissions": ["read", "write"],
            "rowfilter": None, "status": "RUNNING", "error_message": "",
            "created_at": now, "updated_at": now,
        }]

        with patch.object(dag_module, "get_config", return_value=_CONFIG):
            fn(statuses, spark)

        sql = spark.sql.call_args[0][0]
        assert "MERGE INTO policy_tracking.tracking_ranger_policy_status" in sql
        assert "'run_001'" in sql
        assert "'iceberg.db1.t1'" in sql
        assert "array('alice', 'bob')" in sql
        assert "array('analysts')" in sql
        assert "array('read', 'write')" in sql
        assert "rowfilter = NULL" in sql

    def test_skips_invalid_rowfilter(self, dag_module):
        fn = _unwrap(dag_module, "write_policy_statuses")
        spark = MagicMock()
        now = datetime.now(timezone.utc)
        statuses = [{
            "run_id": "run_001", "policy_id": "p1", "policy_name": "iceberg.db1.t1",
            "users": ["u1"], "groups": ["g1"], "permissions": ["read"],
            "rowfilter": "dept='fin'; DROP TABLE --", "status": "RUNNING",
            "error_message": "", "created_at": now, "updated_at": now,
        }]

        with patch.object(dag_module, "get_config", return_value=_CONFIG):
            count = fn(statuses, spark)

        assert count == 0
        spark.sql.assert_not_called()


class TestWritePolicyObjectStatuses:
    def test_merge_sql(self, dag_module):
        fn = _unwrap(dag_module, "write_policy_object_statuses")
        spark = MagicMock()
        now = datetime.now(timezone.utc)
        statuses = [{
            "run_id": "run_001", "object_type": "policy",
            "object_name": "iceberg.db1.t1", "policy_id": "42",
            "policy_name": "iceberg.db1.t1", "status": "CREATED",
            "error_message": "", "attempt": 1,
            "started_at": now, "completed_at": now,
        }]

        with patch.object(dag_module, "get_config", return_value=_CONFIG):
            result = fn(statuses, spark)

        sql = spark.sql.call_args[0][0]
        assert "policy_tracking.tracking_ranger_policy_object_status" in sql
        assert "t.run_id = s.run_id" in sql
        assert "t.object_type = s.object_type" in sql
        assert "t.object_name = s.object_name" in sql
        assert "'run_001'" in sql
        assert "'policy'" in sql
        assert "'iceberg.db1.t1'" in sql
        assert "'42'" in sql
        assert result["written"] == 1

    def test_noop_on_empty_input(self, dag_module):
        fn = _unwrap(dag_module, "write_policy_object_statuses")
        spark = MagicMock()

        with patch.object(dag_module, "get_config", return_value=_CONFIG):
            result = fn([], spark)

        assert result["written"] == 0
        spark.sql.assert_not_called()
