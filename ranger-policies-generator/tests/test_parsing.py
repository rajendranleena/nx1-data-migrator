import pandas as pd
import pytest


class TestValidateRowfilter:
    def test_semicolon_raises(self, dag_module):
        with pytest.raises(ValueError, match="forbidden characters"):
            dag_module.validate_rowfilter("1=1; DROP TABLE x")


def _make_df(rows: list[dict]) -> pd.DataFrame:
    columns = [
        "role", "database", "tables", "columns", "url",
        "permissions", "groups", "users", "rowfilter", "policy_label",
    ]
    for row in rows:
        for col in columns:
            row.setdefault(col, "")
    return pd.DataFrame(rows)


class TestParseExcelRows:

    def test_parse_table_policy(self, dag_module):
        df = _make_df([{
            "role": "analyst", "database": "db1", "tables": "t1", "columns": "c1",
            "permissions": "read,write", "groups": "grp1", "users": "usr1",
        }])
        result = dag_module.parse_excel_rows(df)

        assert "iceberg.db1.t1.c1" in result["policies"]
        policy = result["policies"]["iceberg.db1.t1.c1"]
        assert policy["type"] == "table"
        role_entry = policy["roles"][0]
        assert role_entry["role"] == "analyst"
        assert set(role_entry["permissions"]) == {"read", "write"}
        assert "analyst" in result["role_principals"]
        assert result["skipped_rows"] == []

    def test_url_policy(self, dag_module):
        df = _make_df([{
            "role": "viewer", "url": "s3a://bucket/path",
            "permissions": "read", "groups": "grp_v",
        }])
        result = dag_module.parse_excel_rows(df)

        assert "s3a://bucket/path" in result["policies"]
        assert result["policies"]["s3a://bucket/path"]["type"] == "url"

    def test_skip_row_with_both_db_and_url(self, dag_module):
        df = _make_df([{"role": "r1", "database": "db1", "url": "s3a://b/p", "groups": "g1"}])
        result = dag_module.parse_excel_rows(df)

        assert len(result["policies"]) == 0
        assert len(result["skipped_rows"]) == 1

    def test_users_only_creates_synthetic_role(self, dag_module):
        df = _make_df([{"database": "db1", "users": "alice"}])
        result = dag_module.parse_excel_rows(df)

        assert len(result["policies"]) == 1
        assert "role_alice" in result["role_principals"]

    def test_cartesian_expansion(self, dag_module):
        df = _make_df([{
            "role": "r1", "database": "db1,db2", "tables": "t1,t2",
            "columns": "c1,c2", "groups": "g1",
        }])
        result = dag_module.parse_excel_rows(df)
        assert len(result["policies"]) == 8  # 2 * 2 * 2

    def test_merge_permissions_for_same_role(self, dag_module):
        df = _make_df([
            {"role": "analyst", "database": "db1", "tables": "t1", "permissions": "read", "groups": "g1"},
            {"role": "analyst", "database": "db1", "tables": "t1", "permissions": "write", "groups": "g1"},
        ])
        result = dag_module.parse_excel_rows(df)

        policy = result["policies"]["iceberg.db1.t1"]
        assert len(policy["roles"]) == 1
        assert {"read", "write"} <= set(policy["roles"][0]["permissions"])


class TestPatchPoliciesWithKeycloak:

    def _keycloak_result(self, created_mappings=None, existing_mappings=None):
        return {
            "summary": {
                "created_mappings": created_mappings or [],
                "existing_mappings": existing_mappings or [],
            }
        }

    def test_exclude_role_with_no_mappings(self, dag_module):
        policies = {
            "iceberg.db1.t1": {
                "type": "table",
                "roles": [{"role": "analyst", "permissions": ["read"], "groups": ["grp1"], "users": [], "rowfilter": ""}],
            }
        }
        kc = self._keycloak_result()
        result = dag_module.patch_policies_with_keycloak(policies, kc, "run1")

        assert "iceberg.db1.t1" not in result["patched_policies"]
        assert len(result["failure_statuses"]) == 1
        assert result["failure_statuses"][0]["status"] == "FAILED"

    def test_mixed_roles_partial_pass(self, dag_module):
        policies = {
            "p1": {
                "type": "table",
                "roles": [
                    {"role": "good_role", "permissions": ["read"], "groups": ["g1"], "users": [], "rowfilter": ""},
                    {"role": "bad_role", "permissions": ["write"], "groups": ["g2"], "users": [], "rowfilter": ""},
                ],
            }
        }
        kc = self._keycloak_result(
            created_mappings=[{"role": "good_role", "principal": "g1", "type": "group"}],
        )
        result = dag_module.patch_policies_with_keycloak(policies, kc, "run1")

        assert len(result["patched_policies"]["p1"]["roles"]) == 1
        assert "good_role" in result["applied_roles"]["p1"]
        assert "bad_role" in result["excluded_roles"]["p1"]


class TestComputeRunMetrics:

    def test_completed_when_no_failures(self, dag_module):
        parsed = {"policies": {"p1": {}}, "role_principals": {"r1": {}}}
        ranger = {
            "summary": {
                "groups": {"created": ["g1"], "existing": []},
                "policies": {"created": ["p1"], "updated": [], "failed": []},
            },
            "statuses": [{"status": "CREATED"}],
        }
        keycloak = {
            "summary": {
                "created_roles": ["r1"], "existing_roles": [],
                "created_mappings": [{"role": "r1", "principal": "g1", "type": "group"}],
                "existing_mappings": [],
            },
            "statuses": [{"status": "CREATED"}],
        }
        metrics = dag_module.compute_run_metrics(parsed, ranger, keycloak)

        assert metrics["overall_status"] == "COMPLETED"
        assert metrics["failed_objects"] == 0

    def test_partial_failure_on_failed_policy(self, dag_module):
        parsed = {"policies": {}, "role_principals": {}}
        ranger = {
            "summary": {
                "groups": {"created": [], "existing": []},
                "policies": {"created": [], "updated": [], "failed": ["p1"]},
            },
            "statuses": [{"status": "FAILED"}],
        }
        keycloak = {
            "summary": {"created_roles": [], "existing_roles": [], "created_mappings": [], "existing_mappings": []},
            "statuses": [],
        }
        metrics = dag_module.compute_run_metrics(parsed, ranger, keycloak)

        assert metrics["overall_status"] == "PARTIAL_FAILURE"
        assert metrics["failed_objects"] == 1
