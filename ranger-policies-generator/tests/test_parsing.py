import pandas as pd
import pytest


class TestValidateRowfilter:
    def test_semicolon_raises(self, dag_module):
        with pytest.raises(ValueError, match="forbidden characters"):
            dag_module.validate_rowfilter("1=1; DROP TABLE x")

    def test_newline_raises(self, dag_module):
        with pytest.raises(ValueError, match="forbidden characters"):
            dag_module.validate_rowfilter("col = 'val'\nOR 1=1")

    def test_carriage_return_raises(self, dag_module):
        with pytest.raises(ValueError, match="forbidden characters"):
            dag_module.validate_rowfilter("col = 'val'\rOR 1=1")

    def test_valid_rowfilter_passthrough(self, dag_module):
        # Should not raise and should return the value unchanged
        result = dag_module.validate_rowfilter("department = 'finance'")
        assert result == "department = 'finance'"

    def test_empty_rowfilter_passthrough(self, dag_module):
        assert dag_module.validate_rowfilter("") == ""
        assert dag_module.validate_rowfilter(None) is None


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
                "created_roles": [],
                "existing_roles": [],
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

    def test_user_only_role_excluded_when_user_mapping_failed(self, dag_module):
        # KC realm role was created but alice is not in KC → assignment failed → no mapping.
        # Policy must NOT proceed; a broken Ranger policy with an empty group is worse than no policy.
        policies = {
            "iceberg.hr_db.employees": {
                "type": "table",
                "roles": [{"role": "role_alice", "permissions": ["read"], "groups": [], "users": ["alice"], "rowfilter": ""}],
            }
        }
        kc = self._keycloak_result()  # no mappings — user assignment failed
        result = dag_module.patch_policies_with_keycloak(policies, kc, "run1")

        assert "iceberg.hr_db.employees" not in result["patched_policies"]
        assert len(result["failure_statuses"]) == 1
        assert result["failure_statuses"][0]["status"] == "FAILED"

    def test_user_only_role_passes_when_user_mapping_succeeded(self, dag_module):
        # If alice exists in KC and the user mapping succeeded, policy must proceed.
        policies = {
            "iceberg.hr_db.employees": {
                "type": "table",
                "roles": [{"role": "role_alice", "permissions": ["read"], "groups": [], "users": ["alice"], "rowfilter": ""}],
            }
        }
        kc = self._keycloak_result(
            created_mappings=[{"role": "role_alice", "principal": "alice", "type": "user"}]
        )
        result = dag_module.patch_policies_with_keycloak(policies, kc, "run1")

        assert "iceberg.hr_db.employees" in result["patched_policies"]
        assert "role_alice" in result["applied_roles"]["iceberg.hr_db.employees"]
        assert result["failure_statuses"] == []


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


class TestBuildPolicyName:
    """Tests for build_policy_name, covering the wildcard-table+specific-column edge case."""

    def test_specific_table_and_column(self, dag_module):
        assert dag_module.build_policy_name("iceberg", "db1", "t1", "col1") == "iceberg.db1.t1.col1"

    def test_specific_table_wildcard_column(self, dag_module):
        assert dag_module.build_policy_name("iceberg", "db1", "t1", "*") == "iceberg.db1.t1"

    def test_wildcard_table_wildcard_column(self, dag_module):
        assert dag_module.build_policy_name("iceberg", "db1", "*", "*") == "iceberg.db1"

    def test_wildcard_database_returns_catalog(self, dag_module):
        assert dag_module.build_policy_name("iceberg", "*", "*", "*") == "iceberg"

    def test_wildcard_table_specific_column_includes_both(self, dag_module):
        # Bug-fix regression: must NOT collapse to 'iceberg.db1'
        assert dag_module.build_policy_name("iceberg", "db1", "*", "amount") == "iceberg.db1.*.amount"

    def test_wildcard_table_specific_column_distinct_names(self, dag_module):
        # Two different columns on wildcard table must produce two distinct policy names
        name1 = dag_module.build_policy_name("iceberg", "finance_db", "*", "amount")
        name2 = dag_module.build_policy_name("iceberg", "finance_db", "*", "currency")
        assert name1 != name2
        assert name1 == "iceberg.finance_db.*.amount"
        assert name2 == "iceberg.finance_db.*.currency"


class TestFillDownContextSwitch:
    """Regression tests: switching between table-context and URL-context rows must
    reset the stale fill-down state so the 'both db and url' validation never false-fires."""

    def test_url_row_after_table_rows_is_not_skipped(self, dag_module):
        # URL rows following table rows must NOT inherit the filled-down database.
        df = _make_df([
            {"role": "r1", "database": "sales_db,finance_db", "tables": "reports", "groups": "g1"},
            {"role": "r2", "url": "s3a://data-lake/raw/*", "groups": "g2"},
        ])
        result = dag_module.parse_excel_rows(df)

        assert "s3a://data-lake/raw/*" in result["policies"], (
            "URL row was incorrectly skipped — stale database fill-down contaminated the URL row"
        )
        assert result["skipped_rows"] == []

    def test_url_filldown_row_after_table_rows_is_not_skipped(self, dag_module):
        # A URL continuation row (blank database AND blank url, url fills down)
        # following table rows must also NOT inherit the database.
        df = _make_df([
            {"role": "r1", "database": "sales_db", "tables": "orders", "groups": "g1"},
            {"role": "r2", "url": "s3a://data-lake/raw/*", "groups": "g2"},
            {"role": "r3", "groups": "g3"},   # url fills down from previous row
        ])
        result = dag_module.parse_excel_rows(df)

        assert "s3a://data-lake/raw/*" in result["policies"]
        # Both URL rows should merge into the same policy
        url_policy = result["policies"]["s3a://data-lake/raw/*"]
        roles_in_policy = [r["role"] for r in url_policy["roles"]]
        assert "r2" in roles_in_policy
        assert "r3" in roles_in_policy
        assert result["skipped_rows"] == []

    def test_table_row_after_url_rows_does_not_inherit_url(self, dag_module):
        # A new table row following URL rows must NOT inherit the filled-down URL,
        # which would cause the 'both db and url' skip.
        df = _make_df([
            {"role": "r1", "url": "s3a://data-lake/raw/*", "groups": "g1"},
            {"role": "r2", "database": "db1", "tables": "t1", "groups": "g2"},
        ])
        result = dag_module.parse_excel_rows(df)

        assert "iceberg.db1.t1" in result["policies"], (
            "Table row was incorrectly skipped — stale URL fill-down contaminated the table row"
        )
        assert result["skipped_rows"] == []

    def test_full_sample_excel_no_skipped_rows(self, dag_module):
        # Simulate the full sample_policies.xlsx ordering — no row should be skipped.
        rows = [
            {"role": "role_analyst",  "database": "sales_db",           "tables": "orders",    "columns": "*",               "permissions": "read",       "groups": "grp_analysts",  "item_type": "allow",           "policy_label": "team_sales"},
            {"role": "role_engineer",                                     "tables": "customers", "columns": "*",               "permissions": "write",      "groups": "grp_engineers", "item_type": "allow"},
            {"role": "role_analyst",                                      "tables": "customers", "columns": "*",               "permissions": "read",       "groups": "grp_analysts",  "rowfilter": "region='EMEA'",   "item_type": "allow"},
            {"role": "role_external",                                     "tables": "customers", "columns": "*",               "permissions": "select",     "groups": "grp_external",  "item_type": "deny"},
            {"role": "role_finance",  "database": "finance_db",          "tables": "*",         "columns": "amount,currency", "permissions": "read",       "groups": "grp_finance",   "item_type": "allow",           "policy_label": "team_finance"},
            {"role": "role_finance",                                      "tables": "*",         "columns": "amount",          "permissions": "read",       "groups": "grp_finance",   "item_type": "allow_exception"},
            {                         "database": "hr_db",               "tables": "employees", "columns": "*",               "permissions": "read",                                  "users": "alice",               "item_type": "allow"},
            {"role": "role_ops",      "database": "ops_db",              "tables": "incidents", "columns": "*",               "permissions": "read,write", "groups": "grp_ops",       "users": "bob",                 "item_type": "allow",           "policy_label": "team_ops"},
            {"role": "role_analyst",  "database": "sales_db,finance_db", "tables": "reports",   "columns": "*",               "permissions": "read",       "groups": "grp_analysts",  "item_type": "allow"},
            {"role": "role_data_eng",                                                                                          "url": "s3a://data-lake/raw/*", "permissions": "read",  "groups": "grp_data_eng",       "item_type": "allow",           "policy_label": "team_data"},
            {"role": "role_data_eng",                                                                                                                          "permissions": "write", "groups": "grp_data_eng",       "item_type": "allow"},
            {"role": "role_readonly",                                                                                          "url": "s3a://data-lake/raw/*", "permissions": "read",  "groups": "grp_readonly",       "item_type": "deny"},
        ]
        result = dag_module.parse_excel_rows(_make_df(rows))

        assert result["skipped_rows"] == [], (
            f"Expected no skipped rows, got: {result['skipped_rows']}"
        )
        # Rows 2-4 fill down database=sales_db and all use tables=customers → merge into
        # one policy.  Distinct policies: orders, customers, finance.*.amount,
        # finance.*.currency, hr.employees, ops.incidents, sales.reports,
        # finance.reports, s3a://data-lake/raw/*  → 9 total.
        assert len(result["policies"]) == 9
        assert "s3a://data-lake/raw/*" in result["policies"]



