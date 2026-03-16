"""
Unit tests for ranger_utils module.

These tests instantiate RangerPolicyManager with a mocked RangerClient so they
do not require the apache_ranger / keycloak packages to be installed.
"""
import sys
from unittest.mock import MagicMock, patch
import pytest


# ---------------------------------------------------------------------------
# Minimal stubs so ranger_utils can be imported without the real dependencies
# ---------------------------------------------------------------------------
def _stub_apache_ranger():
    """Insert minimal apache_ranger stubs into sys.modules."""
    ranger_policy = MagicMock()

    # RangerPolicy: track attribute assignments through __setattr__
    class _RangerPolicy:
        def __init__(self):
            self.__dict__["_attrs"] = {}

        def __setattr__(self, key, value):
            self.__dict__["_attrs"][key] = value
            self.__dict__[key] = value

        def __getattr__(self, key):
            return self.__dict__.get(key, None)

    class _Access:
        """Stub for RangerPolicyItemAccess – supports both .type and ["type"] access."""
        def __init__(self, d):
            for k, v in (d.items() if isinstance(d, dict) else {}.items()):
                setattr(self, k, v)
            self._d = dict(d) if isinstance(d, dict) else {}
        def get(self, key, default=None):
            return self._d.get(key, default)
        def __getitem__(self, key):
            return self._d[key]
        def __contains__(self, key):
            return key in self._d
        def items(self):
            return self._d.items()

    ranger_policy.RangerPolicy = _RangerPolicy
    ranger_policy.RangerPolicyResource = MagicMock(side_effect=lambda d: d)
    ranger_policy.RangerPolicyItem = MagicMock
    ranger_policy.RangerPolicyItemAccess = _Access

    stubs = {
        "apache_ranger": MagicMock(),
        "apache_ranger.client": MagicMock(),
        "apache_ranger.client.ranger_client": MagicMock(RangerClient=MagicMock()),
        "apache_ranger.client.ranger_user_mgmt_client": MagicMock(RangerUserMgmtClient=MagicMock()),
        "apache_ranger.model": MagicMock(),
        "apache_ranger.model.ranger_policy": ranger_policy,
        "apache_ranger.model.ranger_service": MagicMock(),
        "apache_ranger.model.ranger_user_mgmt": MagicMock(
            RangerGroup=MagicMock(), RangerUser=MagicMock(), RangerGroupUser=MagicMock()
        ),
        "apache_ranger.exceptions": MagicMock(RangerServiceException=Exception),
        "keycloak": MagicMock(),
    }
    return stubs


@pytest.fixture(scope="module")
def ranger_utils_module():
    """Import ranger_utils with apache_ranger stubbed out."""
    stubs = _stub_apache_ranger()
    saved = {k: sys.modules.get(k) for k in stubs}

    # Also save the ranger_utils stub that dag_module (session fixture) may have placed
    saved_ranger_utils = sys.modules.get("ranger_utils")

    for k, v in stubs.items():
        sys.modules[k] = v

    # Re-import cleanly
    import importlib
    mod_name = "ranger_utils"
    if mod_name in sys.modules:
        del sys.modules[mod_name]

    import sys as _sys
    from pathlib import Path
    ranger_dir = str(Path(__file__).resolve().parent.parent)
    if ranger_dir not in _sys.path:
        _sys.path.insert(0, ranger_dir)

    try:
        module = importlib.import_module(mod_name)
        yield module
    finally:
        for k, original in saved.items():
            if original is None:
                sys.modules.pop(k, None)
            else:
                sys.modules[k] = original
        # Restore the ranger_utils stub (or remove if it wasn't there before)
        if saved_ranger_utils is None:
            sys.modules.pop(mod_name, None)
        else:
            sys.modules[mod_name] = saved_ranger_utils


def _make_manager(ranger_utils_module):
    """Create a RangerPolicyManager with all network calls mocked out."""
    with patch.object(ranger_utils_module.RangerPolicyManager, "_load_supported_access_types_once", return_value=None):
        mgr = ranger_utils_module.RangerPolicyManager(
            ranger_url="http://ranger:6080",
            ranger_username="admin",
            ranger_password="admin",
            service_name="test-svc",
        )
    mgr.client = MagicMock()
    mgr.user_mgmt_client = MagicMock()
    return mgr


class TestUpdatePolicyPolicyType:
    """Tests for the _update_policy policyType regression fix."""

    def _existing_policy_with_rowfilter(self):
        """Simulate an existing policy dict that has policyType=2 and rowFilterPolicyItems."""
        return {
            "id": 99,
            "name": "iceberg.db1.t1",
            "service": "test-svc",
            "policyType": 2,
            "policyItems": [
                {
                    "groups": ["role_analyst"],
                    "users": [],
                    "accesses": [{"type": "select", "isAllowed": True}],
                    "delegateAdmin": False,
                }
            ],
            "rowFilterPolicyItems": [
                {
                    "groups": ["role_analyst"],
                    "users": [],
                    "accesses": [{"type": "select", "isAllowed": True}],
                    "rowFilterInfo": {"filterExpr": "dept='finance'"},
                }
            ],
            "allowExceptions": [],
            "denyPolicyItems": [],
            "denyExceptions": [],
            "resources": {},
            "policyLabels": [],
            "description": "",
        }

    def test_no_row_filters_clears_policytype_and_items(self, ranger_utils_module):
        """When _update_policy is called without row_filter_items on a policy that
        previously had them, policyType must be reset to 0 and rowFilterPolicyItems removed."""
        mgr = _make_manager(ranger_utils_module)
        mgr.client.update_policy_by_id = MagicMock()
        mgr.client.get_policy_by_id = MagicMock(return_value={"id": 99, "name": "iceberg.db1.t1"})

        existing = self._existing_policy_with_rowfilter()

        # Build a minimal allow item (plain object with groups/accesses attributes)
        allow_item = MagicMock()
        allow_item.groups = ["role_analyst"]
        allow_item.users = []
        allow_item.accesses = [MagicMock(type="select")]

        resources = {"schema": {"values": ["db1"]}, "table": {"values": ["t1"]}}

        mgr._update_policy(
            existing_policy=existing,
            resources=resources,
            policy_items=[allow_item],
            row_filter_items=None,   # ← no row filters this time
        )

        # Inspect the RangerPolicy object sent to Ranger
        policy_obj = mgr.client.update_policy_by_id.call_args[0][1]
        assert getattr(policy_obj, "policyType", None) == 0, (
            "policyType should be reset to 0 when no row filters are provided"
        )
        assert not getattr(policy_obj, "rowFilterPolicyItems", None), (
            "rowFilterPolicyItems should be cleared when no row filters are provided"
        )

    def test_with_row_filters_sets_policytype_2(self, ranger_utils_module):
        """When row_filter_items are provided, policyType should be 2."""
        mgr = _make_manager(ranger_utils_module)
        mgr.client.update_policy_by_id = MagicMock()
        mgr.client.get_policy_by_id = MagicMock(return_value={"id": 99, "name": "iceberg.db1.t1"})

        existing = self._existing_policy_with_rowfilter()
        existing["policyType"] = 0
        existing["rowFilterPolicyItems"] = []

        allow_item = MagicMock()
        allow_item.groups = ["role_analyst"]
        allow_item.users = []
        allow_item.accesses = [MagicMock(type="select")]

        new_rf_item = {
            "groups": ["role_analyst"],
            "users": [],
            "accesses": [{"type": "select", "isAllowed": True}],
            "rowFilterInfo": {"filterExpr": "dept='hr'"},
        }

        resources = {"schema": {"values": ["db1"]}, "table": {"values": ["t1"]}}

        mgr._update_policy(
            existing_policy=existing,
            resources=resources,
            policy_items=[allow_item],
            row_filter_items=[new_rf_item],
        )

        policy_obj = mgr.client.update_policy_by_id.call_args[0][1]
        assert getattr(policy_obj, "policyType", None) == 2
        rf_items = getattr(policy_obj, "rowFilterPolicyItems", None)
        assert rf_items, "rowFilterPolicyItems should be set when row filters are provided"


# ---------------------------------------------------------------------------
# Helpers shared across new test classes
# ---------------------------------------------------------------------------

def _make_existing_policy(name="p1", policy_id=1):
    return {
        "id": policy_id,
        "name": name,
        "service": "test-svc",
        "policyType": 0,
        "policyItems": [],
        "rowFilterPolicyItems": [],
        "allowExceptions": [],
        "denyPolicyItems": [],
        "denyExceptions": [],
        "resources": {},
        "policyLabels": [],
        "description": "",
    }


@pytest.fixture
def kc_manager(ranger_utils_module):
    """KeycloakRoleManager with __init__ bypassed and keycloak_admin mocked."""
    mgr = ranger_utils_module.KeycloakRoleManager.__new__(
        ranger_utils_module.KeycloakRoleManager
    )
    mgr.server_url = "https://kc:8080"
    mgr.realm_name = "test_realm"
    mgr.max_retries = 3
    mgr.connection_timeout = 10
    mgr.verify_ssl = False
    mgr.ca_cert_path = None
    mgr.keycloak_admin = MagicMock()
    return mgr


# ---------------------------------------------------------------------------
# Permissions
# ---------------------------------------------------------------------------

class TestPermissions:
    def test_read(self, ranger_utils_module):
        result = ranger_utils_module.Permissions.get_access_types("read")
        assert result == ranger_utils_module.Permissions.READ
        assert "select" in result

    def test_write(self, ranger_utils_module):
        result = ranger_utils_module.Permissions.get_access_types("WRITE")
        assert result == ranger_utils_module.Permissions.WRITE

    def test_all(self, ranger_utils_module):
        result = ranger_utils_module.Permissions.get_access_types("ALL")
        assert result == ranger_utils_module.Permissions.ALL

    def test_specific_access_type_passthrough(self, ranger_utils_module):
        result = ranger_utils_module.Permissions.get_access_types("Select")
        assert result == ["select"]

    def test_unknown_access_type_passthrough(self, ranger_utils_module):
        result = ranger_utils_module.Permissions.get_access_types("custom_perm")
        assert result == ["custom_perm"]


# ---------------------------------------------------------------------------
# safe_deepcopy
# ---------------------------------------------------------------------------

class TestSafeDeepCopy:
    def test_nested_dict_is_independent(self, ranger_utils_module):
        orig = {"a": [1, 2], "b": {"c": 3}}
        copy = ranger_utils_module.safe_deepcopy(orig)
        assert copy == orig
        copy["a"].append(99)
        assert orig["a"] == [1, 2]

    def test_list_is_independent(self, ranger_utils_module):
        orig = [{"x": 1}, {"x": 2}]
        copy = ranger_utils_module.safe_deepcopy(orig)
        copy[0]["x"] = 999
        assert orig[0]["x"] == 1

    def test_tuple_preserved(self, ranger_utils_module):
        orig = (1, 2, [3, 4])
        copy = ranger_utils_module.safe_deepcopy(orig)
        assert isinstance(copy, tuple)
        assert copy[0] == 1

    def test_primitive_returned_as_is(self, ranger_utils_module):
        assert ranger_utils_module.safe_deepcopy(42) == 42
        assert ranger_utils_module.safe_deepcopy("hello") == "hello"


# ---------------------------------------------------------------------------
# generate_secure_password
# ---------------------------------------------------------------------------

class TestGenerateSecurePassword:
    def test_default_length(self, ranger_utils_module):
        pwd = ranger_utils_module.generate_secure_password()
        assert len(pwd) == 16

    def test_custom_length(self, ranger_utils_module):
        pwd = ranger_utils_module.generate_secure_password(24)
        assert len(pwd) == 24

    def test_password_is_string(self, ranger_utils_module):
        pwd = ranger_utils_module.generate_secure_password()
        assert isinstance(pwd, str)


# ---------------------------------------------------------------------------
# _is_valid_role_name
# ---------------------------------------------------------------------------

class TestIsValidRoleName:
    def test_none_is_invalid(self, ranger_utils_module):
        assert ranger_utils_module.RangerPolicyManager._is_valid_role_name(None) is False

    def test_empty_string_is_invalid(self, ranger_utils_module):
        assert ranger_utils_module.RangerPolicyManager._is_valid_role_name("") is False

    def test_nan_is_invalid(self, ranger_utils_module):
        assert ranger_utils_module.RangerPolicyManager._is_valid_role_name("NaN") is False

    def test_null_string_is_invalid(self, ranger_utils_module):
        assert ranger_utils_module.RangerPolicyManager._is_valid_role_name("null") is False

    def test_none_string_is_invalid(self, ranger_utils_module):
        assert ranger_utils_module.RangerPolicyManager._is_valid_role_name("none") is False

    def test_valid_role_name(self, ranger_utils_module):
        assert ranger_utils_module.RangerPolicyManager._is_valid_role_name("role_analyst") is True

    def test_whitespace_only_is_invalid(self, ranger_utils_module):
        assert ranger_utils_module.RangerPolicyManager._is_valid_role_name("   ") is False


# ---------------------------------------------------------------------------
# _load_supported_access_types_once
# ---------------------------------------------------------------------------

class TestLoadSupportedAccessTypes:
    def test_exception_on_get_service_returns_none(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        mgr.client.get_service.side_effect = Exception("Connection refused")
        result = mgr._load_supported_access_types_once()
        assert result is None

    def test_service_def_loads_access_types(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        mgr.client.get_service.return_value = {"typeId": 1, "type": "hive"}
        mgr.client.get_service_def.return_value = {
            "accessTypes": [{"name": "select"}, {"name": "insert"}]
        }
        result = mgr._load_supported_access_types_once()
        assert result == {"select", "insert"}

    def test_service_def_exception_returns_none(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        mgr.client.get_service.return_value = {"typeId": 1}
        mgr.client.get_service_def.side_effect = Exception("Not found")
        result = mgr._load_supported_access_types_once()
        assert result is None

    def test_service_def_not_dict_returns_none(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        mgr.client.get_service.return_value = {"typeId": 1}
        mgr.client.get_service_def.return_value = None
        result = mgr._load_supported_access_types_once()
        assert result is None

    def test_empty_access_types_returns_none(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        mgr.client.get_service.return_value = {"typeId": 1}
        mgr.client.get_service_def.return_value = {"accessTypes": []}
        result = mgr._load_supported_access_types_once()
        assert result is None

    def test_service_def_name_fallback(self, ranger_utils_module):
        """Uses serviceDefName when typeId is absent."""
        mgr = _make_manager(ranger_utils_module)
        mgr.client.get_service.return_value = {"type": "hive"}
        mgr.client.get_service_def.return_value = {
            "accessTypes": [{"name": "select"}]
        }
        result = mgr._load_supported_access_types_once()
        assert result == {"select"}


# ---------------------------------------------------------------------------
# _filter_access_types
# ---------------------------------------------------------------------------

class TestFilterAccessTypes:
    def test_no_supported_types_passes_all_through(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        # supported_access_types is None → no filtering
        result = mgr._filter_access_types({"select", "insert"}, "role1", "p1")
        assert result == {"select", "insert"}

    def test_drops_unsupported_types(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        mgr.supported_access_types = {"select", "insert"}
        result = mgr._filter_access_types({"select", "write", "custom"}, "role1", "p1")
        assert result == {"select"}

    def test_empty_input_returns_empty(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        result = mgr._filter_access_types(set(), "role1", "p1")
        assert result == set()


# ---------------------------------------------------------------------------
# ensure_group_exists (RangerPolicyManager – single)
# ---------------------------------------------------------------------------

class TestRangerEnsureGroupExistsSingle:
    def test_creates_new_group(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        groups_result = MagicMock()
        groups_result.list = []
        mgr.user_mgmt_client.find_groups.return_value = groups_result
        result = mgr.ensure_group_exists("new_group")
        assert result is True
        mgr.user_mgmt_client.create_group.assert_called_once()

    def test_skips_existing_group(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        existing = MagicMock()
        existing.name = "existing_group"
        groups_result = MagicMock()
        groups_result.list = [existing]
        mgr.user_mgmt_client.find_groups.return_value = groups_result
        result = mgr.ensure_group_exists("existing_group")
        assert result is False
        mgr.user_mgmt_client.create_group.assert_not_called()

    def test_raises_on_exception(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        mgr.user_mgmt_client.find_groups.side_effect = Exception("network")
        with pytest.raises(Exception, match="network"):
            mgr.ensure_group_exists("any_group")


# ---------------------------------------------------------------------------
# ensure_groups_exist (RangerPolicyManager – bulk)
# ---------------------------------------------------------------------------

class TestRangerEnsureGroupsExist:
    def test_mixed_create_and_skip(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        existing = MagicMock()
        existing.name = "existing_group"
        all_groups = MagicMock()
        all_groups.list = [existing]
        mgr.user_mgmt_client.find_groups.return_value = all_groups

        result = mgr.ensure_groups_exist(["existing_group", "new_group"])
        assert result["existing_group"] is False
        assert result["new_group"] is True

    def test_handles_fetch_exception_gracefully(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        mgr.user_mgmt_client.find_groups.side_effect = Exception("network error")
        result = mgr.ensure_groups_exist(["any_group"])
        # Exception during bulk-fetch → falls back to create path
        assert "any_group" in result

    def test_create_exception_marks_none(self, ranger_utils_module):
        # None = creation failed (distinct from False = pre-existed)
        mgr = _make_manager(ranger_utils_module)
        all_groups = MagicMock()
        all_groups.list = []
        mgr.user_mgmt_client.find_groups.return_value = all_groups
        mgr.user_mgmt_client.create_group.side_effect = Exception("create failed")
        result = mgr.ensure_groups_exist(["bad_group"])
        assert result["bad_group"] is None


# ---------------------------------------------------------------------------
# get_existing_policy
# ---------------------------------------------------------------------------

class TestGetExistingPolicy:
    def test_returns_matching_policy(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        mgr.client.find_policies.return_value = [
            {"name": "p1", "id": 1},
            {"name": "other", "id": 2},
        ]
        result = mgr.get_existing_policy("p1")
        assert result == {"name": "p1", "id": 1}

    def test_returns_none_when_not_found(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        mgr.client.find_policies.return_value = []
        result = mgr.get_existing_policy("nonexistent")
        assert result is None

    def test_exception_returns_none(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        mgr.client.find_policies.side_effect = Exception("API error")
        result = mgr.get_existing_policy("p1")
        assert result is None

    def test_dict_response_with_policies_key(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        mgr.client.find_policies.return_value = {"policies": [{"name": "p1", "id": 10}]}
        result = mgr.get_existing_policy("p1")
        assert result["id"] == 10


# ---------------------------------------------------------------------------
# _build_items_for_type
# ---------------------------------------------------------------------------

class TestBuildItemsForType:
    def test_builds_allow_items(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        rp_list = [
            {"role": "role_a", "permissions": ["read"], "item_type": "allow", "policy_name": "p1"}
        ]
        items = mgr._build_items_for_type(rp_list, "allow")
        assert len(items) == 1
        assert items[0].groups == ["role_a"]

    def test_skips_wrong_item_type(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        rp_list = [
            {"role": "role_a", "permissions": ["read"], "item_type": "deny", "policy_name": "p1"}
        ]
        items = mgr._build_items_for_type(rp_list, "allow")
        assert len(items) == 0

    def test_default_item_type_is_allow(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        # no item_type key → defaults to 'allow'
        rp_list = [{"role": "role_b", "permissions": ["read"], "policy_name": "p1"}]
        items = mgr._build_items_for_type(rp_list, "allow")
        assert len(items) == 1

    def test_invalid_role_produces_empty_groups(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        rp_list = [
            {"role": None, "permissions": ["read"], "item_type": "allow", "policy_name": "p1"}
        ]
        items = mgr._build_items_for_type(rp_list, "allow")
        assert len(items) == 1
        assert items[0].groups == []

    def test_all_access_filtered_skips_item(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        mgr.supported_access_types = set()  # nothing supported
        rp_list = [
            {"role": "role_a", "permissions": ["read"], "item_type": "allow", "policy_name": "p1"}
        ]
        items = mgr._build_items_for_type(rp_list, "allow")
        assert len(items) == 0


# ---------------------------------------------------------------------------
# _build_url_policy_items
# ---------------------------------------------------------------------------

class TestBuildUrlPolicyItems:
    def test_read_maps_to_read_access(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        rp_list = [{"role": "role_a", "permissions": ["read"], "policy_name": "p1"}]
        items = mgr._build_url_policy_items(rp_list)
        assert len(items) == 1
        assert items[0].groups == ["role_a"]
        types = {a.type for a in items[0].accesses}
        assert "read" in types

    def test_write_maps_to_read_and_write(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        rp_list = [{"role": "role_a", "permissions": ["write"], "policy_name": "p1"}]
        items = mgr._build_url_policy_items(rp_list)
        assert len(items) == 1
        types = {a.type for a in items[0].accesses}
        assert types == {"read", "write"}

    def test_all_perm_maps_to_read_and_write(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        rp_list = [{"role": "role_a", "permissions": ["all"], "policy_name": "p1"}]
        items = mgr._build_url_policy_items(rp_list)
        types = {a.type for a in items[0].accesses}
        assert "read" in types
        assert "write" in types

    def test_invalid_role_produces_empty_groups(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        rp_list = [{"role": "nan", "permissions": ["read"], "policy_name": "p1"}]
        items = mgr._build_url_policy_items(rp_list)
        assert len(items) == 1
        assert items[0].groups == []

    def test_filtered_access_skips_item(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        mgr.supported_access_types = set()
        rp_list = [{"role": "role_a", "permissions": ["read"], "policy_name": "p1"}]
        items = mgr._build_url_policy_items(rp_list)
        assert len(items) == 0


# ---------------------------------------------------------------------------
# _build_row_filter_items
# ---------------------------------------------------------------------------

class TestBuildRowFilterItems:
    def test_builds_item_with_filter(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        rp_list = [{"role": "role_a", "permissions": ["read"], "rowfilter": "dept='finance'"}]
        items = mgr._build_row_filter_items(rp_list)
        assert len(items) == 1
        assert items[0]["rowFilterInfo"]["filterExpr"] == "dept='finance'"
        assert items[0]["groups"] == ["role_a"]

    def test_skips_empty_rowfilter(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        rp_list = [{"role": "role_a", "permissions": ["read"], "rowfilter": ""}]
        items = mgr._build_row_filter_items(rp_list)
        assert len(items) == 0

    def test_skips_nan_rowfilter(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        rp_list = [{"role": "role_a", "permissions": ["read"], "rowfilter": "nan"}]
        items = mgr._build_row_filter_items(rp_list)
        assert len(items) == 0

    def test_invalid_role_produces_empty_groups(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        rp_list = [{"role": "nan", "permissions": ["read"], "rowfilter": "x=1"}]
        items = mgr._build_row_filter_items(rp_list)
        assert len(items) == 1
        assert items[0]["groups"] == []

    def test_access_type_is_select(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        rp_list = [{"role": "role_a", "permissions": ["write"], "rowfilter": "x=1"}]
        items = mgr._build_row_filter_items(rp_list)
        assert items[0]["accesses"] == [{"type": "select", "isAllowed": True}]


# ---------------------------------------------------------------------------
# _create_policy
# ---------------------------------------------------------------------------

class TestCreatePolicy:
    def test_creates_policy_without_row_filters(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        created = MagicMock()
        created.id = 42
        mgr.client.create_policy.return_value = created

        result = mgr._create_policy(
            policy_name="test_policy",
            resources={"schema": {"values": ["db1"]}},
            policy_items=[],
            row_filter_items=[],
            description=None,
        )
        assert result["status"] == "created"
        assert result["policy_id"] == 42

    def test_creates_policy_with_row_filters(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        created = MagicMock()
        created.id = 55
        mgr.client.create_policy.return_value = created

        rf_item = {"groups": ["role_a"], "rowFilterInfo": {"filterExpr": "x=1"}}
        result = mgr._create_policy(
            policy_name="rf_policy",
            resources={},
            policy_items=[],
            row_filter_items=[rf_item],
            description="test",
        )
        assert result["status"] == "created"
        # Policy object sent to Ranger should have policyType=2
        policy_obj = mgr.client.create_policy.call_args[0][0]
        assert getattr(policy_obj, "policyType", None) == 2

    def test_raises_when_create_returns_none(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        mgr.client.create_policy.return_value = None
        with pytest.raises(RuntimeError):
            mgr._create_policy("p", {}, [], [], None)

    def test_creates_policy_with_labels_and_exceptions(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        created = MagicMock()
        created.id = 77
        mgr.client.create_policy.return_value = created

        deny_item = MagicMock()
        deny_item.groups = ["role_deny"]
        allow_exc = MagicMock()
        allow_exc.groups = ["role_exc"]

        result = mgr._create_policy(
            policy_name="p_with_extras",
            resources={},
            policy_items=[],
            row_filter_items=[],
            description="desc",
            policy_labels=["label_a"],
            allow_exceptions=[allow_exc],
            deny_items=[deny_item],
        )
        assert result["status"] == "created"


# ---------------------------------------------------------------------------
# create_table_policy
# ---------------------------------------------------------------------------

class TestCreateTablePolicy:
    _BASE_RP = [{"role": "role_analyst", "permissions": ["read"]}]

    def test_creates_new_policy_no_rowfilter(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        mgr.get_existing_policy = MagicMock(return_value=None)
        created = MagicMock()
        created.id = 10
        mgr.client.create_policy.return_value = created

        result = mgr.create_table_policy(
            policy_name="p1",
            catalog="iceberg",
            schema="db1",
            table="t1",
            column="*",
            role_permissions=self._BASE_RP,
        )
        assert result["status"] == "created"

    def test_updates_existing_policy(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        existing = _make_existing_policy("p1")
        mgr.get_existing_policy = MagicMock(return_value=existing)
        mgr.client.update_policy_by_id = MagicMock()
        mgr.client.get_policy_by_id = MagicMock(return_value=existing)

        result = mgr.create_table_policy(
            policy_name="p1",
            catalog="iceberg",
            schema="db1",
            table="t1",
            column="*",
            role_permissions=self._BASE_RP,
        )
        assert result["status"] == "updated"

    def test_returns_failed_on_exception(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        mgr.get_existing_policy = MagicMock(side_effect=Exception("oops"))
        result = mgr.create_table_policy(
            policy_name="p1",
            catalog="iceberg",
            schema="db1",
            table="t1",
            column="*",
            role_permissions=self._BASE_RP,
        )
        assert result["status"] == "failed"

    def test_no_valid_items_after_filtering_returns_failed(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        mgr.supported_access_types = set()  # filter everything out
        result = mgr.create_table_policy(
            policy_name="p1",
            catalog="iceberg",
            schema="db1",
            table="t1",
            column="*",
            role_permissions=self._BASE_RP,
        )
        assert result["status"] == "failed"

    def test_creates_separate_rowfilter_policy(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        # get_existing_policy returns None for both access + rowfilter policies
        mgr.get_existing_policy = MagicMock(return_value=None)
        created = MagicMock()
        created.id = 20
        mgr.client.create_policy.return_value = created

        rp_with_filter = [
            {"role": "role_analyst", "permissions": ["read"], "rowfilter": "dept='finance'"}
        ]
        result = mgr.create_table_policy(
            policy_name="p_rf",
            catalog="iceberg",
            schema="db1",
            table="t1",
            column="*",
            role_permissions=rp_with_filter,
        )
        assert result["status"] == "created"
        # Rowfilter policy name is built from catalog.schema.table (not the access policy name)
        assert result.get("rowfilter_policy_name") == "iceberg.db1.t1__rowfilter"
        # _create_policy called twice (access + rowfilter)
        assert mgr.client.create_policy.call_count == 2

    def test_updates_existing_rowfilter_policy(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        existing_access = _make_existing_policy("p_rf")
        existing_rf = _make_existing_policy("p_rf__rowfilter")
        mgr.get_existing_policy = MagicMock(side_effect=[existing_access, existing_rf])
        mgr.client.update_policy_by_id = MagicMock()
        mgr.client.get_policy_by_id = MagicMock(return_value=existing_access)

        rp_with_filter = [
            {"role": "role_analyst", "permissions": ["read"], "rowfilter": "dept='hr'"}
        ]
        result = mgr.create_table_policy(
            policy_name="p_rf",
            catalog="iceberg",
            schema="db1",
            table="t1",
            column="*",
            role_permissions=rp_with_filter,
        )
        assert result["status"] == "updated"
        assert mgr.client.update_policy_by_id.call_count == 2

    def test_non_iceberg_catalog_adds_iceberg_alias(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        mgr.get_existing_policy = MagicMock(return_value=None)
        created = MagicMock()
        created.id = 30
        mgr.client.create_policy.return_value = created

        result = mgr.create_table_policy(
            policy_name="p_delta",
            catalog="delta",
            schema="db1",
            table="t1",
            column="*",
            role_permissions=self._BASE_RP,
        )
        assert result["status"] == "created"
        # Verify that the policy resources contain both catalogs
        policy_obj = mgr.client.create_policy.call_args[0][0]
        catalog_resource = getattr(policy_obj, "resources", {}).get("catalog")
        if catalog_resource:
            values = catalog_resource.get("values", []) if isinstance(catalog_resource, dict) else []
            assert "iceberg" in values


# ---------------------------------------------------------------------------
# create_url_policy
# ---------------------------------------------------------------------------

class TestCreateUrlPolicy:
    def test_creates_new_url_policy(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        mgr.get_existing_policy = MagicMock(return_value=None)
        created = MagicMock()
        created.id = 20
        mgr.client.create_policy.return_value = created

        result = mgr.create_url_policy(
            policy_name="url_p1",
            url="s3://my-bucket/data/*",
            role_permissions=[{"role": "role_analyst", "permissions": ["read"]}],
        )
        assert result["status"] == "created"

    def test_updates_existing_url_policy(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        existing = _make_existing_policy("url_p1")
        mgr.get_existing_policy = MagicMock(return_value=existing)
        mgr.client.update_policy_by_id = MagicMock()
        mgr.client.get_policy_by_id = MagicMock(return_value=existing)

        result = mgr.create_url_policy(
            policy_name="url_p1",
            url="s3://my-bucket/data/*",
            role_permissions=[{"role": "role_analyst", "permissions": ["read"]}],
        )
        assert result["status"] == "updated"

    def test_no_valid_items_returns_failed(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        mgr.supported_access_types = set()
        result = mgr.create_url_policy(
            policy_name="url_p1",
            url="s3://bucket/*",
            role_permissions=[{"role": "role_a", "permissions": ["read"]}],
        )
        assert result["status"] == "failed"

    def test_exception_returns_failed(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        mgr.get_existing_policy = MagicMock(side_effect=Exception("boom"))
        result = mgr.create_url_policy(
            policy_name="url_p1",
            url="s3://bucket/*",
            role_permissions=[{"role": "role_a", "permissions": ["read"]}],
        )
        assert result["status"] == "failed"


# ---------------------------------------------------------------------------
# sync_policies_from_dict
# ---------------------------------------------------------------------------

class TestSyncPoliciesFromDict:
    def test_creates_table_policy(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        mgr.get_existing_policy = MagicMock(return_value=None)
        created = MagicMock()
        created.id = 5
        mgr.client.create_policy.return_value = created

        policies = {
            "p1": {
                "type": "table",
                "catalog": "iceberg",
                "schema": "db1",
                "table": "t1",
                "column": "*",
                "roles": [{"role": "role_analyst", "permissions": ["read"]}],
            }
        }
        result = mgr.sync_policies_from_dict(policies)
        assert len(result["created"]) == 1
        assert result["created"][0]["policy_name"] == "p1"

    def test_skips_invalid_role_principal(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        policies = {
            "p1": {
                "type": "table",
                "catalog": "iceberg",
                "schema": "db1",
                "table": "t1",
                "column": "*",
                "roles": [{"role": None, "permissions": ["read"]}],
            }
        }
        result = mgr.sync_policies_from_dict(policies)
        assert len(result["failed"]) == 1
        assert "No valid role" in result["failed"][0]["error"]

    def test_creates_url_policy(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        mgr.get_existing_policy = MagicMock(return_value=None)
        created = MagicMock()
        created.id = 6
        mgr.client.create_policy.return_value = created

        policies = {
            "url_p1": {
                "type": "url",
                "url": "s3://bucket/data/*",
                "roles": [{"role": "role_a", "permissions": ["read"]}],
            }
        }
        result = mgr.sync_policies_from_dict(policies)
        assert len(result["created"]) == 1

    def test_updated_policy_goes_to_updated_list(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        existing = _make_existing_policy("p_upd")
        mgr.get_existing_policy = MagicMock(return_value=existing)
        mgr.client.update_policy_by_id = MagicMock()
        mgr.client.get_policy_by_id = MagicMock(return_value=existing)

        policies = {
            "p_upd": {
                "type": "table",
                "catalog": "iceberg",
                "schema": "db1",
                "table": "t1",
                "column": "*",
                "roles": [{"role": "role_analyst", "permissions": ["read"]}],
            }
        }
        result = mgr.sync_policies_from_dict(policies)
        assert len(result["updated"]) == 1

    def test_failed_policy_goes_to_failed_list(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        mgr.supported_access_types = set()  # causes ValueError inside create_table_policy
        policies = {
            "bad_p": {
                "type": "table",
                "catalog": "iceberg",
                "schema": "db1",
                "table": "t1",
                "column": "*",
                "roles": [{"role": "role_a", "permissions": ["read"]}],
            }
        }
        result = mgr.sync_policies_from_dict(policies)
        assert len(result["failed"]) == 1

    def test_policy_with_label(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        mgr.get_existing_policy = MagicMock(return_value=None)
        created = MagicMock()
        created.id = 7
        mgr.client.create_policy.return_value = created

        policies = {
            "labeled_p": {
                "type": "table",
                "catalog": "iceberg",
                "schema": "db1",
                "table": "t1",
                "column": "*",
                "label": "team_a",
                "roles": [{"role": "role_analyst", "permissions": ["read"]}],
            }
        }
        result = mgr.sync_policies_from_dict(policies)
        assert len(result["created"]) == 1


# ---------------------------------------------------------------------------
# KeycloakRoleManager – __init__
# ---------------------------------------------------------------------------

class TestKeycloakRoleManagerInit:
    def test_successful_init(self, ranger_utils_module):
        # keycloak is stubbed as a MagicMock, so KeycloakAdmin() returns a mock
        # that does not raise, and get_realm() succeeds on first attempt.
        mgr = ranger_utils_module.KeycloakRoleManager(
            server_url="https://kc:8080",
            realm_name="test",
            client_id="client",
            client_secret="secret",
            verify_ssl=False,
        )
        assert mgr.keycloak_admin is not None
        assert mgr.realm_name == "test"

    def test_init_raises_after_max_retries(self, ranger_utils_module):
        """All retry attempts fail → raises ConnectionError."""
        keycloak_mock = MagicMock()
        keycloak_mock.KeycloakAdmin.side_effect = Exception("Connection refused")
        with patch.dict(sys.modules, {"keycloak": keycloak_mock}):
            with pytest.raises(ConnectionError):
                ranger_utils_module.KeycloakRoleManager(
                    server_url="https://kc:8080",
                    realm_name="test",
                    client_id="client",
                    client_secret="secret",
                    max_retries=1,
                )


# ---------------------------------------------------------------------------
# KeycloakRoleManager – ensure_realm_role_exists
# ---------------------------------------------------------------------------

class TestKeycloakEnsureRealmRoleExists:
    def test_creates_new_role(self, kc_manager):
        kc_manager.keycloak_admin.get_realm_roles.return_value = []
        result = kc_manager.ensure_realm_role_exists("new_role")
        assert result is True
        kc_manager.keycloak_admin.create_realm_role.assert_called_once()

    def test_skips_existing_role(self, kc_manager):
        kc_manager.keycloak_admin.get_realm_roles.return_value = [{"name": "existing_role"}]
        result = kc_manager.ensure_realm_role_exists("existing_role")
        assert result is False
        kc_manager.keycloak_admin.create_realm_role.assert_not_called()

    def test_raises_on_exception(self, kc_manager):
        kc_manager.keycloak_admin.get_realm_roles.side_effect = Exception("api error")
        with pytest.raises(Exception, match="api error"):
            kc_manager.ensure_realm_role_exists("any_role")


# ---------------------------------------------------------------------------
# KeycloakRoleManager – ensure_group_exists
# ---------------------------------------------------------------------------

class TestKeycloakEnsureGroupExists:
    def test_creates_new_group(self, kc_manager):
        kc_manager.keycloak_admin.get_groups.side_effect = [
            [],  # first call: group absent
            [{"name": "new_group", "id": "g1"}],  # second call: after create
        ]
        group_id, created = kc_manager.ensure_group_exists("new_group")
        assert created is True
        assert group_id == "g1"

    def test_returns_existing_group(self, kc_manager):
        kc_manager.keycloak_admin.get_groups.return_value = [
            {"name": "existing_group", "id": "g2"}
        ]
        group_id, created = kc_manager.ensure_group_exists("existing_group")
        assert created is False
        assert group_id == "g2"

    def test_raises_on_exception(self, kc_manager):
        kc_manager.keycloak_admin.get_groups.side_effect = Exception("network")
        with pytest.raises(Exception, match="network"):
            kc_manager.ensure_group_exists("any_group")


# ---------------------------------------------------------------------------
# KeycloakRoleManager – assign_role_to_group
# ---------------------------------------------------------------------------

class TestKeycloakAssignRoleToGroup:
    def test_assigns_role_when_not_already_assigned(self, kc_manager):
        kc_manager.keycloak_admin.get_groups.return_value = [
            {"name": "grp1", "id": "gid1"}
        ]
        kc_manager.keycloak_admin.get_realm_role.return_value = {"name": "role1"}
        kc_manager.keycloak_admin.get_group_realm_roles.return_value = []
        result = kc_manager.assign_role_to_group("role1", "grp1")
        assert result is True
        kc_manager.keycloak_admin.assign_group_realm_roles.assert_called_once()

    def test_skips_role_already_assigned(self, kc_manager):
        kc_manager.keycloak_admin.get_groups.return_value = [
            {"name": "grp1", "id": "gid1"}
        ]
        kc_manager.keycloak_admin.get_realm_role.return_value = {"name": "role1"}
        kc_manager.keycloak_admin.get_group_realm_roles.return_value = [{"name": "role1"}]
        result = kc_manager.assign_role_to_group("role1", "grp1")
        assert result is False
        kc_manager.keycloak_admin.assign_group_realm_roles.assert_not_called()


# ---------------------------------------------------------------------------
# KeycloakRoleManager – sync_roles_and_principals
# ---------------------------------------------------------------------------

class TestKeycloakSyncRolesAndPrincipals:
    def _setup_common(self, kc_manager, group_name="g1", group_id="gid1"):
        kc_manager.keycloak_admin.get_realm_roles.return_value = []
        kc_manager.keycloak_admin.get_groups.return_value = [
            {"name": group_name, "id": group_id}
        ]
        kc_manager.keycloak_admin.get_group_realm_roles.return_value = []
        kc_manager.keycloak_admin.get_realm_role.return_value = {"name": "role1"}

    def test_creates_role_and_assigns_group(self, kc_manager):
        self._setup_common(kc_manager)
        result = kc_manager.sync_roles_and_principals(
            {"role1": {"groups": ["g1"], "users": []}}
        )
        assert "role1" in result["created_roles"]
        assert any(m["role"] == "role1" for m in result["created_mappings"])

    def test_existing_role_goes_to_existing_roles(self, kc_manager):
        kc_manager.keycloak_admin.get_realm_roles.return_value = [{"name": "existing_role"}]
        kc_manager.keycloak_admin.get_groups.return_value = []
        result = kc_manager.sync_roles_and_principals(
            {"existing_role": {"groups": [], "users": []}}
        )
        assert "existing_role" in result["existing_roles"]

    def test_role_create_failure_goes_to_failed(self, kc_manager):
        kc_manager.keycloak_admin.get_realm_roles.side_effect = Exception("forbidden")
        result = kc_manager.sync_roles_and_principals(
            {"bad_role": {"groups": ["g1"], "users": []}}
        )
        assert len(result["failed"]) >= 1
        assert result["failed"][0]["operation"] == "create_role"

    def test_group_assignment_failure_goes_to_failed(self, kc_manager):
        kc_manager.keycloak_admin.get_realm_roles.return_value = []
        kc_manager.keycloak_admin.get_groups.side_effect = Exception("group error")
        result = kc_manager.sync_roles_and_principals(
            {"role1": {"groups": ["bad_group"], "users": []}}
        )
        assert any(f["operation"] == "assign_role" for f in result["failed"])

    def test_existing_group_goes_to_existing_groups(self, kc_manager):
        kc_manager.keycloak_admin.get_realm_roles.return_value = []
        kc_manager.keycloak_admin.get_groups.return_value = [{"name": "g1", "id": "gid1"}]
        kc_manager.keycloak_admin.get_group_realm_roles.return_value = [{"name": "role1"}]
        kc_manager.keycloak_admin.get_realm_role.return_value = {"name": "role1"}
        result = kc_manager.sync_roles_and_principals(
            {"role1": {"groups": ["g1"], "users": []}}
        )
        assert "g1" in result["existing_groups"]


class TestGetExistingPolicyNoneResponse:
    """Regression: find_policies returning None must log WARNING and return None, not raise."""

    def test_none_response_returns_none_without_exception(self, ranger_utils_module):
        mgr = _make_manager(ranger_utils_module)
        mgr.client.find_policies.return_value = None
        result = mgr.get_existing_policy("iceberg.db1.t1")
        assert result is None

    def test_none_response_logs_warning_not_error(self, ranger_utils_module):
        import logging
        mgr = _make_manager(ranger_utils_module)
        mgr.client.find_policies.return_value = None
        logger = logging.getLogger("ranger_utils")
        with patch.object(logger, "warning") as mock_warn, \
             patch.object(logger, "error") as mock_err:
            mgr.get_existing_policy("iceberg.db1.t1")
        mock_warn.assert_called_once()
        mock_err.assert_not_called()


class TestRangerUrlSanitization:
    """Regression: stray quotes/whitespace in the ranger_url Variable must not produce an invalid URL."""

    @pytest.mark.parametrize("raw_url,expected", [
        ("http://ranger:6080",    "http://ranger:6080"),
        ("http://ranger:6080'",   "http://ranger:6080"),
        ("'http://ranger:6080'",  "http://ranger:6080"),
        ('"http://ranger:6080"',  "http://ranger:6080"),
        ("  http://ranger:6080 ", "http://ranger:6080"),
        (" 'http://ranger:6080' ", "http://ranger:6080"),
    ])
    def test_url_is_stripped_on_init(self, ranger_utils_module, raw_url, expected):
        with patch.object(ranger_utils_module.RangerPolicyManager, "_load_supported_access_types_once", return_value=None):
            mgr = ranger_utils_module.RangerPolicyManager(
                ranger_url=raw_url,
                ranger_username="admin",
                ranger_password="admin",
                service_name="svc",
            )
        assert mgr.ranger_url == expected
