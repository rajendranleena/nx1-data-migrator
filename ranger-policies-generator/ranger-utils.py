"""
Ranger Utility Module for Airflow DAG integration.

This module provides helper classes and functions for:
- Creating Ranger policies from structured data
- Managing Ranger groups
- Syncing with Keycloak

Based on the existing ranger.py implementation with enhancements for Excel-based automation.
"""

from apache_ranger.client.ranger_client import RangerClient
from apache_ranger.model.ranger_policy import (
    RangerPolicy, RangerPolicyResource,
    RangerPolicyItem, RangerPolicyItemAccess
)
from apache_ranger.model.ranger_service import RangerService
from apache_ranger.model.ranger_user_mgmt import RangerGroup, RangerUser, RangerGroupUser
from apache_ranger.client.ranger_user_mgmt_client import RangerUserMgmtClient
from apache_ranger.exceptions import RangerServiceException
from typing import Dict, List, Optional, Set, Any
import logging
import string
import secrets

logger = logging.getLogger(__name__)


# Permission mappings
class Permissions:
    """Standard permission sets for read/write access patterns."""
    
    READ = ["select", "use", "execute", "show", "read_sysinfo"]
    WRITE = [
        "select", "insert", "update", "delete", "create", "drop",
        "alter", "use", "show", "grant", "revoke", "execute",
        "read", "write", "read_sysinfo", "write_sysinfo"
    ]
    ALL = [
        "select", "insert", "update", "delete", "create", "drop",
        "alter", "use", "show", "grant", "revoke", "impersonate",
        "execute", "read", "write", "read_sysinfo", "write_sysinfo"
    ]
    
    @classmethod
    def get_access_types(cls, permission: str) -> List[str]:
        """Convert permission string to list of access types."""
        perm_lower = permission.lower().strip()
        if perm_lower == 'read':
            return cls.READ.copy()
        elif perm_lower == 'write':
            return cls.WRITE.copy()
        elif perm_lower == 'all':
            return cls.ALL.copy()
        else:
            # Return as-is if it's a specific access type
            return [perm_lower]


def generate_secure_password(length: int = 16) -> str:
    """Generate a secure random password."""
    lowercase = string.ascii_lowercase
    uppercase = string.ascii_uppercase
    digits = string.digits
    special = "!@#$%^&*()_+-=[]{}|;:,.<>?"
    
    password = [
        secrets.choice(lowercase),
        secrets.choice(uppercase),
        secrets.choice(digits),
        secrets.choice(special)
    ]
    
    all_chars = lowercase + uppercase + digits + special
    for _ in range(length - 4):
        password.append(secrets.choice(all_chars))
    
    secrets.SystemRandom().shuffle(password)
    return ''.join(password)


class RangerPolicyManager:
    """
    Manager class for creating and managing Ranger policies.
    
    Designed for use with the policy automation DAG.
    """
    
    def __init__(
        self,
        ranger_url: str,
        ranger_username: str,
        ranger_password: str,
        service_name: str = 'nx1-unifiedsql'
    ):
        """
        Initialize the Ranger Policy Manager.
        
        Args:
            ranger_url: URL of the Ranger admin server
            ranger_username: Ranger admin username
            ranger_password: Ranger admin password
            service_name: Name of the Ranger service
        """
        self.ranger_url = ranger_url
        self.service_name = service_name
        
        ranger_auth = (ranger_username, ranger_password)
        self.client = RangerClient(ranger_url, ranger_auth)
        self.user_mgmt_client = RangerUserMgmtClient(self.client)
        
        logger.info(f"Initialized RangerPolicyManager for service: {service_name}")
    
    def ensure_group_exists(self, group_name: str) -> bool:
        """
        Ensure a Ranger group exists, creating it if necessary.
        
        Args:
            group_name: Name of the group to ensure exists
            
        Returns:
            True if group was created, False if it already existed
        """
        try:
            groups_result = self.user_mgmt_client.find_groups({'name': group_name})
            existing_groups = [g.name for g in groups_result.list] if groups_result.list else []
            
            if group_name not in existing_groups:
                group = RangerGroup({'name': group_name})
                self.user_mgmt_client.create_group(group)
                logger.info(f"Created Ranger group: {group_name}")
                return True
            else:
                logger.debug(f"Group already exists: {group_name}")
                return False
        except Exception as e:
            logger.error(f"Error ensuring group {group_name} exists: {e}")
            raise
    
    def ensure_groups_exist(self, group_names: List[str]) -> Dict[str, bool]:
        """
        Ensure multiple Ranger groups exist.
        
        Args:
            group_names: List of group names to ensure exist
            
        Returns:
            Dictionary mapping group names to whether they were created (True) or existed (False)
        """
        results = {}
        
        # Get all existing groups first
        try:
            all_groups = self.user_mgmt_client.find_groups()
            existing_groups = {g.name for g in all_groups.list} if all_groups.list else set()
        except Exception as e:
            logger.error(f"Error fetching existing groups: {e}")
            existing_groups = set()
        
        for group_name in group_names:
            if group_name in existing_groups:
                results[group_name] = False
                logger.debug(f"Group already exists: {group_name}")
            else:
                try:
                    group = RangerGroup({'name': group_name})
                    self.user_mgmt_client.create_group(group)
                    results[group_name] = True
                    existing_groups.add(group_name)
                    logger.info(f"Created Ranger group: {group_name}")
                except Exception as e:
                    logger.error(f"Failed to create group {group_name}: {e}")
                    results[group_name] = False
        
        return results
    
    def get_existing_policy(self, policy_name: str) -> Optional[Dict]:
        """
        Get an existing policy by name.
        
        Args:
            policy_name: Name of the policy
            
        Returns:
            Policy dictionary if found, None otherwise
        """
        try:
            return self.client.get_policy(self.service_name, policy_name)
        except RangerServiceException:
            return None
        except Exception as e:
            logger.error(f"Error checking for existing policy {policy_name}: {e}")
            return None
    
    def create_table_policy(
        self,
        policy_name: str,
        catalog: str,
        schema: str,
        table: str,
        column: str,
        role_permissions: List[Dict[str, Any]],
        description: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create or update a table-based policy.
        
        Args:
            policy_name: Name of the policy
            catalog: Catalog name (e.g., 'iceberg')
            schema: Schema/database name
            table: Table name or '*' for all
            column: Column name or '*' for all
            role_permissions: List of {'role': str, 'permissions': List[str]}
            description: Optional policy description
            
        Returns:
            Dictionary with 'status' ('created', 'updated', or 'failed') and policy details
        """
        try:
            logger.debug(f"create_table_policy called: policy_name={policy_name}, schema={schema}, table={table}, column={column}")
            logger.debug(f"role_permissions: {role_permissions}")
            
            # Build policy items and row filter items separately
            policy_items = self._build_policy_items(role_permissions)
            row_filter_items = self._build_row_filter_items(role_permissions)
            
            logger.debug(f"Built {len(policy_items)} policy items and {len(row_filter_items)} row filter items")
            
            # Build resources
            # Include both the tag (if different from catalog) and the catalog in values
            catalog_values = list(set([catalog]))
            if catalog != 'iceberg':
                catalog_values.append('iceberg')
            
            resources = {
                'catalog': RangerPolicyResource({'values': catalog_values}),
                'schema': RangerPolicyResource({'values': [schema]}),
                'table': RangerPolicyResource({'values': [table]}),
                'column': RangerPolicyResource({'values': [column]})
            }
            
            # When there are row filters, create TWO separate policies:
            # 1. Access Policy (policyType 0) - grants SELECT permission
            # 2. Row Filter Policy (policyType 2) - applies the SQL filter
            # This avoids Ranger warnings about missing access policies
            if row_filter_items:
                logger.info(f"Row filters detected. Creating separate Access and Row Filter policies")
                
                # Create/update Access Policy (without column resource)
                access_policy_name = policy_name
                resources_for_access = {k: v for k, v in resources.items() if k != 'column'}
                
                existing_access_policy = self.get_existing_policy(access_policy_name)
                if existing_access_policy:
                    result_access = self._update_policy(existing_access_policy, resources_for_access, policy_items, [], None)
                else:
                    result_access = self._create_policy(access_policy_name, resources_for_access, policy_items, [], None)
                
                # Create/update Row Filter Policy (separate policy, also without column resource)
                rowfilter_policy_name = f"{policy_name}__rowfilter"
                resources_for_rowfilter = {k: v for k, v in resources.items() if k != 'column'}
                
                existing_rowfilter_policy = self.get_existing_policy(rowfilter_policy_name)
                if existing_rowfilter_policy:
                    result_rowfilter = self._update_policy(existing_rowfilter_policy, resources_for_rowfilter, [], row_filter_items, description)
                else:
                    result_rowfilter = self._create_policy(rowfilter_policy_name, resources_for_rowfilter, [], row_filter_items, description)
                
                # Return with both policy IDs
                return {
                    'status': result_access.get('status'),
                    'policy_name': policy_name,
                    'policy_id': result_access.get('policy_id'),
                    'rowfilter_policy_name': rowfilter_policy_name,
                    'rowfilter_policy_id': result_rowfilter.get('policy_id')
                }
            else:
                # No row filters - create single Access Policy
                resources_for_policy = resources
                
                existing_policy = self.get_existing_policy(policy_name)
                if existing_policy:
                    return self._update_policy(existing_policy, resources_for_policy, policy_items, [], description)
                else:
                    return self._create_policy(policy_name, resources_for_policy, policy_items, [], description)
                
        except Exception as e:
            logger.error(f"Failed to create/update policy {policy_name}: {e}")
            return {
                'status': 'failed',
                'policy_name': policy_name,
                'error': str(e)
            }
    
    def create_url_policy(
        self,
        policy_name: str,
        url: str,
        role_permissions: List[Dict[str, Any]],
        description: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Create or update a URL-based policy.
        
        Args:
            policy_name: Name of the policy
            url: URL pattern (S3, HDFS, ABFS, GCS path)
            role_permissions: List of {'role': str, 'permissions': List[str]}
            description: Optional policy description
            
        Returns:
            Dictionary with 'status' ('created', 'updated', or 'failed') and policy details
        """
        try:
            logger.debug(f"Processing table policy '{policy_name}' -  url={url}")
            logger.debug(f"Role permissions: {role_permissions}")
            
            existing_policy = self.get_existing_policy(policy_name)
            
            # Build policy items (no row filters for URL policies)
            policy_items = self._build_url_policy_items(role_permissions)
            row_filter_items = []  # URLs don't support row filters
            
            # Build resources
            resources = {
                'url': RangerPolicyResource({'values': [url], 'isRecursive': True})
            }
            
            if existing_policy:
                # Update existing policy
                return self._update_policy(existing_policy, resources, policy_items, row_filter_items, description)
            else:
                # Create new policy
                return self._create_policy(policy_name, resources, policy_items, row_filter_items, description)
                
        except Exception as e:
            logger.error(f"Failed to create/update URL policy {policy_name}: {e}")
            return {
                'status': 'failed',
                'policy_name': policy_name,
                'error': str(e)
            }
    
    def _build_policy_items(self, role_permissions: List[Dict[str, Any]]) -> List[RangerPolicyItem]:
        """Build policy items from role_permissions list, supporting users and rowfilter."""
        policy_items = []
        for rp in role_permissions:
            role = rp.get('role')
            permissions = rp.get('permissions', ['read'])
            groups = rp.get('groups', [])
            users = rp.get('users', [])

            # Expand permissions to access types
            access_types = set()
            for perm in permissions:
                access_types.update(Permissions.get_access_types(perm))

            allow_item = RangerPolicyItem()
            # Add role as Ranger group (NOT Keycloak groups)
            if role:
                allow_item.groups = [role]
            else:
                allow_item.groups = []
            # Add users directly
            allow_item.users = users if users else []

            allow_item.conditions = []
            allow_item.accesses = [
                RangerPolicyItemAccess({'type': a}) for a in access_types
            ]
            policy_items.append(allow_item)
        return policy_items
    
    def _build_url_policy_items(self, role_permissions: List[Dict[str, Any]]) -> List[RangerPolicyItem]:
        """Build policy items for URL-based policies, supporting users and rowfilter."""
        policy_items = []
        for rp in role_permissions:
            role = rp.get('role')
            permissions = rp.get('permissions', ['read'])
            groups = rp.get('groups', [])
            users = rp.get('users', [])

            # For URL policies, map to read/write
            access_types = set()
            for perm in permissions:
                perm_lower = perm.lower().strip()
                if perm_lower in ('read', 'select'):
                    access_types.add('read')
                elif perm_lower in ('write', 'insert', 'update', 'delete', 'all'):
                    access_types.add('read')
                    access_types.add('write')
                else:
                    access_types.add(perm_lower)

            allow_item = RangerPolicyItem()
            # Add role as Ranger group (NOT Keycloak groups)
            if role:
                allow_item.groups = [role]
            else:
                allow_item.groups = []
            # Add users directly
            allow_item.users = users if users else []

            allow_item.conditions = []
            allow_item.accesses = [
                RangerPolicyItemAccess({'type': a}) for a in access_types
            ]
            policy_items.append(allow_item)
        return policy_items
        
    def _create_policy(
        self,
        policy_name: str,
        resources: Dict,
        policy_items: List[RangerPolicyItem],
        row_filter_items: List[Dict[str, Any]],
        description: Optional[str]
    ) -> Dict[str, Any]:
        """Create a new policy."""
        policy = RangerPolicy()
        policy.service = self.service_name
        policy.name = policy_name
        policy.description = description or f"Auto-generated policy: {policy_name}"
        policy.resources = resources
        policy.policyItems = policy_items
        # Add row filter items if present
        if row_filter_items:
            policy.rowFilterPolicyItems = row_filter_items
            policy.policyType = 2  # policyType 2 = Row Filter policy (must be set for UI to display in Row Filter tab)
            logger.debug(f"Added {len(row_filter_items)} row filter items to policy '{policy_name}'")
            logger.debug(f"Row filter items: {row_filter_items}")
        else:
            policy.policyType = 0  # policyType 0 = Access policy
        
        logger.debug(f"Creating policy '{policy_name}' for service '{self.service_name}'")
        logger.debug(f"Resources: {resources}")
        logger.debug(f"Policy items: {[{'groups': item.groups, 'accesses': [a.type for a in item.accesses]} for item in policy_items]}")
        logger.debug(f"Policy object before sending: service={policy.service}, name={policy.name}, policyItems={len(policy.policyItems)}, rowFilterPolicyItems={len(policy.rowFilterPolicyItems) if hasattr(policy, 'rowFilterPolicyItems') and policy.rowFilterPolicyItems else 0}")
        
        try:
            logger.debug(f"Sending create_policy request to Ranger at {self.ranger_url} for service {self.service_name}")
            created_policy = self.client.create_policy(policy)
            # Check if response is None (indicates silent failure)
            if created_policy is None:
                logger.error(f"create_policy returned None for policy '{policy_name}' - this typically indicates SSL/network issues or Ranger server problems")
                raise RuntimeError(f"Ranger API returned None when creating policy '{policy_name}'. The policy was not created in Ranger backend. Check Ranger server logs for more information")
            
            policy_id = created_policy.id if hasattr(created_policy, 'id') else None
            
            logger.debug(f"API Response type: {type(created_policy)}")
            logger.debug(f"API Response attributes: {dir(created_policy)}")
            logger.debug(f"API Response str: {str(created_policy)}")
            logger.debug(f"API Response repr: {repr(created_policy)}")
            
            logger.info(f"Created policy: {policy_name} (ID: {policy_id})")
            logger.debug(f"Policy creation response: {created_policy}")
            
            return {
                'status': 'created',
                'policy_name': policy_name,
                'policy_id': policy_id
            }
        except Exception as e:
            logger.error(f"Policy creation failed for '{policy_name}': {e}", exc_info=True)
            raise
    
    def _update_policy(
        self,
        existing_policy: Dict,
        resources: Dict,
        policy_items: List[RangerPolicyItem],
        row_filter_items: List[Dict[str, Any]],
        description: Optional[str]
    ) -> Dict[str, Any]:
        """Update an existing policy, merging groups, users, and rowfilter conditions."""
        policy_name = existing_policy['name']
        policy_id = existing_policy['id']

        # Update resources
        existing_policy['resources'] = {
            k: {'values': v.values if hasattr(v, 'values') else v.get('values', [])}
            for k, v in resources.items()
        }

        # Build a mapping from group to item index for fast lookup
        group_to_item = {}
        existing_items = existing_policy.get('policyItems', [])
        for idx, item in enumerate(existing_items):
            for group in item.get('groups', []):
                group_to_item[group] = idx

        # Merge or update policy items for each group in new policy_items
        for new_item in policy_items:
            for new_group in new_item.groups:
                if new_group in group_to_item:
                    idx = group_to_item[new_group]
                    item = existing_items[idx]
                    # Merge users
                    existing_users = set(item.get('users', []))
                    new_users = set(new_item.users or [])
                    item['users'] = list(existing_users | new_users)
                    # Merge accesses (union of access types)
                    existing_accesses = {a['type'] for a in item.get('accesses', [])}
                    new_accesses = {a.type for a in new_item.accesses}
                    merged_accesses = existing_accesses | new_accesses
                    item['accesses'] = [{'type': a, 'isAllowed': True} for a in merged_accesses]
                else:
                    # Add new group as new policy item
                    existing_items.append({
                        'groups': [new_group],
                        'users': list(new_item.users or []),
                        'roles': [],
                        'conditions': [],
                        'accesses': [
                            {'type': a.type, 'isAllowed': True}
                            for a in new_item.accesses
                        ],
                        'delegateAdmin': False
                    })
                    group_to_item[new_group] = len(existing_items) - 1

        existing_policy['policyItems'] = existing_items
        
        # Merge row filter items (similar to policy items merging)
        if row_filter_items:
            # Build a mapping from groups to existing row filter items
            group_to_rowfilter = {}
            existing_rowfilters = existing_policy.get('rowFilterPolicyItems', [])
            for idx, item in enumerate(existing_rowfilters):
                for group in item.get('groups', []):
                    group_to_rowfilter[group] = idx
            
            # Merge or add new row filter items
            for new_item in row_filter_items:
                groups = new_item.get('groups', [])
                for group in groups:
                    if group in group_to_rowfilter:
                        # Update existing row filter item for this group
                        idx = group_to_rowfilter[group]
                        existing_item = existing_rowfilters[idx]
                        # Merge users
                        existing_users = set(existing_item.get('users', []))
                        new_users = set(new_item.get('users', []))
                        existing_item['users'] = list(existing_users | new_users)
                        # Update filter expression and accesses
                        existing_item['accesses'] = new_item.get('accesses', [])
                        existing_item['rowFilterInfo'] = new_item.get('rowFilterInfo', {})
                    else:
                        # Add new row filter item
                        existing_rowfilters.append(new_item)
                        for group in groups:
                            group_to_rowfilter[group] = len(existing_rowfilters) - 1
            
            existing_policy['rowFilterPolicyItems'] = existing_rowfilters
            existing_policy['policyType'] = 2  # policyType 2 = Row Filter policy
            logger.debug(f"Merged {len(row_filter_items)} row filter items for policy '{policy_name}'")
        else:
            # No row filters in new policy
            if 'rowFilterPolicyItems' in existing_policy:
                # Keep existing row filters if none provided (don't delete them)
                logger.debug(f"No new row filters provided, keeping existing row filters for policy '{policy_name}'")
                existing_policy['policyType'] = 2  # Keep as row filter policy if it has row filters
            else:
                existing_policy['policyType'] = 0  # Access policy only
        if description:
            existing_policy['description'] = description

        self.client.update_policy(self.service_name, policy_id, existing_policy)
        logger.info(f"Updated policy: {policy_name}")

        return {
            'status': 'updated',
            'policy_name': policy_name,
            'policy_id': policy_id
        }
    
    def delete_policy(self, policy_name: str) -> bool:
        """
        Delete a policy by name.
        
        Args:
            policy_name: Name of the policy to delete
            
        Returns:
            True if deleted successfully, False otherwise
        """
        try:
            self.client.delete_policy(self.service_name, policy_name)
            logger.info(f"Deleted policy: {policy_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to delete policy {policy_name}: {e}")
            return False
    
    def get_all_policies(self) -> List[Dict]:
        """Get all policies from the service."""
        try:
            policies = self.client.find_policies(filter={
                'serviceName': self.service_name
            })
            return list(policies)
        except Exception as e:
            logger.error(f"Error fetching policies: {e}")
            return []
        
    def _build_row_filter_items(self, role_permissions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Build row filter policy items from role_permissions list.
        
        NOTE: Row filters in Ranger only support 'select' access type.
        Other permissions (read, write) are mapped to 'select'.
        """
        row_filter_items = []
        logger.debug(f"Building row filter items from {len(role_permissions)} role permissions")
        
        for idx, rp in enumerate(role_permissions):
            rowfilter = rp.get('rowfilter', '').strip()
            logger.debug(f"Role permission {idx}: {rp.keys()} - rowfilter='{rowfilter}'")
            
            # Skip if no rowfilter or if it's 'nan'
            if not rowfilter or rowfilter.lower() == 'nan':
                logger.debug(f"Skipping role permission {idx} - no valid rowfilter")
                continue
                
            role = rp.get('role')
            groups = rp.get('groups', [])
            users = rp.get('users', [])
            permissions = rp.get('permissions', ['read'])  # Get permissions for row filter
            
            # For rowfilter, only 'select' and '_ALL' are valid access types
            # Map any permission to 'select' (rowfilter is read-only data visibility)
            access_types = set()
            for perm in permissions:
                perm_lower = perm.lower().strip()
                if perm_lower in ('read', 'select', 'write', 'all'):
                    # All map to 'select' for row filters
                    access_types.add('select')
                else:
                    # If it's already a valid access type, check if it's allowable
                    access_types.add('select')
            
            # Build groups list
            group_list = []
            if role:
                group_list.append(role)
            if groups:
                group_list.extend(groups)
            group_list = list(set(group_list))  # Deduplicate
            
            logger.info(f"Creating row filter item: role={role}, groups={group_list}, users={users}, filter='{rowfilter}', accesses={list(access_types)}")
            
            row_filter_items.append({
                "groups": group_list,
                "users": users if users else [],
                "accesses": [{"type": a, "isAllowed": True} for a in access_types],  # Only 'select' for row filters
                "rowFilterInfo": {
                    "filterExpr": rowfilter
                },
                "conditions": [],  # No conditions for basic rowfilter
                "roles": [],  # Roles handled via groups
                "isEnabled": True  # Ensure rowfilter is enabled
            })
        
        logger.debug(f"Built {len(row_filter_items)} row filter items")
        return row_filter_items

    def sync_policies_from_dict(self, policies_dict: Dict[str, Dict]) -> Dict[str, List]:
        """
        Sync policies from a dictionary structure (as parsed from Excel).
        
        Args:
            policies_dict: Dictionary with policy_name as key and policy config as value
                Expected structure:
                {
                    'policy_name': {
                        'type': 'table' | 'url',
                        'catalog': str,  # for table type
                        'schema': str,   # for table type
                        'table': str,    # for table type
                        'column': str,   # for table type
                        'url': str,      # for url type
                        'roles': [{'role': str, 'permissions': List[str]}]
                    }
                }
                
        Returns:
            Dictionary with 'created', 'updated', and 'failed' lists
            Each entry in 'created'/'updated' is a dict with policy_name and policy_id
            Each entry in 'failed' is a dict with name and error
        """
        results = {
            'created': [],
            'updated': [],
            'failed': []
        }
        
        for policy_name, policy_config in policies_dict.items():
            policy_type = policy_config.get('type', 'table')
            role_permissions = policy_config.get('roles', [])
            
            if policy_type == 'url':
                result = self.create_url_policy(
                    policy_name=policy_name,
                    url=policy_config.get('url', ''),
                    role_permissions=role_permissions
                )
            else:
                result = self.create_table_policy(
                    policy_name=policy_name,
                    catalog=policy_config.get('catalog', 'iceberg'),
                    schema=policy_config.get('schema', '*'),
                    table=policy_config.get('table', '*'),
                    column=policy_config.get('column', '*'),
                    role_permissions=role_permissions
                )
            
            status = result.get('status', 'failed')
            if status == 'created':
                # Keep the full result dict with policy_id and rowfilter_policy_id if present
                entry = {
                    'policy_name': result.get('policy_name'),
                    'policy_id': result.get('policy_id')
                }
                # If rowfilter policy was created, add its info too
                if result.get('rowfilter_policy_id'):
                    entry['rowfilter_policy_name'] = result.get('rowfilter_policy_name')
                    entry['rowfilter_policy_id'] = result.get('rowfilter_policy_id')
                results['created'].append(entry)
            elif status == 'updated':
                # Keep the full result dict with policy_id and rowfilter_policy_id if present
                entry = {
                    'policy_name': result.get('policy_name'),
                    'policy_id': result.get('policy_id')
                }
                # If rowfilter policy exists, add its info
                if result.get('rowfilter_policy_id'):
                    entry['rowfilter_policy_name'] = result.get('rowfilter_policy_name')
                    entry['rowfilter_policy_id'] = result.get('rowfilter_policy_id')
                results['updated'].append(entry)
            else:
                results['failed'].append({
                    'name': policy_name,
                    'error': result.get('error', 'Unknown error')
                })
        
        return results


class KeycloakRoleManager:
    """
    Manager class for creating and managing Keycloak realm roles and group mappings.
    """
    
    def __init__(
        self,
        server_url: str,
        realm_name: str,
        client_id: str,
        client_secret: str,
        max_retries: int = 3,
        connection_timeout: int = 10
    ):
        """
        Initialize the Keycloak Role Manager with retry logic.
        
        Args:
            server_url: Keycloak server URL
            realm_name: Realm name
            client_id: Admin client ID
            client_secret: Admin client secret
            max_retries: Number of connection retry attempts (default: 3)
            connection_timeout: Connection timeout in seconds (default: 10)
        """
        from keycloak import KeycloakAdmin
        import time
        
        self.server_url = server_url
        self.realm_name = realm_name
        self.max_retries = max_retries
        self.connection_timeout = connection_timeout
        self.keycloak_admin = None
        
        # Try to connect with exponential backoff retry logic
        last_error = None
        for attempt in range(max_retries):
            try:
                self.keycloak_admin = KeycloakAdmin(
                    server_url=server_url,
                    realm_name=realm_name,
                    client_id=client_id,
                    client_secret_key=client_secret,
                    verify=True,
                    timeout=connection_timeout
                )
                
                # Test the connection by attempting to get realm info
                self.keycloak_admin.get_realm(realm_name)
                logger.info(f"Successfully initialized KeycloakRoleManager for realm: {realm_name}")
                return
                
            except Exception as e:
                last_error = e
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # Exponential backoff: 1s, 2s, 4s
                    logger.warning(
                        f"Keycloak connection attempt {attempt + 1}/{max_retries} failed. "
                        f"Retrying in {wait_time}s... Error: {str(e)}"
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(
                        f"Failed to connect to Keycloak after {max_retries} attempts. "
                        f"Server URL: {server_url}, Realm: {realm_name}"
                    )
        
        # If we get here, all retries failed
        raise ConnectionError(
            f"Could not connect to Keycloak server at {server_url} "
            f"after {max_retries} attempts. Last error: {str(last_error)}"
        )
    
    def _assign_role_to_principal(self, role_name: str, principal_name: str, principal_type: str) -> bool:
        """
        Assign a realm role to a group or user.
        Args:
            role_name: Name of the role to assign
            principal_name: Name of the group or user
            principal_type: 'group' or 'user'
        Returns:
            True if assignment was made, False if already assigned
        """
        try:
            if principal_type == 'group':
                group_id, _ = self.ensure_group_exists(principal_name)  # Unpack tuple, ignore created flag
                role = self.keycloak_admin.get_realm_role(role_name)
                group_roles = self.keycloak_admin.get_group_realm_roles(group_id)
                group_role_names = {r['name'] for r in group_roles}
                if role_name not in group_role_names:
                    self.keycloak_admin.assign_group_realm_roles(group_id, [role])
                    logger.info(f"Assigned role {role_name} to group {principal_name}")
                    return True
                else:
                    logger.debug(f"Group {principal_name} already has role {role_name}")
                    return False
            elif principal_type == 'user':
                user_id = None
                users = self.keycloak_admin.get_users({"username": principal_name})
                for user in users:
                    if user["username"] == principal_name:
                        user_id = user["id"]
                        break
                if not user_id:
                    error_msg = f"User '{principal_name}' not found in Keycloak realm. Cannot assign role."
                    logger.error(error_msg)
                    raise ValueError(error_msg) 
                role = self.keycloak_admin.get_realm_role(role_name)
                user_roles = self.keycloak_admin.get_realm_roles_of_user(user_id)
                user_role_names = {r['name'] for r in user_roles}
                if role_name not in user_role_names:
                    self.keycloak_admin.assign_realm_roles(user_id, [role])
                    logger.info(f"Assigned role {role_name} to user {principal_name}")
                    return True
                else:
                    logger.debug(f"User {principal_name} already has role {role_name}")
                    return False
            else:
                error_msg = f"Unknown principal_type: {principal_type}"
                logger.error(error_msg)
                raise ValueError(error_msg)
        except Exception as e:
            logger.error(f"Error assigning role {role_name} to {principal_type} {principal_name}: {e}")
            raise

    def ensure_realm_role_exists(self, role_name: str, description: Optional[str] = None) -> bool:
        """
        Ensure a realm role exists, creating it if necessary.
        
        Args:
            role_name: Name of the role
            description: Optional role description
            
        Returns:
            True if role was created, False if it already existed
        """
        try:
            existing_roles = {r['name'] for r in self.keycloak_admin.get_realm_roles()}
            
            if role_name not in existing_roles:
                self.keycloak_admin.create_realm_role({
                    'name': role_name,
                    'description': description or f'Role: {role_name}'
                })
                logger.info(f"Created Keycloak realm role: {role_name}")
                return True
            else:
                logger.debug(f"Realm role already exists: {role_name}")
                return False
        except Exception as e:
            logger.error(f"Error ensuring realm role {role_name} exists: {e}")
            raise
    
    def ensure_group_exists(self, group_name: str) -> tuple:
        """
        Ensure a group exists, creating it if necessary.
        
        Args:
            group_name: Name of the group
            
        Returns:
            Tuple of (group_id, created) where created=True if group was newly created, False if already existed
        """
        try:
            groups = {g['name']: g['id'] for g in self.keycloak_admin.get_groups()}
            
            if group_name not in groups:
                self.keycloak_admin.create_group({'name': group_name})
                groups = {g['name']: g['id'] for g in self.keycloak_admin.get_groups()}
                logger.info(f"Created Keycloak group: {group_name}")
                return (groups.get(group_name), True)  # (group_id, created=True)
            else:
                logger.debug(f"Keycloak group {group_name} already exists")
                return (groups.get(group_name), False)  # (group_id, created=False)
        except Exception as e:
            logger.error(f"Error ensuring group {group_name} exists: {e}")
            raise
    
    def assign_role_to_group(self, role_name: str, group_name: str) -> bool:
        """
        Assign a realm role to a group.
        
        Args:
            role_name: Name of the role to assign
            group_name: Name of the group
            
        Returns:
            True if assignment was made, False if already assigned
        """
        try:
            # Ensure group exists and get its ID
            group_id, _ = self.ensure_group_exists(group_name)  # Unpack tuple, ignore created flag
            
            # Get the role object
            role = self.keycloak_admin.get_realm_role(role_name)
            
            # Check if already assigned
            group_roles = self.keycloak_admin.get_group_realm_roles(group_id)
            group_role_names = {r['name'] for r in group_roles}
            
            if role_name not in group_role_names:
                self.keycloak_admin.assign_group_realm_roles(group_id, [role])
                logger.info(f"Assigned role {role_name} to group {group_name}")
                return True
            else:
                logger.debug(f"Group {group_name} already has role {role_name}")
                return False
        except Exception as e:
            logger.error(f"Error assigning role {role_name} to group {group_name}: {e}")
            raise
    
    def sync_roles_and_principals(self, role_principals_dict: Dict[str, Any]) -> Dict[str, List]:
        """
        Sync roles and assignments to groups and users from a dictionary.
        
        Args:
            role_principals_dict: Dictionary mapping role names to dicts with 'groups' and 'users' keys
                {
                    'role_name': {'groups': [...], 'users': [...]},
                    ...
                }
                
        Returns:
            Dictionary with 'created_roles', 'created_groups', 'created_mappings', and 'failed' lists
        """
        results = {
            'created_roles': [],
            'existing_roles': [],
            'created_groups': [],  # Keycloak groups created for role assignments
            'existing_groups': [],  # Keycloak groups that already existed
            'created_mappings': [],
            'existing_mappings': [],
            'failed': []
        }
        
        # Track groups we've already processed to avoid duplicates
        processed_groups = set()
        
        for role_name, mapping in role_principals_dict.items():
            groups = mapping.get('groups', [])
            users = mapping.get('users', [])
            # Create role if needed
            try:
                created = self.ensure_realm_role_exists(role_name)
                if created:
                    results['created_roles'].append(role_name)
                else:
                    results['existing_roles'].append(role_name)
            except Exception as e:
                results['failed'].append({
                    'operation': 'create_role',
                    'role': role_name,
                    'error': str(e)
                })
                continue
            # Assign role to groups
            for group_name in groups:
                try:
                    # Ensure group exists and track whether it was created
                    if group_name not in processed_groups:
                        group_id, created = self.ensure_group_exists(group_name)
                        if created:
                            results['created_groups'].append(group_name)
                        else:
                            results['existing_groups'].append(group_name)
                        processed_groups.add(group_name)
                    
                    # Now assign the role to the group
                    assigned = self._assign_role_to_principal(role_name, group_name, 'group')
                    
                    mapping_obj = {'role': role_name, 'principal': group_name, 'type': 'group'}
                    if assigned:
                        results['created_mappings'].append(mapping_obj)
                    else:
                        results['existing_mappings'].append(mapping_obj)
                except Exception as e:
                    results['failed'].append({
                        'operation': 'assign_role',
                        'role': role_name,
                        'principal': group_name,
                        'type': 'group',
                        'error': str(e)
                    })
            for username in users:
                try:
                    assigned = self._assign_role_to_principal(role_name, username, 'user')
                    mapping_obj = {'role': role_name, 'principal': username, 'type': 'user'}
                    if assigned:
                        results['created_mappings'].append(mapping_obj)
                    else:
                        results['existing_mappings'].append(mapping_obj)
                except Exception as e:
                    results['failed'].append({
                        'operation': 'assign_role_user',
                        'role': role_name,
                        'principal': username,
                        'type': 'user',
                        'error': str(e)
                    })
        return results