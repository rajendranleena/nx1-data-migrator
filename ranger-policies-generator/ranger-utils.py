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
            existing_policy = self.get_existing_policy(policy_name)
            
            # Build policy items from role_permissions
            policy_items = self._build_policy_items(role_permissions)
            
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
            
            if existing_policy:
                # Update existing policy
                return self._update_policy(existing_policy, resources, policy_items, description)
            else:
                # Create new policy
                return self._create_policy(policy_name, resources, policy_items, description)
                
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
            existing_policy = self.get_existing_policy(policy_name)
            
            # Build policy items from role_permissions
            policy_items = self._build_url_policy_items(role_permissions)
            
            # Build resources
            resources = {
                'url': RangerPolicyResource({'values': [url], 'isRecursive': True})
            }
            
            if existing_policy:
                # Update existing policy
                return self._update_policy(existing_policy, resources, policy_items, description)
            else:
                # Create new policy
                return self._create_policy(policy_name, resources, policy_items, description)
                
        except Exception as e:
            logger.error(f"Failed to create/update URL policy {policy_name}: {e}")
            return {
                'status': 'failed',
                'policy_name': policy_name,
                'error': str(e)
            }
    
    def _build_policy_items(self, role_permissions: List[Dict[str, Any]]) -> List[RangerPolicyItem]:
        """Build policy items from role_permissions list."""
        policy_items = []
        
        for rp in role_permissions:
            role = rp['role']
            permissions = rp.get('permissions', ['read'])
            
            # Expand permissions to access types
            access_types = set()
            for perm in permissions:
                access_types.update(Permissions.get_access_types(perm))
            
            allow_item = RangerPolicyItem()
            allow_item.groups = [role]
            allow_item.accesses = [
                RangerPolicyItemAccess({'type': a}) for a in access_types
            ]
            policy_items.append(allow_item)
        
        return policy_items
    
    def _build_url_policy_items(self, role_permissions: List[Dict[str, Any]]) -> List[RangerPolicyItem]:
        """Build policy items for URL-based policies."""
        policy_items = []
        
        # URL policies typically use read/write instead of SQL permissions
        for rp in role_permissions:
            role = rp['role']
            permissions = rp.get('permissions', ['read'])
            
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
            allow_item.groups = [role]
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
        description: Optional[str]
    ) -> Dict[str, Any]:
        """Create a new policy."""
        policy = RangerPolicy()
        policy.service = self.service_name
        policy.name = policy_name
        policy.description = description or f"Auto-generated policy: {policy_name}"
        policy.resources = resources
        policy.policyItems = policy_items
        
        created_policy = self.client.create_policy(policy)
        logger.info(f"Created policy: {policy_name}")
        
        return {
            'status': 'created',
            'policy_name': policy_name,
            'policy_id': created_policy.id if hasattr(created_policy, 'id') else None
        }
    
    def _update_policy(
        self,
        existing_policy: Dict,
        resources: Dict,
        policy_items: List[RangerPolicyItem],
        description: Optional[str]
    ) -> Dict[str, Any]:
        """Update an existing policy."""
        policy_name = existing_policy['name']
        policy_id = existing_policy['id']
        
        # Update resources
        existing_policy['resources'] = {
            k: {'values': v.values if hasattr(v, 'values') else v.get('values', [])}
            for k, v in resources.items()
        }
        
        # Merge policy items (add new groups, preserve existing)
        existing_groups = set()
        existing_items = existing_policy.get('policyItems', [])
        for item in existing_items:
            existing_groups.update(item.get('groups', []))
        
        for new_item in policy_items:
            for new_group in new_item.groups:
                if new_group not in existing_groups:
                    existing_items.append({
                        'groups': [new_group],
                        'users': [],
                        'roles': [],
                        'conditions': [],
                        'accesses': [
                            {'type': a.type, 'isAllowed': True}
                            for a in new_item.accesses
                        ],
                        'delegateAdmin': False
                    })
                    existing_groups.add(new_group)
        
        existing_policy['policyItems'] = existing_items
        
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
                results['created'].append(policy_name)
            elif status == 'updated':
                results['updated'].append(policy_name)
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
        client_secret: str
    ):
        """
        Initialize the Keycloak Role Manager.
        
        Args:
            server_url: Keycloak server URL
            realm_name: Realm name
            client_id: Admin client ID
            client_secret: Admin client secret
        """
        from keycloak import KeycloakAdmin
        
        self.keycloak_admin = KeycloakAdmin(
            server_url=server_url,
            realm_name=realm_name,
            client_id=client_id,
            client_secret_key=client_secret,
            verify=True
        )
        
        logger.info(f"Initialized KeycloakRoleManager for realm: {realm_name}")
    
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
    
    def ensure_group_exists(self, group_name: str) -> str:
        """
        Ensure a group exists, creating it if necessary.
        
        Args:
            group_name: Name of the group
            
        Returns:
            Group ID
        """
        try:
            groups = {g['name']: g['id'] for g in self.keycloak_admin.get_groups()}
            
            if group_name not in groups:
                self.keycloak_admin.create_group({'name': group_name})
                groups = {g['name']: g['id'] for g in self.keycloak_admin.get_groups()}
                logger.info(f"Created Keycloak group: {group_name}")
            
            return groups.get(group_name)
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
            group_id = self.ensure_group_exists(group_name)
            
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
    
    def sync_roles_and_groups(self, role_groups_dict: Dict[str, List[str]]) -> Dict[str, List]:
        """
        Sync roles and group assignments from a dictionary.
        
        Args:
            role_groups_dict: Dictionary mapping role names to list of group names
                {
                    'role_name': ['group1', 'group2'],
                    ...
                }
                
        Returns:
            Dictionary with 'created_roles', 'created_mappings', and 'failed' lists
        """
        results = {
            'created_roles': [],
            'existing_roles': [],
            'created_mappings': [],
            'existing_mappings': [],
            'failed': []
        }
        
        for role_name, group_names in role_groups_dict.items():
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
            for group_name in group_names:
                try:
                    assigned = self.assign_role_to_group(role_name, group_name)
                    mapping = {'role': role_name, 'group': group_name}
                    if assigned:
                        results['created_mappings'].append(mapping)
                    else:
                        results['existing_mappings'].append(mapping)
                except Exception as e:
                    results['failed'].append({
                        'operation': 'assign_role',
                        'role': role_name,
                        'group': group_name,
                        'error': str(e)
                    })
        
        return results
