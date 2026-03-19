import os
import json
import logging
from typing import Dict, List, Any
from app.schema_service.models import SchemaResponse, TableMeta

logger = logging.getLogger(__name__)

POLICY_FILE = os.path.join(os.path.dirname(__file__), "access_policy.json")


def load_policies() -> Dict[str, Any]:
    """Load the access_policy.json file. Returns empty structure if absent."""
    if not os.path.exists(POLICY_FILE):
        return {"users": {}, "role_labels": {"SYSTEM_ADMIN": "System Administrator"}}
    try:
        with open(POLICY_FILE, "r") as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Failed to load policies: {e}")
        return {"users": {}, "role_labels": {}}


def filter_schema_for_user(schema: SchemaResponse, username: str) -> SchemaResponse:
    """Filter the DB schema based on both Role and User RBAC rules."""
    if username == "admin":
        return schema  # Admin always sees everything

    from app.auth.user_manager import user_manager
    user = next((u for u in user_manager.list_users() if u["username"] == username), None)
    if not user:
        return schema

    policies = load_policies()
    user_policy = policies.get("users", {}).get(username, {})
    role_policy = policies.get("roles", {}).get(user["role"], {})

    blocked_tables = {t.lower() for t in user_policy.get("blocked_tables", [])} | \
                     {t.lower() for t in role_policy.get("blocked_tables", [])}
    
    blocked_columns = {c.lower() for c in user_policy.get("blocked_columns", [])} | \
                      {c.lower() for c in role_policy.get("blocked_columns", [])}

    if not blocked_tables and not blocked_columns:
        return schema

    filtered_tables = []
    for table in schema.tables:
        if table.table.lower() in blocked_tables:
            continue
        filtered_cols = [
            col for col in table.columns
            if col.name.lower() not in blocked_columns
        ]
        if filtered_cols:
            new_table = table.model_copy(update={"columns": filtered_cols})
            filtered_tables.append(new_table)

    return schema.model_copy(update={"tables": filtered_tables})


def get_effective_rbac_for_user(username: str) -> Dict[str, List[str]]:
    """
    Return merged RBAC restrictions (user + role) for runtime enforcement.
    """
    if username == "admin":
        return {"blocked_tables": [], "blocked_columns": []}

    from app.auth.user_manager import user_manager
    user = next((u for u in user_manager.list_users() if u["username"] == username), None)
    if not user:
        return {"blocked_tables": [], "blocked_columns": []}

    policies = load_policies()
    user_policy = policies.get("users", {}).get(username, {})
    role_policy = policies.get("roles", {}).get(user["role"], {})

    blocked_tables = {t.lower() for t in user_policy.get("blocked_tables", [])} | {
        t.lower() for t in role_policy.get("blocked_tables", [])
    }
    blocked_columns = {c.lower() for c in user_policy.get("blocked_columns", [])} | {
        c.lower() for c in role_policy.get("blocked_columns", [])
    }

    return {
        "blocked_tables": sorted(blocked_tables),
        "blocked_columns": sorted(blocked_columns)
    }


# Keep old name as alias for backwards compatibility
def filter_schema_by_role(schema: SchemaResponse, role: str) -> SchemaResponse:
    """Legacy alias — role is treated as username for per-user RBAC."""
    return filter_schema_for_user(schema, role)
