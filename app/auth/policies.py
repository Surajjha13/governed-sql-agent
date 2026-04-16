import os
import json
import logging
from typing import Dict, List, Any
from app.schema_service.models import SchemaResponse, TableMeta

logger = logging.getLogger(__name__)

POLICY_FILE = os.path.join(os.path.dirname(__file__), "access_policy.json")

# Roles that bypass RBAC restrictions entirely
_ADMIN_ROLES = {"SUPER_ADMIN", "SYSTEM_ADMIN"}


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


def _is_admin_user(username: str, user_record: dict | None) -> bool:
    """Check whether the user should bypass RBAC restrictions."""
    if username == "admin":
        return True
    if user_record and user_record.get("role") in _ADMIN_ROLES:
        return True
    return False


def _resolve_user(username: str) -> dict | None:
    """Look up a user record by username. Returns None if not found."""
    if username == "solo_user":
        return {"username": "solo_user", "role": "SOLO_USER"}
        
    from app.auth.user_manager import user_manager
    return next((u for u in user_manager.list_users() if u["username"] == username), None)


def _build_blocked_sets(username: str, user_role: str):
    """
    Build merged blocked_tables and blocked_columns sets from user + role policies.
    blocked_columns entries may be either bare names ("salary") or
    fully‐qualified ("employees.salary").  We split them into two
    separate structures so callers can match correctly.

    Returns: (blocked_tables: set, blocked_col_pairs: set, blocked_col_bare: set)
      - blocked_tables: set of lowercase table names
      - blocked_col_pairs: set of "table.column" lowercase pairs
      - blocked_col_bare: set of bare lowercase column names
    """
    policies = load_policies()
    user_policy = policies.get("users", {}).get(username, {})
    role_policy = policies.get("roles", {}).get(user_role, {})

    blocked_tables = (
        {t.lower() for t in user_policy.get("blocked_tables", [])} |
        {t.lower() for t in role_policy.get("blocked_tables", [])}
    )

    raw_columns = (
        {c.lower() for c in user_policy.get("blocked_columns", [])} |
        {c.lower() for c in role_policy.get("blocked_columns", [])}
    )

    blocked_col_pairs = {c for c in raw_columns if "." in c}
    blocked_col_bare = {c for c in raw_columns if "." not in c}

    return blocked_tables, blocked_col_pairs, blocked_col_bare


def filter_schema_for_user(schema: SchemaResponse, username: str) -> SchemaResponse:
    """
    Filter the DB schema based on both Role and User RBAC rules.

    Defence-in-depth: if the user cannot be resolved, return an EMPTY
    schema (fail-closed) instead of the full schema.
    """
    user = _resolve_user(username)

    # Admin roles bypass RBAC
    if _is_admin_user(username, user):
        return schema

    # FAIL-CLOSED: unknown user gets nothing
    if not user:
        logger.warning(
            f"RBAC fail-closed: user '{username}' not found in user list — "
            "returning empty schema."
        )
        return schema.model_copy(update={"tables": []})

    blocked_tables, blocked_col_pairs, blocked_col_bare = _build_blocked_sets(
        username, user["role"]
    )

    if not blocked_tables and not blocked_col_pairs and not blocked_col_bare:
        return schema

    filtered_tables = []
    for table in schema.tables:
        table_lower = table.table.lower()

        # Whole-table block
        if table_lower in blocked_tables:
            continue

        # Column-level block — check both bare name and table.column pair
        filtered_cols = []
        for col in table.columns:
            col_lower = col.name.lower()
            full_name = f"{table_lower}.{col_lower}"

            if col_lower in blocked_col_bare:
                continue
            if full_name in blocked_col_pairs:
                continue
            filtered_cols.append(col)

        if filtered_cols:
            new_table = table.model_copy(update={"columns": filtered_cols})
            filtered_tables.append(new_table)

    return schema.model_copy(update={"tables": filtered_tables})


def get_effective_rbac_for_user(username: str) -> Dict[str, List[str]]:
    """
    Return merged RBAC restrictions (user + role) for runtime SQL enforcement.

    Defence-in-depth: if the user cannot be resolved, return a wildcard
    block marker so the RBAC guard will deny every query (fail-closed).
    """
    user = _resolve_user(username)

    # Admin roles bypass RBAC
    if _is_admin_user(username, user):
        return {"blocked_tables": [], "blocked_columns": []}

    # FAIL-CLOSED: unknown user gets a sentinel that blocks everything
    if not user:
        logger.warning(
            f"RBAC fail-closed: user '{username}' not found in user list — "
            "returning wildcard block."
        )
        return {"blocked_tables": ["*"], "blocked_columns": ["*"]}

    policies = load_policies()
    user_policy = policies.get("users", {}).get(username, {})
    role_policy = policies.get("roles", {}).get(user["role"], {})

    blocked_tables = (
        {t.lower() for t in user_policy.get("blocked_tables", [])} |
        {t.lower() for t in role_policy.get("blocked_tables", [])}
    )
    blocked_columns = (
        {c.lower() for c in user_policy.get("blocked_columns", [])} |
        {c.lower() for c in role_policy.get("blocked_columns", [])}
    )

    return {
        "blocked_tables": sorted(blocked_tables),
        "blocked_columns": sorted(blocked_columns)
    }


# Keep old name as alias for backwards compatibility
def filter_schema_by_role(schema: SchemaResponse, role: str) -> SchemaResponse:
    """Legacy alias — role is treated as username for per-user RBAC."""
    return filter_schema_for_user(schema, role)
