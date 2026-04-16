"""
Tests for RBAC enforcement — policies.py + rbac_guard.py
Validates table-level blocking, column-level blocking (bare and table.column),
fail-closed behavior, and admin bypass.
"""
import pytest
from unittest.mock import patch, MagicMock
from app.auth.policies import (
    filter_schema_for_user,
    get_effective_rbac_for_user,
    _is_admin_user,
)
from app.query_service.rbac_guard import validate_sql_against_rbac
from app.schema_service.models import SchemaResponse, TableMeta, ColumnMeta


# ── Helpers ──────────────────────────────────────────────────────────────────

def _make_col(name, data_type="text"):
    return ColumnMeta(name=name, data_type=data_type, nullable=True)


def _make_schema(*table_defs):
    """Build a SchemaResponse from (table_name, [col_names...]) tuples."""
    tables = []
    for tname, cols in table_defs:
        tables.append(TableMeta(
            schema_name="public",
            table=tname,
            columns=[_make_col(c) for c in cols],
        ))
    return SchemaResponse(engine="postgres", database="test_db", tables=tables)


def _mock_resolve_user(user_record):
    """Mock _resolve_user to return the given user dict (or None)."""
    return patch("app.auth.policies._resolve_user", return_value=user_record)


def _mock_policies(policies):
    """Return a patcher that makes load_policies() return the given dict."""
    return patch("app.auth.policies.load_policies", return_value=policies)


EMPLOYEE = {"username": "emp1", "role": "DATA_ANALYST", "enterprise_id": 1, "owner_admin_id": 1}
ADMIN_USER = {"username": "boss", "role": "SYSTEM_ADMIN", "enterprise_id": 1, "owner_admin_id": None}


# ── filter_schema_for_user ───────────────────────────────────────────────────

class TestFilterSchemaForUser:

    def test_blocked_table_is_removed(self):
        schema = _make_schema(
            ("employees", ["id", "name"]),
            ("salaries", ["id", "amount"]),
        )
        policies = {
            "users": {"emp1": {"blocked_tables": ["salaries"], "blocked_columns": []}},
            "roles": {},
        }
        with _mock_resolve_user(EMPLOYEE), _mock_policies(policies):
            result = filter_schema_for_user(schema, "emp1")

        table_names = [t.table for t in result.tables]
        assert "employees" in table_names
        assert "salaries" not in table_names

    def test_blocked_table_case_insensitive(self):
        schema = _make_schema(("Employees", ["id"]), ("SALARIES", ["id"]))
        policies = {
            "users": {"emp1": {"blocked_tables": ["salaries"], "blocked_columns": []}},
            "roles": {},
        }
        with _mock_resolve_user(EMPLOYEE), _mock_policies(policies):
            result = filter_schema_for_user(schema, "emp1")

        table_names = [t.table for t in result.tables]
        assert "Employees" in table_names
        assert "SALARIES" not in table_names

    def test_blocked_column_bare_name(self):
        schema = _make_schema(("employees", ["id", "name", "salary"]))
        policies = {
            "users": {"emp1": {"blocked_tables": [], "blocked_columns": ["salary"]}},
            "roles": {},
        }
        with _mock_resolve_user(EMPLOYEE), _mock_policies(policies):
            result = filter_schema_for_user(schema, "emp1")

        col_names = [c.name for c in result.tables[0].columns]
        assert "id" in col_names
        assert "name" in col_names
        assert "salary" not in col_names

    def test_blocked_column_table_dot_column(self):
        """The critical bug fix: table.column entries must work."""
        schema = _make_schema(
            ("employees", ["id", "name", "salary"]),
            ("departments", ["id", "salary_budget"]),
        )
        policies = {
            "users": {"emp1": {"blocked_tables": [], "blocked_columns": ["employees.salary"]}},
            "roles": {},
        }
        with _mock_resolve_user(EMPLOYEE), _mock_policies(policies):
            result = filter_schema_for_user(schema, "emp1")

        emp_cols = [c.name for c in result.tables[0].columns]
        assert "salary" not in emp_cols  # blocked via employees.salary
        assert "id" in emp_cols
        assert "name" in emp_cols

        # departments.salary_budget should NOT be affected
        dept_cols = [c.name for c in result.tables[1].columns]
        assert "salary_budget" in dept_cols

    def test_role_level_blocking(self):
        schema = _make_schema(("secrets", ["id", "token"]), ("public_data", ["id"]))
        policies = {
            "users": {},
            "roles": {"DATA_ANALYST": {"blocked_tables": ["secrets"], "blocked_columns": []}},
        }
        with _mock_resolve_user(EMPLOYEE), _mock_policies(policies):
            result = filter_schema_for_user(schema, "emp1")

        table_names = [t.table for t in result.tables]
        assert "secrets" not in table_names
        assert "public_data" in table_names

    def test_merged_user_and_role_blocking(self):
        schema = _make_schema(
            ("employees", ["id", "salary"]),
            ("secrets", ["id", "token"]),
            ("public_data", ["id"]),
        )
        policies = {
            "users": {"emp1": {"blocked_tables": ["employees"], "blocked_columns": []}},
            "roles": {"DATA_ANALYST": {"blocked_tables": ["secrets"], "blocked_columns": []}},
        }
        with _mock_resolve_user(EMPLOYEE), _mock_policies(policies):
            result = filter_schema_for_user(schema, "emp1")

        table_names = [t.table for t in result.tables]
        assert table_names == ["public_data"]

    def test_fail_closed_unknown_user(self):
        """User not found in list_users → empty schema."""
        schema = _make_schema(("employees", ["id"]))
        with _mock_resolve_user(None):  # user not found
            result = filter_schema_for_user(schema, "unknown_user")

        assert result.tables == []

    def test_admin_username_bypass(self):
        schema = _make_schema(("employees", ["id"]))
        with _mock_resolve_user(None):
            result = filter_schema_for_user(schema, "admin")
        assert len(result.tables) == 1

    def test_system_admin_role_bypass(self):
        """SYSTEM_ADMIN role should bypass RBAC."""
        schema = _make_schema(("employees", ["id"]))
        policies = {
            "users": {"boss": {"blocked_tables": ["employees"], "blocked_columns": []}},
            "roles": {},
        }
        with _mock_resolve_user(ADMIN_USER), _mock_policies(policies):
            result = filter_schema_for_user(schema, "boss")
        assert len(result.tables) == 1  # not filtered

    def test_no_restrictions_returns_full_schema(self):
        schema = _make_schema(("a", ["x"]), ("b", ["y"]))
        policies = {"users": {}, "roles": {}}
        with _mock_resolve_user(EMPLOYEE), _mock_policies(policies):
            result = filter_schema_for_user(schema, "emp1")
        assert len(result.tables) == 2


# ── get_effective_rbac_for_user ──────────────────────────────────────────────

class TestGetEffectiveRbac:

    def test_returns_blocked_tables(self):
        policies = {
            "users": {"emp1": {"blocked_tables": ["salaries"], "blocked_columns": []}},
            "roles": {},
        }
        with _mock_resolve_user(EMPLOYEE), _mock_policies(policies):
            result = get_effective_rbac_for_user("emp1")

        assert "salaries" in result["blocked_tables"]

    def test_fail_closed_unknown_user(self):
        """Unknown user → wildcard block."""
        with _mock_resolve_user(None):
            result = get_effective_rbac_for_user("ghost")

        assert "*" in result["blocked_tables"]

    def test_admin_bypass(self):
        with _mock_resolve_user(None):
            result = get_effective_rbac_for_user("admin")
        assert result["blocked_tables"] == []
        assert result["blocked_columns"] == []

    def test_system_admin_bypass(self):
        with _mock_resolve_user(ADMIN_USER):
            result = get_effective_rbac_for_user("boss")
        assert result["blocked_tables"] == []


# ── validate_sql_against_rbac ────────────────────────────────────────────────

class TestRbacGuard:

    def test_blocked_table_denied_postgres(self):
        restrictions = {"blocked_tables": ["salaries"], "blocked_columns": []}
        result = validate_sql_against_rbac(
            "SELECT * FROM salaries", restrictions, engine="postgres"
        )
        assert result is not None
        assert "salaries" in result

    def test_blocked_table_denied_mysql(self):
        restrictions = {"blocked_tables": ["salaries"], "blocked_columns": []}
        result = validate_sql_against_rbac(
            "SELECT * FROM salaries", restrictions, engine="mysql"
        )
        assert result is not None
        assert "salaries" in result

    def test_allowed_table_passes(self):
        restrictions = {"blocked_tables": ["salaries"], "blocked_columns": []}
        result = validate_sql_against_rbac(
            "SELECT * FROM employees", restrictions, engine="postgres"
        )
        assert result is None

    def test_blocked_column_bare(self):
        restrictions = {"blocked_tables": [], "blocked_columns": ["secret_note"]}
        result = validate_sql_against_rbac(
            "SELECT secret_note FROM orders", restrictions, engine="postgres"
        )
        assert result is not None
        assert "secret_note" in result

    def test_blocked_column_table_qualified(self):
        restrictions = {"blocked_tables": [], "blocked_columns": ["orders.secret_note"]}
        result = validate_sql_against_rbac(
            "SELECT orders.secret_note FROM orders", restrictions, engine="postgres"
        )
        assert result is not None
        assert "orders.secret_note" in result

    def test_wildcard_block_denies_everything(self):
        restrictions = {"blocked_tables": ["*"], "blocked_columns": ["*"]}
        result = validate_sql_against_rbac(
            "SELECT 1", restrictions, engine="postgres"
        )
        assert result is not None
        assert "could not be verified" in result

    def test_parse_error_denied_when_restrictions_active(self):
        """Parse failure with active restrictions should DENY (fail-closed)."""
        restrictions = {"blocked_tables": ["salaries"], "blocked_columns": []}
        result = validate_sql_against_rbac(
            "NOT VALID SQL @@@@", restrictions, engine="postgres"
        )
        assert result is not None
        assert "could not be validated" in result

    def test_no_restrictions_always_passes(self):
        restrictions = {"blocked_tables": [], "blocked_columns": []}
        result = validate_sql_against_rbac(
            "SELECT * FROM anything", restrictions, engine="postgres"
        )
        assert result is None

    def test_case_insensitive_table(self):
        restrictions = {"blocked_tables": ["Salaries"], "blocked_columns": []}
        result = validate_sql_against_rbac(
            "SELECT * FROM salaries", restrictions, engine="postgres"
        )
        assert result is not None

    def test_join_with_blocked_table(self):
        restrictions = {"blocked_tables": ["salaries"], "blocked_columns": []}
        result = validate_sql_against_rbac(
            "SELECT e.name FROM employees e JOIN salaries s ON e.id = s.emp_id",
            restrictions, engine="postgres"
        )
        assert result is not None
        assert "salaries" in result

    def test_subquery_with_blocked_table(self):
        restrictions = {"blocked_tables": ["salaries"], "blocked_columns": []}
        result = validate_sql_against_rbac(
            "SELECT * FROM employees WHERE id IN (SELECT emp_id FROM salaries)",
            restrictions, engine="postgres"
        )
        assert result is not None
        assert "salaries" in result
