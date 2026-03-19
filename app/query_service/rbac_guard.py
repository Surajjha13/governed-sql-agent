from typing import Dict, List, Optional
import logging

from sqlglot import exp, parse_one
import sqlglot

logger = logging.getLogger(__name__)


def _normalize_set(values: List[str]) -> set:
    return {str(v).strip().lower() for v in values if str(v).strip()}


def validate_sql_against_rbac(sql: str, restrictions: Dict[str, List[str]]) -> Optional[str]:
    """
    Validate query AST against blocked tables/columns.
    Returns a human-readable denial reason if blocked; otherwise None.
    """
    blocked_tables = _normalize_set((restrictions or {}).get("blocked_tables", []))
    blocked_columns = _normalize_set((restrictions or {}).get("blocked_columns", []))

    if not blocked_tables and not blocked_columns:
        return None

    try:
        expression = parse_one(sql, read="postgres")
    except sqlglot.errors.ParseError as e:
        logger.warning(f"RBAC guard skipped (parse error): {e}")
        return None
    except Exception as e:
        logger.warning(f"RBAC guard skipped (unexpected parse failure): {e}")
        return None

    referenced_tables = set()
    for tbl in expression.find_all(exp.Table):
        if tbl and tbl.name:
            referenced_tables.add(tbl.name.lower())

    blocked_table_hits = sorted(t for t in referenced_tables if t in blocked_tables)
    if blocked_table_hits:
        joined = ", ".join(blocked_table_hits)
        return f"Access denied: administrator restricted table(s): {joined}."

    if not blocked_columns:
        return None

    blocked_column_pairs = {c for c in blocked_columns if "." in c}
    blocked_column_names = {c for c in blocked_columns if "." not in c}

    for col in expression.find_all(exp.Column):
        col_name = (col.name or "").lower()
        table_name = (col.table or "").lower()
        if not col_name:
            continue

        if col_name in blocked_column_names:
            return f"Access denied: administrator restricted column '{col_name}'."

        if table_name:
            full_name = f"{table_name}.{col_name}"
            if full_name in blocked_column_pairs:
                return f"Access denied: administrator restricted column '{full_name}'."
        else:
            for blocked_full in blocked_column_pairs:
                blocked_table, blocked_col = blocked_full.split(".", 1)
                if blocked_col == col_name and (not referenced_tables or blocked_table in referenced_tables):
                    return f"Access denied: administrator restricted column '{blocked_full}'."

    return None

