import logging
from typing import Optional, Set
import sqlglot
from sqlglot import exp

from app.schema_service.models import SchemaResponse

logger = logging.getLogger(__name__)

def build_fk_pairs(schema: SchemaResponse) -> Set[tuple[str, str, str, str]]:
    """
    Extract all valid foreign key relationships from the schema.
    Returns a set of tuples: (table1, col1, table2, col2)
    To handle bidirectionality, we store both directions.
    """
    valid_pairs = set()
    for table in schema.tables:
        t1 = table.table.lower()
        for col in table.columns:
            if col.foreign_key:
                # Format: "schema.table.column" or "table.column" or "column"
                parts = col.foreign_key.split(".")
                if len(parts) >= 1:
                    c1 = col.name.lower()
                    target_col = parts[-1].lower()
                    
                    # If only 1 part ("column"), we might not know the target table!
                    # If 2 parts ("table.col"), index -2 is the table.
                    # If 3 parts ("schema.table.col"), index -2 is the table.
                    target_table = parts[-2].lower() if len(parts) >= 2 else None
                    
                    if target_table:
                        valid_pairs.add((t1, c1, target_table, target_col))
                        valid_pairs.add((target_table, target_col, t1, c1))
    return valid_pairs


def _build_schema_column_map(schema: SchemaResponse) -> dict[str, set[str]]:
    """Build a map of table_name -> set of column_names for fast lookup."""
    col_map = {}
    for table in schema.tables:
        col_map[table.table.lower()] = {col.name.lower() for col in table.columns}
    return col_map


def extract_condition_columns(condition: exp.Expression) -> list[tuple[str, str]]:
    """
    Recursively extract (table, column) pairs from a join condition.
    Returns e.g. [('film', 'film_id'), ('inventory', 'film_id')]
    """
    cols = []
    if isinstance(condition, exp.Column):
        table = condition.table
        name = condition.name
        if table and name:
            cols.append((table.lower(), name.lower()))
    elif hasattr(condition, 'args'):
        for arg in condition.args.values():
            if isinstance(arg, list):
                for item in arg:
                    if isinstance(item, exp.Expression):
                        cols.extend(extract_condition_columns(item))
            elif isinstance(arg, exp.Expression):
                cols.extend(extract_condition_columns(arg))
    return cols

def validate_joins(sql: str, schema: SchemaResponse, engine: str = "postgres") -> Optional[str]:
    """
    Parse SQL AST, find all JOINs, and validate them.
    
    PHILOSOPHY: Only hard-block for clear security/hallucination issues.
    For FK relationship mismatches, log warnings and allow the query through —
    let the database be the judge of correctness.
    
    Hard blocks (returns error string):
      - Hallucinated table names not in schema
    
    Soft warnings (logged but returns None):
      - Join conditions that don't match known FK relationships
      - USING clause (just convert to warning since it's valid SQL)
    
    Returns error string if hard-block issue found, else None.
    """
    if not schema.tables:
        return None
        
    try:
        # 1. Parse SQL
        dialect = "mysql" if engine.lower() == "mysql" else "postgres"
        try:
            # Quick format cleanup just in case
            sql = sql.replace("```sql", "").replace("```", "").strip()
            parsed = sqlglot.parse_one(sql, read=dialect)
        except sqlglot.errors.ParseError:
            # If it doesn't parse, let the normal executor handle the error
            return None
            
        if not parsed:
            return None

        # 2. Build a whitelist of allowed tables
        schema_tables = {t.table.lower() for t in schema.tables}
        allowed_tables = set(schema_tables)
        allowed_tables.update(["information_schema", "pg_catalog"])
        
        dynamic_tables = set()
        # Add generated CTE names and Subquery aliases to dynamic list
        for cte in parsed.find_all(exp.CTE):
            if cte.alias:
                dynamic_tables.add(cte.alias.lower())
                
        for subq in parsed.find_all(exp.Subquery):
            if subq.alias:
                dynamic_tables.add(subq.alias.lower())
                
        allowed_tables.update(dynamic_tables)
        
        # 3. Extract table aliases and verify tables exist (HARD BLOCK for hallucinated tables)
        for table_node in parsed.find_all(exp.Table):
            if table_node.name:
                table_name = table_node.name.lower()
                if table_name not in allowed_tables:
                    return f"Invalid schema join detected. Table '{table_name}' does not exist in the schema."

        # 4. Log warnings for joins that don't match FK metadata, but DON'T block
        valid_fk_pairs = build_fk_pairs(schema)
        schema_col_map = _build_schema_column_map(schema)
        
        table_aliases = {}
        for table_node in parsed.find_all(exp.Table):
            if table_node.name:
                table_name = table_node.name.lower()
                alias = table_node.alias.lower() if table_node.alias else table_name
                table_aliases[alias] = table_name

        for join in parsed.find_all(exp.Join):
            # USING clause: just log a warning instead of blocking
            if join.args.get("using"):
                logger.info("Join uses USING clause — allowing through (valid SQL syntax).")
                continue
                
            on_clause = join.args.get("on")
            if not on_clause:
                continue
                
            condition_cols = extract_condition_columns(on_clause)
            
            # Resolve aliases to actual table names
            resolved_cols = []
            for alias, col in condition_cols:
                true_table = table_aliases.get(alias, alias)
                resolved_cols.append((true_table, col))
                
            is_valid_join = False
            has_cross_table_condition = False
            
            for i in range(len(resolved_cols)):
                for j in range(i+1, len(resolved_cols)):
                    t1, c1 = resolved_cols[i]
                    t2, c2 = resolved_cols[j]
                    
                    if t1 != t2:
                        has_cross_table_condition = True
                        
                        # If either side is a dynamic table (CTE/Subquery), always allow
                        if t1 in dynamic_tables or t2 in dynamic_tables:
                            is_valid_join = True
                            break
                            
                        # Check explicit FK metadata
                        if (t1, c1, t2, c2) in valid_fk_pairs or (t2, c2, t1, c1) in valid_fk_pairs:
                            is_valid_join = True
                            break
                        
                        # PERMISSIVE FALLBACK 1: Matching column names ending in _id or == 'id'
                        if c1 == c2 and (c1.endswith("_id") or c1 == "id"):
                            is_valid_join = True
                            logger.info(f"Join permitted by implicit ID fallback: {t1}.{c1} = {t2}.{c2}")
                            break
                        
                        # PERMISSIVE FALLBACK 2: The join column exists in both tables' schemas
                        t1_cols = schema_col_map.get(t1, set())
                        t2_cols = schema_col_map.get(t2, set())
                        if c1 in t1_cols and c2 in t2_cols:
                            is_valid_join = True
                            logger.info(f"Join permitted by column-existence fallback: {t1}.{c1} = {t2}.{c2}")
                            break
                        
                        # PERMISSIVE FALLBACK 3: One column references the other table's name
                        # e.g., customer.address_id -> address.address_id
                        if c1.replace("_id", "") == t2 or c2.replace("_id", "") == t1:
                            is_valid_join = True
                            logger.info(f"Join permitted by table-name heuristic: {t1}.{c1} = {t2}.{c2}")
                            break
                            
                if is_valid_join:
                    break
                    
            if has_cross_table_condition and not is_valid_join:
                # SOFT WARNING — log it but DON'T block the query
                tables_involved = list(set([t for t, c in resolved_cols]))
                logger.warning(
                    f"Join validation soft warning: Tables {tables_involved} may not share a "
                    f"direct FK relationship. Allowing query through — database will validate."
                )

        return None
        
    except Exception as e:
        logger.warning(f"Join validation failed internally: {e}")
        return None  # Fail open if the validator crashes so we don't break existing queries
