import logging
from typing import Optional
import sqlglot
from sqlglot import exp

logger = logging.getLogger(__name__)

def validate_aggregation(sql: str, engine: str = "postgres") -> Optional[str]:
    """
    Validates structural correctness of aggregations and GROUP BY clauses.
    Returns an error string if invalid, else None.
    
    Only checks for genuine structural issues:
    - Non-aggregated columns in SELECT without GROUP BY when aggregation is used
    
    Does NOT block:
    - COUNT(column) — this is valid SQL that counts non-NULL values
    """
    dialect = "mysql" if engine.lower() == "mysql" else "postgres"
    try:
        sql = sql.replace("```sql", "").replace("```", "").strip()
        parsed = sqlglot.parse_one(sql, read=dialect)
    except Exception:
        # If it doesn't parse, let the normal executor handle the error
        return None
        
    # Examine every SELECT block (including subqueries)
    for select in parsed.find_all(exp.Select):
        select_exprs = select.expressions
        if not select_exprs:
            continue
            
        has_agg = False
        non_agg_cols = []
        
        for expr in select_exprs:
            # 1. Flag if any aggregation exists
            for agg_node in expr.find_all(exp.AggFunc):
                has_agg = True
                # COUNT(col) is valid SQL — it counts non-NULL values
                # No need to block it
                        
            # 2. Extract strictly non-aggregated columns
            for col in expr.find_all(exp.Column):
                is_inside_agg = False
                parent = col.parent
                while parent:
                    if isinstance(parent, exp.AggFunc):
                        is_inside_agg = True
                        break
                    # Also skip columns inside window functions — they're valid
                    if isinstance(parent, exp.Window):
                        is_inside_agg = True
                        break
                    parent = parent.parent
                    
                if not is_inside_agg:
                    non_agg_cols.append(col)

        # Ensure mathematical balance of GROUP BY logic
        if has_agg and non_agg_cols:
            group_by = select.args.get("group")
            if not group_by:
                return (
                    "Aggregation Error: The query contains aggregation functions (e.g., COUNT, SUM) "
                    "but includes non-aggregated columns in SELECT without a GROUP BY clause."
                )
                
            # Verify every non_agg_col is mathematically resolved in the GROUP BY
            group_by_cols = []
            for gb_expr in group_by.expressions:
                # Group by can be an index (e.g. GROUP BY 1) or column reference
                if isinstance(gb_expr, exp.Column):
                    group_by_cols.append(gb_expr)
                elif isinstance(gb_expr, exp.Literal):
                    # GROUP BY 1, 2, etc. — positional references are valid, skip strict check
                    continue
                else:
                    for c in gb_expr.find_all(exp.Column):
                        group_by_cols.append(c)

            gb_col_names = {c.sql(dialect).lower() for c in group_by_cols}
            
            # Also collect aliases from SELECT to handle GROUP BY alias
            select_aliases = set()
            for expr in select_exprs:
                if isinstance(expr, exp.Alias) and expr.alias:
                    select_aliases.add(expr.alias.lower())

            for col in non_agg_cols:
                col_sql = col.sql(dialect).lower()
                col_name_only = col.name.lower() if col.name else col_sql
                
                if col_sql not in gb_col_names and col_name_only not in gb_col_names:
                    # Check if it matches a positional GROUP BY (we can't easily verify positions, 
                    # so if there are positional refs in GROUP BY, be lenient)
                    has_positional = any(
                        isinstance(gb_expr, exp.Literal) 
                        for gb_expr in group_by.expressions
                    )
                    if has_positional:
                        continue
                    
                    return (
                        f"Aggregation Error: Column '{col.sql(dialect)}' is in the SELECT list "
                        f"but not inside an aggregate function or the GROUP BY clause."
                    )

    return None
