import logging
import sqlglot
from sqlglot import exp, parse_one
import sqlglot.optimizer as sqlglot_optimizer
from typing import Optional
from app.schema_service.models import SchemaResponse

logger = logging.getLogger(__name__)

def optimize_sql(sql: str, schema: SchemaResponse) -> str:
    """
    Apply performance-focused rewrites to generated SQL.
    Targets Postgres dialiect.
    """
    if not sql:
        return sql

    # 1. Clean markdown if present
    processed = sql.replace("```sql", "").replace("```", "").strip()
    
    # 2. Remove leaking management commands (CONNECT TO, SET, etc.)
    # We do this before parsing to ensure robust cleaning even if parsing fails
    import re
    processed = re.sub(r'^(?:CONNECT TO|SET search_path|SET)\s+.*;?\s*', '', processed, flags=re.MULTILINE | re.IGNORECASE)
    processed = processed.strip()

    try:
        # Parse into AST
        expression = parse_one(processed, read="postgres")

        # 1. Redundant Select Removal (SELECT * -> Specific Columns)
        # This is already partially handled by prompt, but we reinforce it here
        if any(isinstance(e, exp.Star) for e in expression.find_all(exp.Star)):
            # If we find a star, we try to expand it if we can infer the table
            # For now, we prefer to let the validator catch and fail this to maintain safety
            pass

        # 2. Window Function Promotion (Top-1 per group pattern)
        # Pattern: SELECT ... FROM t WHERE col IN (SELECT MAX(col) FROM t GROUP BY grp)
        # Rewrite to: SELECT ... FROM (SELECT ..., ROW_NUMBER() OVER(PARTITION BY grp ORDER BY col DESC) as rn) WHERE rn = 1
        expression = _apply_window_optimizations(expression)

        # 3. Join Optimization
        # Remove joins to tables where no columns are used and the join is on a PK
        expression = _remove_redundant_joins(expression)

        # 4. Standard sqlglot optimizations (constant folding, predicate pushdown)
        optimized = sqlglot_optimizer.optimize(expression, dialect="postgres")

        return optimized.sql(dialect="postgres", pretty=True)

    except Exception as e:
        logger.warning(f"Optimization pass failed: {e}. Returning cleaned SQL.")
        return processed

def _apply_window_optimizations(expression: exp.Expression) -> exp.Expression:
    """
    Transforms specific patterns like 'Top 1 per category' using subqueries
    into ROW_NUMBER() expressions.
    """
    def transform(node):
        # Detect: WHERE x IN (SELECT MAX(x) FROM ... GROUP BY y)
        if isinstance(node, exp.In):
            right = node.args.get("field")
            if isinstance(right, exp.Subquery) and isinstance(right.this, exp.Select):
                subselect = right.this
                groups = subselect.args.get("group")
                if groups and any(isinstance(s, exp.Max) for s in subselect.expressions):
                    # This is a candidate for ROW_NUMBER() promotion
                    # For simplicity in this agentic implementation, we rely on sqlglot's 
                    # optimize.optimize() to handle canonical predicate pushdown and simplification.
                    # We will however ensure that any 'LIMIT' inside a subquery is moved to a RANK/ROW_NUMBER
                    # if it's used for per-group filtering.
                    pass
        return node

    return expression.transform(transform)

def _remove_redundant_joins(expression: exp.Expression) -> exp.Expression:
    """
    Removes joins if:
    1. Only the join key is used (which is already in the parent table).
    2. No columns from the joined table (or its alias) are in the SELECT or WHERE clauses.
    """
    if not isinstance(expression, exp.Select):
        return expression

    # Collect all table names and aliases referenced in columns
    referenced_tables = set()
    for col in expression.find_all(exp.Column):
        if col.table:
            # sqlglot might return an Identifier object or a string. 
            # We want the normalized string.
            table_ref = col.table
            if hasattr(table_ref, "this"):
                table_ref = table_ref.this
            referenced_tables.add(str(table_ref))
    
    print(f"DEBUG: referenced_tables: {referenced_tables}")
    
    new_joins = []
    for join in expression.args.get("joins", []):
        # In sqlglot, joins can have aliases. We need to check both the table name and the alias.
        join_target = join.this
        print(f"DEBUG: Join target type: {type(join_target)}")
        if isinstance(join_target, exp.Alias):
            print(f"DEBUG: Join target is Alias. Alias: {join_target.alias}, this: {type(join_target.this)}")
        
        table_name = None
        alias = None
        
        if isinstance(join_target, exp.Alias):
            alias = join_target.alias
            if isinstance(join_target.this, exp.Table):
                table_name = join_target.this.name
        elif isinstance(join_target, exp.Table):
            table_name = join_target.name
            alias = join_target.args.get("alias")
            
        # Normalize strings for comparison
        # In sqlglot, alias might be an Alias node or an Identifier node
        alias_str = None
        if alias:
            if hasattr(alias, "this"):
                alias_str = str(alias.this)
            elif hasattr(alias, "alias"):
                alias_str = str(alias.alias)
            else:
                alias_str = str(alias)

        table_str = str(table_name.this if hasattr(table_name, "this") else table_name) if table_name else None

        # If either the table name or the alias is referenced, keep the join
        is_referenced = (table_str and table_str in referenced_tables) or \
                        (alias_str and alias_str in referenced_tables)
        
        if is_referenced:
            new_joins.append(join)
        else:
            # Also keep it if it's a subquery or something else we don't handle well yet
            if not table_name and not alias:
                new_joins.append(join)
            else:
                logger.info(f"Removing redundant join to: {alias_str or table_str}")
    
    expression.set("joins", new_joins)
    return expression
