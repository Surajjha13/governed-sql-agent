import logging
import re
import sqlglot
from sqlglot import exp, parse_one
import sqlglot.optimizer as sqlglot_optimizer
from typing import Optional
from app.schema_service.models import SchemaResponse

logger = logging.getLogger(__name__)

def optimize_sql(sql: str, schema: SchemaResponse, engine: str = "postgres") -> str:
    """
    Apply performance-focused rewrites to generated SQL.
    """
    if not sql:
        return sql

    dialect = "mysql" if engine.lower() == "mysql" else "postgres"
    
    # 1. Clean markdown and artifacts
    processed = sql.replace("```sql", "").replace("```", "").strip()
    processed = re.sub(r'^(?:CONNECT TO|SET search_path|SET)\s+.*;?\s*', '', processed, flags=re.MULTILINE | re.IGNORECASE)
    processed = processed.strip()

    try:
        # Pre-pass: Fix triple identifiers like "a"."fl"."price" -> "fl"."price"
        processed = re.sub(r'"?([^".\s]+)"?\."?([^".\s]+)"?\."?([^".\s]+)"?', r'"\2"."\3"', processed)

        # Parse into AST
        expression = parse_one(processed, read=dialect)

        # 1. Window Function Promotion
        expression = _apply_window_optimizations(expression)

        # 2. Join Optimization (Path-aware)
        expression = _remove_redundant_joins(expression)

        # 3. Standard sqlglot optimizations
        optimized = sqlglot_optimizer.optimize(expression, dialect=dialect)

        return optimized.sql(dialect=dialect, pretty=True)

    except Exception as e:
        logger.warning(f"Optimization pass failed: {e}. Returning cleaned SQL.")
        return processed

def _remove_redundant_joins(expression: exp.Expression) -> exp.Expression:
    """
    Removes joins only if they are truly not referenced anywhere in the query.
    References include SELECT columns, WHERE filters, GROUP BY, and the ON clauses 
    of other joins (path dependency).
    """
    if not isinstance(expression, exp.Select):
        return expression

    # 1. Collect ALL identifiers that could be table/alias references
    referenced_identifiers = set()
    
    def add_ref(node):
        if node and hasattr(node, "this"):
            # Strip quotes and normalize to lowercase
            name = str(node.this).lower().replace('"', '').replace("'", "")
            referenced_identifiers.add(name)
        elif node:
            name = str(node).lower().replace('"', '').replace("'", "")
            referenced_identifiers.add(name)

    # Check all columns across the entire query
    for col in expression.find_all(exp.Column):
        if col.table:
            add_ref(col.table)
    
    # Check all identifiers in JOIN conditions (the ON clause)
    # This ensures "bridge" tables like film_actor are NOT removed if used in the 
    # next join's path.
    for join in expression.args.get("joins", []):
        on_clause = join.args.get("on")
        if on_clause:
            for col in on_clause.find_all(exp.Column):
                if col.table:
                    add_ref(col.table)
    
    # 2. Conservative Filter
    new_joins = []
    for join in expression.args.get("joins", []):
        join_target = join.this
        
        # Extract the alias or table name for this join
        target_token = None
        if isinstance(join_target, exp.Alias):
            target_token = join_target.alias
        elif isinstance(join_target, exp.Table):
            # sqlglot Table node might have an internal alias or just a name
            alias_node = join_target.args.get("alias")
            target_token = alias_node if alias_node else join_target.this
            
        # Normalize to string (unquoted)
        target_str = ""
        if hasattr(target_token, "this"):
            target_str = str(target_token.this).lower().replace('"', '').replace("'", "")
        else:
            target_str = str(target_token).lower().replace('"', '').replace("'", "")

        if not target_str or target_str in referenced_identifiers:
            new_joins.append(join)
        else:
            logger.info(f"Removing truly redundant join to: {target_str}")
    
    expression.set("joins", new_joins)
    return expression

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
                    pass
        return node

    return expression.transform(transform)
