import re
import math
import os
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
import pymysql
import app.app_state as app_state
import logging
from typing import Dict, Any, List, Optional
import asyncio
import sqlglot
from sqlglot import exp

logger = logging.getLogger(__name__)

DEFAULT_ROW_LIMIT = int(os.getenv("QUERY_RESULT_ROW_LIMIT", "500"))
POSTGRES_POOL_MINCONN = int(os.getenv("POSTGRES_POOL_MINCONN", "2"))
POSTGRES_POOL_MAXCONN = int(os.getenv("POSTGRES_POOL_MAXCONN", "10"))


def _pool_signature(conn_info: dict) -> str:
    return "|".join(
        str(conn_info.get(part, ""))
        for part in ("engine", "host", "port", "database", "user", "password")
    )


def _resolve_row_limit(row_limit: int | None) -> int | None:
    effective_limit = DEFAULT_ROW_LIMIT if row_limit is None else row_limit
    if effective_limit is None or effective_limit <= 0:
        return None
    return effective_limit


def _ensure_postgres_pool(state, session_id: str, conn_info: dict) -> ThreadedConnectionPool:
    signature = _pool_signature(conn_info)
    existing_pool = getattr(state, "db_pool", None)
    existing_signature = getattr(state, "pool_signature", None)

    if existing_pool is not None and existing_signature != signature:
        app_state.close_session_db_pool(state)
        existing_pool = None

    if existing_pool is None:
        state.db_pool = ThreadedConnectionPool(
            minconn=POSTGRES_POOL_MINCONN,
            maxconn=POSTGRES_POOL_MAXCONN,
            host=conn_info["host"],
            port=conn_info["port"],
            dbname=conn_info["database"],
            user=conn_info["user"],
            password=conn_info["password"],
        )
        state.pool_signature = signature
        logger.info(
            f"Created PostgreSQL connection pool for session {session_id} "
            f"({POSTGRES_POOL_MINCONN}-{POSTGRES_POOL_MAXCONN} connections)."
        )

    return state.db_pool


class SimpleMySQLPool:
    """A simple connection wrapper to mimic a pool for MySQL."""
    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.conn = None

    def getconn(self):
        if not self.conn or not self.conn.open:
            self.conn = pymysql.connect(**self.kwargs)
        return self.conn

    def putconn(self, conn, close=False):
        if close and self.conn:
            self.conn.close()
            self.conn = None

    def closeall(self):
        if self.conn:
            try:
                self.conn.close()
            except:
                pass
            self.conn = None


def _ensure_mysql_pool(state, session_id: str, conn_info: dict) -> SimpleMySQLPool:
    signature = _pool_signature(conn_info)
    existing_pool = getattr(state, "db_pool", None)
    existing_signature = getattr(state, "pool_signature", None)

    if existing_pool is not None and existing_signature != signature:
        app_state.close_session_db_pool(state)
        existing_pool = None

    if existing_pool is None:
        state.db_pool = SimpleMySQLPool(
            host=conn_info["host"],
            port=conn_info["port"],
            user=conn_info["user"],
            password=conn_info["password"],
            database=conn_info["database"],
            cursorclass=pymysql.cursors.DictCursor
        )
        state.pool_signature = signature
        logger.info(f"Created pseudo-pool for MySQL session {session_id}")

    return state.db_pool


def _sanitize_value(val):
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return None
    return val


def _validate_sql_ast(sql: str, engine: str = "postgres"):
    import re
    # Strip markdown code blocks that the LLM might have returned, ensuring parser accuracy
    clean_sql = re.sub(r'```(?:sql)?\n?', '', sql, flags=re.IGNORECASE)
    clean_sql = re.sub(r'```', '', clean_sql).strip()
    
    try:
        dialect = "mysql" if engine.lower() == "mysql" else "postgres"
        parsed = sqlglot.parse(clean_sql, read=dialect)
        for statement in parsed:
            if not statement:
                continue
                
            # 1. Enforce root node is a Select to block bare TRUNCATE, DROP, DELETE, INSERT, UPDATE, GRANT, etc.
            if not isinstance(statement, exp.Select):
                raise ValueError(f"Only SELECT statements are permitted. Detected root type: {type(statement).__name__}")
                
            # 2. Prevent CTE DML (e.g., WITH d AS (DELETE ...) SELECT)
            forbidden_types = (exp.Drop, exp.Delete, exp.Update, exp.Insert, exp.Alter, exp.Command)
            for forbidden_type in forbidden_types:
                if list(statement.find_all(forbidden_type)):
                    raise ValueError(f"Destructive SQL structure '{forbidden_type.__name__}' detected and blocked.")
                    
            if hasattr(exp, 'Truncate') and list(statement.find_all(getattr(exp, 'Truncate'))):
                 raise ValueError("Destructive TRUNCATE command detected and blocked.")
    except Exception as e:
        if isinstance(e, ValueError):
            raise
        logger.warning(f"Failed to parse SQL AST: {e}, rejecting query safely.")
        raise ValueError("Failed to parse SQL structure safely. Query rejected.")


import anyio

def execute_sql(sql: str, session_id: str = "default", row_limit: int | None = None) -> dict:
    """
    Execute the generated SQL query against the database connected for the given session.
    """
    state = app_state.get_session(session_id)
    conn_info = state.current_connection
    
    if not conn_info or not conn_info.get("connected"):
        logger.warning(f"Session {session_id} attempted to execute SQL without active connection")
        return {"error": "No active database connection"}
        
    engine = conn_info.get("engine", "postgres")
    if engine not in ["postgres", "mysql"]:
        logger.warning(f"Execution not supported for engine: {engine}")
        return {"error": f"Execution not supported for {engine}"}

    try:
        _validate_sql_ast(sql, engine=engine)
    except ValueError as ve:
        logger.warning(f"SQL execution blocked: {ve}")
        return {"error": str(ve), "columns": [], "rows": []}

    effective_row_limit = _resolve_row_limit(row_limit)
    conn = None
    
    # --- PERFORMANCE BOOST: Optimize for single-pass counting ---
    # We use a window function COUNT(*) OVER() to get the total count without a subquery.
    # This reduces DB round trips by 50% for limited results.
    optimized_sql = sql.strip().rstrip(";")
    can_use_window = False
    
    # Basic heuristic: if it's a simple SELECT and we have a limit or truncation expected
    if re.search(r"^\s*SELECT\s+", optimized_sql, re.IGNORECASE):
        # Avoid double-wrapping if the LLM already used window functions or if it's very complex
        if "__total_count" not in optimized_sql.lower() and "count(*) over()" not in optimized_sql.lower():
            try:
                parsed = sqlglot.parse_one(optimized_sql, read=engine)
                if isinstance(parsed, exp.Select):
                    # Add COUNT(*) OVER() as a hidden column
                    parsed.select(exp.Alias(
                        this=exp.Window(this=exp.Count(this=exp.Star()), partition_by=None, order=None),
                        alias=exp.Identifier(this="__total_count", quoted=True)
                    ), copy=False)
                    optimized_sql = parsed.sql(dialect=engine)
                    can_use_window = True
            except Exception as e:
                logger.debug(f"Could not inject window function for counting: {e}")
                optimized_sql = sql # Fallback

    try:
        if engine == "postgres":
            pool = _ensure_postgres_pool(state, session_id, conn_info)
            conn = pool.getconn()
            cursor_cm = conn.cursor()
        else:
            pool = _ensure_mysql_pool(state, session_id, conn_info)
            conn = pool.getconn()
            cursor_cm = conn.cursor()

        with conn:
            with cursor_cm as cur:
                logger.info(f"Executing SQL ({engine}): {optimized_sql}")
                cur.execute(optimized_sql)

                # Fetch results
                if cur.description:
                    columns = [desc[0] for desc in cur.description]
                    
                    # Identify if we have our injected count column
                    has_window_count = "__total_count" in columns
                    count_idx = columns.index("__total_count") if has_window_count else -1
                    
                    # Clean columns if we added the helper
                    ui_columns = [c for c in columns if c != "__total_count"]
                    
                    if engine == "mysql":
                        if effective_row_limit is None:
                            mysql_records = cur.fetchall()
                        else:
                            mysql_records = cur.fetchmany(effective_row_limit + 1)
                        # Convert dict rows into values matching the cursor column order
                        raw_rows = [[record.get(col) for col in columns] for record in mysql_records]
                    else:
                        if effective_row_limit is None:
                            raw_rows = cur.fetchall()
                        else:
                            # Fetch one extra to detect truncation (if window function failed)
                            raw_rows = cur.fetchmany(effective_row_limit + 1)
                    
                    truncated = False
                    if not has_window_count and effective_row_limit is not None and len(raw_rows) > effective_row_limit:
                        truncated = True
                        raw_rows = raw_rows[:effective_row_limit]

                    returned_rows = len(raw_rows)
                    total_count = returned_rows
                    
                    # Extract total_count from the first row if available via Window Function
                    if has_window_count and returned_rows > 0:
                        total_count = raw_rows[0][count_idx]
                        if effective_row_limit is not None and total_count > effective_row_limit:
                           truncated = True
                           raw_rows = raw_rows[:effective_row_limit]
                           returned_rows = len(raw_rows)
                        logger.info(f"Single-pass total count: {total_count}")
                    
                    # Convert rows to list of dictionaries with sanitization
                    rows = []
                    for row in raw_rows:
                        # Skip the hidden count column for UI
                        row_to_process = [val for i, val in enumerate(row) if i != count_idx]
                        sanitized_row = {
                            col: _sanitize_value(val)
                            for col, val in zip(ui_columns, row_to_process)
                        }
                        rows.append(sanitized_row)

                    results = {
                        "columns": ui_columns,
                        "rows": rows,
                        "truncated": truncated,
                        "row_limit": effective_row_limit,
                        "returned_rows": returned_rows,
                        "total_count": total_count
                    }
                    
                    if truncated and total_count is not None and total_count > returned_rows:
                        results["message"] = (
                            f"Showing the first {returned_rows} of {total_count} results. "
                            "Refine the query for more detail."
                        )
                    
                    return results
                else:
                    return {
                        "columns": [], 
                        "rows": [], 
                        "truncated": False,
                        "returned_rows": 0,
                        "total_count": 0,
                        "message": "Query executed successfully (no result set)"
                    }

    except Exception as e:
        logger.error(f"Error executing SQL: {e}")
        if conn is not None and engine == "postgres":
            try:
                conn.rollback()
            except Exception:
                pass
        return {"error": f"Execution error: {str(e)}", "columns": [], "rows": []}
    finally:
        if conn is not None:
            try:
                if engine == "postgres":
                    state.db_pool.putconn(conn)
            except Exception as e:
                logger.warning(f"Failed to return DB connection: {e}")

async def execute_sql_async(sql: str, session_id: str = "default", row_limit: int | None = None) -> dict:
    """
    Asynchronously execute SQL query by offloading to a thread pool.
    """
    return await anyio.to_thread.run_sync(execute_sql, sql, session_id, row_limit)
