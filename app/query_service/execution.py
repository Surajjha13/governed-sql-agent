import math
import os
import psycopg2
from psycopg2.pool import ThreadedConnectionPool
import app.app_state as app_state
import logging

logger = logging.getLogger(__name__)

DEFAULT_ROW_LIMIT = int(os.getenv("QUERY_RESULT_ROW_LIMIT", "0"))
POSTGRES_POOL_MINCONN = int(os.getenv("POSTGRES_POOL_MINCONN", "1"))
POSTGRES_POOL_MAXCONN = int(os.getenv("POSTGRES_POOL_MAXCONN", "5"))


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


def _sanitize_value(val):
    if isinstance(val, float) and (math.isnan(val) or math.isinf(val)):
        return None
    return val


def execute_sql(sql: str, session_id: str = "default", row_limit: int | None = None) -> dict:
    """
    Execute the generated SQL query against the database connected for the given session.
    """
    state = app_state.get_session(session_id)
    conn_info = state.current_connection
    
    if not conn_info or not conn_info.get("connected"):
        logger.warning(f"Session {session_id} attempted to execute SQL without active connection")
        return {"error": "No active database connection"}
        
    if conn_info["engine"] != "postgres":
        logger.warning(f"Execution not supported for engine: {conn_info['engine']}")
        return {"error": f"Execution not supported for {conn_info['engine']}"}

    effective_row_limit = _resolve_row_limit(row_limit)
    conn = None
    try:
        pool = _ensure_postgres_pool(state, session_id, conn_info)
        conn = pool.getconn()
        with conn:
            with conn.cursor() as cur:
                logger.info(f"Executing SQL: {sql}")
                cur.execute(sql)

                # Fetch results
                if cur.description:
                    columns = [desc[0] for desc in cur.description]
                    truncated = False
                    if effective_row_limit is None:
                        raw_rows = cur.fetchall()
                    else:
                        raw_rows = cur.fetchmany(effective_row_limit + 1)
                        truncated = len(raw_rows) > effective_row_limit
                        if truncated:
                            raw_rows = raw_rows[:effective_row_limit]

                    # Convert rows to list of dictionaries with sanitization
                    rows = []
                    for row in raw_rows:
                        sanitized_row = {
                            col: _sanitize_value(val)
                            for col, val in zip(columns, row)
                        }
                        rows.append(sanitized_row)

                    results = {
                        "columns": columns,
                        "rows": rows
                    }
                    if effective_row_limit is not None:
                        results.update({
                            "row_limit": effective_row_limit,
                            "returned_rows": len(rows),
                            "truncated": truncated
                        })
                        if truncated:
                            results["message"] = (
                                f"Showing the first {effective_row_limit} rows only. "
                                "Refine the query or export a narrower slice for more detail."
                            )
                    logger.info(f"Query executed successfully. Returned {len(rows)} rows.")
                    return results
                else:
                    return {"columns": [], "rows": [], "message": "Query executed successfully (no results)"}

    except Exception as e:
        logger.error(f"Error executing SQL: {e}")
        if conn is not None:
            try:
                conn.rollback()
            except Exception:
                pass
        # Return error as part of result to avoid 500ing the whole request if just execution fails
        # or we could re-raise. User wants output, so error message is good.
        return {"error": f"Execution error: {str(e)}"}
    finally:
        if conn is not None:
            try:
                state.db_pool.putconn(conn, close=bool(getattr(conn, "closed", False)))
            except Exception as e:
                logger.warning(f"Failed to return DB connection to pool for session {session_id}: {e}")
