import logging
import datetime
import asyncio
from typing import Optional, Dict, List, Any
from app.schema_service.introspect.postgres import introspect_postgres
from app.schema_service.normalize import normalize_schema
from app.schema_service.models import SchemaResponse, DBConnectionRequest
from app.semantic_service.vector_index import build_vector_index

logger = logging.getLogger(__name__)

# Configuration (Idle timeout in seconds)
IDLE_TIMEOUT = 1800

import os
SYSTEM_MODE = os.getenv("SQL_AGENT_MODE", "enterprise").lower()
logger.info(f"System running in mode: {SYSTEM_MODE}")

class SessionState:
    """Represents the state of a single user session."""
    def __init__(self):
        self.normalized_schema: Optional[SchemaResponse] = None
        self.vector_index: Optional[Any] = None
        self.vector_metadata: Optional[Any] = None
        self.current_connection: Optional[Dict] = None
        self.db_pool: Optional[Any] = None
        self.pool_signature: Optional[str] = None
        self.chat_history: List[Dict] = []
        self.last_activity: datetime.datetime = datetime.datetime.now()
        self.is_connecting: bool = False

sessions: Dict[str, SessionState] = {}
solo_session_llm_cache: Dict[str, Dict[str, Any]] = {}

def get_session(session_id: str) -> SessionState:
    if not session_id:
        session_id = "default"
    if session_id not in sessions:
        logger.info(f"🚀 INITIALIZING NEW SESSION: {session_id} (Reason: Session not found in memory)")
        sessions[session_id] = SessionState()
    else:
        logger.debug(f"Retrieved existing session: {session_id}")
    return sessions[session_id]


def close_session_db_pool(state: SessionState):
    """Close any pooled DB resources associated with a session."""
    pool = getattr(state, "db_pool", None)
    if pool is not None:
        try:
            pool.closeall()
            logger.info("Closed session database connection pool.")
        except Exception as e:
            logger.warning(f"Failed to close session database pool cleanly: {e}")
        finally:
            state.db_pool = None
            state.pool_signature = None

async def connect_to_db(session_id: str, conn_req: DBConnectionRequest):
    state = get_session(session_id)
    logger.info(f"Session {session_id} connecting to DB: host={conn_req.host}, port={conn_req.port}, db={conn_req.database}, user={conn_req.user}")

    try:
        logger.info("Introspecting database...")
        loop = asyncio.get_event_loop()
        if conn_req.engine == "postgres":
            tables = await loop.run_in_executor(None, introspect_postgres, conn_req)
        elif conn_req.engine == "mysql":
            from app.schema_service.introspect.mysql import introspect_mysql
            tables = await loop.run_in_executor(None, introspect_mysql, conn_req)
        elif conn_req.engine == "sqlserver":
            from app.schema_service.introspect.sqlserver import introspect_sqlserver
            tables = await loop.run_in_executor(None, introspect_sqlserver, conn_req)
        else:
            raise ValueError(f"Unsupported engine: {conn_req.engine}")

        raw_schema = SchemaResponse(
            engine=conn_req.engine,
            database=conn_req.database,
            tables=tables
        )
        table_names = [t.table for t in raw_schema.tables]
        logger.info(f"✅ DATABASE CONNECTED: {conn_req.database} ({conn_req.engine})")
        logger.info(f"📊 INTROSPECTION COMPLETE: Found {len(table_names)} tables: {', '.join(table_names)}")

        state.is_connecting = True
        logger.info("Normalizing schema...")
        state.normalized_schema = normalize_schema(raw_schema)
        close_session_db_pool(state)
        
        # KEY FIX: Update metadata BEFORE the long-running index build 
        # to prevent metadata/schema mismatch if a second request hits
        state.current_connection = {
            "engine": conn_req.engine,
            "host": conn_req.host,
            "port": conn_req.port,
            "database": conn_req.database,
            "user": conn_req.user,
            "password": conn_req.password,
            "connected": True
        }

        import json
        import os
        metrics_path = os.path.join(os.path.dirname(__file__), "schema_service", "metrics_config.json")
        if os.path.exists(metrics_path):
            try:
                with open(metrics_path, 'r') as f:
                    metrics_data = json.load(f)
                    from app.schema_service.models import MetricDefinition
                    available_tables = {t.table for t in state.normalized_schema.tables}
                    valid_metrics = []
                    for m in metrics_data:
                        req_tables = set(m.get("required_tables", []))
                        if req_tables.issubset(available_tables):
                            valid_metrics.append(MetricDefinition(**m))
                    state.normalized_schema.metrics = valid_metrics
                    logger.info(f"Successfully loaded {len(state.normalized_schema.metrics)} business metrics.")
            except Exception as me:
                logger.error(f"Failed to load metrics glossary: {me}")
        
        logger.info("Building vector index...")
        # build_vector_index is CPU intensive and triggers heavy imports, run in executor
        state.vector_index, state.vector_metadata = await loop.run_in_executor(
            None, build_vector_index, state.normalized_schema
        )
        
        logger.info(f"Session {session_id} connected successfully.")
        update_activity(session_id)
        return state.normalized_schema

    except Exception as e:
        logger.exception(f"Session {session_id} failed to connect")
        # Clear metadata on failure to avoid stale/mixed state
        state.current_connection = None
        state.normalized_schema = None
        raise e
    finally:
        state.is_connecting = False

def disconnect_db(session_id: str):
    if session_id in sessions:
        close_session_db_pool(sessions[session_id])
        del sessions[session_id]
        logger.info(f"Session {session_id} disconnected and state cleared.")

def update_activity(session_id: str):
    state = get_session(session_id)
    old_time = state.last_activity
    state.last_activity = datetime.datetime.now()
    logger.debug(f"Updated activity for session {session_id}. Last was {old_time}")

def check_and_disconnect():
    now = datetime.datetime.now()
    to_delete = []
    for sid, state in sessions.items():
        elapsed = (now - state.last_activity).total_seconds()
        if elapsed > IDLE_TIMEOUT:
            to_delete.append(sid)
    for sid in to_delete:
        disconnect_db(sid)
