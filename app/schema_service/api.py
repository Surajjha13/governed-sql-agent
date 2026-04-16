from fastapi import APIRouter, HTTPException, Header, Depends, Query
from typing import Optional
from app.schema_service.models import DBConnectionRequest, SchemaResponse
from app.schema_service.introspect.postgres import introspect_postgres
from app.schema_service.introspect.mysql import introspect_mysql
from app.schema_service.normalize import normalize_schema
from app.auth.api import get_current_user
from app.auth.policies import filter_schema_for_user
import app.app_state as app_state
import logging

router = APIRouter()
logger = logging.getLogger(__name__)

# Roles allowed to request unfiltered schema (for RBAC editor)
_ADMIN_ROLES = {"SUPER_ADMIN", "SYSTEM_ADMIN"}


@router.post("/schema", response_model=SchemaResponse)
def discover_schema(conn: DBConnectionRequest):
    """
    User provides DB credentials.
    Backend returns schema ONLY.
    Credentials are NOT stored or forwarded.
    (This remains stateless)
    """
    try:
        if conn.engine == "postgres":
            tables = introspect_postgres(conn)
        elif conn.engine == "mysql":
            tables = introspect_mysql(conn)
        else:
            raise ValueError("Unsupported engine")

        raw_schema = SchemaResponse(
            engine=conn.engine,
            database=conn.database,
            tables=tables
        )

        normalized_schema = normalize_schema(raw_schema)
        return normalized_schema

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/active-schema", response_model=SchemaResponse)
def get_current_schema(
    x_session_id: Optional[str] = Header(None),
    unfiltered: bool = Query(False, description="Admin-only: skip RBAC filtering for the RBAC editor"),
    current_user = Depends(get_current_user)
):
    """
    Get the currently active normalized schema from app state for the session.
    Pass ?unfiltered=true (admin only) to get the full schema for RBAC configuration.
    """
    session_id = f"{current_user.username}_{x_session_id or 'default'}"
    logger.info(f"GET /active-schema called for session {session_id}")
    
    state = app_state.get_session(session_id)
    if state.normalized_schema:
        # Admin requesting unfiltered schema for RBAC editor
        if unfiltered and current_user.role in _ADMIN_ROLES:
            logger.info(
                f"Returning UNFILTERED schema for admin {current_user.username} "
                f"({state.normalized_schema.database}, {len(state.normalized_schema.tables)} tables)"
            )
            return state.normalized_schema

        filtered_schema = filter_schema_for_user(state.normalized_schema, current_user.username)
        logger.info(
            f"Returning RBAC-filtered schema for {filtered_schema.database} "
            f"with {len(filtered_schema.tables)} tables (user={current_user.username})"
        )
        return filtered_schema
    else:
        logger.warning(f"No active schema found for session {session_id}")
        raise HTTPException(status_code=404, detail="No active schema found. Please connect to a database first.")
