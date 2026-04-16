from fastapi import APIRouter, HTTPException, Body, Header, Depends, BackgroundTasks
from typing import Optional
from app.schema_service.models import DBConnectionRequest, SchemaResponse
import app.app_state as app_state
import logging
import os
from app.auth.api import get_current_user
from app.auth.user_manager import user_manager

router = APIRouter()
logger = logging.getLogger(__name__)

@router.post("/connect", response_model=SchemaResponse)
async def connect_database(
    conn: DBConnectionRequest,
    background_tasks: BackgroundTasks,
    x_session_id: Optional[str] = Header(None),
    current_user = Depends(get_current_user)
):
    session_id = f"{current_user.username}_{x_session_id or 'default'}"
    
    is_docker = os.path.exists('/.dockerenv') or os.getenv('IS_DOCKER', '').lower() == 'true'
    if is_docker and conn.host in ["localhost", "127.0.0.1"]:
        logger.info(f"Remapping {conn.host} -> host.docker.internal (Docker environment detected)")
        conn.host = "host.docker.internal"

    try:
        logger.info(f"Session {session_id} connection request for {conn.database} by {current_user.username}")
        schema = await app_state.connect_to_db(session_id, conn)
        
        # Phase 2: Start AI indexing in background
        background_tasks.add_task(app_state.build_semantic_index_background, session_id)
        
        from app.auth.policies import filter_schema_for_user
        filtered_schema = filter_schema_for_user(schema, current_user.username)
        
        # PERSIST CONNECTION metadata — skip for ephemeral Solo sessions
        if current_user.role != "SOLO_USER":
            user_manager.update_last_connection(current_user.username, conn.model_dump())
        
        return filtered_schema
    except Exception as e:
        logger.error(f"Connection failed for session {session_id}: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Failed to connect: {str(e)}")

@router.post("/disconnect")
async def disconnect_database(
    x_session_id: Optional[str] = Header(None),
    current_user = Depends(get_current_user)
):
    session_id = f"{current_user.username}_{x_session_id or 'default'}"
    try:
        app_state.disconnect_db(session_id)
        
        # NEW: Clear persistent connection metadata on explicit disconnect
        user_manager.update_last_connection(current_user.username, None)
        
        if x_session_id:
            from app.auth.api import solo_session_llm_cache
            if x_session_id in solo_session_llm_cache:
                del solo_session_llm_cache[x_session_id]
                logger.info(f"Cleared Solo BYOK cache for session {x_session_id}")
                
        return {"message": f"Session {session_id} disconnected successfully by {current_user.username}"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/status")
async def connection_status(
    x_session_id: Optional[str] = Header(None),
    current_user = Depends(get_current_user)
):
    session_id = f"{current_user.username}_{x_session_id or 'default'}"
    state = app_state.get_session(session_id)
    
    if state.current_connection:
        app_state.update_activity(session_id)
        safe_conn = dict(state.current_connection)
        if "password" in safe_conn:
            safe_conn["password"] = "****"
        safe_conn["is_indexing"] = state.is_indexing
        return safe_conn
    
    # NEW: Prevent redundant restoration if already in progress
    if state.is_connecting:
        logger.info(f"Session {session_id} is already connecting/restoring. Waiting...")
        return {"connected": False, "message": "Connection restoration in progress...", "restoring": True}
    
    # NEW: Try to restore from database if memory is empty
    # Skip restoration for Solo Users to keep sessions ephemeral and isolated
    if current_user.role == "SOLO_USER":
        return {"connected": False, "message": f"No active connection for session {session_id}"}

    last_conn = user_manager.get_last_connection(current_user.username)
    if last_conn:
        if not last_conn.get("password"):
            return {
                "connected": False,
                "message": "Saved connection metadata found, but the password is not stored. Please reconnect to restore access."
            }
        try:
            logger.info(f"Attempting to restore persistent connection for {current_user.username}...")
            from app.schema_service.models import DBConnectionRequest
            conn_req = DBConnectionRequest(**last_conn)
            
            # Special handling for Docker host remapping
            is_docker = os.path.exists('/.dockerenv') or os.getenv('IS_DOCKER', '').lower() == 'true'
            if is_docker and conn_req.host in ["localhost", "127.0.0.1"]:
                 conn_req.host = "host.docker.internal"
                 
            # Background indexing is not supported for auto-restoration yet, 
            # but we can add it if needed. For now, just connect.
            await app_state.connect_to_db(session_id, conn_req)
            
            # Return restored status
            state = app_state.get_session(session_id)
            safe_conn = dict(state.current_connection)
            if "password" in safe_conn:
                safe_conn["password"] = "****"
            return safe_conn
        except Exception as e:
            logger.error(f"Failed to auto-restore connection: {e}")
            return {"connected": False, "message": "Failed to auto-restore connection."}
    
    return {"connected": False, "message": f"No active connection for session {session_id}"}
