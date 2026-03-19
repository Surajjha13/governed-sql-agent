from fastapi import APIRouter, HTTPException, Depends, Header
from pydantic import BaseModel
from typing import Optional, Dict, Any, List
import logging
import asyncio
from app.auth.user_manager import user_manager, User
from app.auth.policies import load_policies
from app import app_state

logger = logging.getLogger(__name__)

router = APIRouter()


# ── Request Models ────────────────────────────────────────────────────────────

class LoginRequest(BaseModel):
    username: str
    password: str

class CreateUserRequest(BaseModel):
    username: str
    password: str
    role: str

class ResetPasswordRequest(BaseModel):
    new_password: str

class ChangePasswordRequest(BaseModel):
    old_password: str
    new_password: str

class LLMConfigRequest(BaseModel):
    active_provider: str
    providers: Dict[str, Any]

class RBACRequest(BaseModel):
    blocked_tables: List[str] = []
    blocked_columns: List[str] = []

class RoleRequest(BaseModel):
    name: str
    description: str = ""


# ── Auth Dependencies ─────────────────────────────────────────────────────────

def get_current_user(x_auth_token: Optional[str] = Header(None)) -> User:
    # Solo Mode: Virtual User
    if app_state.SYSTEM_MODE == "solo":
        return User(
            username="solo_user", 
            password_hash="", 
            salt="", 
            role="SOLO_USER", 
            token="standalone-token"
        )
        
    if not x_auth_token:
        raise HTTPException(status_code=401, detail="Authentication token required")
        
    if x_auth_token == "standalone-token":
        return User(username="solo_user", password_hash="", salt="", role="SOLO_USER", token=x_auth_token)
    user = user_manager.get_user_by_token(x_auth_token)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid or expired authentication token")
    return user

def require_admin(current_user: User = Depends(get_current_user)):
    if current_user.role != "SYSTEM_ADMIN":
        raise HTTPException(status_code=403, detail="Administrator access required")
    return current_user


# ── Auth Endpoints ────────────────────────────────────────────────────────────

@router.post("/login")
async def login(req: LoginRequest):
    user = user_manager.authenticate(req.username, req.password)
    if not user:
        user_manager.record_login_failure(req.username)
        raise HTTPException(status_code=401, detail="Invalid username or password")
    policies = load_policies()
    role_label = policies.get("role_labels", {}).get(user.role, user.role.replace("_", " ").title())
    return {
        "message": "Login successful",
        "token": user.token,
        "user": {"username": user.username, "role": user.role},
        "role_label": role_label
    }

@router.post("/logout")
async def logout(
    x_session_id: Optional[str] = Header(None),
    current_user: User = Depends(get_current_user)
):
    session_id = f"{current_user.username}_{x_session_id or 'default'}"
    try:
        app_state.disconnect_db(session_id)
    except Exception as e:
        logger.warning(f"Failed to disconnect session during logout for {session_id}: {e}")

    if current_user.role != "SOLO_USER":
        user_manager.logout(current_user.username)

    return {"message": "Logout successful"}

@router.get("/me")
async def get_me(current_user: User = Depends(get_current_user)):
    return {"username": current_user.username, "role": current_user.role}

@router.get("/config")
async def get_system_config():
    """Initial config for frontend to determine mode and login requirements."""
    return {
        "status": "ok", 
        "auth_required": app_state.SYSTEM_MODE == "enterprise",
        "mode": app_state.SYSTEM_MODE
    }


# ── User Management (Admin Only) ──────────────────────────────────────────────

@router.get("/users")
async def list_users(current_user: User = Depends(require_admin)):
    return user_manager.list_users()

@router.post("/users")
async def create_user(req: CreateUserRequest, current_user: User = Depends(require_admin)):
    # Validate role exists
    roles = user_manager.list_roles()
    if not any(r["name"] == req.role for r in roles):
        raise HTTPException(status_code=400, detail=f"Invalid role: {req.role}")
    success = user_manager.create_user(req.username, req.password, req.role)
    if not success:
        raise HTTPException(status_code=409, detail="Username already exists")
    user_manager.log_admin_action(
        admin_username=current_user.username,
        action_type="create_user",
        target_type="user",
        target_name=req.username,
        details={"role": req.role}
    )
    return {"message": f"User '{req.username}' created successfully"}

@router.delete("/users/{username}")
async def delete_user(username: str, current_user: User = Depends(require_admin)):
    if username == "admin":
        raise HTTPException(status_code=400, detail="Cannot delete the default admin user")
    success = user_manager.delete_user(username)
    if not success:
        raise HTTPException(status_code=404, detail="User not found")
    user_manager.log_admin_action(
        admin_username=current_user.username,
        action_type="delete_user",
        target_type="user",
        target_name=username
    )
    return {"message": f"User '{username}' deleted"}

@router.post("/users/{username}/reset-password")
async def reset_password(username: str, req: ResetPasswordRequest, current_user: User = Depends(require_admin)):
    success = user_manager.reset_password(username, req.new_password)
    if not success:
        raise HTTPException(status_code=404, detail="User not found")
    user_manager.log_admin_action(
        admin_username=current_user.username,
        action_type="reset_password",
        target_type="user",
        target_name=username
    )
    return {"message": f"Password reset for '{username}'"}

@router.post("/change-password")
async def change_password(req: ChangePasswordRequest, current_user: User = Depends(get_current_user)):
    success = user_manager.change_password(current_user.username, req.old_password, req.new_password)
    if not success:
        user_manager.log_security_event(
            event_type="login_failure",
            severity="medium",
            username=current_user.username,
            role=current_user.role,
            event_source="change_password",
            details={"reason": "incorrect_current_password"}
        )
        raise HTTPException(status_code=400, detail="Current password is incorrect")
    return {"message": "Password changed successfully"}


# ── RBAC Management (Admin Only) ──────────────────────────────────────────────

@router.get("/rbac/{username}")
async def get_rbac(username: str, current_user: User = Depends(require_admin)):
    rbac = user_manager.get_user_rbac(username)
    return {"username": username, **rbac}

@router.post("/rbac/{username}")
async def update_rbac(username: str, req: RBACRequest, current_user: User = Depends(require_admin)):
    # Verify user exists
    users = user_manager.list_users()
    if not any(u["username"] == username for u in users):
        raise HTTPException(status_code=404, detail="User not found")
    success = user_manager.update_user_rbac(username, req.blocked_tables, req.blocked_columns)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to update RBAC policy")
    user_manager.log_admin_action(
        admin_username=current_user.username,
        action_type="update_user_rbac",
        target_type="user",
        target_name=username,
        details={
            "blocked_tables_count": len(req.blocked_tables),
            "blocked_columns_count": len(req.blocked_columns)
        }
    )
    return {"message": f"RBAC updated for '{username}'"}

@router.get("/rbac/role/{role_name}")
async def get_role_rbac(role_name: str, current_user: User = Depends(require_admin)):
    rbac = user_manager.get_role_rbac(role_name)
    return {"role_name": role_name, **rbac}

@router.post("/rbac/role/{role_name}")
async def update_role_rbac(role_name: str, req: RBACRequest, current_user: User = Depends(require_admin)):
    # Verify role exists
    roles = user_manager.list_roles()
    if not any(r["name"] == role_name for r in roles):
        raise HTTPException(status_code=404, detail="Role not found")
    success = user_manager.update_role_rbac(role_name, req.blocked_tables, req.blocked_columns)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to update RBAC policy")
    user_manager.log_admin_action(
        admin_username=current_user.username,
        action_type="update_role_rbac",
        target_type="role",
        target_name=role_name,
        details={
            "blocked_tables_count": len(req.blocked_tables),
            "blocked_columns_count": len(req.blocked_columns)
        }
    )
    return {"message": f"RBAC updated for role '{role_name}'"}


# ── Role Management (Admin Only) ──────────────────────────────────────────────

@router.get("/roles")
async def list_roles(current_user: User = Depends(require_admin)):
    return user_manager.list_roles()

@router.post("/roles")
async def create_role(req: RoleRequest, current_user: User = Depends(require_admin)):
    success = user_manager.create_role(req.name, req.description)
    if not success:
        raise HTTPException(status_code=409, detail="Role already exists")
    user_manager.log_admin_action(
        admin_username=current_user.username,
        action_type="create_role",
        target_type="role",
        target_name=req.name,
        details={"description": req.description}
    )
    return {"message": f"Role '{req.name}' created successfully"}

@router.delete("/roles/{name}")
async def delete_role(name: str, current_user: User = Depends(require_admin)):
    if name == "SYSTEM_ADMIN":
        raise HTTPException(status_code=400, detail="Cannot delete SYSTEM_ADMIN role")
    success = user_manager.delete_role(name)
    if not success:
        raise HTTPException(status_code=404, detail="Role not found")
    user_manager.log_admin_action(
        admin_username=current_user.username,
        action_type="delete_role",
        target_type="role",
        target_name=name
    )
    return {"message": f"Role '{name}' deleted"}


# ── LLM Config ────────────────────────────────────────────────────────────────

@router.get("/llm-config")
async def get_llm_config(
    x_session_id: Optional[str] = Header(None),
    current_user: User = Depends(get_current_user)
):
    if x_session_id and current_user.token == "standalone-token":
        return app_state.solo_session_llm_cache.get(x_session_id, {"active_provider": "groq", "providers": {}})
    return user_manager.get_llm_config(current_user.username)

@router.post("/llm-config")
async def update_llm_config(
    req: LLMConfigRequest,
    x_session_id: Optional[str] = Header(None),
    current_user: User = Depends(get_current_user)
):
    if x_session_id and current_user.token == "standalone-token":
        app_state.solo_session_llm_cache[x_session_id] = {"active_provider": req.active_provider, "providers": req.providers}
        return {"message": "Solo session LLM config updated"}
    success = user_manager.update_llm_config(current_user.username, req.active_provider, req.providers)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to update LLM configuration")
    if current_user.role == "SYSTEM_ADMIN":
        user_manager.log_admin_action(
            admin_username=current_user.username,
            action_type="update_llm_config",
            target_type="llm_config",
            target_name=current_user.username,
            details={"active_provider": req.active_provider, "providers": list(req.providers.keys())}
        )
    return {"message": "LLM configuration updated"}

@router.post("/test-llm")
async def test_llm(
    x_session_id: Optional[str] = Header(None),
    current_user: User = Depends(get_current_user)
):
    """Verifies the current LLM configuration by making a simple request."""
    from app.llm_service.llm_service import generate_summary
    from app.llm_service.exceptions import LLMRateLimitError
    
    sid = x_session_id or "test_session"
    try:
        # Simple test request
        await generate_summary("Is the system working?", {"status": "testing"}, session_id=sid)
        return {"status": "success", "message": "Connection successful"}
    except LLMRateLimitError as e:
        return {
            "status": "rate_limit", 
            "message": str(e), 
            "recommendations": e.recommendations
        }
    except Exception as e:
        return {"status": "error", "message": str(e)}

@router.get("/list-models")
async def list_models(
    provider: str,
    base_url: Optional[str] = None,
    current_user: User = Depends(get_current_user)
):
    """Dynamically lists models for a specific provider."""
    from app.llm_service.llm_service import get_model_recommendations
    
    # Get config to find the API key
    cfg = user_manager.get_llm_config(current_user.username)
    provider_cfg = cfg.get("providers", {}).get(provider.lower(), {})
    api_key = provider_cfg.get("api_key")
    
    if not api_key:
        # Fallback to hardcoded recommendations if no key provided
        recs = await get_model_recommendations(provider, "")
        return {"models": recs, "source": "fallback"}
        
    try:
        from app.llm_service.llm_adapters import get_adapter
        adapter = get_adapter(provider)
        models = await adapter.list_models(api_key, base_url or provider_cfg.get("base_url"))
        return {"models": models, "source": "api"}
    except Exception as e:
        logger.warning(f"Failed to list models for {provider}: {e}")
        # Final fallback
        recs = await get_model_recommendations(provider, "")
        return {"models": recs, "source": "error_fallback"}

# ── Audit Logs (Admin Only) ───────────────────────────────────────────────────

@router.get("/audit-logs")
async def get_audit_logs(current_user: User = Depends(require_admin)):
    return user_manager.list_audit_logs(limit=100)

@router.get("/system-stats")
async def get_system_stats(current_user: User = Depends(require_admin)):
    return user_manager.get_system_stats()


@router.get("/observability")
async def get_observability(current_user: User = Depends(require_admin)):
    return user_manager.get_observability_overview()


@router.get("/admin-actions")
async def get_admin_actions(current_user: User = Depends(require_admin)):
    return user_manager.list_admin_action_logs(limit=100)


@router.get("/security-events")
async def get_security_events(current_user: User = Depends(require_admin)):
    return user_manager.list_security_events(limit=100)


@router.get("/policy-analytics")
async def get_policy_analytics(current_user: User = Depends(require_admin)):
    return user_manager.get_policy_violation_analytics()


@router.get("/slo-panel")
async def get_slo_panel(current_user: User = Depends(require_admin)):
    return user_manager.get_slo_panel()
