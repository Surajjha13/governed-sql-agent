import os
import ipaddress
from urllib.parse import urlparse

from fastapi import APIRouter, HTTPException, Depends, Header, Response, Request
from pydantic import BaseModel, field_validator
from typing import Optional, Dict, Any, List
import logging
import asyncio
from app.auth.user_manager import user_manager, User
from app.auth.policies import load_policies
from app import app_state

logger = logging.getLogger(__name__)
ALLOW_HEADER_AUTH = os.getenv("ALLOW_HEADER_AUTH", "false").lower() == "true"
COOKIE_SECURE_MODE = os.getenv("AUTH_COOKIE_SECURE", "auto").lower()

router = APIRouter()


def validate_provider_base_url(provider: str, base_url: Optional[str]) -> Optional[str]:
    if not base_url:
        return None

    parsed = urlparse(base_url)
    if parsed.scheme not in {"https"}:
        raise ValueError("Custom provider URLs must use HTTPS.")

    hostname = parsed.hostname
    if not hostname:
        raise ValueError("Custom provider URL must include a valid hostname.")

    lowered_host = hostname.lower()
    if lowered_host in {"localhost"} or lowered_host.endswith(".local"):
        raise ValueError("Local provider URLs are not allowed.")

    try:
        ip = ipaddress.ip_address(lowered_host)
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_multicast or ip.is_reserved:
            raise ValueError("Private or local provider IPs are not allowed.")
    except ValueError as exc:
        if "are not allowed" in str(exc):
            raise

    provider = (provider or "").lower()
    allow_custom = os.getenv("ALLOW_CUSTOM_LLM_BASE_URLS", "false").lower() == "true"
    approved_hosts = {
        "groq": ("api.groq.com",),
        "openai": ("api.openai.com",),
        "gemini": ("generativelanguage.googleapis.com",),
        "deepseek": ("api.deepseek.com",),
        "anthropic": ("api.anthropic.com",),
    }
    if provider != "custom":
        allowed = approved_hosts.get(provider, ())
        if allowed and lowered_host not in allowed:
            raise ValueError(f"Custom base URL is not allowed for provider '{provider}'.")
    elif not allow_custom:
        raise ValueError("Custom LLM provider URLs are disabled.")

    return base_url


# ── Request Models ────────────────────────────────────────────────────────────

class LoginRequest(BaseModel):
    username: str
    password: str

class CreateUserRequest(BaseModel):
    username: str
    password: str
    role: str
    enterprise_id: Optional[int] = None

    @field_validator("password")
    @classmethod
    def validate_password(cls, value: str) -> str:
        if len(value or "") < 12:
            raise ValueError("Password must be at least 12 characters long")
        return value

class ResetPasswordRequest(BaseModel):
    new_password: str

    @field_validator("new_password")
    @classmethod
    def validate_password(cls, value: str) -> str:
        if len(value or "") < 12:
            raise ValueError("Password must be at least 12 characters long")
        return value

class ChangePasswordRequest(BaseModel):
    old_password: str
    new_password: str

    @field_validator("new_password")
    @classmethod
    def validate_password(cls, value: str) -> str:
        if len(value or "") < 12:
            raise ValueError("Password must be at least 12 characters long")
        return value

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

def get_current_user(request: Request, x_auth_token: Optional[str] = Header(None)) -> User:
    # Solo Mode: Virtual User
    if app_state.SYSTEM_MODE == "solo":
        return User(
            username="solo_user", 
            password_hash="", 
            salt="", 
            role="SOLO_USER", 
            token="standalone-token"
        )

    # Prefer an explicit per-request token so concurrent admin windows do not
    # inherit whatever auth cookie was written most recently by the browser.
    token = x_auth_token if x_auth_token else None
    if not token:
        token = request.cookies.get("sa_auth_token")
    if not token and ALLOW_HEADER_AUTH:
        token = x_auth_token
        
    if not token:
        raise HTTPException(status_code=401, detail="Authentication token required")
        
    if token == "standalone-token":
        return User(username="solo_user", password_hash="", salt="", role="SOLO_USER", token=token)
        
    user = user_manager.get_user_by_token(token)
    if not user:
        raise HTTPException(status_code=401, detail="Invalid or expired authentication token")
    return user

def require_admin(current_user: User = Depends(get_current_user)):
    if current_user.role not in ["SUPER_ADMIN", "SYSTEM_ADMIN"]:
        raise HTTPException(status_code=403, detail="Administrator access required")
    return current_user

def require_super_admin(current_user: User = Depends(get_current_user)):
    if current_user.role != "SUPER_ADMIN":
        raise HTTPException(status_code=403, detail="Super Admin access required")
    return current_user


# ── Auth Endpoints ────────────────────────────────────────────────────────────

@router.post("/login")
async def login(req: LoginRequest, response: Response, request: Request):
    device_label = request.headers.get("User-Agent", "")[:200] or None
    user = user_manager.authenticate(req.username, req.password, device_label=device_label)
    if not user:
        user_manager.record_login_failure(req.username)
        raise HTTPException(status_code=401, detail="Invalid username or password")
    policies = load_policies()
    role_label = policies.get("role_labels", {}).get(user.role, user.role.replace("_", " ").title())
    
    cookie_secure = COOKIE_SECURE_MODE == "true" or (
        COOKIE_SECURE_MODE == "auto" and request.url.scheme == "https"
    )
    response.set_cookie(
        key="sa_auth_token",
        value=user.token,
        httponly=True,
        secure=cookie_secure,
        samesite="strict",
        max_age=604800
    )
    
    return {
        "message": "Login successful",
        "token": user.token,
        "user": {
            "id": user.id,
            "username": user.username, 
            "role": user.role,
            "enterprise_id": user.enterprise_id,
            "owner_admin_id": user.owner_admin_id
        },
        "role_label": role_label
    }

@router.post("/logout")
async def logout(
    response: Response,
    request: Request,
    x_session_id: Optional[str] = Header(None),
    current_user: User = Depends(get_current_user)
):
    session_id = f"{current_user.username}_{x_session_id or 'default'}"
    try:
        app_state.disconnect_db(session_id)
    except Exception as e:
        logger.warning(f"Failed to disconnect session during logout for {session_id}: {e}")

    if current_user.role != "SOLO_USER":
        user_manager.logout(current_user.token)

    cookie_secure = COOKIE_SECURE_MODE == "true" or (
        COOKIE_SECURE_MODE == "auto" and request.url.scheme == "https"
    )
    response.delete_cookie(key="sa_auth_token", httponly=True, secure=cookie_secure, samesite="strict")
    return {"message": "Logout successful"}

@router.get("/me")
async def get_me(current_user: User = Depends(get_current_user)):
    return {"username": current_user.username, "role": current_user.role}


# ── Session Management ────────────────────────────────────────────────────────

import jwt as _pyjwt  # alias to avoid shadowing the local `jwt` var

def _extract_jti(token: str) -> Optional[str]:
    """Extract jti from a token without full validation."""
    try:
        from app.auth.user_manager import MASTER_KEY_JWT
        payload = _pyjwt.decode(
            token, MASTER_KEY_JWT, algorithms=["HS256"],
            options={"verify_exp": False}
        )
        return payload.get("jti")
    except Exception:
        return None


@router.get("/sessions", summary="List my active sessions")
async def list_sessions(current_user: User = Depends(get_current_user)):
    """Returns all non-revoked, non-expired sessions for the calling user."""
    if current_user.role == "SOLO_USER":
        return []
    sessions = user_manager.list_sessions(current_user.id)
    current_jti = _extract_jti(current_user.token or "")
    for s in sessions:
        s["is_current"] = (s["jti"] == current_jti)
    return sessions


@router.delete("/sessions/others", summary="Revoke all other sessions")
async def revoke_other_sessions(current_user: User = Depends(get_current_user)):
    """Revokes every active session for the caller except the current one."""
    if current_user.role == "SOLO_USER":
        return {"revoked": 0}
    current_jti = _extract_jti(current_user.token or "")
    if not current_jti:
        raise HTTPException(status_code=400, detail="Current session has no jti (legacy token)")
    count = user_manager.revoke_all_other_sessions(current_jti, current_user.id)
    return {"revoked": count, "message": f"{count} other session(s) revoked"}


@router.delete("/sessions/{jti}", summary="Revoke a specific session")
async def revoke_session(
    jti: str,
    current_user: User = Depends(get_current_user)
):
    """Revoke one specific session by its jti. Only the owner may revoke it."""
    if current_user.role == "SOLO_USER":
        raise HTTPException(status_code=400, detail="Not applicable in solo mode")
    success = user_manager.revoke_session_by_jti(jti, current_user.id)
    if not success:
        raise HTTPException(status_code=404, detail="Session not found or already revoked")
    return {"message": f"Session {jti[:8]}… revoked"}

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
    return user_manager.list_users(current_user=current_user)

@router.post("/users")
async def create_user(req: CreateUserRequest, current_user: User = Depends(require_admin)):
    # Validate role exists
    roles = user_manager.list_roles()
    if not any(r["name"] == req.role for r in roles):
        raise HTTPException(status_code=400, detail=f"Invalid role: {req.role}")
    
    # Enforce scope: new users belong to the current admin's enterprise and owner
    enterprise_id = current_user.enterprise_id
    owner_admin_id = current_user.id
    
    # Super Admin can specify enterprise and create enterprise admins without an owner
    if current_user.role == "SUPER_ADMIN":
        if req.enterprise_id is not None:
            enterprise_id = req.enterprise_id
        if req.role == "SYSTEM_ADMIN":
            owner_admin_id = None
    
    success = user_manager.create_user(
        req.username, 
        req.password, 
        req.role,
        enterprise_id=enterprise_id,
        owner_admin_id=owner_admin_id,
        created_by_id=current_user.id
    )
    if not success:
        raise HTTPException(status_code=409, detail="Username already exists")
    user_manager.log_admin_action(
        admin_user=current_user,
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
    success = user_manager.delete_user(username, current_user=current_user)
    if not success:
        raise HTTPException(status_code=404, detail="User not found")
    user_manager.log_admin_action(
        admin_user=current_user,
        action_type="delete_user",
        target_type="user",
        target_name=username
    )
    return {"message": f"User '{username}' deleted"}

@router.post("/users/{username}/reset-password")
async def reset_password(username: str, req: ResetPasswordRequest, current_user: User = Depends(require_admin)):
    success = user_manager.reset_password(username, req.new_password, current_user=current_user)
    if not success:
        raise HTTPException(status_code=404, detail="User not found")
    user_manager.log_admin_action(
        admin_user=current_user,
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
            user=current_user,
            event_type="login_failure",
            severity="medium",
            event_source="change_password",
            details={"reason": "incorrect_current_password"}
        )
        raise HTTPException(status_code=400, detail="Current password is incorrect")
    return {"message": "Password changed successfully"}


# ── RBAC Management (Admin Only) ──────────────────────────────────────────────

@router.get("/rbac/{username}")
async def get_rbac(username: str, current_user: User = Depends(require_admin)):
    rbac = user_manager.get_user_rbac(username, current_user=current_user)
    if rbac is None:
        raise HTTPException(status_code=404, detail="User not found")
    return {"username": username, **rbac}

@router.post("/rbac/{username}")
async def update_rbac(username: str, req: RBACRequest, current_user: User = Depends(require_admin)):
    existing_rbac = user_manager.get_user_rbac(username, current_user=current_user)
    if existing_rbac is None:
        raise HTTPException(status_code=404, detail="User not found")
    success = user_manager.update_user_rbac(
        username,
        req.blocked_tables,
        req.blocked_columns,
        current_user=current_user
    )
    if not success:
        raise HTTPException(status_code=500, detail="Failed to update RBAC policy")
    user_manager.log_admin_action(
        admin_user=current_user,
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
        admin_user=current_user,
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
        admin_user=current_user,
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
        admin_user=current_user,
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
    sanitized_providers = {}
    for provider_name, provider_cfg in req.providers.items():
        safe_cfg = dict(provider_cfg or {})
        if "base_url" in safe_cfg:
            safe_cfg["base_url"] = validate_provider_base_url(provider_name, safe_cfg.get("base_url"))
        sanitized_providers[provider_name] = safe_cfg

    if x_session_id and current_user.token == "standalone-token":
        app_state.solo_session_llm_cache[x_session_id] = {"active_provider": req.active_provider, "providers": sanitized_providers}
        return {"message": "Solo session LLM config updated"}
    success = user_manager.update_llm_config(current_user.username, req.active_provider, sanitized_providers)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to update LLM configuration")
    if current_user.role in ["SUPER_ADMIN", "SYSTEM_ADMIN"]:
        user_manager.log_admin_action(
            admin_user=current_user,
            action_type="update_llm_config",
            target_type="llm_config",
            target_name=current_user.username,
            details={"active_provider": req.active_provider, "providers": list(sanitized_providers.keys())}
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
    
    sid = f"{current_user.username}_{x_session_id or 'test_session'}"
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
    return user_manager.list_audit_logs(current_user=current_user, limit=100)

@router.get("/system-stats")
async def get_system_stats(current_user: User = Depends(require_admin)):
    return user_manager.get_system_stats(current_user=current_user)


@router.get("/observability")
async def get_observability(current_user: User = Depends(require_admin)):
    return user_manager.get_observability_overview(current_user=current_user)


@router.get("/admin-actions")
async def get_admin_actions(current_user: User = Depends(require_admin)):
    return user_manager.list_admin_action_logs(current_user=current_user, limit=100)


@router.get("/security-events")
async def get_security_events(current_user: User = Depends(require_admin)):
    return user_manager.list_security_events(current_user=current_user, limit=100)


@router.get("/policy-analytics")
async def get_policy_analytics(current_user: User = Depends(require_admin)):
    return user_manager.get_policy_violation_analytics(current_user=current_user)


@router.get("/slo-panel")
async def get_slo_panel(current_user: User = Depends(require_admin)):
    return user_manager.get_slo_panel(current_user=current_user)

# ── Enterprise Management (Super Admin ONLY) ──────────────────────────────────
class CreateEnterpriseRequest(BaseModel):
    name: str

@router.get("/enterprises")
async def list_enterprises(current_user: User = Depends(require_super_admin)):
    return user_manager.list_enterprises()

@router.post("/enterprises")
async def create_enterprise(req: CreateEnterpriseRequest, current_user: User = Depends(require_super_admin)):
    ent_id = user_manager.create_enterprise(req.name, current_user.id)
    if not ent_id:
        raise HTTPException(status_code=409, detail="Enterprise name already exists")
    user_manager.log_admin_action(
        admin_user=current_user,
        action_type="create_enterprise",
        target_type="enterprise",
        target_name=req.name
    )
    return {"message": f"Enterprise '{req.name}' created", "id": ent_id}

class UpdateEnterpriseRequest(BaseModel):
    is_active: bool

@router.patch("/enterprises/{enterprise_id}")
async def update_enterprise(enterprise_id: int, req: UpdateEnterpriseRequest, current_user: User = Depends(require_super_admin)):
    success = user_manager.update_enterprise_status(enterprise_id, req.is_active)
    if not success:
        raise HTTPException(status_code=404, detail="Enterprise not found")
    user_manager.log_admin_action(
        admin_user=current_user,
        action_type="update_enterprise",
        target_type="enterprise",
        target_name=str(enterprise_id),
        details={"is_active": req.is_active}
    )
    return {"message": "Enterprise status updated"}

@router.delete("/enterprises/{enterprise_id}")
async def delete_enterprise(enterprise_id: int, current_user: User = Depends(require_super_admin)):
    deleted = user_manager.delete_enterprise(enterprise_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Enterprise not found")
    user_manager.log_admin_action(
        admin_user=current_user,
        action_type="delete_enterprise",
        target_type="enterprise",
        target_name=deleted["enterprise_name"],
        details={
            "enterprise_id": enterprise_id,
            "deleted_users": deleted["deleted_users"],
            "deleted_admins": deleted["deleted_admins"],
        }
    )
    return {
        "message": f"Enterprise '{deleted['enterprise_name']}' deleted",
        **deleted,
    }
