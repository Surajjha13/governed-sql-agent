from sqlalchemy import Column, Integer, String, Text, Float, Boolean, ForeignKey
from sqlalchemy.orm import relationship
from app.auth.database import Base


class AuthSessionModel(Base):
    """
    One row per active login session.

    Identified by the JWT ``jti`` (UUID4) claim so that multiple concurrent
    sessions for the same user each have an independent lifecycle.
    Logout sets ``revoked_at`` on *this row only* — sibling sessions are
    completely unaffected.
    """
    __tablename__ = "auth_sessions"

    id              = Column(Integer, primary_key=True, index=True)
    user_id         = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    jti             = Column(String, unique=True, index=True, nullable=False)
    token_hash      = Column(String, nullable=False)          # SHA-256 of raw JWT
    created_at      = Column(String, nullable=False)          # ISO-8601
    expires_at      = Column(String, nullable=False)          # ISO-8601
    revoked_at      = Column(String, nullable=True)           # set on logout
    last_seen_at    = Column(String, nullable=True)           # updated per request
    device_label    = Column(String, nullable=True)           # e.g. "Chrome/Windows"
    enterprise_id   = Column(Integer, nullable=True, index=True)
    owner_admin_id  = Column(Integer, nullable=True)

class EnterpriseModel(Base):
    __tablename__ = "enterprises"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True)
    created_by_id = Column(Integer, nullable=True) # User ID of Super Admin
    status = Column(String, default="active") # active, suspended, deleted
    is_active = Column(Boolean, default=True)
    created_at = Column(String) # ISO string

class UserModel(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True)
    password_hash = Column(String)
    salt = Column(String)
    role = Column(String) # 'super_admin', 'admin', 'user'
    enterprise_id = Column(Integer, ForeignKey("enterprises.id"), nullable=True)
    owner_admin_id = Column(Integer, nullable=True) # The admin who "owns" visibility for this user
    created_by_id = Column(Integer, nullable=True) # Audit trail: who physically created the record
    is_active = Column(Boolean, default=True)
    token = Column(String, index=True, nullable=True) # Can be deprecated in favor of JWT
    token_expires_at = Column(String, nullable=True)
    llm_config = Column(Text, nullable=True) # JSON stored as text
    last_connection_json = Column(Text, nullable=True) # JSON stored as text

class RoleModel(Base):
    __tablename__ = "roles"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True)
    description = Column(String, nullable=True)

class AuditLogModel(Base):
    __tablename__ = "audit_logs"

    id = Column(Integer, primary_key=True, index=True)
    enterprise_id = Column(Integer, index=True, nullable=True)
    owner_admin_id = Column(Integer, index=True, nullable=True)
    username = Column(String, index=True)
    role = Column(String)
    question = Column(Text)
    sql_query = Column(Text)
    latency_sec = Column(Float, nullable=True)
    success = Column(Boolean, default=True)
    timestamp = Column(String) # For simplicity, ISO string

class ChatHistoryModel(Base):
    __tablename__ = "chat_history"

    id = Column(Integer, primary_key=True, index=True)
    request_id = Column(String, index=True, nullable=True) # Linked to frontend's timestamp-based ID
    enterprise_id = Column(Integer, index=True, nullable=True)
    owner_admin_id = Column(Integer, index=True, nullable=True)
    username = Column(String, index=True)
    db_name = Column(String, index=True)
    question = Column(Text)
    sql_query = Column(Text)
    summary = Column(Text)
    results_json = Column(Text) # JSON stored as string
    visualization_json = Column(Text, nullable=True) # Recommended visualization
    timestamp = Column(String)


class ObservabilityEventModel(Base):
    __tablename__ = "observability_events"

    id = Column(Integer, primary_key=True, index=True)
    request_id = Column(String, index=True, nullable=True)
    enterprise_id = Column(Integer, index=True, nullable=True)
    owner_admin_id = Column(Integer, index=True, nullable=True)
    username = Column(String, index=True)
    role = Column(String)
    db_name = Column(String, index=True, nullable=True)
    question = Column(Text, nullable=True)
    sql_query = Column(Text, nullable=True)
    llm_provider = Column(String, nullable=True)
    llm_model = Column(String, nullable=True)
    sql_gen_ms = Column(Float, nullable=True)
    db_exec_ms = Column(Float, nullable=True)
    summary_ms = Column(Float, nullable=True)
    viz_ms = Column(Float, nullable=True)
    total_ms = Column(Float, nullable=True)
    success = Column(Boolean, default=True)
    had_rate_limit = Column(Boolean, default=False)
    rate_limit_stage = Column(String, nullable=True)
    error_stage = Column(String, nullable=True)
    error_message = Column(Text, nullable=True)
    timestamp = Column(String, index=True)


class AdminActionLogModel(Base):
    __tablename__ = "admin_action_logs"

    id = Column(Integer, primary_key=True, index=True)
    enterprise_id = Column(Integer, index=True, nullable=True)
    owner_admin_id = Column(Integer, index=True, nullable=True)
    admin_username = Column(String, index=True)
    action_type = Column(String, index=True)
    target_type = Column(String, nullable=True)
    target_name = Column(String, nullable=True)
    details_json = Column(Text, nullable=True)
    timestamp = Column(String, index=True)


class SecurityEventModel(Base):
    __tablename__ = "security_events"

    id = Column(Integer, primary_key=True, index=True)
    enterprise_id = Column(Integer, index=True, nullable=True)
    owner_admin_id = Column(Integer, index=True, nullable=True)
    username = Column(String, index=True, nullable=True)
    role = Column(String, nullable=True)
    event_type = Column(String, index=True)
    severity = Column(String, nullable=True)
    event_source = Column(String, nullable=True)
    resource_name = Column(String, nullable=True)
    details_json = Column(Text, nullable=True)
    timestamp = Column(String, index=True)
