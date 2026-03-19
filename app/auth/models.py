from sqlalchemy import Column, Integer, String, Text, Float, Boolean
from app.auth.database import Base

class UserModel(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, unique=True, index=True)
    password_hash = Column(String)
    salt = Column(String)
    role = Column(String)
    token = Column(String, index=True, nullable=True)
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
    username = Column(String, index=True)
    role = Column(String)
    db_name = Column(String, index=True, nullable=True)
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
    timestamp = Column(String, index=True)


class AdminActionLogModel(Base):
    __tablename__ = "admin_action_logs"

    id = Column(Integer, primary_key=True, index=True)
    admin_username = Column(String, index=True)
    action_type = Column(String, index=True)
    target_type = Column(String, nullable=True)
    target_name = Column(String, nullable=True)
    details_json = Column(Text, nullable=True)
    timestamp = Column(String, index=True)


class SecurityEventModel(Base):
    __tablename__ = "security_events"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String, index=True, nullable=True)
    role = Column(String, nullable=True)
    event_type = Column(String, index=True)
    severity = Column(String, nullable=True)
    event_source = Column(String, nullable=True)
    resource_name = Column(String, nullable=True)
    details_json = Column(Text, nullable=True)
    timestamp = Column(String, index=True)
