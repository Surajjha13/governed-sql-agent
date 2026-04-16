import hashlib
import secrets
import json
import logging
import os
import base64
import datetime
import uuid
import jwt
from typing import List, Optional, Dict, Any
from sqlalchemy import or_

try:
    from cryptography.fernet import Fernet
    _raw_key = os.getenv("MASTER_KEY") or os.getenv("MASTER_ENCRYPTION_KEY") or "insecure_default"
    MASTER_KEY_JWT = _raw_key
    if not _raw_key or _raw_key == "insecure_default_key_must_change":
        raise RuntimeError(
            "Missing encryption key. Set MASTER_KEY or MASTER_ENCRYPTION_KEY before starting the application."
        )
    _key_32 = _raw_key.ljust(32, '0')[:32].encode('utf-8')
    FERNET_KEY = base64.urlsafe_b64encode(_key_32)
    cipher_suite = Fernet(FERNET_KEY)

    def encrypt_data(data: str) -> str:
        return cipher_suite.encrypt(data.encode('utf-8')).decode('utf-8')

    def decrypt_data(data: str) -> str:
        try:
            return cipher_suite.decrypt(data.encode('utf-8')).decode('utf-8')
        except Exception:
            return data  # Fallback for unencrypted legacy rows
except ImportError as exc:
    raise RuntimeError(
        "The 'cryptography' package is required for secrets encryption. Install dependencies before starting the application."
    ) from exc
from app.auth.database import SessionLocal, engine, Base
from app.auth.models import (
    AuthSessionModel,
    UserModel,
    RoleModel,
    EnterpriseModel,
    AuditLogModel,
    ChatHistoryModel,
    ObservabilityEventModel,
    AdminActionLogModel,
    SecurityEventModel,
)
logger = logging.getLogger(__name__)
TOKEN_TTL_DAYS = int(os.getenv("AUTH_TOKEN_TTL_DAYS", "7"))
OBSERVABILITY_LOOKBACK_LIMIT = int(os.getenv("OBSERVABILITY_LOOKBACK_LIMIT", "500"))
OBSERVABILITY_ALERT_WINDOW_MINUTES = int(os.getenv("OBSERVABILITY_ALERT_WINDOW_MINUTES", "15"))
OBSERVABILITY_THRESHOLD_SQL_GEN_MS = float(os.getenv("OBSERVABILITY_THRESHOLD_SQL_GEN_MS", "6000"))
OBSERVABILITY_THRESHOLD_DB_EXEC_MS = float(os.getenv("OBSERVABILITY_THRESHOLD_DB_EXEC_MS", "4000"))
OBSERVABILITY_THRESHOLD_SUMMARY_MS = float(os.getenv("OBSERVABILITY_THRESHOLD_SUMMARY_MS", "5000"))
OBSERVABILITY_THRESHOLD_VIZ_MS = float(os.getenv("OBSERVABILITY_THRESHOLD_VIZ_MS", "3000"))
OBSERVABILITY_THRESHOLD_TOTAL_MS = float(os.getenv("OBSERVABILITY_THRESHOLD_TOTAL_MS", "10000"))
OBSERVABILITY_ALERT_429_COUNT = int(os.getenv("OBSERVABILITY_ALERT_429_COUNT", "3"))
OBSERVABILITY_ALERT_FAILURE_COUNT = int(os.getenv("OBSERVABILITY_ALERT_FAILURE_COUNT", "5"))
OBSERVABILITY_ALERT_LOGIN_FAILURE_COUNT = int(os.getenv("OBSERVABILITY_ALERT_LOGIN_FAILURE_COUNT", "5"))
OBSERVABILITY_ALERT_POLICY_DENIAL_COUNT = int(os.getenv("OBSERVABILITY_ALERT_POLICY_DENIAL_COUNT", "3"))

class User:
    def __init__(self, username, password_hash, salt, role, id=None, enterprise_id=None, owner_admin_id=None, token=None, token_expires_at=None, llm_config=None, last_connection=None):
        self.id = id
        self.username = username
        self.password_hash = password_hash
        self.salt = salt
        self.role = role
        self.enterprise_id = enterprise_id
        self.owner_admin_id = owner_admin_id
        self.token = token
        self.token_expires_at = token_expires_at
        self.llm_config = llm_config
        self.last_connection = last_connection


class UserManager:
    def __init__(self):
        Base.metadata.create_all(bind=engine)
        self._ensure_admin()

    def _ensure_admin(self):
        db = SessionLocal()
        try:
            from sqlalchemy import text

            # Run schema migrations before any ORM query touches newly added columns.
            # SKIP for PostgreSQL/Supabase as the schema is already managed by metadata.create_all()
            # and PG is strict about transaction aborts on failed ALTER TABLE.
            is_postgres = "postgresql" in str(engine.url).lower()
            
            if not is_postgres:
                migration_steps = [
                    (
                        "ALTER TABLE chat_history ADD COLUMN visualization_json TEXT;",
                        ("duplicate column name", "already exists"),
                        "Migrated chat_history to include visualization_json column",
                        "Chat history migration error",
                    ),
                    (
                        "ALTER TABLE users ADD COLUMN token_expires_at TEXT;",
                        ("duplicate column name", "already exists"),
                        "Migrated users to include token_expires_at column",
                        "Users table migration error",
                    ),
                    (
                        "ALTER TABLE users ADD COLUMN enterprise_id INTEGER;",
                        ("duplicate column name", "already exists"),
                        "Migrated users to include enterprise_id column",
                        "Column error",
                    ),
                    (
                        "ALTER TABLE users ADD COLUMN owner_admin_id INTEGER;",
                        ("duplicate column name", "already exists"),
                        "Migrated users to include owner_admin_id column",
                        "Column error",
                    ),
                    (
                        "ALTER TABLE users ADD COLUMN created_by_id INTEGER;",
                        ("duplicate column name", "already exists"),
                        "Migrated users to include created_by_id column",
                        "Column error",
                    ),
                    (
                        "ALTER TABLE users ADD COLUMN is_active BOOLEAN DEFAULT 1;",
                        ("duplicate column name", "already exists"),
                        "Migrated users to include is_active column",
                        "Column error",
                    ),
                ]
                
                # Add multi-tenant columns to all log tables
                log_tables = ["audit_logs", "chat_history", "observability_events", "admin_action_logs", "security_events"]
                for table in log_tables:
                    migration_steps.append((
                        f"ALTER TABLE {table} ADD COLUMN enterprise_id INTEGER;",
                        ("duplicate column name", "already exists"),
                        f"Migrated {table} to include enterprise_id", "Column error"
                    ))
                for migration_sql, duplicate_markers, success_message, error_prefix in migration_steps:
                    try:
                        db.execute(text(migration_sql))
                        db.commit()
                        logger.info(success_message)
                    except Exception as e:
                        db.rollback()
                        if any(marker in str(e).lower() for marker in duplicate_markers):
                            pass
                        else:
                            logger.warning(f"Note: {error_prefix}: {e}")
            else:
                logger.info("Supabase (PostgreSQL) detected: Core tables managed by metadata; checking for incremental columns.")

            # Universal migrations (Run on both SQLite and PostgreSQL)
            universal_migrations = [
                (
                    "ALTER TABLE chat_history ADD COLUMN request_id TEXT;",
                    ("duplicate column name", "already exists"),
                    "Migrated chat_history to include request_id",
                    "Chat history request_id migration error"
                ),
                (
                    "ALTER TABLE observability_events ADD COLUMN request_id TEXT;",
                    ("duplicate column name", "already exists"),
                    "Migrated observability_events to include request_id",
                    "Observability request_id migration error"
                ),
                (
                    "ALTER TABLE observability_events ADD COLUMN question TEXT;",
                    ("duplicate column name", "already exists"),
                    "Migrated observability_events to include question",
                    "Observability question migration error"
                ),
                (
                    "ALTER TABLE observability_events ADD COLUMN sql_query TEXT;",
                    ("duplicate column name", "already exists"),
                    "Migrated observability_events to include sql_query",
                    "Observability sql_query migration error"
                ),
                (
                    "ALTER TABLE observability_events ADD COLUMN error_message TEXT;",
                    ("duplicate column name", "already exists"),
                    "Migrated observability_events to include error_message",
                    "Observability error_message migration error"
                ),
            ]
            for migration_sql, duplicate_markers, success_message, error_prefix in universal_migrations:
                try:
                    db.execute(text(migration_sql))
                    db.commit()
                    logger.info(success_message)
                except Exception as e:
                    db.rollback()
                    if any(marker in str(e).lower() for marker in duplicate_markers):
                        pass
                    else:
                        logger.warning(f"Note: {error_prefix}: {e}")

            # Only seed default roles on a FRESH INSTALL (no roles at all).
            # This way, if an admin deletes a role it stays deleted across restarts.
            role_count = db.query(RoleModel).count()
            if role_count == 0:
                default_roles = [
                    ("SUPER_ADMIN", "Global system oversight across all enterprises"),
                    ("SYSTEM_ADMIN", "Full access to enterprise-level features"),
                    ("DATA_SCIENTIST", "Advanced query capabilities and schema insights"),
                    ("ANALYST", "Can query data and create dashboards"),
                    ("MANAGER", "Trusted analyst with team oversight"),
                    ("VIEWER", "Read-only access to results and history"),
                ]
                for rname, rdesc in default_roles:
                    db.add(RoleModel(name=rname, description=rdesc))
                db.commit()
            else:
                # Always ensure SYSTEM_ADMIN role exists (critical, cannot be deleted)
                if not db.query(RoleModel).filter(RoleModel.name == "SYSTEM_ADMIN").first():
                    db.add(RoleModel(name="SYSTEM_ADMIN", description="Full access to all system features"))
                    db.commit()

            admin = db.query(UserModel).filter(UserModel.username == "admin").first()
            if not admin:
                bootstrap_password = os.getenv("BOOTSTRAP_ADMIN_PASSWORD")
                if not bootstrap_password:
                    raise RuntimeError(
                        "Missing bootstrap admin credentials. Set BOOTSTRAP_ADMIN_PASSWORD for first-time startup."
                    )
                logger.info("Initializing bootstrap admin user from BOOTSTRAP_ADMIN_PASSWORD")
                self.create_user("admin", bootstrap_password, "SUPER_ADMIN")
        finally:
            db.close()

    def list_roles(self) -> List[Dict]:
        db = SessionLocal()
        try:
            roles = db.query(RoleModel).all()
            return [{"name": r.name, "description": r.description} for r in roles]
        finally:
            db.close()

    def create_role(self, name: str, description: str = "") -> bool:
        db = SessionLocal()
        try:
            if db.query(RoleModel).filter(RoleModel.name == name).first():
                return False
            db.add(RoleModel(name=name, description=description))
            db.commit()
            return True
        finally:
            db.close()

    def delete_role(self, name: str) -> bool:
        if name == "SYSTEM_ADMIN":
            return False
        db = SessionLocal()
        try:
            role = db.query(RoleModel).filter(RoleModel.name == name).first()
            if role:
                db.delete(role)
                db.commit()
                return True
            return False
        finally:
            db.close()

    def _hash_password(self, password: str, salt: str) -> str:
        return hashlib.pbkdf2_hmac('sha256', password.encode(), salt.encode(), 100000).hex()

    def _safe_json_dumps(self, payload: Optional[Dict[str, Any]]) -> Optional[str]:
        if payload is None:
            return None
        try:
            return json.dumps(payload, default=str)
        except Exception:
            return json.dumps({"raw": str(payload)})

    def create_user(self, username: str, password: str, role: str, enterprise_id: Optional[int] = None, owner_admin_id: Optional[int] = None, created_by_id: Optional[int] = None) -> bool:
        db = SessionLocal()
        try:
            if db.query(UserModel).filter(UserModel.username == username).first():
                return False
            salt = secrets.token_hex(16)
            password_hash = self._hash_password(password, salt)
            new_user = UserModel(
                username=username,
                password_hash=password_hash,
                salt=salt,
                role=role,
                enterprise_id=enterprise_id,
                owner_admin_id=owner_admin_id,
                created_by_id=created_by_id,
                is_active=True
            )
            db.add(new_user)
            db.commit()
            return True
        finally:
            db.close()

    def _is_enterprise_active(self, db, enterprise_id: Optional[int]) -> bool:
        if enterprise_id is None:
            return True
        enterprise = db.query(EnterpriseModel).filter(EnterpriseModel.id == enterprise_id).first()
        return bool(enterprise and enterprise.is_active)

    def _is_user_enterprise_allowed(self, db, db_user: UserModel) -> bool:
        if db_user.role == "SUPER_ADMIN":
            return True
        return self._is_enterprise_active(db, db_user.enterprise_id)

    def authenticate(self, username: str, password: str, device_label: Optional[str] = None) -> Optional[User]:
        db = SessionLocal()
        try:
            db_user = db.query(UserModel).filter(UserModel.username == username, UserModel.is_active == True).first()
            if not db_user:
                return None
            if not self._is_user_enterprise_allowed(db, db_user):
                return None
            if self._hash_password(password, db_user.salt) == db_user.password_hash:
                now = datetime.datetime.utcnow()
                expires = now + datetime.timedelta(days=TOKEN_TTL_DAYS)
                jti = str(uuid.uuid4())
                payload = {
                    "id": db_user.id,
                    "username": db_user.username,
                    "role": db_user.role,
                    "enterprise_id": db_user.enterprise_id,
                    "owner_admin_id": db_user.owner_admin_id,
                    "jti": jti,
                    "exp": expires
                }
                token = jwt.encode(payload, MASTER_KEY_JWT, algorithm="HS256")

                # Insert a per-session record — does NOT touch users.token
                session_row = AuthSessionModel(
                    user_id=db_user.id,
                    jti=jti,
                    token_hash=hashlib.sha256(token.encode()).hexdigest(),
                    created_at=now.isoformat(),
                    expires_at=expires.isoformat(),
                    device_label=device_label,
                    enterprise_id=db_user.enterprise_id,
                    owner_admin_id=db_user.owner_admin_id,
                )
                db.add(session_row)
                db.commit()

                return User(
                    id=db_user.id,
                    username=db_user.username,
                    password_hash=db_user.password_hash,
                    salt=db_user.salt,
                    role=db_user.role,
                    enterprise_id=db_user.enterprise_id,
                    owner_admin_id=db_user.owner_admin_id,
                    token=token,
                    token_expires_at=expires.isoformat(),
                    llm_config=decrypt_data(db_user.llm_config) if db_user.llm_config else None,
                    last_connection=db_user.last_connection_json
                )
            return None
        finally:
            db.close()

    def record_login_failure(self, username: str, reason: str = "invalid_credentials"):
        self.log_security_event(
            event_type="login_failure",
            severity="high",
            username=username,
            event_source="auth",
            details={"reason": reason}
        )

    def get_user_by_token(self, token: str) -> Optional[User]:
        try:
            payload = jwt.decode(token, MASTER_KEY_JWT, algorithms=["HS256"])
            db = SessionLocal()
            try:
                # --- Per-session revocation check ---
                # Tokens issued after this migration carry a `jti` claim.
                # Tokens issued before (no jti) are still accepted for
                # backward compatibility until they expire naturally.
                jti = payload.get("jti")
                if jti:
                    session = db.query(AuthSessionModel).filter(
                        AuthSessionModel.jti == jti,
                        AuthSessionModel.revoked_at.is_(None)
                    ).first()
                    if not session:
                        return None  # session was explicitly revoked

                db_user = db.query(UserModel).filter(
                    UserModel.id == payload["id"],
                    UserModel.is_active == True
                ).first()
                if not db_user:
                    return None
                if not self._is_user_enterprise_allowed(db, db_user):
                    return None
                return User(
                    id=db_user.id,
                    username=db_user.username,
                    password_hash=db_user.password_hash,
                    salt=db_user.salt,
                    role=db_user.role,
                    enterprise_id=db_user.enterprise_id,
                    owner_admin_id=db_user.owner_admin_id,
                    token=token,
                    token_expires_at=db_user.token_expires_at,
                    llm_config=decrypt_data(db_user.llm_config) if db_user.llm_config else None,
                    last_connection=db_user.last_connection_json
                )
            finally:
                db.close()
        except jwt.ExpiredSignatureError:
            return None
        except jwt.InvalidTokenError:
            return None
        except Exception:
            return None

    def logout(self, token: str) -> bool:
        """Revoke the *specific* session identified by this token's jti claim.
        Sibling sessions for the same user are completely unaffected.
        """
        try:
            # Decode without verifying expiry — we want to revoke even if the
            # token is already expired, so it can't be reused if expiry clocks drift.
            payload = jwt.decode(
                token, MASTER_KEY_JWT, algorithms=["HS256"],
                options={"verify_exp": False}
            )
            jti = payload.get("jti")
        except Exception:
            return False

        if not jti:
            # Legacy token (no jti) — nothing to revoke in the new table.
            return True

        db = SessionLocal()
        try:
            session = db.query(AuthSessionModel).filter(
                AuthSessionModel.jti == jti
            ).first()
            if session:
                session.revoked_at = datetime.datetime.utcnow().isoformat()
                db.commit()
            return True
        finally:
            db.close()

    def revoke_session_by_jti(self, jti: str, requesting_user_id: int) -> bool:
        """Revoke an arbitrary session by its jti, scoped to the requesting user."""
        db = SessionLocal()
        try:
            session = db.query(AuthSessionModel).filter(
                AuthSessionModel.jti == jti,
                AuthSessionModel.user_id == requesting_user_id,
                AuthSessionModel.revoked_at.is_(None)
            ).first()
            if not session:
                return False
            session.revoked_at = datetime.datetime.utcnow().isoformat()
            db.commit()
            return True
        finally:
            db.close()

    def revoke_all_other_sessions(self, current_jti: str, user_id: int) -> int:
        """Revoke every active session for this user except the caller's own."""
        db = SessionLocal()
        try:
            sessions = db.query(AuthSessionModel).filter(
                AuthSessionModel.user_id == user_id,
                AuthSessionModel.jti != current_jti,
                AuthSessionModel.revoked_at.is_(None)
            ).all()
            now = datetime.datetime.utcnow().isoformat()
            for s in sessions:
                s.revoked_at = now
            db.commit()
            return len(sessions)
        finally:
            db.close()

    def list_sessions(self, user_id: int) -> List[Dict]:
        """Return all non-expired, non-revoked sessions for a user."""
        db = SessionLocal()
        try:
            now_iso = datetime.datetime.utcnow().isoformat()
            sessions = db.query(AuthSessionModel).filter(
                AuthSessionModel.user_id == user_id,
                AuthSessionModel.revoked_at.is_(None),
                AuthSessionModel.expires_at > now_iso
            ).order_by(AuthSessionModel.created_at.desc()).all()
            return [
                {
                    "jti": s.jti,
                    "created_at": s.created_at,
                    "expires_at": s.expires_at,
                    "last_seen_at": s.last_seen_at,
                    "device_label": s.device_label,
                }
                for s in sessions
            ]
        finally:
            db.close()

    def list_users(self, current_user: Optional[User] = None) -> List[Dict]:
        db = SessionLocal()
        try:
            query = db.query(UserModel).filter(UserModel.is_active == True)
            if current_user:
                if current_user.role == "SUPER_ADMIN":
                    # Super admins should see top-level accounts they manage directly,
                    # not every nested enterprise-owned user created by downstream admins.
                    from sqlalchemy import or_
                    query = query.filter(
                        or_(
                            UserModel.owner_admin_id.is_(None),
                            UserModel.created_by_id == current_user.id,
                        )
                    )
                else:
                    query = self.apply_scope(query, current_user, UserModel)
            users = query.all()
            return [{"username": u.username, "role": u.role, "enterprise_id": u.enterprise_id, "owner_admin_id": u.owner_admin_id} for u in users]
        finally:
            db.close()

    def apply_scope(self, query, current_user: User, model_class):
        """Centralized scope engine to enforce isolation."""
        if current_user.role == "SUPER_ADMIN":
            return query
        
        if current_user.role == "SYSTEM_ADMIN":
            scoped_filters = []
            if hasattr(model_class, "owner_admin_id"):
                scoped_filters.append(model_class.owner_admin_id == current_user.id)
            if hasattr(model_class, "username"):
                scoped_filters.append(model_class.username == current_user.username)
            if hasattr(model_class, "admin_username"):
                scoped_filters.append(model_class.admin_username == current_user.username)
            if hasattr(model_class, "user_id"):
                scoped_filters.append(model_class.user_id == current_user.id)

            if not scoped_filters or not hasattr(model_class, "enterprise_id"):
                return query.filter(False)

            return query.filter(
                model_class.enterprise_id == current_user.enterprise_id,
                or_(*scoped_filters)
            )
        
        # Regular users only see their own data
        if hasattr(model_class, "user_id"):
            return query.filter(model_class.user_id == current_user.id)
        if hasattr(model_class, "username"):
            return query.filter(model_class.username == current_user.username)
        return query.filter(False) # Default deny

    def apply_personal_scope(self, query, current_user: User, model_class):
        """Always filter to the current user's own records only, regardless of role.
        Used for chat history where every user (including admins) should only see their own."""
        if hasattr(model_class, "username"):
            return query.filter(model_class.username == current_user.username)
        if hasattr(model_class, "user_id"):
            return query.filter(model_class.user_id == current_user.id)
        return query.filter(False)

    # ── Enterprise Management (Super Admin) ─────────────────────────────
    def create_enterprise(self, name: str, super_admin_id: int) -> Optional[int]:
        db = SessionLocal()
        try:
            if db.query(EnterpriseModel).filter(EnterpriseModel.name == name).first():
                return None
            new_ent = EnterpriseModel(
                name=name,
                created_by_id=super_admin_id,
                status="active",
                is_active=True,
                created_at=datetime.datetime.now().isoformat()
            )
            db.add(new_ent)
            db.commit()
            return new_ent.id
        finally:
            db.close()

    def update_enterprise_status(self, enterprise_id: int, is_active: bool) -> bool:
        db = SessionLocal()
        try:
            ent = db.query(EnterpriseModel).filter(EnterpriseModel.id == enterprise_id).first()
            if not ent:
                return False
            ent.is_active = is_active
            ent.status = "active" if is_active else "inactive"
            db.commit()
            return True
        finally:
            db.close()

    def delete_enterprise(self, enterprise_id: int) -> Optional[Dict[str, Any]]:
        db = SessionLocal()
        try:
            ent = db.query(EnterpriseModel).filter(EnterpriseModel.id == enterprise_id).first()
            if not ent:
                return None
            enterprise_name = ent.name

            users = db.query(UserModel).filter(UserModel.enterprise_id == enterprise_id).all()
            usernames = [user.username for user in users]
            user_ids = [user.id for user in users]
            admin_count = sum(1 for user in users if user.role == "SYSTEM_ADMIN")
            user_count = len(users)

            if user_ids:
                db.query(AuthSessionModel).filter(AuthSessionModel.user_id.in_(user_ids)).delete(synchronize_session=False)
            if usernames:
                db.query(ChatHistoryModel).filter(ChatHistoryModel.username.in_(usernames)).delete(synchronize_session=False)

            if user_ids:
                db.query(UserModel).filter(UserModel.enterprise_id == enterprise_id).delete(synchronize_session=False)

            db.query(EnterpriseModel).filter(EnterpriseModel.id == enterprise_id).delete(synchronize_session=False)
            db.commit()
        finally:
            db.close()

        if usernames:
            self._remove_users_from_rbac(usernames)

        return {
            "enterprise_id": enterprise_id,
            "enterprise_name": enterprise_name,
            "deleted_users": user_count,
            "deleted_admins": admin_count,
        }

    def list_enterprises(self) -> List[Dict]:
        db = SessionLocal()
        try:
            from sqlalchemy import case, func

            counts = {
                row.enterprise_id: {
                    "total_users": row.total_users or 0,
                    "admin_count": row.admin_count or 0,
                    "employee_count": row.employee_count or 0,
                    "managed_user_count": row.managed_user_count or 0,
                }
                for row in db.query(
                    UserModel.enterprise_id.label("enterprise_id"),
                    func.count(UserModel.id).label("total_users"),
                    func.sum(case((UserModel.role == "SYSTEM_ADMIN", 1), else_=0)).label("admin_count"),
                    func.sum(case((UserModel.role != "SYSTEM_ADMIN", 1), else_=0)).label("employee_count"),
                    func.sum(case((UserModel.owner_admin_id.isnot(None), 1), else_=0)).label("managed_user_count"),
                )
                .filter(UserModel.enterprise_id.isnot(None))
                .group_by(UserModel.enterprise_id)
                .all()
            }

            ents = db.query(EnterpriseModel).all()
            return [
                {
                    "id": e.id,
                    "name": e.name,
                    "status": e.status,
                    "is_active": e.is_active,
                    "created_at": e.created_at,
                    "total_users": counts.get(e.id, {}).get("total_users", 0),
                    "admin_count": counts.get(e.id, {}).get("admin_count", 0),
                    "employee_count": counts.get(e.id, {}).get("employee_count", 0),
                    "managed_user_count": counts.get(e.id, {}).get("managed_user_count", 0),
                }
                for e in ents
            ]
        finally:
            db.close()

    def _remove_users_from_rbac(self, usernames: List[str]) -> None:
        if not usernames:
            return

        policy_file = _policy_file_path()
        if not _policy_exists(policy_file):
            return

        try:
            with open(policy_file, "r") as f:
                data = json.load(f)
            user_policies = data.get("users", {})
            for username in usernames:
                user_policies.pop(username, None)
            data["users"] = user_policies
            with open(policy_file, "w") as f:
                json.dump(data, f, indent=2)
        except Exception as e:
            logger.error(f"Failed to remove RBAC entries for deleted enterprise users: {e}")

    def _get_scoped_target_user(self, db, current_user: Optional[User], username: str) -> Optional[UserModel]:
        query = db.query(UserModel).filter(
            UserModel.username == username,
            UserModel.is_active == True
        )
        if current_user is not None:
            query = self.apply_scope(query, current_user, UserModel)
        return query.first()

    def delete_user(self, username: str, current_user: Optional[User] = None) -> bool:
        db = SessionLocal()
        try:
            user = self._get_scoped_target_user(db, current_user, username)
            if user:
                user_id = user.id
                
                # Fetch all users owned by this admin (if any) to cascade their deletion
                child_users = db.query(UserModel).filter(UserModel.owner_admin_id == user_id).all()
                child_user_ids = [cu.id for cu in child_users]
                child_usernames = [cu.username for cu in child_users]
                
                all_ids = [user_id] + child_user_ids
                all_usernames = [username] + child_usernames
                
                # Cleanup user-specific data
                db.query(AuthSessionModel).filter(AuthSessionModel.user_id.in_(all_ids)).delete(synchronize_session=False)
                db.query(ChatHistoryModel).filter(ChatHistoryModel.username.in_(all_usernames)).delete(synchronize_session=False)
                
                # Note: We deliberately DO NOT delete records from AuditLogModel, 
                # ObservabilityEventModel, AdminActionLogModel, or SecurityEventModel to preserve the 
                # system audit trail for compliance purposes.
                
                # Delete child users first to avoid orphaned records
                if child_user_ids:
                    db.query(UserModel).filter(UserModel.id.in_(child_user_ids)).delete(synchronize_session=False)
                
                db.delete(user)
                db.commit()
                
                self._remove_users_from_rbac(all_usernames)
                return True
            return False
        finally:
            db.close()

    def reset_password(self, username: str, new_password: str, current_user: Optional[User] = None) -> bool:
        db = SessionLocal()
        try:
            user = self._get_scoped_target_user(db, current_user, username)
            if user:
                user.password_hash = self._hash_password(new_password, user.salt)
                db.commit()
                return True
            return False
        finally:
            db.close()

    def change_password(self, username: str, old_password: str, new_password: str) -> bool:
        db = SessionLocal()
        try:
            user = db.query(UserModel).filter(UserModel.username == username).first()
            if user and self._hash_password(old_password, user.salt) == user.password_hash:
                user.password_hash = self._hash_password(new_password, user.salt)
                db.commit()
                return True
            return False
        finally:
            db.close()

    def update_last_connection(self, username: str, connection_data: Dict) -> bool:
        db = SessionLocal()
        try:
            user = db.query(UserModel).filter(UserModel.username == username).first()
            if user:
                safe_connection = None
                if connection_data:
                    safe_connection = {
                        "engine": connection_data.get("engine"),
                        "host": connection_data.get("host"),
                        "port": connection_data.get("port"),
                        "database": connection_data.get("database"),
                        "user": connection_data.get("user"),
                    }
                user.last_connection_json = json.dumps(safe_connection) if safe_connection else None
                db.commit()
                return True
            return False
        finally:
            db.close()

    def get_last_connection(self, username: str) -> Optional[Dict]:
        db = SessionLocal()
        try:
            user = db.query(UserModel).filter(UserModel.username == username).first()
            if user and user.last_connection_json:
                return json.loads(user.last_connection_json)
            return None
        finally:
            db.close()

    def update_llm_config(self, username: str, active_provider: str, configs: Dict) -> bool:
        db = SessionLocal()
        try:
            user = db.query(UserModel).filter(UserModel.username == username).first()
            if user:
                # 1. Fetch existing config safely
                existing_config = {}
                if user.llm_config:
                    try:
                        existing_config = json.loads(decrypt_data(user.llm_config))
                    except Exception as e:
                        logger.error(f"Failed to decrypt/parse existing LLM config for {username}: {e}")
                
                # 2. Merge logic
                merged_providers = existing_config.get("providers", {})
                
                if not configs:
                    # Frontend clicked "Clear Settings", wipe all providers
                    merged_providers = {}
                else:
                    for provider, cfg in configs.items():
                        clean_cfg = dict(cfg)
                        incoming_key = clean_cfg.get("api_key")
                        
                        existing_provider_cfg = merged_providers.get(provider, {})
                        
                        # Soft-merge: Preserve existing key if incoming is empty/masked
                        if not incoming_key or incoming_key == "" or (isinstance(incoming_key, str) and incoming_key.startswith("sk-") and incoming_key.endswith("****")):
                            existing_key = existing_provider_cfg.get("api_key")
                            if existing_key:
                                clean_cfg["api_key"] = existing_key
                            else:
                                clean_cfg.pop("api_key", None)
                        
                        merged_providers[provider] = clean_cfg
                
                user.llm_config = encrypt_data(json.dumps({
                    "active_provider": active_provider,
                    "providers": merged_providers
                }))
                db.commit()
                return True
            return False
        finally:
            db.close()

    def get_llm_config(self, username: str) -> Dict:
        db = SessionLocal()
        try:
            user = db.query(UserModel).filter(UserModel.username == username).first()
            if user and user.llm_config:
                return json.loads(decrypt_data(user.llm_config))
            return {"active_provider": "groq", "providers": {}}
        finally:
            db.close()

    # ── Per-user RBAC ────────────────────────────────────────────────────
    def get_user_rbac(self, username: str, current_user: Optional[User] = None) -> Optional[Dict]:
        """Return the blocked_tables and blocked_columns for a specific user."""
        db = SessionLocal()
        try:
            if not self._get_scoped_target_user(db, current_user, username):
                return None
        finally:
            db.close()

        policy_file = _policy_file_path()
        if not _policy_exists(policy_file):
            return {"blocked_tables": [], "blocked_columns": []}
        try:
            with open(policy_file, "r") as f:
                data = json.load(f)
            return data.get("users", {}).get(username, {"blocked_tables": [], "blocked_columns": []})
        except Exception as e:
            logger.error(f"Failed to read RBAC for {username}: {e}")
            return {"blocked_tables": [], "blocked_columns": []}

    def update_user_rbac(self, username: str, blocked_tables: List[str], blocked_columns: List[str], current_user: Optional[User] = None) -> bool:
        """Set per-user RBAC restrictions (blocked tables/columns)."""
        db = SessionLocal()
        try:
            if not self._get_scoped_target_user(db, current_user, username):
                return False
        finally:
            db.close()

        policy_file = _policy_file_path()
        try:
            data = {"users": {}}
            if _policy_exists(policy_file):
                with open(policy_file, "r") as f:
                    data = json.load(f)
            if "users" not in data:
                data["users"] = {}
            data["users"][username] = {
                "blocked_tables": [t.lower() for t in blocked_tables],
                "blocked_columns": [c.lower() for c in blocked_columns]
            }
            with open(policy_file, "w") as f:
                json.dump(data, f, indent=2)
            return True
        except Exception as e:
            logger.error(f"Failed to update RBAC for {username}: {e}")
            return False


    # ── Role RBAC ────────────────────────────────────────────────────────
    def get_role_rbac(self, role_name: str) -> Dict:
        """Return the blocked_tables and blocked_columns for a specific role."""
        policy_file = _policy_file_path()
        if not _policy_exists(policy_file):
            return {"blocked_tables": [], "blocked_columns": []}
        try:
            with open(policy_file, "r") as f:
                data = json.load(f)
            return data.get("roles", {}).get(role_name, {"blocked_tables": [], "blocked_columns": []})
        except Exception as e:
            logger.error(f"Failed to read RBAC for role {role_name}: {e}")
            return {"blocked_tables": [], "blocked_columns": []}

    def update_role_rbac(self, role_name: str, blocked_tables: List[str], blocked_columns: List[str]) -> bool:
        """Set role-based RBAC restrictions."""
        policy_file = _policy_file_path()
        try:
            data = {"users": {}, "roles": {}}
            if _policy_exists(policy_file):
                with open(policy_file, "r") as f:
                    data = json.load(f)
            if "roles" not in data:
                data["roles"] = {}
            data["roles"][role_name] = {
                "blocked_tables": [t.lower() for t in blocked_tables],
                "blocked_columns": [c.lower() for c in blocked_columns]
            }
            with open(policy_file, "w") as f:
                json.dump(data, f, indent=2)
            return True
        except Exception as e:
            logger.error(f"Failed to update RBAC for role {role_name}: {e}")
            return False

    def log_audit(self, user: User, question: str, sql_query: str, latency_sec: float = None, success: bool = True):
        db = SessionLocal()
        try:
            log = AuditLogModel(
                enterprise_id=user.enterprise_id,
                owner_admin_id=self._resolve_owner_admin_id(user),
                username=user.username,
                role=user.role,
                question=question,
                sql_query=sql_query,
                latency_sec=latency_sec,
                success=success,
                timestamp=datetime.datetime.now().isoformat()
            )
            db.add(log)
            db.commit()
        except Exception as e:
            logger.error(f"Failed to save audit log: {e}")
        finally:
            db.close()

    def list_audit_logs(self, current_user: User, limit: int = 100) -> List[Dict]:
        db = SessionLocal()
        try:
            # Filter enterprise_names metadata to only include the current user's scope
            ent_query = db.query(EnterpriseModel.id, EnterpriseModel.name)
            if current_user.role != "SUPER_ADMIN":
                ent_query = ent_query.filter(EnterpriseModel.id == current_user.enterprise_id)
            
            enterprise_names = {
                enterprise.id: enterprise.name
                for enterprise in ent_query.all()
            }
            query = db.query(AuditLogModel)
            query = self.apply_scope(query, current_user, AuditLogModel)
            logs = query.order_by(AuditLogModel.id.desc()).limit(limit).all()
            return [
                {
                    "enterprise_id": l.enterprise_id,
                    "enterprise_name": enterprise_names.get(l.enterprise_id),
                    "username": l.username,
                    "role": l.role,
                    "question": l.question,
                    "sql": l.sql_query,
                    "latency_sec": l.latency_sec,
                    "success": l.success,
                    "timestamp": l.timestamp
                } for l in logs
            ]
        finally:
            db.close()

    def log_observability_event(self, user: User, payload: Dict) -> Optional[int]:
        db = SessionLocal()
        try:
            event = ObservabilityEventModel(
                request_id=str(payload.get("request_id")) if payload.get("request_id") is not None else None,
                enterprise_id=user.enterprise_id,
                owner_admin_id=self._resolve_owner_admin_id(user),
                username=user.username,
                role=user.role,
                db_name=payload.get("db_name"),
                question=payload.get("question"),
                sql_query=payload.get("sql_query"),
                llm_provider=payload.get("llm_provider"),
                llm_model=payload.get("llm_model"),
                sql_gen_ms=payload.get("sql_gen_ms"),
                db_exec_ms=payload.get("db_exec_ms"),
                summary_ms=payload.get("summary_ms"),
                viz_ms=payload.get("viz_ms"),
                total_ms=payload.get("total_ms"),
                success=payload.get("success", True),
                had_rate_limit=payload.get("had_rate_limit", False),
                rate_limit_stage=payload.get("rate_limit_stage"),
                error_stage=payload.get("error_stage"),
                error_message=payload.get("error_message"),
                timestamp=payload.get("timestamp") or datetime.datetime.now().isoformat()
            )
            db.add(event)
            db.commit()
            db.refresh(event)
            return event.id
        except Exception as e:
            logger.error(f"Failed to save observability event: {e}")
            return None
        finally:
            db.close()

    def update_observability_event(self, event_id: int, updates: Dict) -> None:
        if not event_id:
            return
        db = SessionLocal()
        try:
            event = db.query(ObservabilityEventModel).filter(ObservabilityEventModel.id == event_id).first()
            if event:
                for k, v in updates.items():
                    if hasattr(event, k):
                        setattr(event, k, v)
                db.commit()
        except Exception as e:
            logger.error(f"Failed to update observability event {event_id}: {e}")
        finally:
            db.close()

    def log_admin_action(
        self,
        admin_user: User,
        action_type: str,
        target_type: Optional[str] = None,
        target_name: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ) -> None:
        db = SessionLocal()
        try:
            row = AdminActionLogModel(
                enterprise_id=admin_user.enterprise_id,
                owner_admin_id=admin_user.id, # The admin performing the action IS the owner for this log
                admin_username=admin_user.username,
                action_type=action_type,
                target_type=target_type,
                target_name=target_name,
                details_json=self._safe_json_dumps(details),
                timestamp=datetime.datetime.now().isoformat()
            )
            db.add(row)
            db.commit()
        except Exception as e:
            logger.error(f"Failed to save admin action log: {e}")
        finally:
            db.close()

    def list_admin_action_logs(self, current_user: User, limit: int = 100) -> List[Dict]:
        db = SessionLocal()
        try:
            # Filter enterprise_names metadata to only include the current user's scope
            ent_query = db.query(EnterpriseModel.id, EnterpriseModel.name)
            if current_user.role != "SUPER_ADMIN":
                ent_query = ent_query.filter(EnterpriseModel.id == current_user.enterprise_id)
            
            enterprise_names = {
                enterprise.id: enterprise.name
                for enterprise in ent_query.all()
            }
            query = db.query(AdminActionLogModel)
            query = self.apply_scope(query, current_user, AdminActionLogModel)
            rows = query.order_by(AdminActionLogModel.id.desc()).limit(limit).all()
            return [
                {
                    "enterprise_id": row.enterprise_id,
                    "enterprise_name": enterprise_names.get(row.enterprise_id),
                    "admin_username": row.admin_username,
                    "action_type": row.action_type,
                    "target_type": row.target_type,
                    "target_name": row.target_name,
                    "details": json.loads(row.details_json) if row.details_json else None,
                    "timestamp": row.timestamp
                }
                for row in rows
            ]
        finally:
            db.close()

    def log_security_event(
        self,
        event_type: str,
        severity: str,
        user: Optional[User] = None,
        username: Optional[str] = None,
        role: Optional[str] = None,
        enterprise_id: Optional[int] = None,
        owner_admin_id: Optional[int] = None,
        event_source: Optional[str] = None,
        resource_name: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ) -> None:
        db = SessionLocal()
        try:
            row = SecurityEventModel(
                enterprise_id=user.enterprise_id if user else enterprise_id,
                owner_admin_id=self._resolve_owner_admin_id(user) if user else owner_admin_id,
                username=user.username if user else username,
                role=user.role if user else role,
                event_type=event_type,
                severity=severity,
                event_source=event_source,
                resource_name=resource_name,
                details_json=self._safe_json_dumps(details),
                timestamp=datetime.datetime.now().isoformat()
            )
            db.add(row)
            db.commit()
        except Exception as e:
            logger.error(f"Failed to save security event: {e}")
        finally:
            db.close()

    def list_security_events(self, current_user: User, limit: int = 100) -> List[Dict]:
        db = SessionLocal()
        try:
            # Filter enterprise_names metadata to only include the current user's scope
            ent_query = db.query(EnterpriseModel.id, EnterpriseModel.name)
            if current_user.role != "SUPER_ADMIN":
                ent_query = ent_query.filter(EnterpriseModel.id == current_user.enterprise_id)
            
            enterprise_names = {
                enterprise.id: enterprise.name
                for enterprise in ent_query.all()
            }
            query = db.query(SecurityEventModel)
            query = self.apply_scope(query, current_user, SecurityEventModel)
            rows = query.order_by(SecurityEventModel.id.desc()).limit(limit).all()
            return [
                {
                    "enterprise_id": row.enterprise_id,
                    "enterprise_name": enterprise_names.get(row.enterprise_id),
                    "username": row.username,
                    "role": row.role,
                    "event_type": row.event_type,
                    "severity": row.severity,
                    "event_source": row.event_source,
                    "resource_name": row.resource_name,
                    "details": json.loads(row.details_json) if row.details_json else None,
                    "timestamp": row.timestamp
                }
                for row in rows
            ]
        finally:
            db.close()

    def _summarize_stage(self, values: List[float], threshold_ms: float) -> Dict:
        if not values:
            return {"avg_ms": 0.0, "p95_ms": 0.0, "max_ms": 0.0, "spike_count": 0}

        sorted_values = sorted(float(v) for v in values)
        p95_index = min(len(sorted_values) - 1, max(0, int(len(sorted_values) * 0.95) - 1))
        return {
            "avg_ms": round(sum(sorted_values) / len(sorted_values), 2),
            "p95_ms": round(sorted_values[p95_index], 2),
            "max_ms": round(sorted_values[-1], 2),
            "spike_count": sum(1 for value in sorted_values if value >= threshold_ms)
        }

    def _format_alert(self, severity: str, stage: str, message: str) -> Dict:
        return {"severity": severity, "stage": stage, "message": message}

    def _resolve_owner_admin_id(self, user: Optional[User]) -> Optional[int]:
        if user is None:
            return None
        if user.role in {"SUPER_ADMIN", "SYSTEM_ADMIN"}:
            return user.id
        return user.owner_admin_id

    def get_observability_overview(self, current_user: User, limit: int = OBSERVABILITY_LOOKBACK_LIMIT) -> Dict:
        db = SessionLocal()
        try:
            query = db.query(ObservabilityEventModel)
            query = self.apply_scope(query, current_user, ObservabilityEventModel)
            events = query.order_by(ObservabilityEventModel.id.desc())\
                .limit(limit)\
                .all()

            # Filter out deleted users for the UI representation
            active_usernames = {u[0] for u in db.query(UserModel.username).all()}
            events = [e for e in events if e.username in active_usernames]

            if not events:
                return {
                    "summary": {
                        "request_count": 0,
                        "success_rate": 0,
                        "rate_limit_count": 0,
                        "active_alerts": 0,
                        "p95_total_ms": 0
                    },
                    "stages": {},
                    "alerts": [],
                    "recent_events": []
                }

            now = datetime.datetime.now()
            alert_window_start = now - datetime.timedelta(minutes=OBSERVABILITY_ALERT_WINDOW_MINUTES)
            parsed_events = []
            for event in events:
                try:
                    event_dt = datetime.datetime.fromisoformat(event.timestamp)
                except Exception:
                    event_dt = None
                parsed_events.append((event, event_dt))

            total_events = len(events)
            success_count = sum(1 for event in events if event.success)
            rate_limit_count = sum(1 for event in events if event.had_rate_limit)

            stage_config = {
                "sql_gen": ("sql_gen_ms", OBSERVABILITY_THRESHOLD_SQL_GEN_MS),
                "db_exec": ("db_exec_ms", OBSERVABILITY_THRESHOLD_DB_EXEC_MS),
                "summary": ("summary_ms", OBSERVABILITY_THRESHOLD_SUMMARY_MS),
                "viz": ("viz_ms", OBSERVABILITY_THRESHOLD_VIZ_MS),
                "total": ("total_ms", OBSERVABILITY_THRESHOLD_TOTAL_MS),
            }

            stages = {}
            for stage_name, (field_name, threshold) in stage_config.items():
                values = [
                    getattr(event, field_name)
                    for event in events
                    if getattr(event, field_name) is not None
                ]
                stages[stage_name] = self._summarize_stage(values, threshold)

            recent_window_events = [
                event for event, event_dt in parsed_events
                if event_dt and event_dt >= alert_window_start
            ]
            alerts = []

            for stage_name, (_, threshold) in stage_config.items():
                if stage_name == "total":
                    continue
                stage_summary = stages.get(stage_name, {})
                if stage_summary.get("p95_ms", 0) >= threshold:
                    alerts.append(
                        self._format_alert(
                            "high",
                            stage_name,
                            f"{stage_name} p95 latency is {stage_summary['p95_ms']} ms, above the {int(threshold)} ms threshold."
                        )
                    )

            recent_429s = [event for event in recent_window_events if event.had_rate_limit]
            if len(recent_429s) >= OBSERVABILITY_ALERT_429_COUNT:
                alerts.append(
                    self._format_alert(
                        "critical",
                        "rate_limit",
                        f"{len(recent_429s)} rate-limit events were recorded in the last {OBSERVABILITY_ALERT_WINDOW_MINUTES} minutes."
                    )
                )

            recent_failures = [event for event in recent_window_events if not event.success]
            if len(recent_failures) >= OBSERVABILITY_ALERT_FAILURE_COUNT:
                alerts.append(
                    self._format_alert(
                        "medium",
                        "errors",
                        f"{len(recent_failures)} failed requests were recorded in the last {OBSERVABILITY_ALERT_WINDOW_MINUTES} minutes."
                    )
                )

            recent_events = []
            for event in events:
                recent_events.append({
                    "timestamp": event.timestamp,
                    "username": event.username,
                    "role": event.role,
                    "db_name": event.db_name,
                    "request_id": event.request_id,
                    "question": event.question,
                    "sql_query": event.sql_query,
                    "llm_provider": event.llm_provider,
                    "llm_model": event.llm_model,
                    "sql_gen_ms": round(event.sql_gen_ms or 0, 2),
                    "db_exec_ms": round(event.db_exec_ms or 0, 2),
                    "summary_ms": round(event.summary_ms or 0, 2),
                    "viz_ms": round(event.viz_ms or 0, 2),
                    "total_ms": round(event.total_ms or 0, 2),
                    "success": event.success,
                    "had_rate_limit": event.had_rate_limit,
                    "rate_limit_stage": event.rate_limit_stage,
                    "error_stage": event.error_stage,
                    "error_message": event.error_message
                })

            security_events = db.query(SecurityEventModel)
            security_events = self.apply_scope(security_events, current_user, SecurityEventModel)
            security_events = security_events\
                .order_by(SecurityEventModel.id.desc())\
                .limit(limit)\
                .all()
            recent_security_events = []
            for event in security_events:
                try:
                    event_dt = datetime.datetime.fromisoformat(event.timestamp)
                except Exception:
                    event_dt = None
                if event_dt and event_dt >= alert_window_start:
                    recent_security_events.append(event)

            login_failures = [event for event in recent_security_events if event.event_type == "login_failure"]
            if len(login_failures) >= OBSERVABILITY_ALERT_LOGIN_FAILURE_COUNT:
                alerts.append(
                    self._format_alert(
                        "critical",
                        "login_failure",
                        f"{len(login_failures)} login failures were recorded in the last {OBSERVABILITY_ALERT_WINDOW_MINUTES} minutes."
                    )
                )

            policy_denials = [event for event in recent_security_events if event.event_type == "policy_denial"]
            if len(policy_denials) >= OBSERVABILITY_ALERT_POLICY_DENIAL_COUNT:
                alerts.append(
                    self._format_alert(
                        "high",
                        "policy_denial",
                        f"{len(policy_denials)} RBAC policy denials were recorded in the last {OBSERVABILITY_ALERT_WINDOW_MINUTES} minutes."
                    )
                )

            return {
                "summary": {
                    "request_count": total_events,
                    "success_rate": round((success_count / total_events) * 100, 1),
                    "rate_limit_count": rate_limit_count,
                    "active_alerts": len(alerts),
                    "p95_total_ms": stages["total"]["p95_ms"],
                    "security_event_count": len(recent_security_events)
                },
                "stages": stages,
                "alerts": alerts,
                "recent_events": recent_events
            }
        finally:
            db.close()

    def get_policy_violation_analytics(self, current_user: User, limit: int = 500) -> Dict:
        db = SessionLocal()
        try:
            query = db.query(SecurityEventModel)\
                .filter(SecurityEventModel.event_type == "policy_denial")
            query = self.apply_scope(query, current_user, SecurityEventModel)
            rows = query.order_by(SecurityEventModel.id.desc())\
                .limit(limit)\
                .all()

            user_counts: Dict[str, int] = {}
            role_counts: Dict[str, int] = {}
            resource_counts: Dict[str, int] = {}
            source_counts: Dict[str, int] = {}

            for row in rows:
                if row.username:
                    user_counts[row.username] = user_counts.get(row.username, 0) + 1
                if row.role:
                    role_counts[row.role] = role_counts.get(row.role, 0) + 1
                if row.resource_name:
                    resource_counts[row.resource_name] = resource_counts.get(row.resource_name, 0) + 1
                if row.event_source:
                    source_counts[row.event_source] = source_counts.get(row.event_source, 0) + 1

            recent_rows = []
            for row in rows[:20]:
                recent_rows.append({
                    "username": row.username,
                    "role": row.role,
                    "resource_name": row.resource_name,
                    "event_source": row.event_source,
                    "details": json.loads(row.details_json) if row.details_json else None,
                    "timestamp": row.timestamp
                })

            return {
                "summary": {
                    "total_denials": len(rows),
                    "unique_users": len(user_counts),
                    "unique_resources": len(resource_counts)
                },
                "blocked_by_user": [
                    {"name": name, "count": count}
                    for name, count in sorted(user_counts.items(), key=lambda item: item[1], reverse=True)[:10]
                ],
                "blocked_by_role": [
                    {"name": name, "count": count}
                    for name, count in sorted(role_counts.items(), key=lambda item: item[1], reverse=True)[:10]
                ],
                "blocked_resources": [
                    {"name": name, "count": count}
                    for name, count in sorted(resource_counts.items(), key=lambda item: item[1], reverse=True)[:10]
                ],
                "blocked_sources": [
                    {"name": name, "count": count}
                    for name, count in sorted(source_counts.items(), key=lambda item: item[1], reverse=True)[:10]
                ],
                "recent_denials": recent_rows
            }
        finally:
            db.close()

    def get_slo_panel(self, current_user: User, limit: int = OBSERVABILITY_LOOKBACK_LIMIT) -> Dict:
        db = SessionLocal()
        try:
            query = db.query(ObservabilityEventModel)
            query = self.apply_scope(query, current_user, ObservabilityEventModel)
            events = query.order_by(ObservabilityEventModel.id.desc())\
                .limit(limit)\
                .all()

            if not events:
                return {
                    "summary": {"success_rate": 0, "requests": 0, "avg_total_s": 0},
                    "stages": {},
                    "uptime_trend": []
                }

            stage_fields = {
                "sql_gen": "sql_gen_ms",
                "db_exec": "db_exec_ms",
                "summary": "summary_ms",
                "viz": "viz_ms",
                "total": "total_ms",
            }

            def percentile(values: List[float], pct: float) -> float:
                if not values:
                    return 0.0
                values = sorted(values)
                index = min(len(values) - 1, max(0, int(len(values) * pct) - 1))
                return round(values[index] / 1000, 2)

            stage_summary = {}
            for stage_name, field in stage_fields.items():
                values = [float(getattr(event, field)) for event in events if getattr(event, field) is not None]
                stage_summary[stage_name] = {
                    "p50_s": percentile(values, 0.50),
                    "p95_s": percentile(values, 0.95),
                    "avg_s": round((sum(values) / len(values) / 1000), 2) if values else 0.0,
                    "success_rate": round((sum(1 for event in events if event.success) / len(events)) * 100, 1)
                }

            day_stats: Dict[str, Dict[str, float]] = {}
            for event in events:
                try:
                    day_key = datetime.datetime.fromisoformat(event.timestamp).date().isoformat()
                except Exception:
                    continue
                if day_key not in day_stats:
                    day_stats[day_key] = {"count": 0, "success": 0}
                day_stats[day_key]["count"] += 1
                if event.success:
                    day_stats[day_key]["success"] += 1

            uptime_trend = [
                {
                    "date": date_key,
                    "success_rate": round((stats["success"] / stats["count"]) * 100, 1) if stats["count"] else 0
                }
                for date_key, stats in sorted(day_stats.items())[-7:]
            ]

            total_values = [float(event.total_ms or 0) for event in events if event.total_ms is not None]
            return {
                "summary": {
                    "success_rate": round((sum(1 for event in events if event.success) / len(events)) * 100, 1),
                    "requests": len(events),
                    "avg_total_s": round((sum(total_values) / len(total_values) / 1000), 2) if total_values else 0.0
                },
                "stages": stage_summary,
                "uptime_trend": uptime_trend
            }
        finally:
            db.close()

    def get_system_stats(self, current_user: User) -> Dict:
        """Aggregate audit log data for system health dashboard."""
        db = SessionLocal()
        try:
            from sqlalchemy import func
            query = db.query(AuditLogModel)
            query = self.apply_scope(query, current_user, AuditLogModel)
            total_queries = query.count()
            if total_queries == 0:
                return {
                    "total_queries": 0,
                    "success_rate": 0,
                    "avg_latency": 0,
                    "latency_trend": [],
                    "popular_topics": []
                }

            successful_queries = query.filter(AuditLogModel.success == True).count()
            success_rate = round((successful_queries / total_queries) * 100, 1)

            avg_latency = query.with_entities(func.avg(AuditLogModel.latency_sec)).scalar() or 0
            avg_latency = round(float(avg_latency), 2)
            # Latency Trend (last 7 days)
            now = datetime.datetime.now()
            trend = []
            for i in range(6, -1, -1):
                day_start = (now - datetime.timedelta(days=i)).replace(hour=0, minute=0, second=0, microsecond=0)
                day_end = day_start + datetime.timedelta(days=1)
                
                # We store timestamp as ISO string, so we need a clever way to query or just pull and filter
                # For sqlite/simple usage, pulling recent logs and processing in python is safer
                pass # placeholder for trend logic below
            
            # Simple trend logic: pull last 500 logs — SCOPED
            query_logs = db.query(AuditLogModel).order_by(AuditLogModel.id.desc())
            query_logs = self.apply_scope(query_logs, current_user, AuditLogModel)
            logs = query_logs.limit(500).all()
            
            # Filter out deleted users for the Top User Activity UI
            active_usernames = {u[0] for u in db.query(UserModel.username).all()}
            logs = [l for l in logs if l.username in active_usernames]
            
            day_stats = {}
            for l in logs:
                try:
                    dt = datetime.datetime.fromisoformat(l.timestamp).date().isoformat()
                    if dt not in day_stats:
                        day_stats[dt] = {"total": 0, "latency": 0}
                    day_stats[dt]["total"] += 1
                    day_stats[dt]["latency"] += (l.latency_sec or 0)
                except:
                    continue
            
            sorted_days = sorted(day_stats.keys())[-7:]
            latency_trend = [{"date": d, "value": round(day_stats[d]["latency"] / day_stats[d]["total"], 2)} for d in sorted_days]

            # Popular Topics (Mock keyword extraction from questions)
            topics = {}
            keywords = ["revenue", "sales", "user", "customer", "product", "order", "region", "growth", "top", "best", "worst"]
            for l in logs:
                q = l.question.lower()
                for k in keywords:
                    if k in q:
                        topics[k] = topics.get(k, 0) + 1
            
            popular_topics = [{"topic": t, "count": c} for t, c in sorted(topics.items(), key=lambda x: x[1], reverse=True)[:10]]

            # User Activity Breakdown
            user_stats = {}
            for log in logs:
                u = log.username
                if u not in user_stats:
                    user_stats[u] = {"queries": 0, "success": 0, "latency": 0, "role": log.role}
                user_stats[u]["queries"] += 1
                if log.success:
                    user_stats[u]["success"] += 1
                user_stats[u]["latency"] += log.latency_sec or 0
            
            user_breakdown = []
            for u, s in user_stats.items():
                user_breakdown.append({
                    "username": u,
                    "role": s["role"],
                    "queries": s["queries"],
                    "success_rate": round((s["success"] / s["queries"]) * 100, 1) if s["queries"] > 0 else 0,
                    "avg_latency": round(s["latency"] / s["queries"], 3) if s["queries"] > 0 else 0
                })
            # Sort by query count
            user_breakdown.sort(key=lambda x: x["queries"], reverse=True)

            return {
                "summary": {
                    "total_queries": total_queries,
                    "success_rate": success_rate,
                    "avg_latency": avg_latency,
                },
                "trends": latency_trend,
                "popular_topics": popular_topics,
                "user_activity": user_breakdown[:10]  # Top 10 users
            }
        finally:
            db.close()

    # ── Chat History ──────────────────────────────────────────────────────
    def _json_serializable(self, obj):
        import datetime
        from decimal import Decimal
        if isinstance(obj, (datetime.datetime, datetime.date)):
            return obj.isoformat()
        if isinstance(obj, Decimal):
            return float(obj)
        return str(obj)

    def save_chat_history(
        self,
        user: User,
        db_name: str,
        question: str,
        sql: str,
        summary: Optional[str],
        results: Dict,
        visualization: Optional[Dict] = None,
        request_id: Optional[str] = None
    ):
        db = SessionLocal()
        try:
            # Complex objects in results (Decimals, dates) need special handling
            results_json = json.dumps(results, default=self._json_serializable)
            
            visualization_json = None
            if visualization:
                visualization_json = json.dumps(visualization, default=self._json_serializable)
            
            history = ChatHistoryModel(
                enterprise_id=user.enterprise_id,
                owner_admin_id=user.owner_admin_id,
                username=user.username,
                request_id=str(request_id) if request_id else None,
                db_name=db_name,
                question=question,
                sql_query=sql,
                summary=summary,
                results_json=results_json,
                visualization_json=visualization_json,
                timestamp=datetime.datetime.now().isoformat()
            )
            db.add(history)
            db.commit()
            db.refresh(history)
            return history.id
        except Exception as e:
            logger.error(f"Failed to save chat history for {user.username}: {e}")
            return None
        finally:
            db.close()

    def update_chat_history_partial(self, username: str, request_id: str, updates: Dict[str, Any]):
        """Update specific fields of a chat history record identified by its request_id."""
        if not request_id:
            return
        db = SessionLocal()
        try:
            item = db.query(ChatHistoryModel).filter(
                ChatHistoryModel.username == username,
                ChatHistoryModel.request_id == str(request_id)
            ).first()
            if item:
                for key, val in updates.items():
                    if hasattr(item, key):
                        if key in ["results", "visualization", "structured"]:
                            setattr(item, f"{key}_json" if key != "structured" else key, json.dumps(val, default=self._json_serializable))
                        else:
                            setattr(item, key, val)
                db.commit()
        except Exception as e:
            logger.error(f"Partial history update failed for {username}/{request_id}: {e}")
        finally:
            db.close()

    def get_chat_history(self, current_user: User, db_name: str, limit: int = 20, start_date: str = None, end_date: str = None, sort: str = "desc") -> List[Dict]:
        db = SessionLocal()
        try:
            query = db.query(ChatHistoryModel)\
                      .filter(ChatHistoryModel.db_name == db_name)
            query = self.apply_personal_scope(query, current_user, ChatHistoryModel)
            
            # Date filtering
            if start_date:
                query = query.filter(ChatHistoryModel.timestamp >= start_date)
            if end_date:
                query = query.filter(ChatHistoryModel.timestamp <= end_date)
            
            if sort == "desc":
                items = query.order_by(ChatHistoryModel.id.desc()).limit(limit).all()
            else:
                items = query.order_by(ChatHistoryModel.id.asc()).limit(limit).all()
            
            return [
                {
                    "id": i.id,
                    "request_id": i.request_id,
                    "question": i.question,
                    "sql": i.sql_query,
                    "summary": i.summary,
                    "results": json.loads(i.results_json) if i.results_json else None,
                    "visualization": json.loads(i.visualization_json) if i.visualization_json else None,
                    "timestamp": i.timestamp
                } for i in items
            ]
        finally:
            db.close()

    def clear_chat_history(self, current_user: User, db_name: str) -> bool:
        db = SessionLocal()
        try:
            query = db.query(ChatHistoryModel)\
                      .filter(ChatHistoryModel.db_name == db_name)
            query = self.apply_personal_scope(query, current_user, ChatHistoryModel)
            query.delete(synchronize_session=False)
            db.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to clear chat history: {e}")
            return False
        finally:
            db.close()

    def delete_history_item(self, current_user: User, item_id: int) -> bool:
        db = SessionLocal()
        try:
            query = db.query(ChatHistoryModel).filter(ChatHistoryModel.id == item_id)
            query = self.apply_personal_scope(query, current_user, ChatHistoryModel)
            item = query.first()
            if item:
                db.delete(item)
                db.commit()
                return True
            return False
        except Exception as e:
            logger.error(f"Failed to delete history item: {e}")
            return False
        finally:
            db.close()


def _policy_file_path() -> str:
    import os
    return os.path.join(os.path.dirname(__file__), "access_policy.json")


def _policy_exists(path: str) -> bool:
    import os
    return os.path.exists(path)


user_manager = UserManager()
