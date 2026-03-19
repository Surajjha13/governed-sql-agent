import hashlib
import secrets
import json
import logging
import os
from typing import List, Optional, Dict, Any
from app.auth.database import SessionLocal, engine, Base
from app.auth.models import (
    UserModel,
    RoleModel,
    AuditLogModel,
    ChatHistoryModel,
    ObservabilityEventModel,
    AdminActionLogModel,
    SecurityEventModel,
)
import datetime

logger = logging.getLogger(__name__)
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
    def __init__(self, username, password_hash, salt, role, token=None, llm_config=None, last_connection=None):
        self.username = username
        self.password_hash = password_hash
        self.salt = salt
        self.role = role
        self.token = token
        self.llm_config = llm_config
        self.last_connection = last_connection


class UserManager:
    def __init__(self):
        Base.metadata.create_all(bind=engine)
        self._ensure_admin()

    def _ensure_admin(self):
        db = SessionLocal()
        try:
            # Only seed default roles on a FRESH INSTALL (no roles at all).
            # This way, if an admin deletes a role it stays deleted across restarts.
            role_count = db.query(RoleModel).count()
            if role_count == 0:
                default_roles = [
                    ("SYSTEM_ADMIN", "Full access to all system features"),
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
                logger.info("Initializing default admin user")
                self.create_user("admin", "admin123", "SYSTEM_ADMIN")

            # SCHEMA MIGRATION: Ensure visualization_json column exists in chat_history
            try:
                # Use a raw connection to check and add column if missing
                raw_conn = db.connection()
                # SQLite specific check
                from sqlalchemy import text
                db.execute(text("ALTER TABLE chat_history ADD COLUMN visualization_json TEXT;"))
                db.commit()
                logger.info("Migrated chat_history to include visualization_json column")
            except Exception as e:
                # Silently catch "duplicate column" error
                if "duplicate column name" in str(e).lower() or "already exists" in str(e).lower():
                    pass
                else:
                    logger.warning(f"Note: Chat history migration error: {e}")
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

    def create_user(self, username: str, password: str, role: str) -> bool:
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
                role=role
            )
            db.add(new_user)
            db.commit()
            return True
        finally:
            db.close()

    def authenticate(self, username: str, password: str) -> Optional[User]:
        db = SessionLocal()
        try:
            db_user = db.query(UserModel).filter(UserModel.username == username).first()
            if not db_user:
                return None
            if self._hash_password(password, db_user.salt) == db_user.password_hash:
                token = secrets.token_hex(32)
                db_user.token = token
                db.commit()
                return User(
                    username=db_user.username,
                    password_hash=db_user.password_hash,
                    salt=db_user.salt,
                    role=db_user.role,
                    token=token,
                    llm_config=db_user.llm_config,
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
        db = SessionLocal()
        try:
            db_user = db.query(UserModel).filter(UserModel.token == token).first()
            if not db_user:
                return None
            return User(
                username=db_user.username,
                password_hash=db_user.password_hash,
                salt=db_user.salt,
                role=db_user.role,
                token=db_user.token,
                llm_config=db_user.llm_config,
                last_connection=db_user.last_connection_json
            )
        finally:
            db.close()

    def logout(self, username: str) -> bool:
        db = SessionLocal()
        try:
            user = db.query(UserModel).filter(UserModel.username == username).first()
            if not user:
                return False
            user.token = None
            user.last_connection_json = None
            db.commit()
            return True
        finally:
            db.close()

    def list_users(self) -> List[Dict]:
        db = SessionLocal()
        try:
            users = db.query(UserModel).all()
            return [{"username": u.username, "role": u.role} for u in users]
        finally:
            db.close()

    def delete_user(self, username: str) -> bool:
        db = SessionLocal()
        try:
            user = db.query(UserModel).filter(UserModel.username == username).first()
            if user:
                db.delete(user)
                db.commit()
                return True
            return False
        finally:
            db.close()

    def reset_password(self, username: str, new_password: str) -> bool:
        db = SessionLocal()
        try:
            user = db.query(UserModel).filter(UserModel.username == username).first()
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
                user.last_connection_json = json.dumps(connection_data)
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
                # Strip null/empty api_keys — treat them as "cleared" so the system fallback activates
                clean_configs = {}
                for provider, cfg in configs.items():
                    clean_cfg = dict(cfg)
                    if not clean_cfg.get("api_key"):  # catches None, "", 0
                        clean_cfg.pop("api_key", None)
                    clean_configs[provider] = clean_cfg
                user.llm_config = json.dumps({
                    "active_provider": active_provider,
                    "providers": clean_configs
                })
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
                return json.loads(user.llm_config)
            return {"active_provider": "groq", "providers": {}}
        finally:
            db.close()

    # ── Per-user RBAC ────────────────────────────────────────────────────
    def get_user_rbac(self, username: str) -> Dict:
        """Return the blocked_tables and blocked_columns for a specific user."""
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

    def update_user_rbac(self, username: str, blocked_tables: List[str], blocked_columns: List[str]) -> bool:
        """Set per-user RBAC restrictions (blocked tables/columns)."""
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

    def log_audit(self, username: str, role: str, question: str, sql_query: str, latency_sec: float = None, success: bool = True):
        db = SessionLocal()
        try:
            log = AuditLogModel(
                username=username,
                role=role,
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

    def list_audit_logs(self, limit: int = 100) -> List[Dict]:
        db = SessionLocal()
        try:
            logs = db.query(AuditLogModel).order_by(AuditLogModel.id.desc()).limit(limit).all()
            return [
                {
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

    def log_observability_event(self, payload: Dict) -> None:
        db = SessionLocal()
        try:
            event = ObservabilityEventModel(
                username=payload.get("username"),
                role=payload.get("role"),
                db_name=payload.get("db_name"),
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
                timestamp=payload.get("timestamp") or datetime.datetime.now().isoformat()
            )
            db.add(event)
            db.commit()
        except Exception as e:
            logger.error(f"Failed to save observability event: {e}")
        finally:
            db.close()

    def log_admin_action(
        self,
        admin_username: str,
        action_type: str,
        target_type: Optional[str] = None,
        target_name: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ) -> None:
        db = SessionLocal()
        try:
            row = AdminActionLogModel(
                admin_username=admin_username,
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

    def list_admin_action_logs(self, limit: int = 100) -> List[Dict]:
        db = SessionLocal()
        try:
            rows = db.query(AdminActionLogModel).order_by(AdminActionLogModel.id.desc()).limit(limit).all()
            return [
                {
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
        username: Optional[str] = None,
        role: Optional[str] = None,
        event_source: Optional[str] = None,
        resource_name: Optional[str] = None,
        details: Optional[Dict[str, Any]] = None
    ) -> None:
        db = SessionLocal()
        try:
            row = SecurityEventModel(
                username=username,
                role=role,
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

    def list_security_events(self, limit: int = 100) -> List[Dict]:
        db = SessionLocal()
        try:
            rows = db.query(SecurityEventModel).order_by(SecurityEventModel.id.desc()).limit(limit).all()
            return [
                {
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

    def get_observability_overview(self, limit: int = OBSERVABILITY_LOOKBACK_LIMIT) -> Dict:
        db = SessionLocal()
        try:
            events = db.query(ObservabilityEventModel)\
                .order_by(ObservabilityEventModel.id.desc())\
                .limit(limit)\
                .all()

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
            for event in events[:20]:
                recent_events.append({
                    "timestamp": event.timestamp,
                    "username": event.username,
                    "role": event.role,
                    "db_name": event.db_name,
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
                    "error_stage": event.error_stage
                })

            security_events = db.query(SecurityEventModel)\
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

    def get_policy_violation_analytics(self, limit: int = 500) -> Dict:
        db = SessionLocal()
        try:
            rows = db.query(SecurityEventModel)\
                .filter(SecurityEventModel.event_type == "policy_denial")\
                .order_by(SecurityEventModel.id.desc())\
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

    def get_slo_panel(self, limit: int = OBSERVABILITY_LOOKBACK_LIMIT) -> Dict:
        db = SessionLocal()
        try:
            events = db.query(ObservabilityEventModel)\
                .order_by(ObservabilityEventModel.id.desc())\
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

    def get_system_stats(self) -> Dict:
        """Aggregate audit log data for system health dashboard."""
        db = SessionLocal()
        try:
            from sqlalchemy import func
            total_queries = db.query(AuditLogModel).count()
            if total_queries == 0:
                return {
                    "total_queries": 0,
                    "success_rate": 0,
                    "avg_latency": 0,
                    "latency_trend": [],
                    "popular_topics": []
                }

            successful_queries = db.query(AuditLogModel).filter(AuditLogModel.success == True).count()
            success_rate = round((successful_queries / total_queries) * 100, 1)

            avg_latency = db.query(func.avg(AuditLogModel.latency_sec)).scalar() or 0
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
            
            # Simple trend logic: pull last 500 logs and group by date
            logs = db.query(AuditLogModel).order_by(AuditLogModel.id.desc()).limit(500).all()
            
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
        username: str,
        db_name: str,
        question: str,
        sql: str,
        summary: str,
        results: Dict,
        visualization: Optional[Dict] = None
    ):
        db = SessionLocal()
        try:
            # Complex objects in results (Decimals, dates) need special handling
            results_json = json.dumps(results, default=self._json_serializable)
            
            visualization_json = None
            if visualization:
                visualization_json = json.dumps(visualization, default=self._json_serializable)
            
            history = ChatHistoryModel(
                username=username,
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
        except Exception as e:
            logger.error(f"Failed to save chat history for {username}: {e}")
        finally:
            db.close()

    def get_chat_history(self, username: str, db_name: str, limit: int = 20, start_date: str = None, end_date: str = None, sort: str = "desc") -> List[Dict]:
        db = SessionLocal()
        try:
            # Isolated by username AND db_name
            query = db.query(ChatHistoryModel)\
                      .filter(ChatHistoryModel.username == username, ChatHistoryModel.db_name == db_name)
            
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
                    "question": i.question,
                    "sql": i.sql_query,
                    "summary": i.summary,
                    "results": json.loads(i.results_json),
                    "visualization": json.loads(i.visualization_json) if i.visualization_json else None,
                    "timestamp": i.timestamp
                } for i in items
            ]
        finally:
            db.close()

    def clear_chat_history(self, username: str, db_name: str) -> bool:
        db = SessionLocal()
        try:
            db.query(ChatHistoryModel)\
              .filter(ChatHistoryModel.username == username, ChatHistoryModel.db_name == db_name)\
              .delete()
            db.commit()
            return True
        except Exception as e:
            logger.error(f"Failed to clear chat history: {e}")
            return False
        finally:
            db.close()

    def delete_history_item(self, username: str, item_id: int) -> bool:
        db = SessionLocal()
        try:
            item = db.query(ChatHistoryModel).filter(ChatHistoryModel.id == item_id, ChatHistoryModel.username == username).first()
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
