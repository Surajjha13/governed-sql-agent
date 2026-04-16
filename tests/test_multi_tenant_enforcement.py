import asyncio
from datetime import datetime

import pytest
from fastapi import HTTPException
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

import app.auth.user_manager as user_manager_module
import app.query_service.api as query_api
from app.auth.api import UpdateEnterpriseRequest, update_enterprise
from app.auth.database import Base
from app.auth.models import EnterpriseModel, ObservabilityEventModel, SecurityEventModel, UserModel
from app.auth.user_manager import User, UserManager


def build_manager(tmp_path, monkeypatch):
    db_path = tmp_path / "tenant_enforcement.db"
    engine = create_engine(f"sqlite:///{db_path}", connect_args={"check_same_thread": False})
    testing_session = sessionmaker(autocommit=False, autoflush=False, bind=engine)
    Base.metadata.create_all(bind=engine)
    monkeypatch.setattr(user_manager_module, "SessionLocal", testing_session)
    manager = UserManager.__new__(UserManager)
    return manager, testing_session


def add_user(session, manager, username, password, role, enterprise_id=None, owner_admin_id=None):
    salt = f"salt_{username}"
    user = UserModel(
        username=username,
        password_hash=manager._hash_password(password, salt),
        salt=salt,
        role=role,
        enterprise_id=enterprise_id,
        owner_admin_id=owner_admin_id,
        is_active=True,
    )
    session.add(user)
    session.commit()
    session.refresh(user)
    return user


def test_inactive_enterprise_blocks_login_and_existing_tokens(tmp_path, monkeypatch):
    manager, testing_session = build_manager(tmp_path, monkeypatch)
    session = testing_session()
    try:
        enterprise = EnterpriseModel(name="Acme", is_active=True, status="active", created_at=datetime.now().isoformat())
        session.add(enterprise)
        session.commit()
        session.refresh(enterprise)

        add_user(
            session,
            manager,
            username="tenant_user",
            password="very-secure-pass",
            role="ANALYST",
            enterprise_id=enterprise.id,
            owner_admin_id=11,
        )
    finally:
        session.close()

    authenticated = manager.authenticate("tenant_user", "very-secure-pass")
    assert authenticated is not None

    session = testing_session()
    try:
        enterprise = session.query(EnterpriseModel).first()
        enterprise.is_active = False
        session.commit()
    finally:
        session.close()

    assert manager.authenticate("tenant_user", "very-secure-pass") is None
    assert manager.get_user_by_token(authenticated.token) is None


def test_system_admin_mutations_are_limited_to_owned_users(tmp_path, monkeypatch):
    manager, testing_session = build_manager(tmp_path, monkeypatch)
    policy_file = tmp_path / "access_policy.json"
    monkeypatch.setattr(user_manager_module, "_policy_file_path", lambda: str(policy_file))

    session = testing_session()
    try:
        enterprise = EnterpriseModel(name="Scoped", is_active=True, status="active", created_at=datetime.now().isoformat())
        session.add(enterprise)
        session.commit()
        session.refresh(enterprise)
        enterprise_id = enterprise.id

        admin_a = add_user(session, manager, "admin_a", "admin-pass-123", "SYSTEM_ADMIN", enterprise.id, None)
        admin_b = add_user(session, manager, "admin_b", "admin-pass-456", "SYSTEM_ADMIN", enterprise.id, None)
        admin_a_id = admin_a.id
        admin_b_username = admin_b.username
        add_user(session, manager, "owned_user", "owned-pass-123", "ANALYST", enterprise.id, admin_a.id)
        add_user(session, manager, "peer_user", "peer-pass-123", "ANALYST", enterprise.id, admin_b.id)
    finally:
        session.close()

    current_admin = User(
        id=admin_a_id,
        username="admin_a",
        password_hash="",
        salt="",
        role="SYSTEM_ADMIN",
        enterprise_id=enterprise_id,
        owner_admin_id=None,
    )

    assert manager.reset_password("owned_user", "new-owned-pass-123", current_user=current_admin) is True
    assert manager.reset_password("peer_user", "new-peer-pass-123", current_user=current_admin) is False
    assert manager.reset_password(admin_b_username, "new-admin-pass-123", current_user=current_admin) is False

    assert manager.get_user_rbac("owned_user", current_user=current_admin) == {
        "blocked_tables": [],
        "blocked_columns": [],
    }
    assert manager.get_user_rbac("peer_user", current_user=current_admin) is None
    assert manager.update_user_rbac(
        "owned_user",
        ["payments"],
        ["payments.secret"],
        current_user=current_admin,
    ) is True
    assert manager.update_user_rbac(
        "peer_user",
        ["orders"],
        ["orders.internal_note"],
        current_user=current_admin,
    ) is False

    assert manager.delete_user("peer_user", current_user=current_admin) is False
    assert manager.delete_user("owned_user", current_user=current_admin) is True


def test_observability_security_alerts_use_scoped_security_events(tmp_path, monkeypatch):
    manager, testing_session = build_manager(tmp_path, monkeypatch)
    session = testing_session()
    now = datetime.now().isoformat()
    try:
        enterprise = EnterpriseModel(name="Observe", is_active=True, status="active", created_at=now)
        session.add(enterprise)
        session.commit()
        session.refresh(enterprise)
        enterprise_id = enterprise.id

        session.add(
            ObservabilityEventModel(
                enterprise_id=enterprise.id,
                owner_admin_id=10,
                username="owned_user",
                role="ANALYST",
                db_name="db1",
                total_ms=100.0,
                success=True,
                had_rate_limit=False,
                timestamp=now,
            )
        )
        session.add(
            SecurityEventModel(
                enterprise_id=enterprise.id,
                owner_admin_id=10,
                username="owned_user",
                role="ANALYST",
                event_type="login_failure",
                severity="high",
                event_source="auth",
                timestamp=now,
            )
        )
        session.add(
            SecurityEventModel(
                enterprise_id=enterprise.id,
                owner_admin_id=99,
                username="other_user",
                role="ANALYST",
                event_type="login_failure",
                severity="high",
                event_source="auth",
                timestamp=now,
            )
        )
        session.commit()
    finally:
        session.close()

    current_admin = User(
        id=10,
        username="admin_a",
        password_hash="",
        salt="",
        role="SYSTEM_ADMIN",
        enterprise_id=enterprise_id,
        owner_admin_id=None,
    )

    overview = manager.get_observability_overview(current_admin, limit=20)
    assert overview["summary"]["security_event_count"] == 1


def test_system_admin_observability_includes_own_events_and_question_details(tmp_path, monkeypatch):
    manager, testing_session = build_manager(tmp_path, monkeypatch)
    session = testing_session()
    now = datetime.now().isoformat()
    try:
        enterprise = EnterpriseModel(name="Telemetry", is_active=True, status="active", created_at=now)
        session.add(enterprise)
        session.commit()
        session.refresh(enterprise)
        enterprise_id = enterprise.id

        session.add_all([
            ObservabilityEventModel(
                enterprise_id=enterprise_id,
                owner_admin_id=None,
                username="admin_a",
                role="SYSTEM_ADMIN",
                db_name="warehouse",
                question="How many orders closed today?",
                sql_query="select count(*) from orders;",
                request_id="req-1",
                total_ms=120.0,
                success=True,
                had_rate_limit=False,
                timestamp=now,
            ),
            ObservabilityEventModel(
                enterprise_id=enterprise_id,
                owner_admin_id=10,
                username="owned_user",
                role="ANALYST",
                db_name="warehouse",
                question="Show my top customers",
                sql_query="select * from customers;",
                request_id="req-2",
                total_ms=220.0,
                success=True,
                had_rate_limit=False,
                timestamp=now,
            ),
            ObservabilityEventModel(
                enterprise_id=enterprise_id,
                owner_admin_id=99,
                username="other_user",
                role="ANALYST",
                db_name="warehouse",
                question="Show peer data",
                sql_query="select * from peer_data;",
                request_id="req-3",
                total_ms=320.0,
                success=True,
                had_rate_limit=False,
                timestamp=now,
            ),
        ])
        session.commit()
    finally:
        session.close()

    current_admin = User(
        id=10,
        username="admin_a",
        password_hash="",
        salt="",
        role="SYSTEM_ADMIN",
        enterprise_id=enterprise_id,
        owner_admin_id=None,
    )

    overview = manager.get_observability_overview(current_admin, limit=20)
    usernames = {event["username"] for event in overview["recent_events"]}

    assert usernames == {"admin_a", "owned_user"}
    own_event = next(event for event in overview["recent_events"] if event["username"] == "admin_a")
    assert own_event["question"] == "How many orders closed today?"
    assert own_event["sql_query"] == "select count(*) from orders;"
    assert own_event["request_id"] == "req-1"


def test_list_enterprises_includes_admin_and_employee_counts(tmp_path, monkeypatch):
    manager, testing_session = build_manager(tmp_path, monkeypatch)
    session = testing_session()
    try:
        enterprise = EnterpriseModel(name="Aviation Corp", is_active=True, status="active", created_at=datetime.now().isoformat())
        session.add(enterprise)
        session.commit()
        session.refresh(enterprise)

        add_user(session, manager, "aviation_admin", "admin-pass-123", "SYSTEM_ADMIN", enterprise.id, None)
        add_user(session, manager, "aviation_analyst", "analyst-pass-123", "ANALYST", enterprise.id, 1)
        add_user(session, manager, "aviation_viewer", "viewer-pass-123", "VIEWER", enterprise.id, 1)
    finally:
        session.close()

    enterprises = manager.list_enterprises()
    aviation = next(item for item in enterprises if item["name"] == "Aviation Corp")

    assert aviation["admin_count"] == 1
    assert aviation["employee_count"] == 2
    assert aviation["total_users"] == 3


def test_delete_enterprise_cascades_enterprise_users_and_rbac(tmp_path, monkeypatch):
    manager, testing_session = build_manager(tmp_path, monkeypatch)
    policy_file = tmp_path / "access_policy.json"
    policy_file.write_text(
        '{"users":{"aviation_admin":{"blocked_tables":["payments"],"blocked_columns":[]},"aviation_user":{"blocked_tables":["orders"],"blocked_columns":[]}},"roles":{}}',
        encoding="utf-8",
    )
    monkeypatch.setattr(user_manager_module, "_policy_file_path", lambda: str(policy_file))

    session = testing_session()
    try:
        enterprise = EnterpriseModel(name="Aviation Corp", is_active=True, status="active", created_at=datetime.now().isoformat())
        session.add(enterprise)
        session.commit()
        session.refresh(enterprise)
        enterprise_id = enterprise.id

        add_user(session, manager, "aviation_admin", "admin-pass-123", "SYSTEM_ADMIN", enterprise_id, None)
        add_user(session, manager, "aviation_user", "user-pass-123", "ANALYST", enterprise_id, 1)
        add_user(session, manager, "other_user", "other-pass-123", "ANALYST", None, None)
    finally:
        session.close()

    deleted = manager.delete_enterprise(enterprise_id)
    assert deleted is not None
    assert deleted["enterprise_name"] == "Aviation Corp"
    assert deleted["deleted_users"] == 2
    assert deleted["deleted_admins"] == 1

    session = testing_session()
    try:
        assert session.query(EnterpriseModel).filter(EnterpriseModel.id == enterprise_id).first() is None
        assert session.query(UserModel).filter(UserModel.username == "aviation_admin").first() is None
        assert session.query(UserModel).filter(UserModel.username == "aviation_user").first() is None
        assert session.query(UserModel).filter(UserModel.username == "other_user").first() is not None
    finally:
        session.close()

    policy_payload = policy_file.read_text(encoding="utf-8")
    assert "aviation_admin" not in policy_payload
    assert "aviation_user" not in policy_payload


def test_super_admin_list_users_hides_nested_enterprise_owned_users(tmp_path, monkeypatch):
    manager, testing_session = build_manager(tmp_path, monkeypatch)
    session = testing_session()
    try:
        enterprise = EnterpriseModel(name="Aviation Corp", is_active=True, status="active", created_at=datetime.now().isoformat())
        session.add(enterprise)
        session.commit()
        session.refresh(enterprise)

        super_admin = add_user(session, manager, "admin_root", "root-pass-123", "SUPER_ADMIN", None, None)
        aviation_admin = UserModel(
            username="aviation_admin",
            password_hash=manager._hash_password("admin-pass-123", "salt_admin"),
            salt="salt_admin",
            role="SYSTEM_ADMIN",
            enterprise_id=enterprise.id,
            owner_admin_id=None,
            created_by_id=super_admin.id,
            is_active=True,
        )
        session.add(aviation_admin)
        session.commit()
        session.refresh(aviation_admin)

        nested_manager = UserModel(
            username="avi_manager",
            password_hash=manager._hash_password("manager-pass-123", "salt_manager"),
            salt="salt_manager",
            role="SYSTEM_ADMIN",
            enterprise_id=enterprise.id,
            owner_admin_id=aviation_admin.id,
            created_by_id=aviation_admin.id,
            is_active=True,
        )
        nested_employee = UserModel(
            username="avi_employee",
            password_hash=manager._hash_password("employee-pass-123", "salt_employee"),
            salt="salt_employee",
            role="DATA_SCIENTIST",
            enterprise_id=enterprise.id,
            owner_admin_id=aviation_admin.id,
            created_by_id=aviation_admin.id,
            is_active=True,
        )
        direct_super_user = UserModel(
            username="direct_user",
            password_hash=manager._hash_password("direct-pass-123", "salt_direct"),
            salt="salt_direct",
            role="ANALYST",
            enterprise_id=enterprise.id,
            owner_admin_id=super_admin.id,
            created_by_id=super_admin.id,
            is_active=True,
        )
        session.add_all([nested_manager, nested_employee, direct_super_user])
        session.commit()
    finally:
        session.close()

    current_super_admin = User(
        id=super_admin.id,
        username="admin_root",
        password_hash="",
        salt="",
        role="SUPER_ADMIN",
        enterprise_id=None,
        owner_admin_id=None,
    )

    usernames = {user["username"] for user in manager.list_users(current_user=current_super_admin)}
    assert "admin_root" in usernames
    assert "aviation_admin" in usernames
    assert "direct_user" in usernames
    assert "avi_manager" not in usernames
    assert "avi_employee" not in usernames


def test_enterprise_export_passes_full_user_object(monkeypatch):
    captured = {}

    class DummyState:
        current_connection = {"database": "analytics"}
        chat_history = []

    user = User(
        id=7,
        username="tenant_user",
        password_hash="",
        salt="",
        role="ANALYST",
        enterprise_id=5,
        owner_admin_id=2,
    )

    monkeypatch.setattr(query_api.app_state, "SYSTEM_MODE", "enterprise", raising=False)
    monkeypatch.setattr(query_api.app_state, "get_session", lambda session_id: DummyState())

    def fake_get_chat_history(current_user, db_name):
        captured["current_user"] = current_user
        captured["db_name"] = db_name
        return []

    monkeypatch.setattr(query_api.user_manager, "get_chat_history", fake_get_chat_history)

    with pytest.raises(HTTPException) as exc:
        asyncio.run(query_api.export_excel(current_user=user, x_session_id="sess-1", item_ids=None))

    assert exc.value.status_code == 404
    assert captured["current_user"] is user
    assert captured["db_name"] == "analytics"


def test_update_enterprise_route_logs_without_invalid_kwargs(monkeypatch):
    captured = {}
    super_admin = User(
        id=1,
        username="admin",
        password_hash="",
        salt="",
        role="SUPER_ADMIN",
        enterprise_id=None,
        owner_admin_id=None,
    )

    monkeypatch.setattr("app.auth.api.user_manager.update_enterprise_status", lambda enterprise_id, is_active: True)

    def fake_log_admin_action(**kwargs):
        captured.update(kwargs)

    monkeypatch.setattr("app.auth.api.user_manager.log_admin_action", fake_log_admin_action)

    response = asyncio.run(
        update_enterprise(
            enterprise_id=42,
            req=UpdateEnterpriseRequest(is_active=False),
            current_user=super_admin,
        )
    )

    assert response == {"message": "Enterprise status updated"}
    assert captured["target_name"] == "42"


def test_filter_history_by_export_ids_accepts_request_id_and_item_id():
    history = [
        {"id": 101, "request_id": "req-101", "question": "first"},
        {"id": 202, "request_id": "req-202", "question": "second"},
    ]

    by_item_id = query_api._filter_history_by_export_ids(history, "101")
    by_request_id = query_api._filter_history_by_export_ids(history, "req-202")

    assert by_item_id == [history[0]]
    assert by_request_id == [history[1]]


def test_export_excel_reexecutes_full_results_when_filtered_by_request_id(monkeypatch):
    captured = {}

    class DummyState:
        current_connection = {"database": "analytics"}
        chat_history = [
            {
                "id": 501,
                "request_id": "req-501",
                "question": "List long films",
                "sql": "SELECT title, length FROM film ORDER BY length DESC",
                "results": {
                    "columns": ["title", "length"],
                    "rows": [{"title": "Preview Film", "length": 185}],
                    "total_count": 1000,
                    "truncated": True,
                },
            }
        ]

    user = User(
        id=8,
        username="solo_user",
        password_hash="",
        salt="",
        role="SOLO_USER",
        enterprise_id=None,
        owner_admin_id=None,
    )

    def fake_execute_sql(sql, session_id, row_limit):
        captured["sql"] = sql
        captured["session_id"] = session_id
        captured["row_limit"] = row_limit
        return {
            "columns": ["title", "length"],
            "rows": [
                {"title": "Preview Film", "length": 185},
                {"title": "Full Film", "length": 184},
            ],
        }

    monkeypatch.setattr(query_api.app_state, "SYSTEM_MODE", "solo", raising=False)
    monkeypatch.setattr(query_api.app_state, "get_session", lambda session_id: DummyState())
    monkeypatch.setattr("app.query_service.execution.execute_sql", fake_execute_sql)

    response = asyncio.run(
        query_api.export_excel(current_user=user, x_session_id="sess-9", item_ids="req-501")
    )

    assert captured["sql"] == "SELECT title, length FROM film ORDER BY length DESC"
    assert captured["session_id"] == "solo_user_sess-9"
    assert captured["row_limit"] == 0
    assert response.headers["content-disposition"] == 'attachment; filename="query_results_501.xlsx"'
