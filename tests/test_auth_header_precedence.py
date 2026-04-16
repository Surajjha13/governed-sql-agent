from fastapi.testclient import TestClient

from app.auth.user_manager import User
from main import app


client = TestClient(app)


def test_explicit_auth_header_wins_over_shared_cookie(monkeypatch):
    super_admin = User(
        id=1,
        username="admin",
        password_hash="",
        salt="",
        role="SUPER_ADMIN",
        enterprise_id=None,
        owner_admin_id=None,
        token="super-token",
    )
    system_admin = User(
        id=2,
        username="tenant_admin",
        password_hash="",
        salt="",
        role="SYSTEM_ADMIN",
        enterprise_id=10,
        owner_admin_id=None,
        token="system-token",
    )

    token_map = {
        "super-token": super_admin,
        "system-token": system_admin,
    }

    monkeypatch.setattr(
        "app.auth.api.user_manager.get_user_by_token",
        lambda token: token_map.get(token),
    )

    response = client.get(
        "/auth/enterprises",
        headers={"X-Auth-Token": "super-token"},
        cookies={"sa_auth_token": "system-token"},
    )

    assert response.status_code == 200
