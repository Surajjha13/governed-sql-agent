import pytest
import datetime
from fastapi.testclient import TestClient
from main import app
import app.app_state as app_state

client = TestClient(app)

def test_session_isolation():
    headers_a = {"X-Session-ID": "user_a", "X-Auth-Token": "standalone-token"}
    headers_b = {"X-Session-ID": "user_b", "X-Auth-Token": "standalone-token"}
    real_session_a = "solo_a_user_a"
    real_session_b = "solo_b_user_b"
    
    resp_a = client.get("/status", headers=headers_a)
    assert resp_a.json()["connected"] is False
    
    state_a = app_state.get_session(real_session_a)
    state_a.current_connection = {"database": "db_a", "connected": True}
    
    resp_a = client.get("/status", headers=headers_a)
    assert resp_a.json()["database"] == "db_a"
    
    resp_b = client.get("/status", headers=headers_b)
    assert resp_b.json()["connected"] is False

def test_chat_history_isolation():
    state_a = app_state.get_session("solo_a_hist_a")
    state_b = app_state.get_session("solo_b_hist_b")
    state_a.chat_history.append({"user": "Hello A"})
    state_b.chat_history.append({"user": "Hello B"})
    assert len(state_a.chat_history) == 1
    assert state_a.chat_history != state_b.chat_history
