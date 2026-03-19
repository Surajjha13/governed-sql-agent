import pytest
from fastapi.testclient import TestClient
from main import app
import app.app_state as app_state

client = TestClient(app)

def test_session_isolation():
    """
    Verify that two different sessions have isolated connection states.
    """
    session_a = "user_a_session"
    session_b = "user_b_session"
    
    # 1. Initially both should be disconnected
    resp_a = client.get("/status", headers={"X-Session-ID": session_a})
    assert resp_a.status_code == 200
    assert resp_a.json()["connected"] is False
    
    resp_b = client.get("/status", headers={"X-Session-ID": session_b})
    assert resp_b.status_code == 200
    assert resp_b.json()["connected"] is False
    
    # 2. Mock a connection for Session A by manually injecting into app_state
    # (Easier than full DB connect for isolation check)
    state_a = app_state.get_session(session_a)
    state_a.current_connection = {"database": "db_a", "connected": True}
    
    # 3. Check Session A status (should be connected)
    resp_a = client.get("/status", headers={"X-Session-ID": session_a})
    assert resp_a.json()["database"] == "db_a"
    assert resp_a.json()["connected"] is True
    
    # 4. Check Session B status (should still be disconnected)
    resp_b = client.get("/status", headers={"X-Session-ID": session_b})
    assert resp_b.json()["connected"] is False
    assert "db_a" not in resp_b.text

def test_chat_history_isolation():
    """
    Verify that chat history is isolated between sessions.
    """
    session_a = "user_a_history"
    session_b = "user_b_history"
    
    state_a = app_state.get_session(session_a)
    state_b = app_state.get_session(session_b)
    
    state_a.chat_history.append({"user": "Hello from A", "assistant": "Hi A"})
    state_b.chat_history.append({"user": "Hello from B", "assistant": "Hi B"})
    
    # Verify via state retrieval (since /query API would actually run a query)
    # We can also check if the state objects are different
    assert state_a is not state_b
    assert state_a.chat_history != state_b.chat_history
    assert len(state_a.chat_history) == 1
    assert state_a.chat_history[0]["user"] == "Hello from A"

def test_session_cleanup():
    """
    Verify that idle session cleanup works for individual sessions.
    """
    import datetime
    session_idle = "idle_session"
    session_active = "active_session"
    
    state_idle = app_state.get_session(session_idle)
    state_active = app_state.get_session(session_active)
    
    state_idle.current_connection = {"db": "idle"}
    state_active.current_connection = {"db": "active"}
    
    # Set idle session activity to way in the past
    state_idle.last_activity = datetime.datetime.now() - datetime.timedelta(seconds=app_state.IDLE_TIMEOUT + 10)
    # Set active session activity to now
    state_active.last_activity = datetime.datetime.now()
    
    # Run cleanup
    app_state.check_and_disconnect()
    
    # Check sessions
    assert session_idle not in app_state.sessions
    assert session_active in app_state.sessions
    assert app_state.sessions[session_active].current_connection["db"] == "active"
