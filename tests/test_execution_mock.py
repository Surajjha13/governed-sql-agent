from unittest.mock import MagicMock

import app.app_state as app_state
from app.query_service.execution import execute_sql


def _build_mock_connection(rows):
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_cursor.description = [("id",), ("name",)]
    mock_cursor.fetchmany.return_value = rows
    mock_cursor.fetchall.return_value = rows

    mock_cursor_ctx = MagicMock()
    mock_cursor_ctx.__enter__.return_value = mock_cursor
    mock_conn.cursor.return_value = mock_cursor_ctx
    return mock_conn, mock_cursor


def test_execute_sql_uses_session_pool_and_caps_results():
    session_id = "pool_test"
    state = app_state.get_session(session_id)
    state.current_connection = {
        "engine": "postgres",
        "host": "localhost",
        "port": 5432,
        "database": "testdb",
        "user": "user",
        "password": "password",
        "connected": True,
    }

    mock_pool = MagicMock()
    mock_conn, mock_cursor = _build_mock_connection(
        [(1, "one"), (2, "two"), (3, "three")]
    )
    mock_pool.getconn.return_value = mock_conn
    state.db_pool = mock_pool
    state.pool_signature = "postgres|localhost|5432|testdb|user|password"

    results = execute_sql("SELECT id, name FROM table_name", session_id=session_id, row_limit=2)

    assert results["columns"] == ["id", "name"]
    assert results["returned_rows"] == 2
    assert results["truncated"] is True
    assert results["rows"][0]["id"] == 1
    assert results["rows"][1]["name"] == "two"
    mock_cursor.fetchmany.assert_called_once_with(3)
    mock_pool.putconn.assert_called_once()

    app_state.disconnect_db(session_id)


def test_execute_sql_without_connection_returns_error():
    session_id = "no_connection"
    state = app_state.get_session(session_id)
    state.current_connection = None

    results = execute_sql("SELECT id FROM table_name", session_id=session_id)

    assert "error" in results

    app_state.disconnect_db(session_id)
