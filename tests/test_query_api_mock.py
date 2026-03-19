import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
from main import app

client = TestClient(app)

@patch("app.query_service.api.build_context")
@patch("app.query_service.api.search_vector_index")
@patch("app.query_service.api.app_state")
def test_run_query_mock(mock_app_state, mock_search, mock_build):
    """
    Test the /query endpoint with mocked dependencies.
    """
    # 1. Setup mocks
    mock_build.return_value = "This is a dummy context string."
    mock_search.return_value = [
        {"table": "users", "column": "id", "score": 0.95},
        {"table": "orders", "column": "user_id", "score": 0.88}
    ]
    # Mock app_state attributes that are used in the endpoint
    mock_app_state.normalized_schema = MagicMock()
    mock_app_state.vector_index = MagicMock()
    mock_app_state.vector_metadata = MagicMock()
    mock_app_state.update_activity = MagicMock()

    # 2. Make request
    payload = {"question": "Who are the top customers?"}
    response = client.post("/query", json=payload)

    # 3. Assertions
    assert response.status_code == 200
    data = response.json()
    assert data["question"] == payload["question"]
    assert data["context"] == "This is a dummy context string."
    assert len(data["candidates"]) == 2
    assert data["candidates"][0]["table"] == "users"

    # Verify mocks were called correctly
    mock_build.assert_called_once_with(payload["question"], mock_app_state.normalized_schema)
    mock_search.assert_called_once_with(
        query=payload["question"],
        index=mock_app_state.vector_index,
        metadata=mock_app_state.vector_metadata
    )
    mock_app_state.update_activity.assert_called_once()

@patch("app.query_service.api.build_context")
def test_run_query_error_handling(mock_build):
    """
    Test the /query endpoint's error handling for build_context.
    """
    # 1. Setup mock to raise exception
    mock_build.side_effect = Exception("Mocked error")

    # 2. Make request
    payload = {"question": "Who are the top customers?"}
    response = client.post("/query", json=payload)

    # 3. Assertions
    assert response.status_code == 500
    assert "Failed to build query context: Mocked error" in response.json()["detail"]

def test_run_query_validation_error():
    """
    Test the /query endpoint's input validation (no mocks needed for Pydantic).
    """
    # Too short question
    payload = {"question": "Hi"}
    response = client.post("/query", json=payload)
    assert response.status_code == 422 
    
    # Dangerous question
    payload = {"question": "DROP TABLE users;"}
    response = client.post("/query", json=payload)
    assert response.status_code == 422
