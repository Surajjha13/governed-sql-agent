import pytest
import asyncio
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock, AsyncMock
from main import app
from app.schema_service.models import SchemaResponse
from app.auth.api import get_current_user

def override_get_current_user():
    mock_user = MagicMock()
    mock_user.username = "test_user"
    mock_user.role = "admin"
    return mock_user

app.dependency_overrides[get_current_user] = override_get_current_user
client = TestClient(app)

@pytest.fixture
def auth_headers():
    return {"X-Auth-Token": "standalone-token", "X-Session-ID": "test-session"}

@pytest.mark.asyncio
async def test_run_query_mock(auth_headers):
    with patch("app.query_service.api.build_context") as mock_build, \
         patch("app.query_service.api.search_vector_index") as mock_search, \
         patch("app.query_service.api.app_state") as mock_app_state, \
         patch("app.query_service.api.generate_sql", new_callable=AsyncMock) as mock_gen, \
         patch("app.query_service.api.execute_sql", return_value={"columns": ["id"], "rows": []}), \
         patch("app.llm_service.llm_service.generate_summary", new_callable=AsyncMock) as mock_sum, \
         patch("app.llm_service.llm_service.analyze_visualization_intent", new_callable=AsyncMock) as mock_intent, \
         patch("app.llm_service.llm_service.extract_structured_memory", new_callable=AsyncMock) as mock_mem, \
         patch("app.services.visualization_service.VisualizationService.recommend_visualization_intelligent", new_callable=AsyncMock) as mock_viz:

        mock_build.return_value = "Context"
        mock_search.return_value = []
        mock_session_obj = MagicMock()
        mock_session_obj.normalized_schema = SchemaResponse(tables=[], engine="postgres", database="test_db")
        mock_app_state.get_session.return_value = mock_session_obj
        mock_app_state.update_activity = MagicMock()

        mock_gen.return_value = "SELECT * FROM users"
        mock_sum.return_value = "Summary"
        mock_intent.return_value = {"intent": "detail"}
        mock_mem.return_value = {}
        mock_viz.return_value = {"recommended_chart": "table"}

        payload = {"question": "Who are the top customers?"}
        response = client.post("/query", json=payload, headers=auth_headers)

        assert response.status_code == 200
        assert response.json()["sql"] == "SELECT * FROM users"

@pytest.mark.asyncio
async def test_run_query_error_handling(auth_headers):
    with patch("app.query_service.api.build_context") as mock_build, \
         patch("app.query_service.api.app_state") as mock_app_state:
        mock_build.side_effect = Exception("Mocked error")
        mock_app_state.get_session.return_value = MagicMock()
        payload = {"question": "Who are the top customers?"}
        response = client.post("/query", json=payload, headers=auth_headers)
        assert response.status_code == 500
        assert "Failed to build query context: Mocked error" in response.json()["detail"]
