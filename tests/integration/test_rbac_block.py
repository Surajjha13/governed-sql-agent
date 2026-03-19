import pytest
from unittest.mock import patch, MagicMock, AsyncMock
from app.schema_service.models import SchemaResponse
from app.query_service.api import QueryRequest, run_query

class MockUser:
    def __init__(self, role, username="test_user"):
        self.role = role
        self.username = username

@pytest.mark.asyncio
async def test_rbac_blocking():
    req = QueryRequest(question="show me the categories table")
    user = MockUser(role="DATA_SCIENTIST")

    with patch("app.app_state.get_session") as mock_get_session, \
         patch("app.query_service.api.load_policies") as mock_load:
         
        mock_state = MagicMock()
        mock_state.normalized_schema = SchemaResponse(engine="postgres", database="test_db", tables=[], metrics=[])
        mock_get_session.return_value = mock_state
        
        mock_load.return_value = {
            "policies": {
                "DATA_SCIENTIST": {
                    "blocked_tables": ["categories"]
                }
            }
        }
        
        result = await run_query(req=req, x_session_id="test", current_user=user)
        
        assert result["sql"] is None
        assert "you are not allowed" in result["summary"].lower()
