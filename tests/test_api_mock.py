from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock
from app.query_service.api import router, app_state

# Setup TestClient (need a full app or just mount router, but simpler to just call function directly or mock)
# Let's mock the internal calls of run_query

def test_run_query_no_execution():
    mock_req = MagicMock()
    mock_req.question = "Test question"
    
    # Mock dependencies
    with patch('app.query_service.api.build_context') as mock_ctx, \
         patch('app.query_service.api.search_vector_index') as mock_search, \
         patch('app.query_service.api.generate_sql') as mock_gen, \
         patch('app.query_service.api.execute_sql') as mock_exec:
         
        mock_ctx.return_value = {}
        mock_search.return_value = []
        mock_gen.return_value = "SELECT * FROM table"
        
        # Test function directly
        from app.query_service.api import run_query, QueryRequest
        
        req = QueryRequest(question="Test question")
        response = run_query(req)
        
        # Verify response structure
        assert response["sql"] == "SELECT * FROM table"
        assert "results" not in response # Should be removed/commented out
        
        # Verify execute_sql was NOT called
        mock_exec.assert_not_called()
        print("test_run_query_no_execution passed!")

if __name__ == "__main__":
    test_run_query_no_execution()
