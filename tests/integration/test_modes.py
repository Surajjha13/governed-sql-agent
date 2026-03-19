import pytest
from fastapi.testclient import TestClient
import json
import os
from unittest.mock import patch

# Set environment for tests
os.environ["SQL_AGENT_MODE"] = "enterprise"

from app.main import app

client = TestClient(app)

def test_config_endpoint_enterprise():
    response = client.get("/auth/config")
    assert response.status_code == 200
    data = response.json()
    assert data["mode"] == "enterprise"
    assert data["auth_required"] == True

@patch.dict(os.environ, {"SQL_AGENT_MODE": "solo"})
def test_config_endpoint_solo():
    # Reload app_state to reflect the mocked env var
    import importlib
    import app.app_state
    importlib.reload(app.app_state)
    import app.query_service.api
    importlib.reload(app.query_service.api)
    import app.auth.api
    importlib.reload(app.auth.api)
    
    from app.main import app as updated_app
    test_client = TestClient(updated_app)
    
    response = test_client.get("/auth/config")
    assert response.status_code == 200
    data = response.json()
    assert data["mode"] == "solo"
    assert data["auth_required"] == False
    
    # Clean up by restoring to enterprise mode
    os.environ["SQL_AGENT_MODE"] = "enterprise"
    importlib.reload(app.app_state)
    importlib.reload(app.query_service.api)
    importlib.reload(app.auth.api)
