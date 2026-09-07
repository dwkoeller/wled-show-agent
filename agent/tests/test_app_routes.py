from fastapi.testclient import TestClient
from app_factory import create_app


def test_complete_application_registers_api_routes_and_openapi():
    app = create_app()
    assert all(route.path.startswith('/api') for route in app.routes)
    client = TestClient(app)
    assert client.get('/api/health').status_code == 200
    assert client.get('/api/docs').status_code == 200
    schema = client.get('/api/openapi.json')
    assert schema.status_code == 200
    assert '/api/chat/threads/{thread_id}/messages' in schema.json()['paths']
    assert '/api/fpp/playlists' in schema.json()['paths']
    paths = schema.json()['paths']
    assert '/api/wled/discover' in paths
    assert '/api/wled/calibration' in paths
    assert '/api/wled/preview' in paths
    assert '/api/wled/undo' in paths
