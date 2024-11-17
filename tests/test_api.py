from fastapi.testclient import TestClient
from api_server import app  # Assuming you use FastAPI for your API

client = TestClient(app)

def test_trigger_etl(mocker):
    mock_etl = mocker.patch("api_server.run_etl_pipeline")
    response = client.post("/trigger-etl")
    assert response.status_code == 200
    assert response.json() == {"message": "ETL pipeline triggered successfully"}
    mock_etl.assert_called_once()
