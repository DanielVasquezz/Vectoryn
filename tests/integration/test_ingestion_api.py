import pytest
from fastapi.testclient import TestClient
from mock import MagicMock, patch
import json

# ============================================================
# LESSON: API Integration Testing
# ============================================================
# Here we test that the API components are correctly connected.
# We use TestClient to simulate real HTTP calls.
# 
# We mock the Kafka Producer because we don't want to send 
# "junk" messages to a real Kafka instance during tests.
# What matters to us is: If I send a valid JSON, does the API 
# attempt to send it to Kafka and respond with 200?
# ============================================================

# Mock Kafka before importing the app
with patch("confluent_kafka.Producer"):
    from ingestion.main import app

client = TestClient(app)

def test_health_endpoint():
    """Tests that the health check responds with 200."""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "healthy"

def test_ingest_document_success():
    """
    Tests the Happy Path: valid payload -> queued in Kafka -> 200.
    """
    # We mock the global producer that was already instantiated in main.py
    with patch("ingestion.main.producer.produce") as mock_produce:
        test_payload = {
            "content": "This is a test document for integration."
        }
        
        response = client.post("/ingest", json=test_payload)
        
        assert response.status_code == 200
        assert response.json()["status"] == "accepted"
        
        # Verify that an attempt was made to send to Kafka exactly once
        mock_produce.assert_called_once()
        # Verify that the data sent to 'produce' are JSON bytes
        args, kwargs = mock_produce.call_args
        sent_data = json.loads(kwargs["value"].decode("utf-8"))
        assert sent_data["content"] == test_payload["content"]

def test_ingest_document_invalid_payload():
    """
    Tests Pydantic validation: empty document -> 422.
    """
    test_payload = {
        "content": ""  # Invalid according to our validator
    }
    
    response = client.post("/ingest", json=test_payload)
    
    # FastAPI returns 422 by default when schema validation fails
    assert response.status_code == 422
    assert "Document content cannot be empty" in response.text

def test_ingest_document_too_long():
    """
    Tests character limits (Defense in Depth).
    """
    test_payload = {
        "content": "a" * 100_001  # Exceeds the 100k limit
    }
    
    response = client.post("/ingest", json=test_payload)
    assert response.status_code == 422
    assert "exceeds the limit" in response.text