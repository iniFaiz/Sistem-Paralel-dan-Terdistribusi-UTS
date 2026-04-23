import pytest
from fastapi.testclient import TestClient
from src.main import app, get_db
from src.database import Base, engine, SessionLocal
import time

# Use a separate test database
# For simplicity in this UTS, we'll just use the same one or clear it
# But better to use an in-memory or different file

@pytest.fixture(scope="module")
def client():
    # Setup: Create tables
    Base.metadata.create_all(bind=engine)
    with TestClient(app) as c:
        yield c
    # Teardown: Clear tables
    Base.metadata.drop_all(bind=engine)

def test_publish_single_event(client):
    event = {
        "topic": "test",
        "event_id": "1",
        "timestamp": "2026-04-21T21:00:00Z",
        "source": "sensor-1",
        "payload": {"temp": 25.5}
    }
    response = client.post("/publish", json=event)
    assert response.status_code == 200
    assert response.json()["status"] == "accepted"

def test_deduplication(client):
    event = {
        "topic": "test",
        "event_id": "2",
        "timestamp": "2026-04-21T21:00:00Z",
        "source": "sensor-1",
        "payload": {"temp": 25.5}
    }
    # Send twice
    client.post("/publish", json=event)
    client.post("/publish", json=event)
    
    # Give some time for the background worker
    time.sleep(0.5)
    
    response = client.get("/stats")
    data = response.json()
    # Depending on previous tests, unique_processed should increment by 1
    # and duplicate_dropped should increment by 1
    assert data["duplicate_dropped"] >= 1

def test_get_events(client):
    event = {
        "topic": "topic_x",
        "event_id": "x1",
        "timestamp": "2026-04-21T21:00:00Z",
        "source": "sensor-1",
        "payload": {"data": "val"}
    }
    client.post("/publish", json=event)
    time.sleep(0.5)
    
    response = client.get("/events?topic=topic_x")
    assert response.status_code == 200
    assert len(response.json()) >= 1
    assert response.json()[0]["event_id"] == "x1"

def test_stats_consistency(client):
    response = client.get("/stats")
    assert response.status_code == 200
    data = response.json()
    assert "received" in data
    assert "unique_processed" in data
    assert "duplicate_dropped" in data
    assert "topics" in data
    assert "uptime" in data

def test_batch_publish(client):
    events = [
        {
            "topic": "batch",
            "event_id": f"b{i}",
            "timestamp": "2026-04-21T21:00:00Z",
            "source": "sensor-1",
            "payload": {"i": i}
        } for i in range(5)
    ]
    response = client.post("/publish", json=events)
    assert response.status_code == 200
    assert response.json()["count"] == 5

def test_invalid_event(client):
    # Missing required field 'topic'
    event = {
        "event_id": "err1",
        "timestamp": "now",
        "source": "src",
        "payload": {}
    }
    response = client.post("/publish", json=event)
    assert response.status_code == 422 # Unprocessable Entity

def test_unique_per_topic(client):
    # Same event_id but different topics should be allowed if that's the design
    # But usually event_id is globally unique. 
    # The requirement says dedup based on (topic, event_id).
    event1 = {
        "topic": "topic1",
        "event_id": "same_id",
        "timestamp": "now",
        "source": "src",
        "payload": {}
    }
    event2 = {
        "topic": "topic2",
        "event_id": "same_id",
        "timestamp": "now",
        "source": "src",
        "payload": {}
    }
    client.post("/publish", json=event1)
    client.post("/publish", json=event2)
    time.sleep(0.5)
    
    res1 = client.get("/events?topic=topic1")
    res2 = client.get("/events?topic=topic2")
    assert len(res1.json()) == 1
    assert len(res2.json()) == 1
