import time
import requests
import uuid
import random

AGGREGATOR_URL = "http://aggregator:8000/publish"

def send_event(topic, event_id, payload):
    event = {
        "topic": topic,
        "event_id": event_id,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "source": "publisher-sim",
        "payload": payload
    }
    try:
        response = requests.post(AGGREGATOR_URL, json=event)
        print(f"Sent event {event_id} to {topic}: {response.status_code}")
    except Exception as e:
        print(f"Failed to send event: {e}")

if __name__ == "__main__":
    print("Publisher simulation started...")
    time.sleep(5) # Wait for aggregator to start
    
    topics = ["logs", "metrics", "alerts"]
    
    while True:
        topic = random.choice(topics)
        event_id = str(uuid.uuid4())
        
        # Send a normal event
        send_event(topic, event_id, {"value": random.random()})
        
        # Occasionally send a duplicate to test deduplication
        if random.random() < 0.2:
            print(f"Simulating duplicate for {event_id}")
            time.sleep(1)
            send_event(topic, event_id, {"value": "duplicate"})
            
        time.sleep(2)
