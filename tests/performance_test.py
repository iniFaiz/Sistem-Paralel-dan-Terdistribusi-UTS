import requests
import uuid
import time
import random

URL = "http://localhost:8000/publish"

def run_performance_test(total_events=5000, duplicate_rate=0.2):
    print(f"Starting performance test: {total_events} events, {duplicate_rate*100}% duplicates...")
    
    unique_ids = [str(uuid.uuid4()) for _ in range(int(total_events * (1 - duplicate_rate)))]
    event_ids = unique_ids.copy()
    
    # Add duplicates
    num_duplicates = total_events - len(unique_ids)
    for _ in range(num_duplicates):
        event_ids.append(random.choice(unique_ids))
    
    random.shuffle(event_ids)
    
    start_time = time.time()
    
    # Send in batches of 100
    batch_size = 100
    for i in range(0, total_events, batch_size):
        batch = []
        for j in range(i, min(i + batch_size, total_events)):
            batch.append({
                "topic": "perf_test",
                "event_id": event_ids[j],
                "timestamp": "2026-04-21T00:00:00Z",
                "source": "perf_bot",
                "payload": {"data": "performance_test"}
            })
        
        requests.post(URL, json=batch)
        if i % 1000 == 0:
            print(f"Sent {i} events...")

    end_time = time.time()
    print(f"Finished sending {total_events} events in {end_time - start_time:.2f} seconds.")
    
    # Wait for background worker to catch up
    print("Waiting for aggregator to process...")
    time.sleep(5)
    
    stats = requests.get("http://localhost:8000/stats").json()
    print("Results:")
    print(f" - Received by API: {stats['received']}")
    print(f" - Unique Processed: {stats['unique_processed']}")
    print(f" - Duplicates Dropped: {stats['duplicate_dropped']}")

if __name__ == "__main__":
    # Note: Make sure the aggregator is running before executing this script
    try:
        run_performance_test()
    except Exception as e:
        print(f"Test failed: {e}")
        print("Ensure aggregator is running at http://localhost:8000")
