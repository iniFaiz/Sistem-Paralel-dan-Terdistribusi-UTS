import asyncio
import datetime
from contextlib import asynccontextmanager
from typing import List, Union
from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from . import database, schemas, models
from .database import SessionLocal, engine
from .models import ProcessedEvent

# Create tables
models.Base.metadata.create_all(bind=engine)

# Stats tracking
stats = {
    "received": 0,
    "duplicate_dropped": 0,
}
start_time = datetime.datetime.now()
event_queue = asyncio.Queue()

async def worker():
    while True:
        event_data = await event_queue.get()
        try:
            await process_event(event_data)
        except Exception as e:
            print(f"Error processing event: {e}")
        finally:
            event_queue.task_done()

async def process_event(event: dict):
    db = SessionLocal()
    try:
        # Check for duplication (topic, event_id)
        existing = db.query(ProcessedEvent).filter(
            ProcessedEvent.topic == event["topic"],
            ProcessedEvent.event_id == event["event_id"]
        ).first()

        if existing:
            stats["duplicate_dropped"] += 1
            print(f" LOG: Duplicate detected and dropped: {event['topic']} | {event['event_id']}")
        else:
            new_event = ProcessedEvent(
                topic=event["topic"],
                event_id=event["event_id"],
                timestamp=event["timestamp"],
                source=event["source"],
                payload=event["payload"]
            )
            db.add(new_event)
            db.commit()
    finally:
        db.close()

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup: Start background consumer
    worker_task = asyncio.create_task(worker())
    yield
    # Shutdown: Stop worker
    worker_task.cancel()
    try:
        await worker_task
    except asyncio.CancelledError:
        pass

app = FastAPI(title="Pub-Sub Log Aggregator", lifespan=lifespan)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.post("/publish")
async def publish_events(events: Union[schemas.EventBase, List[schemas.EventBase]]):
    if isinstance(events, schemas.EventBase):
        events = [events]
    
    for event in events:
        stats["received"] += 1
        await event_queue.put(event.dict())
    
    return {"status": "accepted", "count": len(events)}

@app.get("/events")
async def get_events(topic: str, db: Session = Depends(get_db)):
    events = db.query(ProcessedEvent).filter(ProcessedEvent.topic == topic).all()
    return events

@app.get("/stats", response_model=schemas.StatsResponse)
async def get_stats(db: Session = Depends(get_db)):
    unique_processed = db.query(ProcessedEvent).count()
    topics = [t[0] for t in db.query(ProcessedEvent.topic).distinct().all()]
    uptime = str(datetime.datetime.now() - start_time)
    
    return {
        "received": stats["received"],
        "unique_processed": unique_processed,
        "duplicate_dropped": stats["duplicate_dropped"],
        "topics": topics,
        "uptime": uptime
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
