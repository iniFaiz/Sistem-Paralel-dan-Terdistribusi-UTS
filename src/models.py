from sqlalchemy import Column, Integer, String, JSON, DateTime
import datetime
from .database import Base

class ProcessedEvent(Base):
    __tablename__ = "processed_events"

    id = Column(Integer, primary_key=True, index=True)
    topic = Column(String, index=True)
    event_id = Column(String, index=True)
    timestamp = Column(String)
    source = Column(String)
    payload = Column(JSON)
    processed_at = Column(DateTime, default=datetime.datetime.utcnow)
