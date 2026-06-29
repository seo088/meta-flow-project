"""Kafka 이벤트 스키마 — 모든 Producer/Consumer가 이 클래스 사용."""
import uuid
from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field
from .types import TrashType, Severity, DataSource, GeoPoint


def _id() -> str:
    return str(uuid.uuid4())


class ReportCreatedEvent(BaseModel):
    event_id: str = Field(default_factory=_id)
    event_type: str = "plogging.report.created"
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    report_id: str
    reporter_id: Optional[str] = None
    location: GeoPoint
    zone_id: Optional[str] = None
    trash_type: TrashType
    severity: Severity
    image_urls: List[str] = []
    source: DataSource


class AIDetectedEvent(BaseModel):
    event_id: str = Field(default_factory=_id)
    event_type: str = "plogging.ai.detected"
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    report_id: str
    trash_type: TrashType
    severity: Severity
    confidence: float
    bounding_boxes: Optional[List[dict]] = None
    model_version: str = "dummy_v1"


class MissionCompletedEvent(BaseModel):
    event_id: str = Field(default_factory=_id)
    event_type: str = "plogging.mission.completed"
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    cleanup_id: str
    report_id: str
    cleaner_id: Optional[str] = None
    cleaner_type: str = "human"
    verify_score: float = 0.0
    points_awarded: int = 0
    zone_id: Optional[str] = None


class DroneStreamEvent(BaseModel):
    event_id: str = Field(default_factory=_id)
    event_type: str = "drone.stream.uploaded"
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    drone_id: str
    video_url: str
    location: GeoPoint
    zone_id: Optional[str] = None
    duration_sec: int = 0


class PointsUpdatedEvent(BaseModel):
    event_id: str = Field(default_factory=_id)
    event_type: str = "user.points.updated"
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    user_id: str
    delta: int
    new_total: int
    reason: str
