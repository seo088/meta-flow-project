"""공유 타입 정의 — 모든 서비스에서 이 파일만 import. 중복 정의 금지."""
from enum import Enum
from typing import Optional, List
from pydantic import BaseModel, Field
from datetime import datetime


class TrashType(str, Enum):
    PLASTIC = "plastic"
    FOOD = "food"
    GENERAL = "general"
    LARGE = "large"
    UNKNOWN = "unknown"


class Severity(str, Enum):
    LOW = "low"
    MID = "mid"
    HIGH = "high"
    BOSS = "boss"


class ReportStatus(str, Enum):
    PENDING = "pending"
    AI_VERIFIED = "ai_verified"
    ASSIGNED = "assigned"
    COMPLETED = "completed"


class DataSource(str, Enum):
    SNS = "sns"
    DRONE = "drone"
    MOBILITY = "mobility"
    UNITY_SIM = "unity_sim"


class ZoneKey(str, Enum):
    KU_CAMPUS = "KU_CAMPUS"
    EUNPA = "EUNPA"
    SAEMANGEUM = "SAEMANGEUM"
    GEUMGANG = "GEUMGANG"


class GeoPoint(BaseModel):
    lat: float = Field(..., ge=-90, le=90)
    lon: float = Field(..., ge=-180, le=180)


class APIResponse(BaseModel):
    success: bool = True
    data: Optional[dict] = None
    error: Optional[str] = None
    code: Optional[str] = None
