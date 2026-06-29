# 메타 플로깅 — 백엔드 마이크로서비스 설계

> **참조**: `00_overview.md` §6 Kafka 토픽, `01_infra.md` §1 디렉터리  
> **원칙**: `shared/core/`는 타입·이벤트·인증·로거만 관리. 비즈니스 로직은 각 서비스에 귀속.

---

## 1. 공통 라이브러리 (`shared/core/`)

### 1.1 `shared/core/types.py` — 공유 타입 (중복 정의 금지)

```python
from enum import Enum
from typing import Optional, List
from pydantic import BaseModel, Field
from datetime import datetime

class TrashType(str, Enum):
    PLASTIC = "plastic"; FOOD = "food"
    GENERAL = "general"; LARGE = "large"; UNKNOWN = "unknown"

class Severity(str, Enum):
    LOW = "low"; MID = "mid"; HIGH = "high"; BOSS = "boss"

class ReportStatus(str, Enum):
    PENDING = "pending"; AI_VERIFIED = "ai_verified"
    ASSIGNED = "assigned"; COMPLETED = "completed"

class DataSource(str, Enum):
    SNS = "sns"; DRONE = "drone"
    MOBILITY = "mobility"; UNITY_SIM = "unity_sim"

class ZoneKey(str, Enum):
    KU_CAMPUS = "KU_CAMPUS"; EUNPA = "EUNPA"
    SAEMANGEUM = "SAEMANGEUM"; GEUMGANG = "GEUMGANG"

class GeoPoint(BaseModel):
    lat: float = Field(..., ge=-90, le=90)
    lon: float = Field(..., ge=-180, le=180)

class APIResponse(BaseModel):
    success: bool = True
    data: Optional[dict] = None
    error: Optional[str] = None
    code: Optional[str] = None
```

### 1.2 `shared/core/events.py` — Kafka 이벤트 스키마

```python
"""모든 Kafka 이벤트는 여기서만 정의. Producer/Consumer 모두 이 클래스 사용."""
import uuid
from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field
from .types import TrashType, Severity, DataSource, GeoPoint

def _id() -> str:
    return str(uuid.uuid4())

class ReportCreatedEvent(BaseModel):
    event_id:    str = Field(default_factory=_id)
    event_type:  str = "plogging.report.created"
    timestamp:   datetime = Field(default_factory=datetime.utcnow)
    report_id:   str
    reporter_id: Optional[str]
    location:    GeoPoint
    zone_id:     Optional[str]
    trash_type:  TrashType
    severity:    Severity
    image_urls:  List[str]
    source:      DataSource

class AIDetectedEvent(BaseModel):
    event_id:       str = Field(default_factory=_id)
    event_type:     str = "plogging.ai.detected"
    timestamp:      datetime = Field(default_factory=datetime.utcnow)
    report_id:      str
    trash_type:     TrashType
    severity:       Severity
    confidence:     float
    bounding_boxes: Optional[List[dict]]
    model_version:  str

class MissionCompletedEvent(BaseModel):
    event_id:       str = Field(default_factory=_id)
    event_type:     str = "plogging.mission.completed"
    timestamp:      datetime = Field(default_factory=datetime.utcnow)
    cleanup_id:     str
    report_id:      str
    cleaner_id:     Optional[str]
    cleaner_type:   str
    verify_score:   float
    points_awarded: int
    zone_id:        Optional[str]

class DroneStreamEvent(BaseModel):
    event_id:     str = Field(default_factory=_id)
    event_type:   str = "drone.stream.uploaded"
    timestamp:    datetime = Field(default_factory=datetime.utcnow)
    drone_id:     str
    video_url:    str
    location:     GeoPoint
    zone_id:      Optional[str]
    duration_sec: int = 0

class PointsUpdatedEvent(BaseModel):
    event_id:   str = Field(default_factory=_id)
    event_type: str = "user.points.updated"
    timestamp:  datetime = Field(default_factory=datetime.utcnow)
    user_id:    str
    delta:      int
    new_total:  int
    reason:     str
```

### 1.3 `shared/core/kafka_client.py` — Kafka 팩토리

```python
"""각 서비스는 이 팩토리만 사용. confluent_kafka 직접 임포트 금지."""
import json, asyncio, logging
from typing import Callable, Type
from confluent_kafka import Producer, Consumer
from pydantic import BaseModel

logger = logging.getLogger(__name__)

class KafkaProducerClient:
    def __init__(self, bootstrap_servers: str):
        self._p = Producer({
            "bootstrap.servers": bootstrap_servers,
            "acks": "all", "retries": 3,
            "linger.ms": 10, "compression.type": "lz4",
        })

    def produce(self, topic: str, event: BaseModel, key: str = None):
        self._p.produce(
            topic=topic,
            value=event.model_dump_json().encode(),
            key=key.encode() if key else None,
            on_delivery=lambda e, m: logger.error(f"Kafka err: {e}") if e else None,
        )
        self._p.poll(0)

    def flush(self):
        self._p.flush()


class KafkaConsumerClient:
    def __init__(self, servers: str, group_id: str, topics: list[str]):
        self._c = Consumer({
            "bootstrap.servers": servers,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        })
        self._c.subscribe(topics)

    async def consume_loop(self, handler: Callable, event_class: Type, stop: asyncio.Event):
        loop = asyncio.get_event_loop()
        while not stop.is_set():
            msg = await loop.run_in_executor(None, lambda: self._c.poll(1.0))
            if msg is None or msg.error():
                continue
            try:
                data = json.loads(msg.value().decode())
                await handler(data if event_class is dict else event_class(**data))
                self._c.commit(msg)
            except Exception as e:
                logger.error(f"Event error: {e}", exc_info=True)

    def close(self):
        self._c.close()
```

### 1.4 `shared/core/auth.py` — JWT 미들웨어

```python
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
import os

security = HTTPBearer()

def get_current_user(cred: HTTPAuthorizationCredentials = Depends(security)) -> dict:
    try:
        payload = jwt.decode(
            cred.credentials,
            os.environ["JWT_SECRET_KEY"],
            algorithms=[os.environ.get("JWT_ALGORITHM", "HS256")]
        )
        uid = payload.get("sub")
        if not uid:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid token")
        return {"user_id": uid, "role": payload.get("role", "user")}
    except JWTError:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Could not validate credentials")

def require_role(role: str):
    def _check(u: dict = Depends(get_current_user)):
        if u["role"] != role:
            raise HTTPException(status.HTTP_403_FORBIDDEN, "Insufficient permissions")
        return u
    return _check
```

### 1.5 `shared/core/gamification.py` — XP·레벨 계산

```python
"""포인트 계산 및 레벨 로직. 모든 서비스에서 import해 사용."""
from datetime import datetime

LEVEL_TABLE = [
    (1, 0,     "새싹 플로거"),   (2, 100,   "초보 플로거"),
    (3, 300,   "활동 플로거"),   (4, 600,   "열정 플로거"),
    (5, 1000,  "베테랑 플로거"), (6, 1500,  "클린 헌터"),
    (7, 2200,  "에코 워리어"),   (8, 3000,  "환경 수호자"),
    (9, 4000,  "군산 레인저"),   (10, 5500, "마스터 플로거"),
    (11, 7500, "그랜드 마스터"), (12, 10000,"군산 레전드"),
]

ACTION_XP = {
    "report_created":    20,
    "drone_uploaded":    30,
    "cleanup_verified":  50,
    "boss_raid_joined":  100,
    "first_zone_report": 100,
    "like_received":     2,
}

def get_level(xp: int) -> tuple[int, str, int]:
    """Returns (level, title, xp_to_next)"""
    lv, title = 1, "새싹 플로거"
    for level, threshold, t in LEVEL_TABLE:
        if xp >= threshold:
            lv, title = level, t
        else:
            return lv, title, threshold - xp
    return lv, title, 0

def calc_xp(action: str, zone_bonus: float = 1.0) -> int:
    base = ACTION_XP.get(action, 0)
    is_weekend = datetime.utcnow().weekday() >= 5
    mult = zone_bonus * (1.5 if is_weekend and action in
                         ("report_created", "cleanup_verified") else 1.0)
    return int(base * mult)
```

---

## 2. SNS 서비스 (`services/sns/main.py`)

```python
"""
SNS 서비스 — 게시물·피드·좋아요·해시태그.
Kafka Producer: plogging.report.created
Kafka Consumer: plogging.mission.completed (피드 자동 갱신)
"""
import os, sys, asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, UploadFile, File, Form, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional
import asyncpg
from minio import Minio

sys.path.insert(0, "/app/shared")
from core.types import TrashType, Severity, DataSource, GeoPoint
from core.events import ReportCreatedEvent, KafkaProducerClient
from core.auth import get_current_user
from core.gamification import calc_xp

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = await asyncpg.create_pool(
        dsn=os.environ["DATABASE_URL"], min_size=10, max_size=50
    )
    app.state.kafka = KafkaProducerClient(os.environ["KAFKA_BOOTSTRAP_SERVERS"])
    app.state.minio = Minio(
        os.environ["MINIO_ENDPOINT"],
        access_key=os.environ["MINIO_ACCESS_KEY"],
        secret_key=os.environ["MINIO_SECRET_KEY"],
        secure=False,
    )
    yield
    await app.state.db.close()
    app.state.kafka.flush()

app = FastAPI(title="SNS Service", lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"],
                   allow_methods=["*"], allow_headers=["*"])

@app.post("/posts/report")
async def create_report_post(
    lat:        float      = Form(...),
    lon:        float      = Form(...),
    trash_type: TrashType  = Form(...),
    severity:   Severity   = Form(...),
    content:    str        = Form(""),
    hashtags:   str        = Form(""),
    files:      List[UploadFile] = File(default=[]),
    current_user: dict     = Depends(get_current_user),
):
    db    = app.state.db
    kafka = app.state.kafka
    minio = app.state.minio

    # 이미지 업로드
    image_urls = []
    for f in files:
        key = f"reports/{current_user['user_id']}/{f.filename}"
        minio.put_object(os.environ["MINIO_BUCKET_IMAGES"], key,
                         f.file, length=-1, part_size=5*1024*1024)
        image_urls.append(key)

    async with db.transaction():
        # 지오펜싱 자동 매핑
        zone = await db.fetchrow("""
            SELECT id, zone_key, bonus_multiplier
            FROM geofence_zones
            WHERE ST_Contains(geom, ST_SetSRID(ST_MakePoint($1,$2),4326))
              AND is_active = TRUE
            LIMIT 1
        """, lon, lat)

        report_id = await db.fetchval("""
            INSERT INTO trash_reports
              (reporter_id, location, zone_id, trash_type, severity, image_urls, source)
            VALUES
              ($1, ST_SetSRID(ST_MakePoint($2,$3),4326), $4, $5, $6, $7, 'sns')
            RETURNING id
        """, current_user["user_id"], lon, lat,
             zone["id"] if zone else None,
             trash_type.value, severity.value, image_urls)

        tags = [t.strip() for t in hashtags.split(",") if t.strip()]
        await db.execute("""
            INSERT INTO posts
              (user_id, report_id, content, image_urls, hashtags, location, zone_id, post_type)
            VALUES ($1,$2,$3,$4,$5,ST_SetSRID(ST_MakePoint($6,$7),4326),$8,'report')
        """, current_user["user_id"], report_id, content,
             image_urls, tags, lon, lat, zone["id"] if zone else None)

    # Kafka 이벤트
    kafka.produce(
        "plogging.report.created",
        ReportCreatedEvent(
            report_id=str(report_id),
            reporter_id=current_user["user_id"],
            location=GeoPoint(lat=lat, lon=lon),
            zone_id=str(zone["id"]) if zone else None,
            trash_type=trash_type, severity=severity,
            image_urls=image_urls, source=DataSource.SNS,
        ),
        key=str(report_id),
    )
    return {"success": True, "data": {"report_id": str(report_id)}}


@app.get("/feed")
async def get_feed(
    zone_id: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    size: int = Query(20, le=50),
    current_user: dict = Depends(get_current_user),
):
    db = app.state.db
    offset = (page - 1) * size
    args = [size, offset]
    zone_clause = ""
    if zone_id:
        args.insert(0, zone_id)
        zone_clause = "AND p.zone_id = $1"

    shift = 1 if zone_id else 0
    rows = await db.fetch(f"""
        SELECT p.id, p.content, p.image_urls, p.hashtags,
               p.post_type, p.like_count, p.created_at,
               u.display_name, u.avatar_url,
               gz.name AS zone_name,
               tr.trash_type, tr.severity, tr.status
        FROM posts p
        JOIN users u ON u.id = p.user_id
        LEFT JOIN geofence_zones gz ON gz.id = p.zone_id
        LEFT JOIN trash_reports tr ON tr.id = p.report_id
        WHERE 1=1 {zone_clause}
        ORDER BY p.created_at DESC
        LIMIT ${1+shift} OFFSET ${2+shift}
    """, *args)
    return {"success": True, "data": [dict(r) for r in rows]}


@app.post("/posts/{post_id}/like")
async def toggle_like(post_id: str, current_user: dict = Depends(get_current_user)):
    new_count = await app.state.db.fetchval("""
        UPDATE posts
        SET like_count = CASE
            WHEN $2::uuid = ANY(SELECT unnest($3::uuid[]))
            THEN like_count - 1 ELSE like_count + 1 END
        WHERE id = $1::uuid RETURNING like_count
    """, post_id, current_user["user_id"], [])
    return {"success": True, "data": {"like_count": new_count}}


@app.get("/search")
async def search(q: str = Query(..., min_length=1)):
    rows = await app.state.db.fetch("""
        SELECT p.id, p.content, p.hashtags, p.like_count, p.created_at, u.display_name
        FROM posts p JOIN users u ON u.id = p.user_id
        WHERE $1 = ANY(p.hashtags) OR p.content ILIKE '%'||$1||'%'
        ORDER BY p.created_at DESC LIMIT 30
    """, q)
    return {"success": True, "data": [dict(r) for r in rows]}
```

---

## 3. GIS 서비스 (`services/gis/main.py`)

```python
"""GIS 서비스 — 지오펜싱·핀맵·GeoJSON 내보내기."""
import os, sys
from contextlib import asynccontextmanager
from fastapi import FastAPI, Query
from typing import Optional
import asyncpg

sys.path.insert(0, "/app/shared")
from core.types import GeoPoint, ZoneKey
from core.events import AIDetectedEvent, KafkaProducerClient
from core.logger import get_logger

logger = get_logger("gis-service")

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = await asyncpg.create_pool(
        dsn=os.environ["DATABASE_URL"], min_size=5, max_size=20
    )
    yield
    await app.state.db.close()

app = FastAPI(title="GIS Service", lifespan=lifespan)

@app.get("/zones/stats")
async def zone_stats():
    rows = await app.state.db.fetch("""
        SELECT gz.zone_key, gz.name, gz.bonus_multiplier,
            COUNT(tr.id) FILTER (WHERE tr.status='pending')   AS pending,
            COUNT(tr.id) FILTER (WHERE tr.status='completed') AS completed,
            COUNT(tr.id) FILTER (WHERE tr.severity='boss')    AS boss_count
        FROM geofence_zones gz
        LEFT JOIN trash_reports tr ON tr.zone_id = gz.id
          AND tr.created_at > NOW() - INTERVAL '7 days'
        WHERE gz.is_active = TRUE
        GROUP BY gz.id ORDER BY gz.phase
    """)
    return {"success": True, "data": [dict(r) for r in rows]}

@app.get("/pins/nearby")
async def nearby_pins(lat: float = Query(...), lon: float = Query(...),
                      radius: int = Query(500)):
    rows = await app.state.db.fetch("""
        SELECT tr.id, tr.trash_type, tr.severity, tr.status,
            ST_X(tr.location::geometry) AS lon,
            ST_Y(tr.location::geometry) AS lat,
            ST_Distance(tr.location, ST_SetSRID(ST_MakePoint($1,$2),4326)::geography) AS dist_m,
            gz.name AS zone_name
        FROM trash_reports tr
        LEFT JOIN geofence_zones gz ON gz.id = tr.zone_id
        WHERE ST_DWithin(tr.location,
            ST_SetSRID(ST_MakePoint($1,$2),4326)::geography, $3)
          AND tr.status != 'completed'
        ORDER BY dist_m LIMIT 50
    """, lon, lat, radius)
    return {"success": True, "data": [dict(r) for r in rows]}

@app.post("/check-zone-entry")
async def check_zone(point: GeoPoint):
    row = await app.state.db.fetchrow("""
        SELECT id, zone_key, name, bonus_multiplier
        FROM geofence_zones
        WHERE ST_Contains(geom, ST_SetSRID(ST_MakePoint($1,$2),4326))
          AND is_active = TRUE LIMIT 1
    """, point.lon, point.lat)
    return {"success": True, "in_zone": bool(row),
            "data": dict(row) if row else None}

@app.get("/export/geojson")
async def export_geojson(zone_key: Optional[str] = Query(None),
                          limit: int = Query(1000, le=5000)):
    where = "AND gz.zone_key = $2" if zone_key else ""
    params = [limit] + ([zone_key] if zone_key else [])
    rows = await app.state.db.fetch(f"""
        SELECT tr.id, tr.trash_type, tr.severity, tr.status, tr.source,
            ST_X(tr.location::geometry) AS lon,
            ST_Y(tr.location::geometry) AS lat,
            tr.created_at::text, gz.name AS zone_name
        FROM trash_reports tr
        LEFT JOIN geofence_zones gz ON gz.id = tr.zone_id
        WHERE 1=1 {where} ORDER BY tr.created_at DESC LIMIT $1
    """, *params)
    features = [{"type":"Feature",
                 "geometry":{"type":"Point","coordinates":[r["lon"],r["lat"]]},
                 "properties":{k:v for k,v in dict(r).items() if k not in("lat","lon")}}
                for r in rows]
    return {"type":"FeatureCollection","features":features,
            "metadata":{"count":len(features),"license":"CC-BY-4.0",
                        "source":"meta-plogging-gunsan"}}
```

---

## 4. AI 서비스 (`services/ai/main.py`)

```python
"""
AI 서비스 — YOLO 탐지·RL 경로·인증 검증.
Kafka Consumer: plogging.report.created, drone.stream.uploaded
Kafka Producer: plogging.ai.detected
GPU: TorchServe 연동
"""
import os, sys, asyncio
from contextlib import asynccontextmanager
from fastapi import FastAPI
import asyncpg, httpx

sys.path.insert(0, "/app/shared")
from core.events import (ReportCreatedEvent, DroneStreamEvent,
                          AIDetectedEvent, KafkaProducerClient)
from core.kafka_client import KafkaConsumerClient
from core.types import TrashType, Severity
from core.logger import get_logger

logger = get_logger("ai-service")
TS = lambda: f"http://{os.environ['TORCHSERVE_HOST']}:{os.environ['TORCHSERVE_PORT']}"

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db    = await asyncpg.create_pool(dsn=os.environ["DATABASE_URL"], min_size=5, max_size=20)
    app.state.kafka = KafkaProducerClient(os.environ["KAFKA_BOOTSTRAP_SERVERS"])
    app.state.http  = httpx.AsyncClient(timeout=30.0)
    stop = asyncio.Event()
    consumer = KafkaConsumerClient(
        os.environ["KAFKA_BOOTSTRAP_SERVERS"], "ai-service",
        ["plogging.report.created", "drone.stream.uploaded"]
    )
    task = asyncio.create_task(consumer.consume_loop(_dispatch, dict, stop))
    yield
    stop.set(); await task; consumer.close()
    await app.state.db.close(); await app.state.http.aclose()

app = FastAPI(title="AI Service", lifespan=lifespan)

async def _dispatch(raw: dict):
    etype = raw.get("event_type", "")
    if etype == "plogging.report.created":
        await _detect_report(ReportCreatedEvent(**raw))
    elif etype == "drone.stream.uploaded":
        await _detect_drone(DroneStreamEvent(**raw))

async def _detect_report(e: ReportCreatedEvent):
    if not e.image_urls: return
    try:
        r = await app.state.http.post(
            f"{TS()}/predictions/{os.environ['YOLO_MODEL_NAME']}",
            json={"image_urls": e.image_urls}
        )
        res = r.json()
        conf = float(res.get("confidence", 0.0))
        ttype = TrashType(res.get("class", "unknown"))
        sev   = ("boss" if ttype==TrashType.LARGE else
                 "high" if conf>0.85 else "mid" if conf>0.65 else "low")
        await app.state.db.execute("""
            UPDATE trash_reports
            SET ai_confidence=$1, ai_label=$2, status='ai_verified',
                trash_type=$3, severity=$4
            WHERE id=$5
        """, conf, res, ttype.value, sev, e.report_id)
        app.state.kafka.produce(
            "plogging.ai.detected",
            AIDetectedEvent(report_id=e.report_id, trash_type=ttype,
                            severity=Severity(sev), confidence=conf,
                            bounding_boxes=res.get("boxes"),
                            model_version="yolo_v8"),
            key=e.report_id,
        )
    except Exception as ex:
        logger.error(f"Detection failed {e.report_id}: {ex}")

async def _detect_drone(e: DroneStreamEvent):
    r = await app.state.http.post(
        f"{TS()}/predictions/{os.environ['YOLO_MODEL_NAME']}/batch",
        json={"video_url": e.video_url, "sample_fps": 1}
    )
    for det in r.json().get("detections", []):
        if det.get("confidence", 0) < 0.5: continue
        await app.state.db.execute("""
            INSERT INTO trash_reports
              (location, zone_id, trash_type, severity, image_urls,
               source, ai_confidence, ai_label, status)
            VALUES (ST_SetSRID(ST_MakePoint($1,$2),4326),$3,$4,$5,$6,
                    'drone',$7,$8,'ai_verified')
            ON CONFLICT DO NOTHING
        """, det["lon"], det["lat"], e.zone_id, det["class"], det["severity"],
             [det.get("frame_url")], det["confidence"], det)

@app.post("/verify-cleanup")
async def verify(payload: dict):
    r = await app.state.http.post(
        f"{TS()}/predictions/{os.environ['VERIFY_MODEL_NAME']}",
        json={"before_url": payload["before_url"], "after_url": payload["after_url"]}
    )
    res = r.json()
    return {"success": True, "data": {
        "score":    res["ssim_score"],
        "verified": res["ssim_score"] > 0.6,
    }}

@app.post("/route/optimize")
async def optimize(payload: dict):
    r = await app.state.http.post(
        f"{TS()}/predictions/rl_route_agent", json=payload
    )
    return {"success": True, "data": r.json()}
```

---

## 5. Kong 선언형 설정 (`services/gateway/kong.yml`)

```yaml
_format_version: "3.0"

services:
  - name: sns-service
    url: http://sns-service:8001
    routes:
      - name: sns-routes
        paths: ["/api/v1/posts", "/api/v1/feed", "/api/v1/search", "/api/v1/sns"]
    plugins:
      - name: rate-limiting
        config: { minute: 60, policy: redis, redis_host: redis }

  - name: game-service
    url: http://game-service:8002
    routes:
      - name: game-routes
        paths: ["/api/v1/game", "/api/v1/quests", "/api/v1/ranking", "/api/v1/points"]

  - name: gis-service
    url: http://gis-service:8003
    routes:
      - name: gis-routes
        paths: ["/api/v1/gis", "/api/v1/zones", "/api/v1/pins"]

  - name: data-service
    url: http://data-service:8005
    routes:
      - name: data-routes
        paths: ["/api/v1/data", "/api/v1/opendata", "/api/v1/statistics"]

  - name: ws-server
    url: http://ws-server:8200
    routes:
      - name: ws-routes
        paths: ["/ws"]

plugins:
  - name: jwt
    config: { claims_to_verify: [exp] }

  - name: cors
    config:
      origins: ["*"]
      methods: [GET, POST, PUT, DELETE, OPTIONS]
      headers: [Authorization, Content-Type]

  - name: prometheus
```

---

## 6. WebSocket 서버 (`services/ws/main.py`)

```python
"""
WebSocket 서버 — 실시간 핀맵·랭킹·드론 위치 브로드캐스트.
Redis Pub/Sub로 마이크로서비스 → 클라이언트 이벤트 전달.
동시 1,000 연결: asyncio coroutine 기반.
"""
import os, sys, asyncio, json
from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from typing import Dict, Set
import redis.asyncio as aioredis

sys.path.insert(0, "/app/shared")
from core.logger import get_logger

logger = get_logger("ws-server")

class ConnMgr:
    def __init__(self):
        self.zone: Dict[str, Set[WebSocket]] = {}
        self.all:  Set[WebSocket] = set()

    async def connect(self, ws: WebSocket, zone_id: str = None):
        await ws.accept()
        self.all.add(ws)
        if zone_id:
            self.zone.setdefault(zone_id, set()).add(ws)

    def disconnect(self, ws: WebSocket, zone_id: str = None):
        self.all.discard(ws)
        if zone_id:
            self.zone.get(zone_id, set()).discard(ws)

    async def _send(self, ws: WebSocket, msg: dict) -> bool:
        try:
            await ws.send_json(msg); return True
        except:
            return False

    async def broadcast_zone(self, zone_id: str, msg: dict):
        dead = {ws for ws in list(self.zone.get(zone_id, set()))
                if not await self._send(ws, msg)}
        for ws in dead: self.disconnect(ws, zone_id)

    async def broadcast_all(self, msg: dict):
        dead = {ws for ws in list(self.all) if not await self._send(ws, msg)}
        for ws in dead: self.all.discard(ws)

mgr = ConnMgr()

@asynccontextmanager
async def lifespan(app: FastAPI):
    redis = aioredis.from_url(
        f"redis://:{os.environ['REDIS_PASSWORD']}@{os.environ['REDIS_HOST']}:6379",
        decode_responses=True
    )
    app.state.redis = redis
    pubsub = redis.pubsub()
    await pubsub.psubscribe("ws:zone:*", "ws:ranking", "ws:drone:live")

    async def listen():
        async for m in pubsub.listen():
            if m["type"] not in ("message", "pmessage"): continue
            ch, data = m["channel"], json.loads(m["data"])
            if ch.startswith("ws:zone:"):
                await mgr.broadcast_zone(ch.split(":")[-1], data)
            else:
                await mgr.broadcast_all(data)

    task = asyncio.create_task(listen())
    yield
    task.cancel()
    await redis.aclose()

app = FastAPI(title="WebSocket Server", lifespan=lifespan)

@app.websocket("/ws/zone/{zone_id}")
async def zone_ws(ws: WebSocket, zone_id: str):
    await mgr.connect(ws, zone_id)
    try:
        while True:
            data = await ws.receive_text()
            if data == "ping": await ws.send_text("pong")
    except WebSocketDisconnect:
        mgr.disconnect(ws, zone_id)

@app.websocket("/ws/global")
async def global_ws(ws: WebSocket):
    await mgr.connect(ws)
    try:
        while True:
            if await ws.receive_text() == "ping":
                await ws.send_text("pong")
    except WebSocketDisconnect:
        mgr.disconnect(ws)
```

---

## 7. 공통 Dockerfile

```dockerfile
# services/{service}/Dockerfile
FROM python:3.11-slim
WORKDIR /app
RUN groupadd -r app && useradd -r -g app app

COPY shared/ ./shared/
COPY services/${SERVICE_NAME}/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY services/${SERVICE_NAME}/ ./service/
WORKDIR /app/service

RUN chown -R app:app /app
USER app

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8001",
     "--workers", "4", "--loop", "uvloop", "--http", "h11",
     "--timeout-keep-alive", "30"]
```
