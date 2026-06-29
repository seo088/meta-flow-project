# 메타 플로깅 — Multi-Agent 시스템 & MCP 서버

> **참조**: `02_backend.md` §1 공통 라이브러리 (이벤트·타입 재사용)  
> **원칙**: 각 에이전트는 단일 책임. MCP 도구는 서비스 간 공유 허브. Kafka로만 서비스와 통신.

---

## 1. 에이전트 시스템 구조

```
MCP Server (:8100) — 도구 레지스트리
    ├── detect_trash        (YOLO → DB → Kafka)
    ├── verify_cleanup      (SSIM → 포인트 → Kafka)
    ├── get_zone_pins       (PostGIS 쿼리)
    ├── optimize_route      (RL TorchServe)
    ├── award_points        (원장 → Redis 랭킹)
    ├── trigger_opendata_export
    └── get_zone_statistics

에이전트 (MCP 클라이언트)
    ├── DroneAgent      ← 드론 SDK 웹훅 / 스케줄
    ├── MobilityAgent   ← 모빌리티 로그 수신
    ├── QuestAgent      ← Kafka mission.completed
    ├── DataAgent       ← Kafka opendata.triggered
    └── SchedulerAgent  ← cron 기반
```

---

## 2. MCP 서버 (`services/mcp/server.py`)

```python
"""
FastMCP 기반 MCP 서버.
에이전트들이 SSE로 연결해 도구를 호출하는 공유 허브.
포인트 지급은 반드시 award_points 도구만 사용 (중복 방지).
"""
import os, sys, asyncio, json, uuid
import asyncpg, httpx
import redis.asyncio as aioredis
from fastmcp import FastMCP
from typing import Optional, List

sys.path.insert(0, "/app/shared")
from core.types import TrashType, Severity, ZoneKey
from core.events import (KafkaProducerClient, PointsUpdatedEvent,
                          AIDetectedEvent, MissionCompletedEvent)
from core.logger import get_logger

logger = get_logger("mcp-server")

mcp = FastMCP(
    name="MetaPlogging MCP",
    version="1.0.0",
    instructions="군산대 메타 플로깅 플랫폼 MCP 서버. 탐지·인증·포인트·공공데이터 도구 제공."
)

# 공유 클라이언트 (서버 기동 시 초기화)
_db:    asyncpg.Pool       = None
_redis: aioredis.Redis     = None
_kafka: KafkaProducerClient = None
_http:  httpx.AsyncClient  = None

async def init_clients():
    global _db, _redis, _kafka, _http
    _db    = await asyncpg.create_pool(dsn=os.environ["DATABASE_URL"], min_size=5, max_size=20)
    _redis = aioredis.from_url(
        f"redis://:{os.environ['REDIS_PASSWORD']}@{os.environ['REDIS_HOST']}:6379",
        decode_responses=True
    )
    _kafka = KafkaProducerClient(os.environ["KAFKA_BOOTSTRAP_SERVERS"])
    _http  = httpx.AsyncClient(timeout=30.0)

TS = lambda: f"http://{os.environ['TORCHSERVE_HOST']}:{os.environ['TORCHSERVE_PORT']}"

# ── 도구 1: 쓰레기 탐지 ──────────────────────────────────

@mcp.tool()
async def detect_trash(image_urls: List[str], report_id: str,
                        source: str = "sns") -> dict:
    """
    YOLO v8로 이미지를 탐지하고 결과를 DB에 저장한 뒤 Kafka 이벤트를 발행합니다.
    Args:
        image_urls: 탐지할 이미지 URL 목록
        report_id:  trash_reports.id
        source:     sns|drone|mobility
    Returns:
        trash_type, severity, confidence, boxes
    """
    r    = await _http.post(f"{TS()}/predictions/{os.environ['YOLO_MODEL_NAME']}",
                             json={"image_urls": image_urls})
    res  = r.json()
    conf = float(res.get("confidence", 0.0))
    tt   = res.get("class", "unknown")
    sev  = ("boss" if tt=="large" else "high" if conf>0.85 else
            "mid"  if conf>0.65  else "low")

    await _db.execute("""
        UPDATE trash_reports
        SET ai_confidence=$1, ai_label=$2, status='ai_verified',
            trash_type=$3, severity=$4
        WHERE id=$5
    """, conf, json.dumps(res), tt, sev, report_id)

    _kafka.produce("plogging.ai.detected",
        AIDetectedEvent(report_id=report_id, trash_type=TrashType(tt),
                        severity=Severity(sev), confidence=conf,
                        bounding_boxes=res.get("boxes"), model_version="yolo_v8"),
        key=report_id)

    logger.info(f"[detect_trash] {report_id}: {tt} ({conf:.3f})")
    return {"trash_type": tt, "severity": sev, "confidence": conf}


# ── 도구 2: 청소 인증 ────────────────────────────────────

@mcp.tool()
async def verify_cleanup(before_url: str, after_url: str, report_id: str,
                          cleaner_id: Optional[str] = None,
                          cleaner_type: str = "human") -> dict:
    """
    전후 이미지를 SSIM으로 비교해 청소를 인증하고 포인트를 지급합니다.
    포인트는 award_points 도구를 내부 호출해 단일 경로로만 지급합니다.
    """
    r     = await _http.post(f"{TS()}/predictions/{os.environ['VERIFY_MODEL_NAME']}",
                              json={"before_url": before_url, "after_url": after_url})
    score = float(r.json()["ssim_score"])
    if score <= 0.6:
        return {"verified": False, "score": score, "points_awarded": 0}

    zone  = await _db.fetchrow("""
        SELECT gz.bonus_multiplier FROM trash_reports tr
        JOIN geofence_zones gz ON gz.id = tr.zone_id WHERE tr.id = $1
    """, report_id)
    mult  = float(zone["bonus_multiplier"]) if zone else 1.0
    pts   = int(50 * mult)

    cleanup_id = await _db.fetchval("""
        INSERT INTO cleanups
          (report_id, cleaner_id, cleaner_type, before_image, after_image,
           verify_score, verified, points_awarded)
        VALUES ($1,$2,$3,$4,$5,$6,TRUE,$7) RETURNING id
    """, report_id, cleaner_id, cleaner_type, before_url, after_url, score, pts)

    await _db.execute("UPDATE trash_reports SET status='completed' WHERE id=$1", report_id)

    if cleaner_id:
        await award_points(user_id=cleaner_id, delta=pts,
                           reason="cleanup_verified", ref_id=str(cleanup_id))

    zone_row = await _db.fetchrow(
        "SELECT zone_id FROM trash_reports WHERE id=$1", report_id)
    _kafka.produce("plogging.mission.completed",
        MissionCompletedEvent(cleanup_id=str(cleanup_id), report_id=report_id,
                              cleaner_id=cleaner_id, cleaner_type=cleaner_type,
                              verify_score=score, points_awarded=pts,
                              zone_id=str(zone_row["zone_id"]) if zone_row else None),
        key=report_id)

    return {"verified": True, "score": score, "points_awarded": pts}


# ── 도구 3: 구역 핀 조회 ─────────────────────────────────

@mcp.tool()
async def get_zone_pins(zone_key: str, status_filter: Optional[str] = None,
                         limit: int = 100) -> List[dict]:
    """
    특정 지오펜싱 구역의 수거 대상 핀 목록을 반환합니다.
    드론/모빌리티 에이전트가 경로 계획에 사용합니다.
    """
    where  = "AND tr.status = $3" if status_filter else ""
    params = [zone_key, limit] + ([status_filter] if status_filter else [])
    rows   = await _db.fetch(f"""
        SELECT tr.id, ST_Y(tr.location::geometry) AS lat,
               ST_X(tr.location::geometry) AS lon,
               tr.trash_type, tr.severity, tr.status
        FROM trash_reports tr
        JOIN geofence_zones gz ON gz.id = tr.zone_id
        WHERE gz.zone_key = $1 {where}
        ORDER BY CASE tr.severity WHEN 'boss' THEN 1 WHEN 'high' THEN 2
                                  WHEN 'mid' THEN 3 ELSE 4 END
        LIMIT $2
    """, *params)
    return [dict(r) for r in rows]


# ── 도구 4: RL 경로 최적화 ──────────────────────────────

@mcp.tool()
async def optimize_route(start_lat: float, start_lon: float,
                          target_ids: List[str], agent_type: str = "drone") -> dict:
    """
    수거 대상 핀 목록에 대해 RL 에이전트로 최적 경로를 생성합니다.
    Unity ML-Agents 학습 후 ONNX로 내보낸 모델을 사용합니다.
    """
    targets = await _db.fetch("""
        SELECT id, ST_Y(location::geometry) AS lat, ST_X(location::geometry) AS lon
        FROM trash_reports WHERE id = ANY($1::uuid[])
    """, target_ids)
    r = await _http.post(f"{TS()}/predictions/rl_route_agent",
                          json={"start":{"lat":start_lat,"lon":start_lon},
                                "targets":[dict(t) for t in targets],
                                "agent_type":agent_type})
    return r.json()


# ── 도구 5: 포인트 지급 (단일 경로) ─────────────────────

@mcp.tool()
async def award_points(user_id: str, delta: int, reason: str,
                        ref_id: Optional[str] = None) -> dict:
    """
    포인트를 지급합니다. 모든 포인트 지급은 이 도구를 통해서만 처리합니다.
    ON CONFLICT로 중복 지급을 DB 레벨에서 차단합니다.
    """
    async with _db.transaction():
        inserted = await _db.fetchval("""
            INSERT INTO point_ledger (user_id, delta, reason, ref_id)
            VALUES ($1,$2,$3,$4)
            ON CONFLICT (user_id, ref_id, reason) DO NOTHING
            RETURNING id
        """, user_id, delta, reason, ref_id)

        if not inserted:
            existing = await _db.fetchval(
                "SELECT COALESCE(SUM(delta),0) FROM point_ledger WHERE user_id=$1", user_id)
            return {"new_total": existing, "delta": 0, "duplicate": True}

        new_total = await _db.fetchval(
            "SELECT COALESCE(SUM(delta),0) FROM point_ledger WHERE user_id=$1", user_id)

    await _redis.zadd("ranking:global", {user_id: new_total})
    _kafka.produce("user.points.updated",
        PointsUpdatedEvent(user_id=user_id, delta=delta,
                           new_total=new_total, reason=reason),
        key=user_id)

    logger.info(f"[award_points] {user_id} {delta:+d} → {new_total}")
    return {"new_total": new_total, "delta": delta, "duplicate": False}


# ── 도구 6: 공공데이터 내보내기 트리거 ──────────────────

@mcp.tool()
async def trigger_opendata_export(export_type: str = "all",
                                   date_from: Optional[str] = None,
                                   date_to: Optional[str] = None) -> dict:
    """DataAgent가 구독하는 Kafka 토픽으로 내보내기 이벤트를 발행합니다."""
    job_id = str(uuid.uuid4())
    _kafka.produce("opendata.export.triggered", type("E", (), {
        "model_dump_json": lambda s: json.dumps({
            "event_id": job_id, "event_type": "opendata.export.triggered",
            "export_type": export_type,
            "date_from": date_from, "date_to": date_to,
        })
    })(), key=job_id)
    return {"job_id": job_id, "status": "queued"}


# ── 도구 7: 구역 통계 ────────────────────────────────────

@mcp.tool()
async def get_zone_statistics(zone_key: Optional[str] = None) -> List[dict]:
    """구역별 실시간 쓰레기 현황 통계를 반환합니다."""
    where  = "AND gz.zone_key = $1" if zone_key else ""
    params = [zone_key] if zone_key else []
    rows   = await _db.fetch(f"""
        SELECT gz.zone_key, gz.name,
            COUNT(tr.id) FILTER (WHERE tr.status='pending')   AS pending,
            COUNT(tr.id) FILTER (WHERE tr.status='completed') AS completed,
            COUNT(tr.id) FILTER (WHERE tr.severity='boss')    AS boss_count
        FROM geofence_zones gz
        LEFT JOIN trash_reports tr ON tr.zone_id = gz.id
            AND tr.created_at > NOW() - INTERVAL '24 hours'
        WHERE gz.is_active = TRUE {where}
        GROUP BY gz.id, gz.zone_key, gz.name
    """, *params)
    return [dict(r) for r in rows]


# ── 리소스: 구역 목록 ────────────────────────────────────

@mcp.resource("zones://all")
async def list_zones() -> str:
    rows = await _db.fetch(
        "SELECT zone_key, name, phase, bonus_multiplier FROM geofence_zones ORDER BY phase"
    )
    return json.dumps([dict(r) for r in rows], ensure_ascii=False)


if __name__ == "__main__":
    import uvicorn
    asyncio.run(init_clients())
    uvicorn.run(mcp.get_asgi_app(), host="0.0.0.0",
                port=int(os.environ.get("PORT", 8100)))
```

---

## 3. 드론 에이전트 (`agents/drone_agent/agent.py`)

```python
"""
DroneAgent — 드론 영상 수신 → MCP 탐지 → 경로 최적화 → 수거 완료 처리.
MCP 도구: detect_trash, get_zone_pins, optimize_route, verify_cleanup
"""
import os, sys, asyncio, json
from fastapi import FastAPI
from mcp.client.sse import sse_client
from mcp import ClientSession

sys.path.insert(0, "/app/shared")
from core.events import DroneStreamEvent, KafkaProducerClient
from core.logger import get_logger

logger = get_logger("drone-agent")
MCP_URL = os.environ.get("MCP_SERVER_URL", "http://mcp-server:8100/sse")

async def _mcp(tool: str, args: dict) -> dict:
    async with sse_client(MCP_URL) as (r, w):
        async with ClientSession(r, w) as s:
            await s.initialize()
            res = await s.call_tool(tool, args)
            return json.loads(res.content[0].text)

class DroneAgent:
    async def handle_video(self, drone_id: str, video_url: str,
                            lat: float, lon: float, zone_id: str = None):
        # 영상 → Kafka → AI Service가 탐지 처리
        kafka = KafkaProducerClient(os.environ["KAFKA_BOOTSTRAP_SERVERS"])
        kafka.produce("drone.stream.uploaded",
            DroneStreamEvent(drone_id=drone_id, video_url=video_url,
                             location={"lat":lat,"lon":lon},
                             zone_id=zone_id, duration_sec=0),
            key=drone_id)
        kafka.flush()

        # AI 처리 대기 후 핀 조회
        await asyncio.sleep(15)
        if zone_id:
            pins = await _mcp("get_zone_pins", {
                "zone_key": zone_id, "status_filter": "ai_verified", "limit": 20
            })
            if pins:
                return await _mcp("optimize_route", {
                    "start_lat": lat, "start_lon": lon,
                    "target_ids": [p["id"] for p in pins],
                    "agent_type": "drone"
                })
        return {"status": "no_targets"}

    async def complete_collection(self, drone_id: str, report_id: str,
                                   before_url: str, after_url: str):
        return await _mcp("verify_cleanup", {
            "before_url": before_url, "after_url": after_url,
            "report_id": report_id, "cleaner_type": "drone"
        })

    async def patrol(self, zone_key: str, lat: float, lon: float):
        pins = await _mcp("get_zone_pins", {
            "zone_key": zone_key, "status_filter": "pending", "limit": 30
        })
        if not pins:
            return {"status": "no_pending"}
        return await _mcp("optimize_route", {
            "start_lat": lat, "start_lon": lon,
            "target_ids": [p["id"] for p in pins], "agent_type": "drone"
        })

app = FastAPI(title="Drone Agent")
_agent = DroneAgent()

@app.post("/webhook/video-upload")
async def video_upload(payload: dict):
    return {"success": True, "data": await _agent.handle_video(**payload)}

@app.post("/webhook/collection-complete")
async def collection_complete(payload: dict):
    return {"success": True, "data": await _agent.complete_collection(**payload)}
```

---

## 4. 퀘스트 에이전트 (`agents/quest_agent/agent.py`)

```python
"""
QuestAgent — 미션 완료 이벤트 수신 → 퀘스트 진행 +1 → 배지 지급.
Kafka Consumer: plogging.mission.completed
MCP 도구: award_points
"""
import os, sys, asyncio, json
from contextlib import asynccontextmanager
from fastapi import FastAPI
import asyncpg
from mcp.client.sse import sse_client
from mcp import ClientSession

sys.path.insert(0, "/app/shared")
from core.events import MissionCompletedEvent, KafkaProducerClient
from core.kafka_client import KafkaConsumerClient
from core.logger import get_logger

logger = get_logger("quest-agent")
MCP_URL = os.environ.get("MCP_SERVER_URL", "http://mcp-server:8100/sse")

# 퀘스트 키 → 트리거 이벤트 타입 맵
TRIGGERS = {
    "weekly_plogging_3":    "plogging.mission.completed",
    "eunpa_guardian_5":     "plogging.mission.completed",
    "boss_raid_saemangeum": "plogging.mission.completed",
    "report_5_times":       "plogging.report.created",
    "drone_upload_1":       "drone.stream.uploaded",
}

async def _mcp(tool: str, args: dict) -> dict:
    async with sse_client(MCP_URL) as (r, w):
        async with ClientSession(r, w) as s:
            await s.initialize()
            res = await s.call_tool(tool, args)
            return json.loads(res.content[0].text)

class QuestAgent:
    def __init__(self, db):
        self.db = db

    async def on_mission_completed(self, event: MissionCompletedEvent):
        if not event.cleaner_id: return
        await self._update(event.cleaner_id,
                           "plogging.mission.completed", event.zone_id)

    async def _update(self, user_id: str, event_type: str, zone_id: str = None):
        quests = await self.db.fetch("""
            SELECT uq.id, uq.quest_id, uq.progress,
                   qd.target_count, qd.reward_xp, qd.quest_key,
                   qd.zone_id AS qzone, qd.reward_badge_id
            FROM user_quests uq
            JOIN quest_definitions qd ON qd.id = uq.quest_id
            WHERE uq.user_id=$1 AND uq.is_completed=FALSE AND qd.is_active=TRUE
        """, user_id)

        for q in quests:
            if TRIGGERS.get(q["quest_key"]) != event_type: continue
            if q["qzone"] and str(q["qzone"]) != zone_id: continue

            new_prog = q["progress"] + 1
            if new_prog >= q["target_count"]:
                await self.db.execute("""
                    UPDATE user_quests SET progress=$1, is_completed=TRUE,
                    completed_at=NOW() WHERE id=$2
                """, new_prog, q["id"])
                await self._complete(user_id, q)
            else:
                await self.db.execute(
                    "UPDATE user_quests SET progress=$1 WHERE id=$2",
                    new_prog, q["id"])

    async def _complete(self, user_id: str, q: dict):
        await _mcp("award_points", {
            "user_id": user_id, "delta": q["reward_xp"],
            "reason": f"quest_completed:{q['quest_key']}",
            "ref_id": str(q["id"])
        })
        if q["reward_badge_id"]:
            await self.db.execute("""
                INSERT INTO user_badges (user_id, badge_id)
                VALUES ($1,$2) ON CONFLICT DO NOTHING
            """, user_id, q["reward_badge_id"])
        logger.info(f"Quest completed: {q['quest_key']} by {user_id}")

@asynccontextmanager
async def lifespan(app: FastAPI):
    db   = await asyncpg.create_pool(dsn=os.environ["DATABASE_URL"], min_size=5, max_size=20)
    stop = asyncio.Event()
    consumer = KafkaConsumerClient(
        os.environ["KAFKA_BOOTSTRAP_SERVERS"], "quest-agent",
        ["plogging.mission.completed"]
    )
    agent = QuestAgent(db)

    async def handle(raw: dict):
        etype = raw.get("event_type","")
        if etype == "plogging.mission.completed":
            await agent.on_mission_completed(MissionCompletedEvent(**raw))

    task = asyncio.create_task(consumer.consume_loop(handle, dict, stop))
    app.state.agent = agent
    yield
    stop.set(); await task; consumer.close(); await db.close()

app = FastAPI(title="Quest Agent", lifespan=lifespan)
```

---

## 5. 데이터 에이전트 (`agents/data_agent/agent.py`)

```python
"""
DataAgent — GeoJSON·COCO·통계 내보내기 → MinIO 업로드 → DB 이력 기록.
Kafka Consumer: opendata.export.triggered
"""
import os, sys, asyncio, json, io
from contextlib import asynccontextmanager
from datetime import datetime
from fastapi import FastAPI
import asyncpg
from minio import Minio

sys.path.insert(0, "/app/shared")
from core.kafka_client import KafkaConsumerClient
from core.logger import get_logger

logger = get_logger("data-agent")

CATEGORIES = [
    {"id":1,"name":"plastic"},{"id":2,"name":"food"},
    {"id":3,"name":"general"},{"id":4,"name":"large"},
]
CAT_MAP = {c["name"]: c["id"] for c in CATEGORIES}

class DataAgent:
    def __init__(self, db, minio):
        self.db    = db
        self.minio = minio

    async def export_geojson(self, date_from=None, date_to=None) -> str:
        where, params = "", []
        if date_from:
            params.append(date_from); where += f" AND tr.created_at >= ${len(params)}"
        if date_to:
            params.append(date_to);   where += f" AND tr.created_at <= ${len(params)}"

        rows = await self.db.fetch(f"""
            SELECT tr.id, tr.trash_type, tr.severity, tr.status, tr.source,
                   ST_X(tr.location::geometry) AS lon,
                   ST_Y(tr.location::geometry) AS lat,
                   tr.ai_confidence, tr.created_at::text, gz.zone_key, gz.name
            FROM trash_reports tr LEFT JOIN geofence_zones gz ON gz.id = tr.zone_id
            WHERE tr.source != 'unity_sim' {where}
            ORDER BY tr.created_at DESC LIMIT 10000
        """, *params)

        geojson = {
            "type": "FeatureCollection",
            "features": [
                {"type":"Feature",
                 "geometry":{"type":"Point","coordinates":[r["lon"],r["lat"]]},
                 "properties":{k:v for k,v in dict(r).items() if k not in("lat","lon")}}
                for r in rows
            ],
            "metadata": {
                "generated_at": datetime.utcnow().isoformat(),
                "source": "meta-plogging-gunsan-v1",
                "count": len(rows),
                "license": "CC-BY-4.0",
                "contact": "meta-plogging@kunsan.ac.kr",
            }
        }
        content = json.dumps(geojson, ensure_ascii=False).encode("utf-8")
        key = f"opendata/geojson/{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.geojson"
        self.minio.put_object("plogging-opendata", key,
                              io.BytesIO(content), len(content),
                              "application/geo+json")
        await self.db.execute("""
            INSERT INTO opendata_exports (export_type, file_url, record_count)
            VALUES ('geojson', $1, $2)
        """, f"minio://plogging-opendata/{key}", len(rows))
        logger.info(f"GeoJSON exported: {len(rows)} records → {key}")
        return key

    async def export_coco(self, min_conf: float = 0.7) -> str:
        rows = await self.db.fetch("""
            SELECT tr.id::text, tr.image_urls, tr.trash_type,
                   tr.ai_confidence, tr.ai_label
            FROM trash_reports tr
            WHERE tr.ai_confidence >= $1
              AND array_length(tr.image_urls,1) > 0
              AND tr.source != 'unity_sim'
            ORDER BY tr.created_at DESC LIMIT 5000
        """, min_conf)

        images, annotations, ann_id = [], [], 1
        for img_id, r in enumerate(rows, 1):
            label = r["ai_label"] or {}
            bbox  = label.get("bbox", [0,0,100,100])
            images.append({"id": img_id, "file_name": f"{r['id']}.jpg",
                           "width": label.get("width",640), "height": label.get("height",480)})
            annotations.append({
                "id": ann_id, "image_id": img_id,
                "category_id": CAT_MAP.get(r["trash_type"], 3),
                "bbox": bbox, "area": bbox[2]*bbox[3], "iscrowd": 0
            })
            ann_id += 1

        coco = {
            "info": {"description":"메타 플로깅 군산시 쓰레기 데이터셋",
                     "version":"1.0", "year":datetime.utcnow().year,
                     "contributor":"군산대학교 메타 플로깅 연구팀",
                     "date_created":datetime.utcnow().isoformat()},
            "licenses": [{"id":1,"name":"CC BY 4.0"}],
            "images": images, "annotations": annotations, "categories": CATEGORIES,
        }
        content = json.dumps(coco, ensure_ascii=False).encode("utf-8")
        key = f"opendata/coco/{datetime.utcnow().strftime('%Y%m%d')}_coco.json"
        self.minio.put_object("plogging-opendata", key,
                              io.BytesIO(content), len(content), "application/json")
        logger.info(f"COCO exported: {len(images)} images → {key}")
        return key

    async def handle_trigger(self, raw: dict):
        etype = raw.get("export_type", "all")
        if etype in ("geojson", "all"):
            await self.export_geojson(raw.get("date_from"), raw.get("date_to"))
        if etype in ("coco", "all"):
            await self.export_coco()

@asynccontextmanager
async def lifespan(app: FastAPI):
    db    = await asyncpg.create_pool(dsn=os.environ["DATABASE_URL"], min_size=3, max_size=10)
    minio = Minio(os.environ["MINIO_ENDPOINT"],
                  os.environ["MINIO_ACCESS_KEY"], os.environ["MINIO_SECRET_KEY"], secure=False)
    if not minio.bucket_exists("plogging-opendata"):
        minio.make_bucket("plogging-opendata")

    agent = DataAgent(db, minio)
    stop  = asyncio.Event()
    cons  = KafkaConsumerClient(
        os.environ["KAFKA_BOOTSTRAP_SERVERS"], "data-agent", ["opendata.export.triggered"]
    )
    task  = asyncio.create_task(cons.consume_loop(agent.handle_trigger, dict, stop))
    app.state.agent = agent
    yield
    stop.set(); await task; cons.close(); await db.close()

app = FastAPI(title="Data Agent", lifespan=lifespan)

@app.post("/export/manual")
async def manual(payload: dict):
    await app.state.agent.handle_trigger(payload)
    return {"success": True}
```

---

## 6. 스케줄러 에이전트 (`agents/scheduler_agent/agent.py`)

```python
"""
SchedulerAgent — cron 기반 주기적 작업.
- 5분: PostgreSQL Materialized View 갱신
- 1시간: 보스 핀 확인 → 레이드 퀘스트 생성
- 매일: 랭킹 스냅샷 + 공공데이터 내보내기 트리거
- 매주 월: 주간 퀘스트 초기화
"""
import os, sys, asyncio
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import asyncpg, httpx
import redis.asyncio as aioredis

sys.path.insert(0, "/app/shared")
from core.logger import get_logger

logger = get_logger("scheduler-agent")
MCP_URL = os.environ.get("MCP_SERVER_URL", "http://mcp-server:8100")

async def refresh_mv(db):
    await db.execute("SELECT refresh_user_points()")

async def check_boss_raids(db, http):
    pins = await db.fetch("""
        SELECT tr.id, gz.zone_key, gz.id AS zone_id
        FROM trash_reports tr JOIN geofence_zones gz ON gz.id = tr.zone_id
        WHERE tr.severity='boss' AND tr.status='ai_verified'
          AND NOT EXISTS (
              SELECT 1 FROM quest_definitions qd
              WHERE qd.quest_type='raid' AND qd.zone_id=gz.id
                AND qd.created_at > NOW() - INTERVAL '7 days')
    """)
    for p in pins:
        await db.execute("""
            INSERT INTO quest_definitions
              (quest_key,title,description,quest_type,zone_id,target_count,reward_xp)
            VALUES ($1,'💀 보스 레이드: '||$2,'3인 이상 팀 대형 폐기물 처리',
                    'raid',$3,3,400) ON CONFLICT (quest_key) DO NOTHING
        """, f"boss_raid_{p['id']}", p["zone_key"], p["zone_id"])
    if pins: logger.info(f"Created {len(pins)} boss raid quests")

async def reset_weekly(db):
    await db.execute("""
        UPDATE user_quests SET progress=0, is_completed=FALSE, completed_at=NULL
        WHERE quest_id IN (SELECT id FROM quest_definitions WHERE quest_type='weekly')
    """)
    logger.info("Weekly quests reset")

async def daily_export(http):
    await http.post(f"{MCP_URL}/tools/trigger_opendata_export",
                    json={"export_type":"all"})

async def run():
    db    = await asyncpg.create_pool(dsn=os.environ["DATABASE_URL"], min_size=2, max_size=5)
    redis = aioredis.from_url(
        f"redis://:{os.environ['REDIS_PASSWORD']}@{os.environ['REDIS_HOST']}:6379")
    http  = httpx.AsyncClient()
    sch   = AsyncIOScheduler()

    sch.add_job(refresh_mv,       CronTrigger(minute="*/5"),             args=[db])
    sch.add_job(check_boss_raids, CronTrigger(minute=0),                 args=[db, http])
    sch.add_job(daily_export,     CronTrigger(hour=1, minute=0),         args=[http])
    sch.add_job(reset_weekly,     CronTrigger(day_of_week="mon",hour=0), args=[db])

    sch.start()
    logger.info("Scheduler started")
    try:
        await asyncio.Event().wait()
    finally:
        sch.shutdown()
        await db.close(); await redis.aclose(); await http.aclose()

if __name__ == "__main__":
    asyncio.run(run())
```

---

## 7. `docker-compose.agents.yml`

```yaml
version: "3.9"

services:
  drone-agent:
    build: { context: ., dockerfile: agents/drone_agent/Dockerfile }
    restart: unless-stopped
    env_file: .env
    ports: ["8301:8301"]
    networks: [plogging-net]
    depends_on: [mcp-server, kafka]

  quest-agent:
    build: { context: ., dockerfile: agents/quest_agent/Dockerfile }
    restart: unless-stopped
    env_file: .env
    networks: [plogging-net]
    depends_on: [mcp-server, kafka, postgres]

  data-agent:
    build: { context: ., dockerfile: agents/data_agent/Dockerfile }
    restart: unless-stopped
    env_file: .env
    ports: ["8302:8302"]
    networks: [plogging-net]
    depends_on: [mcp-server, kafka, minio, postgres]

  scheduler-agent:
    build: { context: ., dockerfile: agents/scheduler_agent/Dockerfile }
    restart: unless-stopped
    env_file: .env
    networks: [plogging-net]
    depends_on: [mcp-server, postgres, redis]
```
