# 메타 플로깅 — 데이터 파이프라인 설계

> **참조**: `01_infra.md` §7 Kafka 토픽, `03_agents_mcp.md` §2 MCP 도구  
> **역할**: SNS·드론·모빌리티·Unity 데이터를 로컬 클라우드에 집적하고 AI-Ready 형태로 배포.

---

## 1. 수집 원천별 파이프라인

```
SNS 앱        POST /posts/report
              → MinIO(이미지) + PostgreSQL(보고)
              → Kafka: plogging.report.created
              → AI Service: YOLO 탐지
              → Kafka: plogging.ai.detected
              → GIS Service: 핀맵 등록 + WebSocket 브로드캐스트

드론 SDK      POST /webhook/video-upload (DroneAgent)
              → MinIO(원본 영상)
              → Kafka: drone.stream.uploaded
              → AI Service: 프레임 배치 탐지
              → PostgreSQL: trash_reports 자동 등록

자율주행      POST /webhook/route-completed (MobilityAgent)
모빌리티      → PostgreSQL: drone_events 기록
              → Kafka: mobility.route.completed
              → GameService: 수거 포인트 지급

Unity 시뮬    POST /posts/report (source=unity_sim)
              → 동일 파이프라인 처리
              → 공공데이터 배포 시 source 필터로 제외 가능
```

---

## 2. 드론 영상 처리 (`agents/drone_agent/video_processor.py`)

```python
"""
드론 영상 → OpenCV 프레임 추출 → TorchServe 배치 탐지 → GPS 좌표 보정.
A6000 GPU의 CUDA 가속 활용.
"""
import cv2, math, asyncio, httpx, os
from dataclasses import dataclass
from typing import List, Optional

@dataclass
class FrameDetection:
    frame_index:   int
    timestamp_sec: float
    lat:           float
    lon:           float
    trash_type:    str
    confidence:    float
    bbox:          dict
    frame_url:     Optional[str] = None


def _pixel_to_gps(px: float, py: float, drone_lat: float, drone_lon: float,
                   altitude_m: float, heading_deg: float) -> tuple[float, float]:
    """
    이미지 픽셀 위치(0~1) → GPS 좌표 변환.
    FOV: 수평 90°, 수직 60° 가정. 실제 운영 시 텔레메트리 데이터와 결합.
    """
    gw = 2 * altitude_m * math.tan(math.radians(45))   # 수평 커버리지(m)
    gh = 2 * altitude_m * math.tan(math.radians(30))   # 수직 커버리지(m)
    dx = (px - 0.5) * gw
    dy = (0.5 - py) * gh
    hr = math.radians(heading_deg)
    east_m  = dx * math.cos(hr) - dy * math.sin(hr)
    north_m = dx * math.sin(hr) + dy * math.cos(hr)
    return (drone_lat + north_m / 111_111,
            drone_lon + east_m / (111_111 * math.cos(math.radians(drone_lat))))


async def process_drone_video(
    video_url: str,
    start_lat: float, start_lon: float,
    heading_deg: float = 0.0,
    altitude_m: float = 30.0,
    sample_fps: int = 1,
    confidence_threshold: float = 0.5,
) -> List[FrameDetection]:

    TS = f"http://{os.environ['TORCHSERVE_HOST']}:{os.environ['TORCHSERVE_PORT']}"

    cap          = cv2.VideoCapture(video_url)
    fps_video    = cap.get(cv2.CAP_PROP_FPS) or 30
    frame_interval = max(1, int(fps_video / sample_fps))
    frames, idx  = [], 0

    while True:
        ret, frame = cap.read()
        if not ret: break
        if idx % frame_interval == 0:
            _, buf = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, 85])
            frames.append({"idx": idx, "ts": idx / fps_video,
                           "data": buf.tobytes().hex()})
        idx += 1
    cap.release()
    if not frames:
        return []

    async with httpx.AsyncClient(timeout=120.0) as client:
        r = await client.post(
            f"{TS}/predictions/{os.environ['YOLO_MODEL_NAME']}/batch",
            json={"frames": frames}
        )
    results = r.json().get("detections", [])

    detections = []
    for det in results:
        if det.get("confidence", 0) < confidence_threshold:
            continue
        lat, lon = _pixel_to_gps(
            det.get("center_x", 0.5), det.get("center_y", 0.5),
            start_lat, start_lon, altitude_m, heading_deg
        )
        detections.append(FrameDetection(
            frame_index=det["frame_idx"], timestamp_sec=det["timestamp"],
            lat=lat, lon=lon, trash_type=det["class"],
            confidence=det["confidence"], bbox=det.get("bbox", {})
        ))
    return detections
```

---

## 3. 자율주행 모빌리티 에이전트 (`agents/mobility_agent/agent.py`)

```python
"""
MobilityAgent — 자율주행 로봇/차량 수거 완료 처리.
MCP 도구: get_zone_pins, optimize_route, verify_cleanup, award_points
"""
import os, sys, json, asyncio
from fastapi import FastAPI
from mcp.client.sse import sse_client
from mcp import ClientSession

sys.path.insert(0, "/app/shared")
from core.logger import get_logger

logger = get_logger("mobility-agent")
MCP_URL = os.environ.get("MCP_SERVER_URL", "http://mcp-server:8100/sse")

async def _mcp(tool: str, args: dict) -> dict:
    async with sse_client(MCP_URL) as (r, w):
        async with ClientSession(r, w) as s:
            await s.initialize()
            res = await s.call_tool(tool, args)
            return json.loads(res.content[0].text)

app = FastAPI(title="Mobility Agent")

@app.post("/route/request")
async def request_route(payload: dict):
    """
    모빌리티가 출발 전 최적 경로 요청.
    payload: { zone_key, start_lat, start_lon, agent_type }
    """
    pins = await _mcp("get_zone_pins", {
        "zone_key":      payload["zone_key"],
        "status_filter": "ai_verified",
        "limit":         20,
    })
    if not pins:
        return {"success": True, "data": {"waypoints": [], "msg": "no_targets"}}

    route = await _mcp("optimize_route", {
        "start_lat":   payload["start_lat"],
        "start_lon":   payload["start_lon"],
        "target_ids":  [p["id"] for p in pins],
        "agent_type":  payload.get("agent_type", "mobility"),
    })
    return {"success": True, "data": route}

@app.post("/collection/complete")
async def collection_done(payload: dict):
    """
    수거 완료 후 인증 처리.
    payload: { report_id, before_url, after_url, mobility_id }
    """
    result = await _mcp("verify_cleanup", {
        "before_url":   payload["before_url"],
        "after_url":    payload["after_url"],
        "report_id":    payload["report_id"],
        "cleaner_type": "mobility",
    })
    logger.info(f"[MobilityAgent] {payload['mobility_id']} verified: {result}")
    return {"success": True, "data": result}
```

---

## 4. Elasticsearch 집계 파이프라인

### 4.1 핀맵 클러스터링 쿼리

```json
POST /plogging-reports/_search
{
  "size": 0,
  "query": {
    "bool": {
      "filter": [
        { "term": { "status": "pending" } },
        { "geo_bounding_box": {
            "location": {
              "top_left":     { "lat": 36.05, "lon": 126.50 },
              "bottom_right": { "lat": 35.70, "lon": 126.90 }
            }
        }}
      ]
    }
  },
  "aggs": {
    "clusters": {
      "geohash_grid": { "field": "location", "precision": 6, "size": 500 },
      "aggs": {
        "center":   { "geo_centroid": { "field": "location" } },
        "severity": { "terms": { "field": "severity" } }
      }
    }
  }
}
```

### 4.2 Elasticsearch 인덱스 매핑 (`infra/elasticsearch/reports_mapping.json`)

```json
{
  "mappings": {
    "properties": {
      "id":           { "type": "keyword" },
      "zone_key":     { "type": "keyword" },
      "zone_name":    { "type": "keyword" },
      "trash_type":   { "type": "keyword" },
      "severity":     { "type": "keyword" },
      "status":       { "type": "keyword" },
      "source":       { "type": "keyword" },
      "location":     { "type": "geo_point" },
      "ai_confidence":{ "type": "float" },
      "created_at":   { "type": "date" }
    }
  },
  "settings": {
    "number_of_shards":   3,
    "number_of_replicas": 0,
    "refresh_interval":   "5s"
  }
}
```

### 4.3 DB → Elasticsearch 동기화 (`services/data/es_sync.py`)

```python
"""
PostgreSQL → Elasticsearch 실시간 동기화.
Kafka Consumer: plogging.ai.detected (탐지 완료 후 ES 인덱싱)
"""
import os, sys, json, asyncio
import asyncpg
from elasticsearch import AsyncElasticsearch

sys.path.insert(0, "/app/shared")
from core.events import AIDetectedEvent
from core.kafka_client import KafkaConsumerClient
from core.logger import get_logger

logger = get_logger("es-sync")

async def run_sync():
    db  = await asyncpg.create_pool(dsn=os.environ["DATABASE_URL"], min_size=3, max_size=10)
    es  = AsyncElasticsearch(f"http://{os.environ['ES_HOST']}:{os.environ['ES_PORT']}")
    stop = asyncio.Event()

    consumer = KafkaConsumerClient(
        os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "es-sync-worker",
        ["plogging.ai.detected"]
    )

    async def handler(raw: dict):
        event   = AIDetectedEvent(**raw)
        row = await db.fetchrow("""
            SELECT tr.id, tr.trash_type, tr.severity, tr.status, tr.source,
                   ST_Y(tr.location::geometry) AS lat,
                   ST_X(tr.location::geometry) AS lon,
                   tr.ai_confidence, tr.created_at,
                   gz.zone_key, gz.name AS zone_name
            FROM trash_reports tr
            LEFT JOIN geofence_zones gz ON gz.id = tr.zone_id
            WHERE tr.id = $1
        """, event.report_id)

        if not row:
            return

        doc = {
            "id":           str(row["id"]),
            "zone_key":     row["zone_key"],
            "zone_name":    row["zone_name"],
            "trash_type":   row["trash_type"],
            "severity":     row["severity"],
            "status":       row["status"],
            "source":       row["source"],
            "location":     {"lat": row["lat"], "lon": row["lon"]},
            "ai_confidence":float(row["ai_confidence"] or 0),
            "created_at":   row["created_at"].isoformat(),
        }
        await es.index(index="plogging-reports", id=str(row["id"]), document=doc)
        logger.debug(f"ES indexed: {row['id']}")

    await consumer.consume_loop(handler, dict, stop)

if __name__ == "__main__":
    asyncio.run(run_sync())
```

---

## 5. 공공데이터 품질 검증 (`agents/data_agent/quality_check.py`)

```python
"""
공공데이터 배포 전 품질 검증 — 좌표 범위·분류 유효성·필수값 확인.
"""
import asyncpg
from typing import Tuple, List

GUNSAN_BBOX = {"lat_min":35.70,"lat_max":36.05,"lon_min":126.50,"lon_max":126.90}

async def validate(db: asyncpg.Pool) -> Tuple[int, List[str]]:
    errors = []

    # 1. 좌표 범위 이탈
    n = await db.fetchval("""
        SELECT COUNT(*) FROM trash_reports
        WHERE ST_Y(location::geometry) NOT BETWEEN $1 AND $2
           OR ST_X(location::geometry) NOT BETWEEN $3 AND $4
    """, GUNSAN_BBOX["lat_min"], GUNSAN_BBOX["lat_max"],
         GUNSAN_BBOX["lon_min"], GUNSAN_BBOX["lon_max"])
    if n: errors.append(f"좌표 범위 이탈: {n}건")

    # 2. 유효하지 않은 trash_type
    n = await db.fetchval("""
        SELECT COUNT(*) FROM trash_reports
        WHERE trash_type NOT IN ('plastic','food','general','large','unknown')
    """)
    if n: errors.append(f"비유효 trash_type: {n}건")

    # 3. AI 신뢰도 누락 (시뮬 제외)
    n = await db.fetchval("""
        SELECT COUNT(*) FROM trash_reports
        WHERE ai_confidence IS NULL AND source != 'unity_sim'
    """)
    if n: errors.append(f"AI 신뢰도 누락: {n}건 (경고)")

    valid = await db.fetchval(
        "SELECT COUNT(*) FROM trash_reports WHERE status != 'pending'"
    )
    return valid, errors
```

---

## 6. Data Service API (`services/data/main.py`)

```python
"""
Data Service — Elasticsearch 집계·시계열 통계·공공데이터 API.
"""
import os, sys
from contextlib import asynccontextmanager
from fastapi import FastAPI, Query
from typing import Optional
import asyncpg
from elasticsearch import AsyncElasticsearch

sys.path.insert(0, "/app/shared")
from core.logger import get_logger

logger = get_logger("data-service")

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = await asyncpg.create_pool(
        dsn=os.environ["DATABASE_URL"], min_size=3, max_size=10
    )
    app.state.es = AsyncElasticsearch(
        f"http://{os.environ['ES_HOST']}:{os.environ['ES_PORT']}"
    )
    yield
    await app.state.db.close()
    await app.state.es.close()

app = FastAPI(title="Data Service", lifespan=lifespan)

@app.get("/statistics/zones")
async def zone_stats():
    """구역별 집계 — Elasticsearch aggregation 사용."""
    body = {
        "size": 0,
        "aggs": {
            "by_zone": {
                "terms": {"field": "zone_key", "size": 10},
                "aggs": {
                    "by_severity": {"terms": {"field": "severity"}},
                    "by_status":   {"terms": {"field": "status"}},
                }
            }
        }
    }
    res = await app.state.es.search(index="plogging-reports", body=body)
    buckets = res["aggregations"]["by_zone"]["buckets"]
    return {"success": True, "data": [
        {
            "zone_key": b["key"],
            "total":    b["doc_count"],
            "severity": {s["key"]: s["doc_count"] for s in b["by_severity"]["buckets"]},
            "status":   {s["key"]: s["doc_count"] for s in b["by_status"]["buckets"]},
        }
        for b in buckets
    ]}

@app.get("/statistics/timeseries")
async def timeseries(
    zone_key: Optional[str] = Query(None),
    interval: str           = Query("1d", regex="^(1h|1d|1w)$"),
    days:     int           = Query(30, ge=1, le=365),
):
    """시계열 집계 — 공공데이터 포털 연계용."""
    must = [{"range": {"created_at": {"gte": f"now-{days}d/d"}}}]
    if zone_key:
        must.append({"term": {"zone_key": zone_key}})

    body = {
        "size": 0,
        "query": {"bool": {"must": must}},
        "aggs": {
            "over_time": {
                "date_histogram": {"field": "created_at", "calendar_interval": interval},
                "aggs": {
                    "by_type":   {"terms": {"field": "trash_type"}},
                    "completed": {"filter": {"term": {"status": "completed"}}},
                }
            }
        }
    }
    res = await app.state.es.search(index="plogging-reports", body=body)
    return {"success": True, "data": res["aggregations"]["over_time"]["buckets"]}

@app.get("/opendata/list")
async def opendata_list():
    """배포된 공공데이터 파일 목록."""
    rows = await app.state.db.fetch("""
        SELECT export_type, file_url, record_count, date_from, date_to, created_at
        FROM opendata_exports ORDER BY created_at DESC LIMIT 50
    """)
    return {"success": True, "data": [dict(r) for r in rows]}

@app.get("/opendata/export/geojson")
async def direct_geojson(zone_key: Optional[str] = Query(None),
                          limit: int = Query(1000, le=5000)):
    """실시간 GeoJSON 스트리밍 (소량 요청용)."""
    where = "AND gz.zone_key = $2" if zone_key else ""
    params = [limit] + ([zone_key] if zone_key else [])
    rows = await app.state.db.fetch(f"""
        SELECT tr.id, tr.trash_type, tr.severity, tr.status,
               ST_X(tr.location::geometry) AS lon,
               ST_Y(tr.location::geometry) AS lat,
               tr.created_at::text, gz.zone_key
        FROM trash_reports tr
        LEFT JOIN geofence_zones gz ON gz.id = tr.zone_id
        WHERE tr.source != 'unity_sim' {where}
        ORDER BY tr.created_at DESC LIMIT $1
    """, *params)
    return {
        "type": "FeatureCollection",
        "features": [
            {"type":"Feature",
             "geometry":{"type":"Point","coordinates":[r["lon"],r["lat"]]},
             "properties":{k:v for k,v in dict(r).items() if k not in("lat","lon")}}
            for r in rows
        ],
        "metadata": {
            "count": len(rows), "license": "CC-BY-4.0",
            "source": "meta-plogging-gunsan",
            "generated_at": __import__("datetime").datetime.utcnow().isoformat(),
        }
    }
```

---

## 7. Unity 시뮬레이터 연동 브리지

```python
# agents/unity_bridge/bridge.py
"""
Unity ML-Agents 시뮬레이션 이벤트를 실 파이프라인으로 주입.
시뮬 데이터는 source='unity_sim'으로 구분 → 공공데이터 배포 시 선택 제외 가능.
"""
import os, httpx, asyncio
from dataclasses import dataclass
from typing import List

@dataclass
class SimEvent:
    scene_id:   str
    drone_id:   str
    lat:        float
    lon:        float
    trash_items: List[dict]   # [{type, severity, lat_offset, lon_offset}]
    waypoints:   List[dict]

UNITY_TOKEN = os.environ.get("UNITY_SERVICE_TOKEN", "")

async def inject(events: List[SimEvent], api_url: str):
    async with httpx.AsyncClient() as client:
        for e in events:
            for item in e.trash_items:
                await client.post(
                    f"{api_url}/api/v1/posts/report",
                    data={
                        "lat": e.lat + item.get("lat_offset", 0),
                        "lon": e.lon + item.get("lon_offset", 0),
                        "trash_type": item["type"],
                        "severity":   item["severity"],
                        "content":    f"[시뮬레이션] Scene {e.scene_id}",
                        "hashtags":   "#시뮬레이션,#UnityRL",
                    },
                    headers={
                        "X-Source": "unity_sim",
                        "Authorization": f"Bearer {UNITY_TOKEN}",
                    }
                )
    return {"injected": len(events)}
```

---

## 8. 데이터 흐름 요약 — 로컬 → 클라우드 전환 시 변경점

| 컴포넌트 | 로컬 (현재) | 클라우드 (목표) | 변경 필요 |
|----------|------------|----------------|----------|
| 오브젝트 스토리지 | MinIO | AWS S3 | `.env`: `MINIO_ENDPOINT` → S3 endpoint |
| 메시지 큐 | 로컬 Kafka | AWS MSK | `.env`: `KAFKA_BOOTSTRAP_SERVERS` |
| 데이터베이스 | 로컬 PostgreSQL+PostGIS | AWS RDS PostGIS | `.env`: `DATABASE_URL` |
| AI 추론 | A6000 TorchServe | SageMaker / EC2 G5 | `.env`: `TORCHSERVE_HOST` |
| 검색 | 로컬 Elasticsearch | AWS OpenSearch | `.env`: `ES_HOST` |

**코드 변경 없이 `.env` 교체만으로 전환 가능** — 모든 외부 의존성이 환경변수로 추상화됨.
