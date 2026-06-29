"""
DataAgent — GeoJSON·COCO·통계 내보내기 → MinIO 업로드 → DB 이력 기록.
Kafka Consumer: opendata.export.triggered
포트: DATA_AGENT_PORT 환경변수 (기본 8532)
"""
import os
import sys
import asyncio
import json
import io
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "shared"))

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from core.kafka_client import KafkaConsumerClient
from core.db import create_db_pool
from core.logger import get_logger

logger = get_logger("data-agent")

CATEGORIES = [
    {"id": 1, "name": "plastic"},
    {"id": 2, "name": "food"},
    {"id": 3, "name": "general"},
    {"id": 4, "name": "large"},
]
CAT_MAP = {c["name"]: c["id"] for c in CATEGORIES}


class DataAgent:
    def __init__(self, db, minio_client):
        self.db = db
        self.minio = minio_client

    async def export_geojson(self, date_from=None, date_to=None) -> str:
        where, params = "", []
        if date_from:
            params.append(date_from)
            where += f" AND tr.created_at >= ${len(params)}"
        if date_to:
            params.append(date_to)
            where += f" AND tr.created_at <= ${len(params)}"

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
                {
                    "type": "Feature",
                    "geometry": {"type": "Point", "coordinates": [r["lon"], r["lat"]]},
                    "properties": {
                        k: v for k, v in dict(r).items() if k not in ("lat", "lon")
                    },
                }
                for r in rows
            ],
            "metadata": {
                "generated_at": datetime.utcnow().isoformat(),
                "source": "meta-plogging-gunsan-v1",
                "count": len(rows),
                "license": "CC-BY-4.0",
                "contact": "meta-plogging@kunsan.ac.kr",
            },
        }
        content = json.dumps(geojson, ensure_ascii=False).encode("utf-8")
        key = f"opendata/geojson/{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.geojson"

        if self.minio:
            try:
                self.minio.put_object(
                    "plogging-opendata", key,
                    io.BytesIO(content), len(content), "application/geo+json",
                )
            except Exception as e:
                logger.warning(f"MinIO upload failed: {e}")

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
            if isinstance(label, str):
                try:
                    label = json.loads(label)
                except json.JSONDecodeError:
                    label = {}
            bbox = label.get("bbox", [0, 0, 100, 100])
            images.append({
                "id": img_id,
                "file_name": f"{r['id']}.jpg",
                "width": label.get("width", 640),
                "height": label.get("height", 480),
            })
            annotations.append({
                "id": ann_id,
                "image_id": img_id,
                "category_id": CAT_MAP.get(r["trash_type"], 3),
                "bbox": bbox,
                "area": bbox[2] * bbox[3],
                "iscrowd": 0,
            })
            ann_id += 1

        coco = {
            "info": {
                "description": "메타 플로깅 군산시 쓰레기 데이터셋",
                "version": "1.0",
                "year": datetime.utcnow().year,
                "contributor": "군산대학교 메타 플로깅 연구팀",
                "date_created": datetime.utcnow().isoformat(),
            },
            "licenses": [{"id": 1, "name": "CC BY 4.0"}],
            "images": images,
            "annotations": annotations,
            "categories": CATEGORIES,
        }
        content = json.dumps(coco, ensure_ascii=False).encode("utf-8")
        key = f"opendata/coco/{datetime.utcnow().strftime('%Y%m%d')}_coco.json"

        if self.minio:
            try:
                self.minio.put_object(
                    "plogging-opendata", key,
                    io.BytesIO(content), len(content), "application/json",
                )
            except Exception as e:
                logger.warning(f"MinIO upload failed: {e}")

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
    db = await create_db_pool(min_size=3, max_size=10)

    minio_client = None
    try:
        from minio import Minio
        minio_client = Minio(
            os.environ.get("MINIO_ENDPOINT", "localhost:9001"),
            os.environ.get("MINIO_ACCESS_KEY", "plogging_admin"),
            os.environ.get("MINIO_SECRET_KEY", ""),
            secure=False,
        )
        if not minio_client.bucket_exists("plogging-opendata"):
            minio_client.make_bucket("plogging-opendata")
    except Exception as e:
        logger.warning(f"MinIO not available: {e}")

    agent = DataAgent(db, minio_client)
    stop = asyncio.Event()
    cons = KafkaConsumerClient(
        os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093"),
        "data-agent",
        ["opendata.export.triggered"],
    )
    task = asyncio.create_task(cons.consume_loop(agent.handle_trigger, dict, stop))
    app.state.agent = agent
    yield
    stop.set()
    await task
    cons.close()
    await db.close()


app = FastAPI(title="Data Agent", version="1.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)


@app.get("/health")
async def health():
    return {"status": "ok", "agent": "data"}


@app.post("/export/manual")
async def manual(payload: dict):
    await app.state.agent.handle_trigger(payload)
    return {"success": True}


@app.get("/quality-check")
async def quality_check():
    """공공데이터 품질 검증 리포트."""
    from agents.data_agent.quality_check import generate_report
    report = await generate_report(app.state.agent.db)
    return {"success": True, "data": report}


# ── AIHub 새만금 쓰레기 데이터 연동 ──

@app.get("/aihub/collect")
async def aihub_collect(count: int = 50):
    """AIHub 새만금 방조제 쓰레기 데이터 수집 (시뮬레이션)."""
    from agents.data_agent.aihub_collector import (
        generate_batch, get_category_stats, get_location_stats, to_platform_reports
    )
    batch = generate_batch(count)
    reports = to_platform_reports(batch)
    cat_stats = get_category_stats(batch)
    loc_stats = get_location_stats(batch)

    return {
        "success": True,
        "data": {
            "source": "aihub",
            "dataset_id": 71594,
            "dataset_name": "전북 새만금 방조제 유입 하천 쓰레기 데이터",
            "collected_images": len(batch),
            "total_annotations": len(reports),
            "category_stats": cat_stats,
            "location_stats": loc_stats,
            "sample_records": batch[:5],
            "platform_reports": reports[:10],
        },
    }


@app.post("/aihub/ingest")
async def aihub_ingest(payload: dict):
    """AIHub 데이터를 플랫폼 DB에 적재."""
    from agents.data_agent.aihub_collector import generate_batch, to_platform_reports
    count = payload.get("count", 30)
    batch = generate_batch(count)
    reports = to_platform_reports(batch)

    db = app.state.agent.db
    ingested = 0
    for r in reports:
        try:
            await db.execute("""
                INSERT INTO trash_reports
                  (id, reporter_id, location, zone_id, trash_type, severity,
                   status, image_urls, source, ai_confidence, created_at)
                VALUES (
                  gen_random_uuid(),
                  (SELECT id FROM users ORDER BY random() LIMIT 1),
                  ST_SetSRID(ST_MakePoint($1, $2), 4326),
                  (SELECT id FROM geofence_zones WHERE zone_key = 'SAEMANGEUM'),
                  $3, $4, 'ai_verified',
                  $5, 'aihub', $6, NOW()
                )
            """, r["lon"], r["lat"], r["trash_type"], r["severity"],
                [f"https://picsum.photos/seed/sm-{r['image_file'][:8]}/640/480"],
                r["ai_confidence"])
            ingested += 1
        except Exception:
            pass

    return {
        "success": True,
        "data": {
            "ingested": ingested,
            "total": len(reports),
            "source": "aihub",
        },
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("DATA_AGENT_PORT", 8532))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
