"""
Data Service — Elasticsearch 집계·시계열 통계·공공데이터 API.
포트: DATA_PORT 환경변수 (기본 8505)
"""
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "shared"))

from contextlib import asynccontextmanager
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional

from core.db import create_db_pool
from core.logger import get_logger

logger = get_logger("data-service")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = await create_db_pool(min_size=3, max_size=10)
    try:
        from elasticsearch import AsyncElasticsearch
        es_host = os.environ.get("ES_HOST", "localhost")
        es_port = os.environ.get("ES_PORT", "9201")
        app.state.es = AsyncElasticsearch(f"http://{es_host}:{es_port}")
        app.state.es_connected = True
    except Exception as e:
        logger.warning(f"Elasticsearch not available: {e}")
        app.state.es = None
        app.state.es_connected = False
    yield
    await app.state.db.close()
    if app.state.es_connected and app.state.es:
        await app.state.es.close()


app = FastAPI(title="Data Service", version="1.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "data",
            "es_connected": getattr(app.state, "es_connected", False)}


@app.get("/statistics/zones")
async def zone_stats():
    """구역별 집계 — ES 사용 가능 시 ES, 아니면 PostgreSQL 폴백."""
    if app.state.es_connected:
        try:
            body = {
                "size": 0,
                "aggs": {
                    "by_zone": {
                        "terms": {"field": "zone_key", "size": 10},
                        "aggs": {
                            "by_severity": {"terms": {"field": "severity"}},
                            "by_status": {"terms": {"field": "status"}},
                        },
                    }
                },
            }
            res = await app.state.es.search(index="plogging-reports", body=body)
            buckets = res["aggregations"]["by_zone"]["buckets"]
            return {"success": True, "source": "elasticsearch", "data": [
                {
                    "zone_key": b["key"],
                    "total": b["doc_count"],
                    "severity": {s["key"]: s["doc_count"] for s in b["by_severity"]["buckets"]},
                    "status": {s["key"]: s["doc_count"] for s in b["by_status"]["buckets"]},
                }
                for b in buckets
            ]}
        except Exception as e:
            logger.warning(f"ES query failed, using PG fallback: {e}")

    # PostgreSQL 폴백
    rows = await app.state.db.fetch("""
        SELECT gz.zone_key,
            COUNT(tr.id) AS total,
            COUNT(tr.id) FILTER (WHERE tr.severity='boss') AS boss,
            COUNT(tr.id) FILTER (WHERE tr.severity='high') AS high,
            COUNT(tr.id) FILTER (WHERE tr.status='pending') AS pending,
            COUNT(tr.id) FILTER (WHERE tr.status='completed') AS completed
        FROM geofence_zones gz
        LEFT JOIN trash_reports tr ON tr.zone_id = gz.id
        WHERE gz.is_active = TRUE
        GROUP BY gz.zone_key
    """)
    return {"success": True, "source": "postgresql", "data": [dict(r) for r in rows]}


@app.get("/statistics/timeseries")
async def timeseries(
    zone_key: Optional[str] = Query(None),
    interval: str = Query("1d", pattern="^(1h|1d|1w)$"),
    days: int = Query(30, ge=1, le=365),
):
    """시계열 집계 — ES 사용 가능 시 ES, 아니면 PG 폴백."""
    if app.state.es_connected:
        try:
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
                            "by_type": {"terms": {"field": "trash_type"}},
                            "completed": {"filter": {"term": {"status": "completed"}}},
                        },
                    }
                },
            }
            res = await app.state.es.search(index="plogging-reports", body=body)
            return {"success": True, "data": res["aggregations"]["over_time"]["buckets"]}
        except Exception as e:
            logger.warning(f"ES timeseries failed: {e}")

    # PG 폴백: 일별 집계
    if zone_key:
        rows = await app.state.db.fetch("""
            SELECT DATE(tr.created_at) AS date, COUNT(*) AS count,
                   COUNT(*) FILTER (WHERE tr.status='completed') AS completed
            FROM trash_reports tr
            JOIN geofence_zones gz ON gz.id = tr.zone_id
            WHERE gz.zone_key = $1 AND tr.created_at > NOW() - ($2 || ' days')::interval
            GROUP BY DATE(tr.created_at) ORDER BY date
        """, zone_key, str(days))
    else:
        rows = await app.state.db.fetch("""
            SELECT DATE(created_at) AS date, COUNT(*) AS count,
                   COUNT(*) FILTER (WHERE status='completed') AS completed
            FROM trash_reports
            WHERE created_at > NOW() - ($1 || ' days')::interval
            GROUP BY DATE(created_at) ORDER BY date
        """, str(days))
    return {"success": True, "data": [dict(r) for r in rows]}


@app.get("/opendata/list")
async def opendata_list():
    rows = await app.state.db.fetch("""
        SELECT export_type, file_url, record_count, date_from, date_to, created_at
        FROM opendata_exports ORDER BY created_at DESC LIMIT 50
    """)
    return {"success": True, "data": [dict(r) for r in rows]}


@app.get("/opendata/export/geojson")
async def direct_geojson(
    zone_key: Optional[str] = Query(None),
    limit: int = Query(1000, le=5000),
):
    """실시간 GeoJSON 스트리밍 (소량 요청용)."""
    if zone_key:
        rows = await app.state.db.fetch("""
            SELECT tr.id, tr.trash_type, tr.severity, tr.status,
                   ST_X(tr.location::geometry) AS lon,
                   ST_Y(tr.location::geometry) AS lat,
                   tr.created_at::text, gz.zone_key
            FROM trash_reports tr
            LEFT JOIN geofence_zones gz ON gz.id = tr.zone_id
            WHERE tr.source != 'unity_sim' AND gz.zone_key = $1
            ORDER BY tr.created_at DESC LIMIT $2
        """, zone_key, limit)
    else:
        rows = await app.state.db.fetch("""
            SELECT tr.id, tr.trash_type, tr.severity, tr.status,
                   ST_X(tr.location::geometry) AS lon,
                   ST_Y(tr.location::geometry) AS lat,
                   tr.created_at::text, gz.zone_key
            FROM trash_reports tr
            LEFT JOIN geofence_zones gz ON gz.id = tr.zone_id
            WHERE tr.source != 'unity_sim'
            ORDER BY tr.created_at DESC LIMIT $1
        """, limit)

    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [r["lon"], r["lat"]]},
                "properties": {k: v for k, v in dict(r).items() if k not in ("lat", "lon")},
            }
            for r in rows
        ],
        "metadata": {
            "count": len(rows),
            "license": "CC-BY-4.0",
            "source": "meta-plogging-gunsan",
            "generated_at": datetime.utcnow().isoformat(),
        },
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("DATA_PORT", 8505))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
