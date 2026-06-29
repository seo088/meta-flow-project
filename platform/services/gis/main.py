"""
GIS Service — 지오펜싱·핀맵·GeoJSON 내보내기.
포트: GIS_PORT 환경변수 (기본 8503)
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "shared"))

from contextlib import asynccontextmanager
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional

from core.types import GeoPoint
from core.db import create_db_pool
from core.logger import get_logger
from core.auth import get_current_user, get_optional_user
from core.gamification import grant_xp
from fastapi import Depends

logger = get_logger("gis-service")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = await create_db_pool(min_size=5, max_size=20)
    yield
    await app.state.db.close()


app = FastAPI(title="GIS Service", version="1.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "gis"}


@app.get("/zones/stats")
async def zone_stats():
    rows = await app.state.db.fetch("""
        SELECT gz.id, gz.zone_key, gz.name, gz.bonus_multiplier,
            ST_Y(ST_Centroid(gz.geom)) AS lat, ST_X(ST_Centroid(gz.geom)) AS lon,
            COUNT(tr.id) FILTER (WHERE tr.status='pending')   AS pending,
            COUNT(tr.id) FILTER (WHERE tr.status='completed') AS completed,
            COUNT(tr.id) FILTER (WHERE tr.severity='boss')    AS boss_count
        FROM geofence_zones gz
        LEFT JOIN trash_reports tr ON tr.zone_id = gz.id
        WHERE gz.is_active = TRUE
        GROUP BY gz.id ORDER BY gz.phase
    """)
    return {"success": True, "data": [dict(r) for r in rows]}


@app.get("/zones/list")
async def zone_list():
    rows = await app.state.db.fetch("""
        SELECT zone_key, name, phase, bonus_multiplier, is_active,
               ST_Y(ST_Centroid(geom)) AS lat, ST_X(ST_Centroid(geom)) AS lon,
               ST_AsGeoJSON(geom)::json AS geojson
        FROM geofence_zones ORDER BY phase
    """)
    return {"success": True, "data": [dict(r) for r in rows]}


@app.get("/pins/nearby")
async def nearby_pins(
    lat: float = Query(...),
    lon: float = Query(...),
    radius: int = Query(500, le=50000),
    size: int = Query(50, le=200),
):
    db = app.state.db
    # trash_reports 기반 핀
    rows = await db.fetch("""
        SELECT tr.id, tr.trash_type, tr.severity, tr.status,
            ST_X(tr.location::geometry) AS lon,
            ST_Y(tr.location::geometry) AS lat,
            tr.created_at,
            gz.name AS zone_name
        FROM trash_reports tr
        LEFT JOIN geofence_zones gz ON gz.id = tr.zone_id
        WHERE ST_DWithin(tr.location,
            ST_SetSRID(ST_MakePoint($1,$2),4326)::geography, $3)
        ORDER BY tr.created_at DESC LIMIT $4
    """, lon, lat, radius, size)
    result = [dict(r) for r in rows]

    # SNS 제보 게시글 (post_type='report', 위치 있는 것)도 포함
    post_rows = await db.fetch("""
        SELECT p.id, 'general' AS trash_type, 'mid' AS severity, 'pending' AS status,
            ST_X(p.location::geometry) AS lon,
            ST_Y(p.location::geometry) AS lat,
            p.created_at,
            gz.name AS zone_name
        FROM posts p
        LEFT JOIN geofence_zones gz ON gz.id = p.zone_id
        WHERE p.post_type = 'report'
          AND p.location IS NOT NULL
          AND (p.is_hidden = false OR p.is_hidden IS NULL)
          AND ST_DWithin(p.location,
              ST_SetSRID(ST_MakePoint($1,$2),4326)::geography, $3)
        ORDER BY p.created_at DESC LIMIT $4
    """, lon, lat, radius, size)

    # 중복 제거 (같은 id가 있으면 trash_reports 우선)
    existing_ids = {str(r['id']) for r in result}
    for pr in post_rows:
        if str(pr['id']) not in existing_ids:
            result.append(dict(pr))

    return {"success": True, "data": result}


@app.get("/pins/recent")
async def recent_pins(size: int = Query(50, le=200)):
    """최근 제보 목록 (위치 유무 무관) — 제보 타임라인용."""
    db = app.state.db
    rows = await db.fetch("""
        SELECT tr.id, tr.trash_type, tr.severity, tr.status,
            ST_X(tr.location::geometry) AS lon,
            ST_Y(tr.location::geometry) AS lat,
            tr.created_at,
            gz.name AS zone_name,
            u.username, u.display_name
        FROM trash_reports tr
        LEFT JOIN geofence_zones gz ON gz.id = tr.zone_id
        LEFT JOIN users u ON u.id = tr.reporter_id
        ORDER BY tr.created_at DESC LIMIT $1
    """, size)
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


@app.post("/zones/update-location")
async def update_zone_location(payload: dict, current_user: dict | None = Depends(get_optional_user)):
    """구역 마커 좌표 업데이트 (크라우드소싱). 로그인 시 zone당 +10 XP 1회 부여."""
    zone_key = payload.get("zone_key")
    lat = payload.get("lat")
    lon = payload.get("lon")
    if not zone_key or lat is None or lon is None:
        return {"success": False, "error": "zone_key, lat, lon 필수"}
    try:
        db = app.state.db
        old = await db.fetchrow(
            "SELECT id, ST_Y(ST_Centroid(geom)) as old_lat, ST_X(ST_Centroid(geom)) as old_lon "
            "FROM geofence_zones WHERE zone_key=$1",
            zone_key)
        if not old:
            return {"success": False, "error": "구역을 찾을 수 없습니다"}
        dx = lon - old["old_lon"]
        dy = lat - old["old_lat"]
        await db.execute(
            "UPDATE geofence_zones SET geom = ST_Translate(geom, $1, $2) WHERE zone_key = $3",
            dx, dy, zone_key)
        logger.info(f"Zone {zone_key} moved to lat={lat}, lon={lon} (dx={dx:.6f}, dy={dy:.6f})")

        # XP 부여: 로그인 사용자 한정, 동일 zone에 대해 1회만 (ref_id=zone uuid)
        xp_earned = 0
        if current_user and current_user.get("user_id"):
            try:
                _, dup = await grant_xp(
                    db, None, current_user["user_id"], 10, "zone_fix",
                    ref_id=str(old["id"]), kafka=None, logger=logger,
                )
                xp_earned = 0 if dup else 10
            except Exception as xp_err:
                logger.warning(f"zone_fix XP 부여 실패: {xp_err}")
        return {"success": True, "data": {"xp_earned": xp_earned}}
    except Exception as e:
        logger.error(f"Zone update failed: {e}")
        return {"success": False, "error": str(e)[:200]}


@app.get("/export/geojson")
async def export_geojson(
    zone_key: Optional[str] = Query(None),
    limit: int = Query(1000, le=5000),
):
    if zone_key:
        rows = await app.state.db.fetch("""
            SELECT tr.id, tr.trash_type, tr.severity, tr.status, tr.source,
                ST_X(tr.location::geometry) AS lon,
                ST_Y(tr.location::geometry) AS lat,
                tr.created_at::text, gz.name AS zone_name
            FROM trash_reports tr
            LEFT JOIN geofence_zones gz ON gz.id = tr.zone_id
            WHERE gz.zone_key = $1
            ORDER BY tr.created_at DESC LIMIT $2
        """, zone_key, limit)
    else:
        rows = await app.state.db.fetch("""
            SELECT tr.id, tr.trash_type, tr.severity, tr.status, tr.source,
                ST_X(tr.location::geometry) AS lon,
                ST_Y(tr.location::geometry) AS lat,
                tr.created_at::text, gz.name AS zone_name
            FROM trash_reports tr
            LEFT JOIN geofence_zones gz ON gz.id = tr.zone_id
            ORDER BY tr.created_at DESC LIMIT $1
        """, limit)

    features = [
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [r["lon"], r["lat"]]},
            "properties": {
                k: v for k, v in dict(r).items() if k not in ("lat", "lon")
            },
        }
        for r in rows
    ]
    return {
        "type": "FeatureCollection",
        "features": features,
        "metadata": {
            "count": len(features),
            "license": "CC-BY-4.0",
            "source": "meta-plogging-gunsan",
        },
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("GIS_PORT", 8503))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
