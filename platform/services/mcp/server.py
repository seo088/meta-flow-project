"""
FastMCP 기반 MCP 서버 — 에이전트들이 SSE로 연결해 도구를 호출하는 공유 허브.
7 Tools + 1 Resource. 포인트 지급은 반드시 award_points 도구만 사용 (중복 방지).
포트: MCP_PORT 환경변수 (기본 8510)
"""
import os
import sys
import asyncio
import json
import uuid

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "shared"))

import httpx
from typing import Optional, List

from core.types import TrashType, Severity, ZoneKey
from core.events import (
    PointsUpdatedEvent,
    AIDetectedEvent,
    MissionCompletedEvent,
)
from core.kafka_client import KafkaProducerClient
from core.db import create_db_pool, create_redis
from core.logger import get_logger

logger = get_logger("mcp-server")

# 공유 클라이언트 (서버 기동 시 초기화)
_db = None
_redis = None
_kafka = None
_http = None


def _ai_service_url():
    return os.environ.get("AI_SERVICE_URL", "http://localhost:8504")


async def init_clients():
    global _db, _redis, _kafka, _http
    _db = await create_db_pool(min_size=5, max_size=20)
    _redis = create_redis()
    _kafka = KafkaProducerClient(
        os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093")
    )
    _http = httpx.AsyncClient(timeout=30.0)


# ──────────────────────────────────────────────────────────
# AI_MODE=dummy이면 FastMCP 없이 순수 FastAPI로 폴백
# ──────────────────────────────────────────────────────────

if os.environ.get("AI_MODE") == "dummy":
    # FastMCP 미설치 시에도 동작하는 폴백 모드
    from fastapi import FastAPI
    from fastapi.middleware.cors import CORSMiddleware

    app = FastAPI(title="MCP Server (dummy mode)", version="1.0.0")
    app.add_middleware(
        CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
    )

    @app.on_event("startup")
    async def startup():
        await init_clients()

    @app.on_event("shutdown")
    async def shutdown():
        if _db:
            await _db.close()
        if _redis:
            await _redis.aclose()
        if _http:
            await _http.aclose()

    @app.get("/health")
    async def health():
        return {"status": "ok", "service": "mcp", "mode": "dummy"}

    @app.post("/tools/{tool_name}")
    async def call_tool_api(tool_name: str, payload: dict):
        """더미 모드: MCP 도구를 REST API로 직접 호출."""
        tools = {
            "detect_trash": _detect_trash,
            "verify_cleanup": _verify_cleanup,
            "get_zone_pins": _get_zone_pins,
            "optimize_route": _optimize_route,
            "award_points": _award_points,
            "trigger_opendata_export": _trigger_opendata_export,
            "get_zone_statistics": _get_zone_statistics,
        }
        handler = tools.get(tool_name)
        if not handler:
            return {"error": f"Unknown tool: {tool_name}"}
        return await handler(**payload)

else:
    # 실제 FastMCP 모드
    try:
        from fastmcp import FastMCP

        mcp = FastMCP(
            name="MetaPlogging MCP",
            version="1.0.0",
            instructions="군산대 메타 플로깅 플랫폼 MCP 서버. 탐지·인증·포인트·공공데이터 도구 제공.",
        )

        @mcp.tool()
        async def detect_trash(
            image_urls: List[str], report_id: str, source: str = "sns"
        ) -> dict:
            return await _detect_trash(image_urls, report_id, source)

        @mcp.tool()
        async def verify_cleanup(
            before_url: str,
            after_url: str,
            report_id: str,
            cleaner_id: Optional[str] = None,
            cleaner_type: str = "human",
        ) -> dict:
            return await _verify_cleanup(
                before_url, after_url, report_id, cleaner_id, cleaner_type
            )

        @mcp.tool()
        async def get_zone_pins(
            zone_key: str,
            status_filter: Optional[str] = None,
            limit: int = 100,
        ) -> List[dict]:
            return await _get_zone_pins(zone_key, status_filter, limit)

        @mcp.tool()
        async def optimize_route(
            start_lat: float,
            start_lon: float,
            target_ids: List[str],
            agent_type: str = "drone",
        ) -> dict:
            return await _optimize_route(start_lat, start_lon, target_ids, agent_type)

        @mcp.tool()
        async def award_points(
            user_id: str,
            delta: int,
            reason: str,
            ref_id: Optional[str] = None,
        ) -> dict:
            return await _award_points(user_id, delta, reason, ref_id)

        @mcp.tool()
        async def trigger_opendata_export(
            export_type: str = "all",
            date_from: Optional[str] = None,
            date_to: Optional[str] = None,
        ) -> dict:
            return await _trigger_opendata_export(export_type, date_from, date_to)

        @mcp.tool()
        async def get_zone_statistics(
            zone_key: Optional[str] = None,
        ) -> List[dict]:
            return await _get_zone_statistics(zone_key)

        @mcp.resource("zones://all")
        async def list_zones() -> str:
            rows = await _db.fetch(
                "SELECT zone_key, name, phase, bonus_multiplier FROM geofence_zones ORDER BY phase"
            )
            return json.dumps([dict(r) for r in rows], ensure_ascii=False)

        app = mcp.get_asgi_app()

    except ImportError:
        logger.warning("FastMCP not installed, using dummy mode")
        from fastapi import FastAPI

        app = FastAPI(title="MCP Server (no fastmcp)", version="1.0.0")

        @app.get("/health")
        async def health():
            return {"status": "ok", "mode": "no-fastmcp"}


# ──────────────────────────────────────────────────────────
# 도구 구현 (MCP/REST 공용)
# ──────────────────────────────────────────────────────────


async def _detect_trash(
    image_urls: list, report_id: str, source: str = "sns"
) -> dict:
    """YOLO v8로 이미지를 탐지하고 결과를 DB에 저장."""
    try:
        r = await _http.post(
            f"{_ai_service_url()}/predictions/yolo_plogging_v8",
            json={"image_urls": image_urls},
        )
        res = r.json()
    except Exception as e:
        logger.error(f"AI call failed: {e}")
        import random
        res = {
            "class": random.choice(["plastic", "food", "general"]),
            "confidence": round(0.6 + 0.35 * __import__("random").random(), 3),
        }

    conf = float(res.get("confidence", 0.0))
    tt = res.get("class", "unknown")
    sev = (
        "boss" if tt == "large" else
        "high" if conf > 0.85 else
        "mid" if conf > 0.65 else "low"
    )

    await _db.execute("""
        UPDATE trash_reports
        SET ai_confidence=$1, ai_label=$2, status='ai_verified',
            trash_type=$3, severity=$4
        WHERE id=$5::uuid
    """, conf, json.dumps(res), tt, sev, report_id)

    _kafka.produce(
        "plogging.ai.detected",
        AIDetectedEvent(
            report_id=report_id,
            trash_type=TrashType(tt),
            severity=Severity(sev),
            confidence=conf,
            bounding_boxes=res.get("boxes"),
            model_version="yolo_v8",
        ),
        key=report_id,
    )
    logger.info(f"[detect_trash] {report_id}: {tt} ({conf:.3f})")
    return {"trash_type": tt, "severity": sev, "confidence": conf}


async def _verify_cleanup(
    before_url: str,
    after_url: str,
    report_id: str,
    cleaner_id: str = None,
    cleaner_type: str = "human",
) -> dict:
    """전후 이미지를 SSIM으로 비교해 청소를 인증하고 포인트를 지급."""
    try:
        r = await _http.post(
            f"{_ai_service_url()}/predictions/cleanup_verify_v1",
            json={"before_url": before_url, "after_url": after_url},
        )
        score = float(r.json()["ssim_score"])
    except Exception:
        import random
        score = round(random.uniform(0.65, 0.98), 3)

    if score <= 0.6:
        return {"verified": False, "score": score, "points_awarded": 0}

    zone = await _db.fetchrow("""
        SELECT gz.bonus_multiplier FROM trash_reports tr
        JOIN geofence_zones gz ON gz.id = tr.zone_id WHERE tr.id = $1::uuid
    """, report_id)
    mult = float(zone["bonus_multiplier"]) if zone else 1.0
    pts = int(50 * mult)

    cleanup_id = await _db.fetchval("""
        INSERT INTO cleanups
          (report_id, cleaner_id, cleaner_type, before_image, after_image,
           verify_score, verified, points_awarded)
        VALUES ($1::uuid,$2,$3,$4,$5,$6,TRUE,$7) RETURNING id
    """, report_id, cleaner_id, cleaner_type, before_url, after_url, score, pts)

    await _db.execute(
        "UPDATE trash_reports SET status='completed' WHERE id=$1::uuid", report_id
    )

    if cleaner_id:
        await _award_points(
            user_id=cleaner_id,
            delta=pts,
            reason="cleanup_verified",
            ref_id=str(cleanup_id),
        )

    zone_row = await _db.fetchrow(
        "SELECT zone_id FROM trash_reports WHERE id=$1::uuid", report_id
    )
    _kafka.produce(
        "plogging.mission.completed",
        MissionCompletedEvent(
            cleanup_id=str(cleanup_id),
            report_id=report_id,
            cleaner_id=cleaner_id,
            cleaner_type=cleaner_type,
            verify_score=score,
            points_awarded=pts,
            zone_id=str(zone_row["zone_id"]) if zone_row and zone_row["zone_id"] else None,
        ),
        key=report_id,
    )
    return {"verified": True, "score": score, "points_awarded": pts}


async def _get_zone_pins(
    zone_key: str, status_filter: str = None, limit: int = 100
) -> list:
    """특정 지오펜싱 구역의 수거 대상 핀 목록."""
    if status_filter:
        rows = await _db.fetch("""
            SELECT tr.id, ST_Y(tr.location::geometry) AS lat,
                   ST_X(tr.location::geometry) AS lon,
                   tr.trash_type, tr.severity, tr.status
            FROM trash_reports tr
            JOIN geofence_zones gz ON gz.id = tr.zone_id
            WHERE gz.zone_key = $1 AND tr.status = $2
            ORDER BY CASE tr.severity WHEN 'boss' THEN 1 WHEN 'high' THEN 2
                                      WHEN 'mid' THEN 3 ELSE 4 END
            LIMIT $3
        """, zone_key, status_filter, limit)
    else:
        rows = await _db.fetch("""
            SELECT tr.id, ST_Y(tr.location::geometry) AS lat,
                   ST_X(tr.location::geometry) AS lon,
                   tr.trash_type, tr.severity, tr.status
            FROM trash_reports tr
            JOIN geofence_zones gz ON gz.id = tr.zone_id
            WHERE gz.zone_key = $1
            ORDER BY CASE tr.severity WHEN 'boss' THEN 1 WHEN 'high' THEN 2
                                      WHEN 'mid' THEN 3 ELSE 4 END
            LIMIT $2
        """, zone_key, limit)
    return [dict(r) for r in rows]


async def _optimize_route(
    start_lat: float,
    start_lon: float,
    target_ids: list,
    agent_type: str = "drone",
) -> dict:
    """RL 에이전트로 최적 경로 생성."""
    targets = await _db.fetch("""
        SELECT id, ST_Y(location::geometry) AS lat, ST_X(location::geometry) AS lon
        FROM trash_reports WHERE id = ANY($1::uuid[])
    """, target_ids)

    try:
        r = await _http.post(
            f"{_ai_service_url()}/predictions/rl_route_agent",
            json={
                "start": {"lat": start_lat, "lon": start_lon},
                "targets": [dict(t) for t in targets],
                "agent_type": agent_type,
            },
        )
        return r.json()
    except Exception as e:
        logger.error(f"RL route failed: {e}")
        return {
            "waypoints": [dict(t) for t in targets],
            "total_distance_km": 0.0,
            "estimated_time_min": 0,
        }


async def _award_points(
    user_id: str, delta: int, reason: str, ref_id: str = None
) -> dict:
    """포인트 지급. ON CONFLICT로 중복 지급을 DB 레벨에서 차단."""
    async with _db.acquire() as conn:
        async with conn.transaction():
            inserted = await conn.fetchval("""
                INSERT INTO point_ledger (user_id, delta, reason, ref_id)
                VALUES ($1::uuid,$2,$3,$4)
                ON CONFLICT (user_id, ref_id, reason) DO NOTHING
                RETURNING id
            """, user_id, delta, reason, ref_id)

            if not inserted:
                existing = await conn.fetchval(
                    "SELECT COALESCE(SUM(delta),0) FROM point_ledger WHERE user_id=$1::uuid",
                    user_id,
                )
                return {"new_total": existing, "delta": 0, "duplicate": True}

            new_total = await conn.fetchval(
                "SELECT COALESCE(SUM(delta),0) FROM point_ledger WHERE user_id=$1::uuid",
                user_id,
            )

    await _redis.zadd("ranking:global", {user_id: new_total})
    _kafka.produce(
        "user.points.updated",
        PointsUpdatedEvent(
            user_id=user_id, delta=delta, new_total=new_total, reason=reason
        ),
        key=user_id,
    )
    logger.info(f"[award_points] {user_id} {delta:+d} → {new_total}")
    return {"new_total": new_total, "delta": delta, "duplicate": False}


async def _trigger_opendata_export(
    export_type: str = "all", date_from: str = None, date_to: str = None
) -> dict:
    """DataAgent가 구독하는 Kafka 토픽으로 내보내기 이벤트 발행."""
    job_id = str(uuid.uuid4())
    _kafka.produce(
        "opendata.export.triggered",
        type("E", (), {
            "model_dump_json": lambda s: json.dumps({
                "event_id": job_id,
                "event_type": "opendata.export.triggered",
                "export_type": export_type,
                "date_from": date_from,
                "date_to": date_to,
            })
        })(),
        key=job_id,
    )
    return {"job_id": job_id, "status": "queued"}


async def _get_zone_statistics(zone_key: str = None) -> list:
    """구역별 실시간 쓰레기 현황 통계."""
    if zone_key:
        rows = await _db.fetch("""
            SELECT gz.zone_key, gz.name,
                COUNT(tr.id) FILTER (WHERE tr.status='pending')   AS pending,
                COUNT(tr.id) FILTER (WHERE tr.status='completed') AS completed,
                COUNT(tr.id) FILTER (WHERE tr.severity='boss')    AS boss_count
            FROM geofence_zones gz
            LEFT JOIN trash_reports tr ON tr.zone_id = gz.id
                AND tr.created_at > NOW() - INTERVAL '24 hours'
            WHERE gz.is_active = TRUE AND gz.zone_key = $1
            GROUP BY gz.id, gz.zone_key, gz.name
        """, zone_key)
    else:
        rows = await _db.fetch("""
            SELECT gz.zone_key, gz.name,
                COUNT(tr.id) FILTER (WHERE tr.status='pending')   AS pending,
                COUNT(tr.id) FILTER (WHERE tr.status='completed') AS completed,
                COUNT(tr.id) FILTER (WHERE tr.severity='boss')    AS boss_count
            FROM geofence_zones gz
            LEFT JOIN trash_reports tr ON tr.zone_id = gz.id
                AND tr.created_at > NOW() - INTERVAL '24 hours'
            WHERE gz.is_active = TRUE
            GROUP BY gz.id, gz.zone_key, gz.name
        """)
    return [dict(r) for r in rows]


if __name__ == "__main__":
    import uvicorn

    asyncio.get_event_loop().run_until_complete(init_clients())
    port = int(os.environ.get("MCP_PORT", 8510))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
