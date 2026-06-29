"""
MobilityAgent — 자율주행 로봇/차량 수거 완료 처리.
MCP 도구: get_zone_pins, optimize_route, verify_cleanup, award_points
포트: MOBILITY_AGENT_PORT 환경변수 (기본 8533)
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "shared"))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from core.mcp_client import call_tool
from core.logger import get_logger

logger = get_logger("mobility-agent")

app = FastAPI(title="Mobility Agent", version="1.0.0")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)


@app.get("/health")
async def health():
    return {"status": "ok", "agent": "mobility"}


@app.post("/route/request")
async def request_route(payload: dict):
    """
    모빌리티가 출발 전 최적 경로 요청.
    payload: { zone_key, start_lat, start_lon, agent_type }
    """
    pins = await call_tool("get_zone_pins", {
        "zone_key": payload["zone_key"],
        "status_filter": "ai_verified",
        "limit": 20,
    })
    if not pins or not isinstance(pins, list) or len(pins) == 0:
        return {"success": True, "data": {"waypoints": [], "msg": "no_targets"}}

    route = await call_tool("optimize_route", {
        "start_lat": payload["start_lat"],
        "start_lon": payload["start_lon"],
        "target_ids": [p["id"] for p in pins],
        "agent_type": payload.get("agent_type", "mobility"),
    })
    return {"success": True, "data": route}


@app.post("/collection/complete")
async def collection_done(payload: dict):
    """
    수거 완료 후 인증 처리.
    payload: { report_id, before_url, after_url, mobility_id }
    """
    result = await call_tool("verify_cleanup", {
        "before_url": payload["before_url"],
        "after_url": payload["after_url"],
        "report_id": payload["report_id"],
        "cleaner_type": "mobility",
    })
    logger.info(
        f"[MobilityAgent] {payload.get('mobility_id', 'unknown')} verified: {result}"
    )
    return {"success": True, "data": result}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("MOBILITY_AGENT_PORT", 8533))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
