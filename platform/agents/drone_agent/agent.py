"""
DroneAgent — 드론 영상 수신 → MCP 탐지 → 경로 최적화 → 수거 완료 처리.
MCP 도구: detect_trash, get_zone_pins, optimize_route, verify_cleanup
포트: DRONE_AGENT_PORT 환경변수 (기본 8531)
"""
import os
import sys
import asyncio

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "shared"))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from core.events import DroneStreamEvent
from core.kafka_client import KafkaProducerClient
from core.mcp_client import call_tool
from core.logger import get_logger

# 영상 프레임 처리 모듈
from agents.drone_agent.video_processor import process_drone_video, FrameDetection

logger = get_logger("drone-agent")


class DroneAgent:
    def __init__(self):
        self.kafka = KafkaProducerClient(
            os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093")
        )

    async def handle_video(
        self, drone_id: str, video_url: str, lat: float, lon: float,
        zone_id: str = None, **kwargs,
    ):
        """드론 영상 → Kafka 이벤트 → AI 처리 대기 → 핀 조회 → 경로 최적화."""
        self.kafka.produce(
            "drone.stream.uploaded",
            DroneStreamEvent(
                drone_id=drone_id,
                video_url=video_url,
                location={"lat": lat, "lon": lon},
                zone_id=zone_id,
                duration_sec=0,
            ),
            key=drone_id,
        )
        self.kafka.flush()

        # AI 처리 대기 (더미 모드에서는 즉시 반환)
        await asyncio.sleep(3 if os.environ.get("AI_MODE") == "dummy" else 15)

        if zone_id:
            pins = await call_tool("get_zone_pins", {
                "zone_key": zone_id,
                "status_filter": "ai_verified",
                "limit": 20,
            })
            if pins:
                return await call_tool("optimize_route", {
                    "start_lat": lat,
                    "start_lon": lon,
                    "target_ids": [p["id"] for p in pins] if isinstance(pins, list) else [],
                    "agent_type": "drone",
                })
        return {"status": "no_targets"}

    async def complete_collection(
        self, drone_id: str, report_id: str, before_url: str, after_url: str,
        **kwargs,
    ):
        """수거 완료 인증."""
        return await call_tool("verify_cleanup", {
            "before_url": before_url,
            "after_url": after_url,
            "report_id": report_id,
            "cleaner_type": "drone",
        })

    async def patrol(self, zone_key: str, lat: float, lon: float, **kwargs):
        """구역 순찰 — pending 핀 조회 → 경로 최적화."""
        pins = await call_tool("get_zone_pins", {
            "zone_key": zone_key,
            "status_filter": "pending",
            "limit": 30,
        })
        if not pins or not isinstance(pins, list) or len(pins) == 0:
            return {"status": "no_pending"}
        return await call_tool("optimize_route", {
            "start_lat": lat,
            "start_lon": lon,
            "target_ids": [p["id"] for p in pins],
            "agent_type": "drone",
        })


app = FastAPI(title="Drone Agent", version="1.0.0")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)
_agent = DroneAgent()


@app.get("/health")
async def health():
    return {"status": "ok", "agent": "drone"}


@app.post("/webhook/video-upload")
async def video_upload(payload: dict):
    result = await _agent.handle_video(**payload)
    return {"success": True, "data": result}


@app.post("/webhook/collection-complete")
async def collection_complete(payload: dict):
    result = await _agent.complete_collection(**payload)
    return {"success": True, "data": result}


@app.post("/patrol")
async def patrol(payload: dict):
    result = await _agent.patrol(**payload)
    return {"success": True, "data": result}


@app.post("/process-video")
async def process_video(payload: dict):
    """영상 프레임 추출 + AI 탐지 (video_processor.py 연동)."""
    detections = await process_drone_video(
        video_url=payload.get("video_url", ""),
        start_lat=payload.get("lat", 35.97),
        start_lon=payload.get("lon", 126.74),
        heading_deg=payload.get("heading", 0.0),
        altitude_m=payload.get("altitude", 30.0),
    )
    return {
        "success": True,
        "data": {
            "detection_count": len(detections),
            "detections": [
                {
                    "lat": d.lat, "lon": d.lon,
                    "trash_type": d.trash_type,
                    "confidence": d.confidence,
                    "frame_index": d.frame_index,
                }
                for d in detections
            ],
        },
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("DRONE_AGENT_PORT", 8531))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
