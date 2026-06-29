"""
WebSocket 서버 — 실시간 핀맵·랭킹·드론 위치 브로드캐스트.
Redis Pub/Sub로 마이크로서비스 → 클라이언트 이벤트 전달.
동시 1,000 연결: asyncio coroutine 기반.
포트: WS_PORT 환경변수 (기본 8520)
"""
import os
import sys
import asyncio
import json

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "shared"))

from contextlib import asynccontextmanager
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, Set

from core.db import create_redis
from core.logger import get_logger

logger = get_logger("ws-server")


class ConnectionManager:
    """WebSocket 연결 관리자 — 구역별/전역 브로드캐스트."""

    def __init__(self):
        self.zone: Dict[str, Set[WebSocket]] = {}
        self.all: Set[WebSocket] = set()

    async def connect(self, ws: WebSocket, zone_id: str = None):
        await ws.accept()
        self.all.add(ws)
        if zone_id:
            self.zone.setdefault(zone_id, set()).add(ws)
        logger.info(
            f"WS connected (zone={zone_id}). total={len(self.all)}"
        )

    def disconnect(self, ws: WebSocket, zone_id: str = None):
        self.all.discard(ws)
        if zone_id:
            self.zone.get(zone_id, set()).discard(ws)

    async def _send(self, ws: WebSocket, msg: dict) -> bool:
        try:
            await ws.send_json(msg)
            return True
        except Exception:
            return False

    async def broadcast_zone(self, zone_id: str, msg: dict):
        dead = {
            ws
            for ws in list(self.zone.get(zone_id, set()))
            if not await self._send(ws, msg)
        }
        for ws in dead:
            self.disconnect(ws, zone_id)

    async def broadcast_all(self, msg: dict):
        dead = {ws for ws in list(self.all) if not await self._send(ws, msg)}
        for ws in dead:
            self.all.discard(ws)

    @property
    def count(self):
        return len(self.all)


mgr = ConnectionManager()


@asynccontextmanager
async def lifespan(app: FastAPI):
    redis = create_redis()
    app.state.redis = redis
    pubsub = redis.pubsub()
    await pubsub.psubscribe("ws:zone:*", "ws:ranking", "ws:drone:live")

    async def listen():
        try:
            async for m in pubsub.listen():
                if m["type"] not in ("message", "pmessage"):
                    continue
                ch = m.get("channel", "")
                try:
                    data = json.loads(m["data"])
                except (json.JSONDecodeError, TypeError):
                    continue

                if ch.startswith("ws:zone:"):
                    zone_id = ch.split(":")[-1]
                    await mgr.broadcast_zone(zone_id, data)
                else:
                    await mgr.broadcast_all(data)
        except asyncio.CancelledError:
            pass

    task = asyncio.create_task(listen())
    yield
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    await pubsub.unsubscribe()
    await redis.aclose()


app = FastAPI(title="WebSocket Server", version="1.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "ws", "connections": mgr.count}


@app.websocket("/ws/zone/{zone_id}")
async def zone_ws(ws: WebSocket, zone_id: str):
    await mgr.connect(ws, zone_id)
    try:
        while True:
            data = await ws.receive_text()
            if data == "ping":
                await ws.send_text("pong")
    except WebSocketDisconnect:
        mgr.disconnect(ws, zone_id)


@app.websocket("/ws/global")
async def global_ws(ws: WebSocket):
    await mgr.connect(ws)
    try:
        while True:
            data = await ws.receive_text()
            if data == "ping":
                await ws.send_text("pong")
    except WebSocketDisconnect:
        mgr.disconnect(ws)


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("WS_PORT", 8520))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
