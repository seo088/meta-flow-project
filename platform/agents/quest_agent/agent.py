"""
QuestAgent — 미션 완료 이벤트 수신 → 퀘스트 진행 +1 → 배지 지급.
Kafka Consumer: plogging.mission.completed
MCP 도구: award_points
"""
import os
import sys
import asyncio

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "shared"))

from contextlib import asynccontextmanager
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from core.events import MissionCompletedEvent
from core.kafka_client import KafkaConsumerClient
from core.mcp_client import call_tool
from core.db import create_db_pool
from core.logger import get_logger

logger = get_logger("quest-agent")

# 퀘스트 키 → 트리거 이벤트 타입 맵
TRIGGERS = {
    "weekly_plogging_3": "plogging.mission.completed",
    "eunpa_guardian_5": "plogging.mission.completed",
    "boss_raid_saemangeum": "plogging.mission.completed",
    "report_5_times": "plogging.report.created",
    "drone_upload_1": "drone.stream.uploaded",
    "weekly_report_5": "plogging.report.created",
    "weekly_drone_1": "drone.stream.uploaded",
    "saemangeum_mission_3": "plogging.mission.completed",
    "geumgang_mission_3": "plogging.mission.completed",
}


class QuestAgent:
    def __init__(self, db):
        self.db = db

    async def on_mission_completed(self, event: MissionCompletedEvent):
        if not event.cleaner_id:
            return
        await self._update(
            event.cleaner_id, "plogging.mission.completed", event.zone_id
        )

    async def _update(self, user_id: str, event_type: str, zone_id: str = None):
        quests = await self.db.fetch("""
            SELECT uq.id, uq.quest_id, uq.progress,
                   qd.target_count, qd.reward_xp, qd.quest_key,
                   qd.zone_id AS qzone, qd.reward_badge_id
            FROM user_quests uq
            JOIN quest_definitions qd ON qd.id = uq.quest_id
            WHERE uq.user_id=$1::uuid AND uq.is_completed=FALSE AND qd.is_active=TRUE
        """, user_id)

        for q in quests:
            if TRIGGERS.get(q["quest_key"]) != event_type:
                continue
            if q["qzone"] and str(q["qzone"]) != zone_id:
                continue

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
                    new_prog, q["id"],
                )
            logger.info(
                f"Quest progress: {q['quest_key']} {new_prog}/{q['target_count']}"
            )

    async def _complete(self, user_id: str, q: dict):
        await call_tool("award_points", {
            "user_id": user_id,
            "delta": q["reward_xp"],
            "reason": f"quest_completed:{q['quest_key']}",
            "ref_id": str(q["id"]),
        })
        if q["reward_badge_id"]:
            await self.db.execute("""
                INSERT INTO user_badges (user_id, badge_id)
                VALUES ($1::uuid,$2) ON CONFLICT DO NOTHING
            """, user_id, q["reward_badge_id"])
        logger.info(f"Quest completed: {q['quest_key']} by {user_id}")


@asynccontextmanager
async def lifespan(app: FastAPI):
    db = await create_db_pool(min_size=5, max_size=20)
    stop = asyncio.Event()
    consumer = KafkaConsumerClient(
        os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093"),
        "quest-agent",
        ["plogging.mission.completed"],
    )
    agent = QuestAgent(db)

    async def handle(raw: dict):
        etype = raw.get("event_type", "")
        if etype == "plogging.mission.completed":
            await agent.on_mission_completed(MissionCompletedEvent(**raw))

    task = asyncio.create_task(consumer.consume_loop(handle, dict, stop))
    app.state.agent = agent
    yield
    stop.set()
    await task
    consumer.close()
    await db.close()


app = FastAPI(title="Quest Agent", version="1.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)


@app.get("/health")
async def health():
    return {"status": "ok", "agent": "quest"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8540, log_level="info")
