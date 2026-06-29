"""
SchedulerAgent — cron 기반 주기적 작업.
- 5분: PostgreSQL Materialized View 갱신
- 1시간: 보스 핀 확인 → 레이드 퀘스트 생성
- 매일: 랭킹 스냅샷 + 공공데이터 내보내기 트리거
- 매주 월: 주간 퀘스트 초기화
"""
import os
import sys
import asyncio

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "shared"))

import httpx

from core.db import create_db_pool, create_redis
from core.logger import get_logger

logger = get_logger("scheduler-agent")


async def refresh_mv(db):
    """Materialized View 갱신."""
    try:
        await db.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY user_points")
        logger.debug("MV refreshed")
    except Exception as e:
        # CONCURRENTLY 실패 시 일반 갱신
        try:
            await db.execute("REFRESH MATERIALIZED VIEW user_points")
        except Exception as e2:
            logger.error(f"MV refresh failed: {e2}")


async def check_boss_raids(db, http):
    """보스급 쓰레기 발견 시 레이드 퀘스트 자동 생성."""
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
    if pins:
        logger.info(f"Created {len(pins)} boss raid quests")


async def reset_weekly(db):
    """매주 월요일 주간 퀘스트 초기화."""
    await db.execute("""
        UPDATE user_quests SET progress=0, is_completed=FALSE, completed_at=NULL
        WHERE quest_id IN (SELECT id FROM quest_definitions WHERE quest_type='weekly')
    """)
    logger.info("Weekly quests reset")


async def daily_export(http):
    """매일 공공데이터 내보내기 트리거."""
    mcp_port = os.environ.get("MCP_PORT", "8510")
    try:
        await http.post(
            f"http://localhost:{mcp_port}/tools/trigger_opendata_export",
            json={"export_type": "all"},
        )
        logger.info("Daily export triggered")
    except Exception as e:
        logger.warning(f"Daily export trigger failed: {e}")


async def run():
    db = await create_db_pool(min_size=2, max_size=5)
    redis = create_redis()
    http = httpx.AsyncClient()

    try:
        from apscheduler.schedulers.asyncio import AsyncIOScheduler
        from apscheduler.triggers.cron import CronTrigger

        sch = AsyncIOScheduler()
        sch.add_job(refresh_mv, CronTrigger(minute="*/5"), args=[db])
        sch.add_job(check_boss_raids, CronTrigger(minute=0), args=[db, http])
        sch.add_job(daily_export, CronTrigger(hour=1, minute=0), args=[http])
        sch.add_job(
            reset_weekly, CronTrigger(day_of_week="mon", hour=0), args=[db]
        )
        sch.start()
        logger.info("Scheduler started with APScheduler")
    except ImportError:
        logger.warning(
            "APScheduler not installed. Using simple loop (5min interval)."
        )
        while True:
            await refresh_mv(db)
            await asyncio.sleep(300)

    try:
        await asyncio.Event().wait()
    finally:
        await db.close()
        await redis.aclose()
        await http.aclose()


if __name__ == "__main__":
    asyncio.run(run())
