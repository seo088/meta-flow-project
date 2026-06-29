#!/usr/bin/env python3
"""
더미 데이터 시드 스크립트 — DB에 사용자 50명, 제보 200건, 게시물, 포인트를 주입.
Usage: python scripts/seed_dummy.py
"""
import asyncio
import os
import sys
import random

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))

import asyncpg
from core.dummy.generators import (
    gen_users, gen_report, gen_post, gen_drone_event,
    gen_point_entry, ZONES, random_zone,
)
from core.gamification import ACTION_XP
from core.logger import get_logger

logger = get_logger("seed-dummy")


async def main():
    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL 환경변수를 설정하세요 (source .env)")
    logger.info(f"Connecting to {db_url}")
    pool = await asyncpg.create_pool(dsn=db_url, min_size=3, max_size=10)

    # 1. 사용자 50명
    users = gen_users(50)
    logger.info(f"Inserting {len(users)} users...")
    for u in users:
        await pool.execute("""
            INSERT INTO users (id, username, email, password_hash, display_name, avatar_url, role)
            VALUES ($1::uuid, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (username) DO NOTHING
        """, u["id"], u["username"], u["email"], u["password_hash"],
             u["display_name"], u["avatar_url"], u["role"])

    # 지오펜싱 zone_id 매핑
    zone_rows = await pool.fetch("SELECT id, zone_key FROM geofence_zones")
    zone_map = {r["zone_key"]: str(r["id"]) for r in zone_rows}

    # 2. 제보 200건 + 게시물
    reports = []
    logger.info("Generating 200 reports + posts...")
    for i in range(200):
        user = random.choice(users)
        report = gen_report(user["id"])
        reports.append(report)
        zid = zone_map.get(report["zone_key"])

        await pool.execute("""
            INSERT INTO trash_reports
              (id, reporter_id, location, zone_id, trash_type, severity, status,
               image_urls, source, ai_confidence, created_at)
            VALUES ($1::uuid, $2::uuid,
                    ST_SetSRID(ST_MakePoint($3, $4), 4326),
                    $5::uuid, $6, $7, $8, $9, $10, $11, $12)
            ON CONFLICT DO NOTHING
        """, report["id"], report["reporter_id"],
             report["lon"], report["lat"],
             zid, report["trash_type"], report["severity"],
             report["status"], report["image_urls"],
             report["source"], report["ai_confidence"],
             report["created_at"])

        # 게시물 (80% 확률)
        if random.random() > 0.2:
            post = gen_post(user, report)
            await pool.execute("""
                INSERT INTO posts
                  (id, user_id, report_id, content, image_urls, hashtags,
                   location, zone_id, post_type, like_count, created_at)
                VALUES ($1::uuid, $2::uuid, $3::uuid, $4, $5, $6,
                        ST_SetSRID(ST_MakePoint($7, $8), 4326),
                        $9::uuid, $10, $11, $12)
                ON CONFLICT DO NOTHING
            """, post["id"], post["user_id"], post["report_id"],
                 post["content"], post["image_urls"], post["hashtags"],
                 post["lon"], post["lat"], zid,
                 post["post_type"], post["like_count"], post["created_at"])

    # 3. 포인트 & 랭킹
    logger.info("Generating points for users...")
    actions = list(ACTION_XP.keys())
    for user in users:
        n_actions = random.randint(3, 30)
        total_xp = 0
        for _ in range(n_actions):
            action = random.choice(actions)
            xp = ACTION_XP[action]
            entry = gen_point_entry(user["id"], xp, action)
            total_xp += xp
            await pool.execute("""
                INSERT INTO point_ledger (id, user_id, delta, reason, ref_id, created_at)
                VALUES ($1::uuid, $2::uuid, $3, $4, $5::uuid, $6)
                ON CONFLICT DO NOTHING
            """, entry["id"], entry["user_id"], entry["delta"],
                 entry["reason"], entry["ref_id"], entry["created_at"])

    # MV 갱신
    await pool.execute("REFRESH MATERIALIZED VIEW user_points")

    # 4. 드론 이벤트 20건
    logger.info("Generating 20 drone events...")
    for _ in range(20):
        zk = random.choice(["EUNPA", "SAEMANGEUM", "GEUMGANG"])
        de = gen_drone_event(zk)
        zid = zone_map.get(zk)
        await pool.execute("""
            INSERT INTO drone_events
              (id, drone_id, event_type, location, zone_id, video_url,
               detected_count, created_at)
            VALUES ($1::uuid, $2, $3,
                    ST_SetSRID(ST_MakePoint($4, $5), 4326),
                    $6::uuid, $7, $8, $9)
            ON CONFLICT DO NOTHING
        """, de["id"], de["drone_id"], de["event_type"],
             de["lon"], de["lat"], zid, de["video_url"],
             de["detected_count"], de["created_at"])

    # 5. 배지 부여 (랜덤)
    logger.info("Assigning badges...")
    badge_rows = await pool.fetch("SELECT id, badge_key FROM badge_definitions")
    for user in users[:30]:  # 상위 30명에게
        n_badges = random.randint(1, 5)
        for badge in random.sample(badge_rows, min(n_badges, len(badge_rows))):
            await pool.execute("""
                INSERT INTO user_badges (user_id, badge_id)
                VALUES ($1::uuid, $2::uuid)
                ON CONFLICT DO NOTHING
            """, user["id"], badge["id"])

    # 6. 퀘스트 수락 (랜덤)
    logger.info("Assigning quests...")
    quest_rows = await pool.fetch("SELECT id FROM quest_definitions WHERE is_active=TRUE")
    for user in users[:40]:
        n_quests = random.randint(1, 3)
        for quest in random.sample(quest_rows, min(n_quests, len(quest_rows))):
            prog = random.randint(0, 4)
            await pool.execute("""
                INSERT INTO user_quests (user_id, quest_id, progress)
                VALUES ($1::uuid, $2::uuid, $3)
                ON CONFLICT DO NOTHING
            """, user["id"], quest["id"], prog)

    # 통계 출력
    counts = {
        "users": await pool.fetchval("SELECT COUNT(*) FROM users"),
        "reports": await pool.fetchval("SELECT COUNT(*) FROM trash_reports"),
        "posts": await pool.fetchval("SELECT COUNT(*) FROM posts"),
        "points": await pool.fetchval("SELECT COUNT(*) FROM point_ledger"),
        "drones": await pool.fetchval("SELECT COUNT(*) FROM drone_events"),
    }
    logger.info(f"=== 시드 완료 === {counts}")

    await pool.close()


if __name__ == "__main__":
    asyncio.run(main())
