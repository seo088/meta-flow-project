"""포인트·레벨·XP 계산 로직 — 모든 서비스에서 import."""
from datetime import datetime
import json

LEVEL_TABLE = [
    (1, 0, "새싹 플로거"),
    (2, 100, "초보 플로거"),
    (3, 300, "활동 플로거"),
    (4, 600, "열정 플로거"),
    (5, 1000, "베테랑 플로거"),
    (6, 1500, "클린 헌터"),
    (7, 2200, "에코 워리어"),
    (8, 3000, "환경 수호자"),
    (9, 4000, "군산 레인저"),
    (10, 5500, "마스터 플로거"),
    (11, 7500, "그랜드 마스터"),
    (12, 10000, "군산 레전드"),
]

ACTION_XP = {
    # 작성/참여 (능동적 활동)
    "post_create": 20,        # 일반 게시글 작성
    "report_created": 20,     # 제보 게시글 (주말 ×1.5)
    "quest_post": 30,         # 퀘스트 글 작성
    "cleanup_verified": 50,   # 청소 인증
    "comment_created": 3,     # 댓글 작성 (일일 5회 한도)
    "zone_fix": 10,           # 구역 좌표 수정 제보 (zone당 1회)
    # 받음 (수동적 활동 — 콘텐츠 호응 보상)
    "like_received": 2,       # 좋아요 받음 (게시글당 1회)
    "comment_received": 3,    # 댓글 받음 (게시글당 1회)
}

# 일일 한도 (reason → 1일 최대 횟수). 한도 초과 호출은 무시됨
DAILY_CAP = {
    "comment_created": 5,
}

# 예약된 reason — 향후 구현 예정 (현재 호출 코드 없음)
# - boss_raid_joined: 100  보스 레이드 참여
# - first_zone_report: 100  첫 구역 제보


def get_level(xp: int) -> tuple:
    """Returns (level, title, xp_to_next)"""
    lv, title = 1, "새싹 플로거"
    for level, threshold, t in LEVEL_TABLE:
        if xp >= threshold:
            lv, title = level, t
        else:
            return lv, title, threshold - xp
    return lv, title, 0


def calc_xp(action: str, zone_bonus: float = 1.0) -> int:
    base = ACTION_XP.get(action, 0)
    is_weekend = datetime.utcnow().weekday() >= 5
    mult = zone_bonus * (
        1.5
        if is_weekend and action in ("report_created", "cleanup_verified")
        else 1.0
    )
    return int(base * mult)


async def grant_xp(db, redis, user_id: str, delta: int, reason: str,
                   ref_id: str = None, kafka=None, logger=None):
    """
    통합 XP 부여 함수 — 모든 서비스에서 이 함수를 사용.

    1. point_ledger INSERT (ref_id 있으면 중복 방지)
    2. REFRESH MATERIALIZED VIEW user_points
    3. Redis ranking:global ZADD
    4. Kafka user.points.updated 발행 (kafka 제공 시)

    Returns: (new_total: int, was_duplicate: bool)
    """
    try:
        if ref_id:
            # 중복 방지: 동일 user + ref_id + reason은 무시
            existing = await db.fetchval(
                "SELECT id FROM point_ledger WHERE user_id=$1::uuid AND ref_id=$2::uuid AND reason=$3",
                user_id, ref_id, reason,
            )
            if existing:
                total = await db.fetchval(
                    "SELECT COALESCE(SUM(delta),0) FROM point_ledger WHERE user_id=$1::uuid", user_id
                )
                return int(total), True

        # 일일 한도 검사 (예: comment_created는 일일 5회까지)
        cap = DAILY_CAP.get(reason)
        if cap:
            today_count = await db.fetchval(
                "SELECT COUNT(*) FROM point_ledger "
                "WHERE user_id=$1::uuid AND reason=$2 "
                "AND created_at >= date_trunc('day', NOW())",
                user_id, reason,
            )
            if today_count and int(today_count) >= cap:
                total = await db.fetchval(
                    "SELECT COALESCE(SUM(delta),0) FROM point_ledger WHERE user_id=$1::uuid", user_id
                )
                if logger:
                    logger.info(f"[grant_xp] {user_id} daily-cap reached ({reason}: {today_count}/{cap})")
                return int(total), True

        await db.execute("""
            INSERT INTO point_ledger (id, user_id, delta, reason, ref_id, created_at)
            VALUES (gen_random_uuid(), $1::uuid, $2, $3, $4::uuid, NOW())
        """, user_id, delta, reason, ref_id)

        # MV 갱신
        await db.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY user_points")

        # 새 총합 계산
        total = await db.fetchval(
            "SELECT COALESCE(SUM(delta),0) FROM point_ledger WHERE user_id=$1::uuid", user_id
        )
        total = int(total)

        # Redis 랭킹 갱신
        if redis:
            try:
                await redis.zadd("ranking:global", {user_id: total})
            except Exception:
                pass

        # Kafka 이벤트 발행
        if kafka:
            try:
                from core.events import PointsUpdatedEvent
                kafka.produce(
                    "user.points.updated",
                    PointsUpdatedEvent(user_id=user_id, delta=delta, new_total=total, reason=reason),
                    key=user_id,
                )
            except Exception:
                pass

        if logger:
            logger.info(f"[grant_xp] {user_id} +{delta} ({reason}) → total={total}")

        return total, False

    except Exception as e:
        if logger:
            logger.error(f"[grant_xp] FAILED: {user_id} +{delta} ({reason}): {e}")
        return 0, False


# 배지 희귀도별 보상 XP
BADGE_XP = {"common": 20, "rare": 50, "epic": 100, "legendary": 200}


async def check_and_award_badges(db, redis, user_id, kafka=None, logger=None):
    """업적 기준 충족 시 미보유 배지 자동 수여 + badge_earn XP. 신규 수여 badge_key 리스트 반환."""
    try:
        s = await db.fetchrow("""
            SELECT
              (SELECT count(*) FROM trash_reports WHERE reporter_id=$1::uuid) AS reports,
              (SELECT count(*) FROM cleanups WHERE cleaner_id=$1::uuid AND verified) AS cleanups,
              (SELECT count(*) FROM cleanups c JOIN trash_reports tr ON tr.id=c.report_id
                 JOIN geofence_zones gz ON gz.id=tr.zone_id
                 WHERE c.cleaner_id=$1::uuid AND c.verified AND gz.zone_key='EUNPA') AS eunpa,
              (SELECT count(*) FROM cleanups c JOIN trash_reports tr ON tr.id=c.report_id
                 JOIN geofence_zones gz ON gz.id=tr.zone_id
                 WHERE c.cleaner_id=$1::uuid AND c.verified AND gz.zone_key='SAEMANGEUM') AS saemangeum,
              (SELECT count(*) FROM cleanups c JOIN trash_reports tr ON tr.id=c.report_id
                 JOIN geofence_zones gz ON gz.id=tr.zone_id
                 WHERE c.cleaner_id=$1::uuid AND c.verified AND gz.zone_key='GEUMGANG') AS geumgang,
              COALESCE((SELECT SUM(delta) FROM point_ledger WHERE user_id=$1::uuid),0) AS xp
        """, user_id)
        earned = {
            "first_report": s["reports"] >= 1,
            "report_5": s["reports"] >= 5,
            "report_20": s["reports"] >= 20,
            "cleanup_first": s["cleanups"] >= 1,
            "cleanup_10": s["cleanups"] >= 10,
            "eunpa_guardian": s["eunpa"] >= 5,
            "saemangeum_warrior": s["saemangeum"] >= 1,
            "geumgang_ranger": s["geumgang"] >= 1,
            "eco_legend": s["xp"] >= 10000,
        }
        keys = [k for k, v in earned.items() if v]
        if not keys:
            return []
        rows = await db.fetch("""
            SELECT bd.id, bd.badge_key, bd.rarity FROM badge_definitions bd
            WHERE bd.badge_key = ANY($2::text[])
              AND NOT EXISTS (SELECT 1 FROM user_badges ub
                  WHERE ub.user_id=$1::uuid AND ub.badge_id=bd.id)
        """, user_id, keys)
        new_badges = []
        for r in rows:
            await db.execute(
                "INSERT INTO user_badges(user_id, badge_id) VALUES($1::uuid,$2) ON CONFLICT DO NOTHING",
                user_id, r["id"])
            await grant_xp(db, redis, user_id, BADGE_XP.get(r["rarity"], 20),
                           "badge_earn", ref_id=str(r["id"]), kafka=kafka, logger=logger)
            new_badges.append(r["badge_key"])
        if new_badges and logger:
            logger.info(f"[badge] 수여 {user_id} → {new_badges}")
        return new_badges
    except Exception as e:
        if logger:
            logger.warning(f"[check_and_award_badges] {user_id}: {e}")
        return []
