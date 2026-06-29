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
