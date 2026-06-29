"""
Game Service — 랭킹·레벨·퀘스트·배지·포인트이력.
포인트 지급은 MCP award_points에 위임 (직접 수행 금지).
Kafka Consumer: user.points.updated → Redis 랭킹 갱신
포트: GAME_PORT 환경변수 (기본 8502)
"""
import os
import sys
import json
import asyncio

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "shared"))

from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional, List
import uuid as _uuid

from core.auth import get_current_user, get_optional_user
from core.kafka_client import KafkaConsumerClient
from core.events import PointsUpdatedEvent
from core.gamification import get_level, grant_xp
from core.db import create_db_pool, create_redis
from core.logger import get_logger
from core.content_filter import check_content

logger = get_logger("game-service")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = await create_db_pool(min_size=5, max_size=30)
    app.state.redis = create_redis()
    from core.cache_agent import CacheAgent
    app.state.cache = CacheAgent(app.state.redis, logger)

    # system_config 테이블 자동 생성
    try:
        await app.state.db.execute("""
            CREATE TABLE IF NOT EXISTS system_config (
                key VARCHAR(100) PRIMARY KEY,
                value JSONB NOT NULL,
                description TEXT,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        await app.state.db.execute("""
            INSERT INTO system_config (key, value, description)
            VALUES ('quest_create_min_xp', '100', '퀘스트 생성에 필요한 최소 XP')
            ON CONFLICT (key) DO NOTHING
        """)
        await app.state.db.execute("""
            INSERT INTO system_config (key, value, description)
            VALUES ('report_require_location', 'false', 'HTTP 환경: 제보 시 위치 필수 여부 (HTTPS 전환 시 true)')
            ON CONFLICT (key) DO NOTHING
        """)
    except Exception as e:
        logger.warning(f"system_config 초기화: {e}")

    stop = asyncio.Event()
    consumer = KafkaConsumerClient(
        os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093"),
        "game-service-ranking",
        ["user.points.updated"],
    )

    async def sync_ranking(raw: dict):
        e = PointsUpdatedEvent(**raw)
        await app.state.redis.zadd("ranking:global", {e.user_id: e.new_total})
        await app.state.redis.publish(
            "ws:ranking",
            json.dumps({"type": "ranking_updated",
                         "user_id": e.user_id, "new_total": e.new_total}),
        )

    task = asyncio.create_task(consumer.consume_loop(sync_ranking, dict, stop))
    app.state.stop = stop
    app.state.task = task
    yield
    stop.set()
    await task
    consumer.close()
    await app.state.db.close()
    await app.state.redis.aclose()


app = FastAPI(title="Game Service", version="1.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "game"}


@app.get("/profile/me")
async def my_profile(u: dict = Depends(get_current_user)):
    db, redis = app.state.db, app.state.redis
    cached = await redis.zscore("ranking:global", u["user_id"])
    xp = int(cached) if cached is not None else (
        await db.fetchval(
            "SELECT COALESCE(SUM(delta),0) FROM point_ledger WHERE user_id=$1::uuid",
            u["user_id"],
        ) or 0
    )
    level, title, xp_next = get_level(xp)
    rank = await redis.zrevrank("ranking:global", u["user_id"])

    badges = await db.fetch("""
        SELECT bd.badge_key, bd.name, bd.icon_url, bd.rarity, ub.earned_at
        FROM user_badges ub JOIN badge_definitions bd ON bd.id = ub.badge_id
        WHERE ub.user_id = $1::uuid ORDER BY ub.earned_at DESC
    """, u["user_id"])

    return {"success": True, "data": {
        "user_id": u["user_id"], "total_xp": xp,
        "level": level, "level_title": title, "xp_to_next": xp_next,
        "global_rank": (rank + 1) if rank is not None else None,
        "badges": [dict(b) for b in badges],
    }}


@app.get("/ranking/global")
async def global_ranking(page: int = Query(1, ge=1), size: int = Query(50, le=100)):
    """글로벌 랭킹. (적응형 캐싱)"""
    cache = app.state.cache
    cached = await cache.get("ranking_global", page=page, size=size)
    if cached:
        return cached
    db = app.state.db
    offset = (page - 1) * size
    rows = await db.fetch("""
        SELECT u.id::text AS user_id, u.username, u.display_name, u.avatar_url,
               COALESCE(up.total_points, 0) AS total_xp,
               COALESCE(pc.cnt, 0) AS post_count,
               COALESCE(rc.cnt, 0) AS report_count
        FROM users u
        LEFT JOIN user_points up ON up.user_id = u.id
        LEFT JOIN (SELECT user_id, COUNT(*) AS cnt FROM posts GROUP BY user_id) pc ON pc.user_id = u.id
        LEFT JOIN (SELECT reporter_id, COUNT(*) AS cnt FROM trash_reports GROUP BY reporter_id) rc ON rc.reporter_id = u.id
        WHERE COALESCE(up.total_points, 0) > 0
        ORDER BY COALESCE(up.total_points, 0) DESC
        LIMIT $1 OFFSET $2
    """, size, offset)
    result = []
    for i, r in enumerate(rows):
        xp = r["total_xp"]
        lv, lt, _ = get_level(xp)
        result.append({
            "rank": offset + i + 1,
            "user_id": r["user_id"],
            "username": r["username"],
            "display_name": r["display_name"],
            "avatar_url": r["avatar_url"],
            "total_xp": xp,
            "level": lv,
            "level_title": lt,
            "post_count": r["post_count"],
            "report_count": r["report_count"],
        })
    resp = {"success": True, "data": result}
    await cache.set("ranking_global", resp, page=page, size=size)
    return resp


@app.post("/ranking/add-xp")
async def add_xp(payload: dict = Body(...), u: dict = Depends(get_current_user)):
    """수동 XP 부여 (구역 수정 제보 등)."""
    db = app.state.db
    delta = int(payload.get("delta", 0))
    reason = payload.get("reason", "manual")
    if delta <= 0 or delta > 100:
        return {"success": False, "error": "XP는 1~100 범위"}
    try:
        await db.execute("""
            INSERT INTO point_ledger (id, user_id, delta, reason, created_at)
            VALUES (gen_random_uuid(), $1::uuid, $2, $3, NOW())
        """, u["user_id"], delta, reason)
        await db.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY user_points")
        return {"success": True, "data": {"xp_earned": delta}}
    except Exception as e:
        return {"success": False, "error": str(e)[:200]}


@app.get("/ranking/user/{username}/activity")
async def user_activity(username: str):
    """사용자 활동 분석 — XP 내역, 해시태그, 최근 게시글."""
    db = app.state.db
    user = await db.fetchrow("SELECT id, username, display_name FROM users WHERE username=$1", username)
    if not user:
        return {"success": False, "error": "사용자를 찾을 수 없습니다"}
    uid = str(user["id"])

    # XP 내역 (최근 30건)
    xp_rows = await db.fetch("""
        SELECT delta, reason, created_at FROM point_ledger
        WHERE user_id=$1::uuid ORDER BY created_at DESC LIMIT 30
    """, uid)

    # 해시태그 집계
    tag_rows = await db.fetch("""
        SELECT unnest(hashtags) AS tag, COUNT(*) AS cnt
        FROM posts WHERE user_id=$1::uuid AND hashtags IS NOT NULL
        GROUP BY tag ORDER BY cnt DESC LIMIT 15
    """, uid)

    # 최근 게시글 (5건)
    post_rows = await db.fetch("""
        SELECT p.id::text, p.content, p.image_urls, p.hashtags, p.like_count, p.created_at,
               (SELECT COUNT(*) FROM post_comments pc WHERE pc.post_id=p.id) AS comment_count
        FROM posts p WHERE p.user_id=$1::uuid
        ORDER BY p.created_at DESC LIMIT 5
    """, uid)

    # 일별 XP 추이 (최근 14일)
    xp_daily = await db.fetch("""
        SELECT DATE(created_at) AS d, SUM(delta) AS xp
        FROM point_ledger WHERE user_id=$1::uuid
          AND created_at > NOW() - INTERVAL '14 days'
        GROUP BY DATE(created_at) ORDER BY d
    """, uid)

    # XP 사유별 집계
    xp_by_reason = await db.fetch("""
        SELECT reason, SUM(delta) AS total, COUNT(*) AS cnt
        FROM point_ledger WHERE user_id=$1::uuid
        GROUP BY reason ORDER BY total DESC
    """, uid)

    reason_labels = {
        'post_create': '게시글 작성',
        'report_create': '제보 등록',
        'quest_post': '퀘스트 활동',
        'cleanup_verify': '청소 인증',
        'quest_complete': '퀘스트 완료',
        'badge_earn': '배지 획득',
    }

    return {
        "success": True,
        "data": {
            "xp_history": [{"delta": r["delta"], "reason": reason_labels.get(r["reason"], r["reason"]), "created_at": r["created_at"].isoformat()} for r in xp_rows],
            "hashtags": [{"tag": r["tag"], "count": r["cnt"]} for r in tag_rows],
            "recent_posts": [
                {
                    "id": r["id"], "content": (r["content"] or "")[:100],
                    "image_urls": r["image_urls"] or [],
                    "hashtags": r["hashtags"] or [],
                    "like_count": r["like_count"] or 0,
                    "comment_count": r["comment_count"] or 0,
                    "created_at": r["created_at"].isoformat(),
                }
                for r in post_rows
            ],
            "xp_daily": [{"date": str(r["d"]), "xp": r["xp"]} for r in xp_daily],
            "xp_by_reason": [{"reason": reason_labels.get(r["reason"], r["reason"]), "total": r["total"], "count": r["cnt"]} for r in xp_by_reason],
        },
    }


@app.get("/ranking/zone/{zone_key}")
async def zone_ranking(zone_key: str, size: int = Query(20, le=50)):
    db = app.state.db
    rows = await db.fetch("""
        SELECT u.id::text AS user_id, u.username, u.display_name, u.avatar_url,
               COUNT(DISTINCT tr.id) AS report_count,
               COALESCE(up.total_points, 0) AS total_xp
        FROM users u
        JOIN trash_reports tr ON tr.reporter_id = u.id
        JOIN geofence_zones gz ON gz.id = tr.zone_id
        LEFT JOIN user_points up ON up.user_id = u.id
        WHERE gz.zone_key = $1
        GROUP BY u.id, u.username, u.display_name, u.avatar_url, up.total_points
        ORDER BY COUNT(DISTINCT tr.id) DESC, up.total_points DESC
        LIMIT $2
    """, zone_key, size)
    result = []
    for i, r in enumerate(rows):
        d = dict(r)
        xp = d["total_xp"]
        d["rank"] = i + 1
        lv, _, _ = get_level(xp)
        d["level"] = lv
        result.append(d)
    return {"success": True, "data": result}


@app.get("/quests/me")
async def my_quests(u: dict = Depends(get_current_user)):
    rows = await app.state.db.fetch("""
        SELECT uq.progress, uq.is_completed, uq.completed_at,
               qd.quest_key, qd.title, qd.description, qd.quest_type,
               qd.target_count, qd.reward_xp,
               gz.name AS zone_name,
               bd.name AS badge_name, bd.icon_url AS badge_icon
        FROM user_quests uq
        JOIN quest_definitions qd ON qd.id = uq.quest_id
        LEFT JOIN geofence_zones gz ON gz.id = qd.zone_id
        LEFT JOIN badge_definitions bd ON bd.id = qd.reward_badge_id
        WHERE uq.user_id=$1::uuid AND qd.is_active=TRUE
        ORDER BY uq.is_completed, qd.reward_xp DESC
    """, u["user_id"])
    return {"success": True, "data": [dict(r) for r in rows]}


@app.post("/quests/{quest_key}/accept")
async def accept_quest(quest_key: str, u: dict = Depends(get_current_user)):
    db = app.state.db
    qid = await db.fetchval(
        "SELECT id FROM quest_definitions WHERE quest_key=$1 AND is_active=TRUE",
        quest_key,
    )
    if not qid:
        return {"success": False, "error": "Quest not found"}
    await db.execute("""
        INSERT INTO user_quests (user_id, quest_id)
        VALUES ($1::uuid,$2) ON CONFLICT (user_id, quest_id) DO NOTHING
    """, u["user_id"], qid)
    return {"success": True, "data": {"quest_id": str(qid)}}


@app.get("/points/history")
async def point_history(
    page: int = Query(1, ge=1),
    size: int = Query(20, le=50),
    u: dict = Depends(get_current_user),
):
    offset = (page - 1) * size
    rows = await app.state.db.fetch("""
        SELECT delta, reason, ref_id, created_at
        FROM point_ledger WHERE user_id=$1::uuid
        ORDER BY created_at DESC LIMIT $2 OFFSET $3
    """, u["user_id"], size, offset)
    return {"success": True, "data": [dict(r) for r in rows]}


@app.get("/badges/all")
async def all_badges(u: dict = Depends(get_current_user)):
    """전체 배지 목록 (획득 여부 포함)."""
    db = app.state.db
    rows = await db.fetch("""
        SELECT bd.badge_key, bd.name, bd.description, bd.icon_url, bd.rarity,
               ub.earned_at IS NOT NULL AS earned
        FROM badge_definitions bd
        LEFT JOIN user_badges ub ON ub.badge_id = bd.id AND ub.user_id = $1::uuid
        ORDER BY bd.rarity, bd.name
    """, u["user_id"])
    return {"success": True, "data": [dict(r) for r in rows]}


# ═══ USER-CREATED QUESTS ═══════════════════════════════════

QUEST_TYPE_ICONS = {
    "cleanup": "🧹", "manitto": "🎁", "marathon": "🏃",
    "karaoke": "🎤", "custom": "🎯", "plogging": "🌿",
}


async def _check_member(db, quest_id: str, user_id: str):
    """멤버 확인. Returns member row or None."""
    return await db.fetchrow(
        "SELECT * FROM quest_members WHERE quest_id=$1::uuid AND user_id=$2::uuid AND status='accepted'",
        quest_id, user_id,
    )


async def _check_host(db, quest_id: str, user_id: str):
    """host/cohost 확인."""
    m = await _check_member(db, quest_id, user_id)
    if m and m["role"] in ("host", "cohost"):
        return m
    return None


# ── 퀘스트 CRUD ──

@app.post("/quests/create")
async def create_quest(payload: dict = Body(...), u: dict = Depends(get_current_user)):
    db = app.state.db
    # 퀘스트 생성 최소 XP 확인 (관리자 설정 가능)
    user_xp = await db.fetchval(
        "SELECT COALESCE(total_points,0) FROM user_points WHERE user_id=$1::uuid", u["user_id"]
    ) or 0
    min_xp = 100
    try:
        row = await db.fetchval("SELECT value FROM system_config WHERE key='quest_create_min_xp'")
        if row: min_xp = int(str(row).strip('"'))
    except Exception: pass
    if u.get("role") != "admin" and user_xp < min_xp:
        return {"success": False, "error": f"퀘스트를 생성하려면 최소 {min_xp} XP가 필요합니다. (현재: {user_xp} XP)",
                "data": {"required_xp": min_xp, "current_xp": user_xp}}
    # 비공개 퀘스트는 XP 300 이상 필요
    if payload.get("is_public") is False and user_xp < 300:
        return {"success": False, "error": "비공개 퀘스트는 XP 300 이상 (Lv.3)부터 생성할 수 있습니다"}
    from datetime import datetime as _dt
    qid = str(_uuid.uuid4())
    # 날짜 문자열 → datetime 변환 (date 또는 datetime-local 둘 다 처리)
    def parse_date(v):
        if not v:
            return None
        try:
            if 'T' in str(v):
                return _dt.fromisoformat(str(v))
            return _dt.fromisoformat(str(v) + 'T00:00:00')
        except Exception:
            return None
    try:
        await db.execute("""
            INSERT INTO user_created_quests
              (id, title, description, quest_type, creator_id, zone_id,
               target_count, max_members, start_date, end_date,
               reward_xp, is_public, cover_image_url)
            VALUES ($1::uuid,$2,$3,$4,$5::uuid,$6,$7,$8,$9,$10,$11,$12,$13)
        """,
            qid, payload.get("title", "새 퀘스트")[:30], payload.get("description"),
            payload.get("quest_type", "custom"), u["user_id"],
            payload.get("zone_id") or None, int(payload.get("target_count", 1)),
            int(payload.get("max_members", 50)), parse_date(payload.get("start_date")),
            parse_date(payload.get("end_date")), int(payload.get("reward_xp", 0)),
            payload.get("is_public", True), payload.get("cover_image_url"),
        )
    except Exception as e:
        logger.error(f"Quest create failed: {e}")
        return {"success": False, "error": f"퀘스트 생성 실패: {str(e)[:200]}"}
    # 생성자를 host로 등록
    await db.execute("""
        INSERT INTO quest_members (quest_id, user_id, role, status)
        VALUES ($1::uuid, $2::uuid, 'host', 'accepted')
    """, qid, u["user_id"])
    logger.info(f"Quest created: {qid} by {u['username']}")
    return {"success": True, "data": {"quest_id": qid}}


@app.get("/quests/popular")
async def popular_quests(size: int = Query(10, le=30), u: dict | None = Depends(get_optional_user)):
    """인기 퀘스트 — 최근 7일 활동량(새 멤버 + 새 글) × 시간 decay 기반.

    점수 = (recent_members×5 + recent_posts×3) × 24/(24 + age_hours_since_last_activity)
    - 새 멤버 가입과 글 작성이 곧 활동량 지표
    - 가입자 5점 > 글 3점 (가입이 더 강한 신호)
    - 마지막 활동 기준 시간 decay → 정체된 퀘스트는 자동 하락
    - tie: 전체 member_count
    """
    uid = u["user_id"] if u else None
    is_admin = u and u.get("role") == "admin"
    public_filter = "" if is_admin else "AND q.is_public = TRUE"
    rows = await app.state.db.fetch(f"""
        WITH recent_activity AS (
            SELECT q.id AS quest_id,
                   COALESCE(rm.cnt, 0) AS recent_members,
                   COALESCE(rp.cnt, 0) AS recent_posts,
                   GREATEST(
                     COALESCE(rm.last_at, q.created_at),
                     COALESCE(rp.last_at, q.created_at)
                   ) AS last_activity_at
            FROM user_created_quests q
            LEFT JOIN (
                SELECT quest_id, COUNT(*) AS cnt, MAX(joined_at) AS last_at
                FROM quest_members
                WHERE joined_at > NOW() - INTERVAL '7 days' AND status='accepted'
                GROUP BY quest_id
            ) rm ON rm.quest_id = q.id
            LEFT JOIN (
                SELECT quest_id, COUNT(*) AS cnt, MAX(created_at) AS last_at
                FROM quest_posts
                WHERE created_at > NOW() - INTERVAL '7 days'
                GROUP BY quest_id
            ) rp ON rp.quest_id = q.id
        )
        SELECT q.id, q.title, q.description, q.quest_type, q.status,
               q.member_count, q.max_members, q.like_count, q.reward_xp, q.is_public,
               q.start_date, q.end_date, q.created_at, q.cover_image_url,
               u.username AS creator_username, u.display_name AS creator_name,
               ra.recent_members, ra.recent_posts,
               (ra.recent_members*5 + ra.recent_posts*3) AS activity_raw,
               ((ra.recent_members*5 + ra.recent_posts*3)
                 * 24.0 / (24.0 + EXTRACT(EPOCH FROM (NOW() - ra.last_activity_at))/3600.0)
               )::numeric(10,2) AS activity_score
        FROM user_created_quests q
        JOIN users u ON u.id = q.creator_id
        JOIN recent_activity ra ON ra.quest_id = q.id
        WHERE q.status = 'active' {public_filter}
        ORDER BY activity_score DESC, q.member_count DESC, q.created_at DESC
        LIMIT $1
    """, size)
    result = [dict(r) for r in rows]
    quest_ids = [str(d["id"]) for d in result]
    # 배치: 멤버십 확인
    member_map = {}
    if uid and quest_ids:
        members = await app.state.db.fetch("""
            SELECT quest_id::text, role FROM quest_members
            WHERE quest_id::text = ANY($1) AND user_id=$2::uuid AND status='accepted'
        """, quest_ids, uid)
        member_map = {m["quest_id"]: m["role"] for m in members}
    # 배치: 썸네일
    img_map = {}
    if quest_ids:
        imgs = await app.state.db.fetch("""
            SELECT DISTINCT ON (quest_id) quest_id::text, image_urls[1] as img
            FROM quest_posts
            WHERE quest_id::text = ANY($1) AND array_length(image_urls, 1) > 0
            ORDER BY quest_id, like_count DESC, created_at DESC
        """, quest_ids)
        img_map = {i["quest_id"]: i["img"] for i in imgs}
    for d in result:
        qid = str(d["id"])
        d["is_member"] = qid in member_map
        d["my_role"] = member_map.get(qid)
        d["top_image_url"] = img_map.get(qid)
    return {"success": True, "data": result}


@app.get("/quests/my-quests")
async def my_quests_user(u: dict = Depends(get_current_user)):
    rows = await app.state.db.fetch("""
        SELECT q.id, q.title, q.description, q.quest_type, q.status,
               q.member_count, q.like_count, q.reward_xp, q.is_public,
               q.start_date, q.end_date, q.created_at, q.cover_image_url,
               u.username AS creator_username, u.display_name AS creator_name,
               qm.role AS my_role
        FROM quest_members qm
        JOIN user_created_quests q ON q.id = qm.quest_id
        JOIN users u ON u.id = q.creator_id
        WHERE qm.user_id = $1::uuid AND qm.status = 'accepted'
        ORDER BY q.created_at DESC
    """, u["user_id"])
    return {"success": True, "data": [dict(r) for r in rows]}


@app.get("/quests/{quest_id}")
async def quest_detail(quest_id: str, u: dict | None = Depends(get_optional_user)):
    db = app.state.db
    q = await db.fetchrow("""
        SELECT q.*, u.username AS creator_username, u.display_name AS creator_name
        FROM user_created_quests q
        JOIN users u ON u.id = q.creator_id
        WHERE q.id = $1::uuid
    """, quest_id)
    if not q:
        return {"success": False, "error": "Quest not found"}
    d = dict(q)
    # UUID → str
    for k in ("id", "creator_id", "zone_id"):
        if d.get(k):
            d[k] = str(d[k])
    # 멤버 여부
    d["is_member"] = False
    d["my_role"] = None
    if u:
        m = await _check_member(db, quest_id, u["user_id"])
        if m:
            d["is_member"] = True
            d["my_role"] = m["role"]
    return {"success": True, "data": d}


@app.put("/quests/{quest_id}")
async def update_quest(quest_id: str, payload: dict = Body(...), u: dict = Depends(get_current_user)):
    db = app.state.db
    host = await _check_host(db, quest_id, u["user_id"])
    if not host:
        return {"success": False, "error": "권한이 없습니다 (host/cohost만 수정 가능)"}
    fields = []
    vals = []
    idx = 1
    for k in ("title", "description", "quest_type", "target_count", "max_members",
              "start_date", "end_date", "reward_xp", "is_public", "cover_image_url", "status"):
        if k in payload:
            idx += 1
            fields.append(f"{k}=${idx}")
            vals.append(payload[k])
    if not fields:
        return {"success": False, "error": "수정할 필드가 없습니다"}
    await db.execute(
        f"UPDATE user_created_quests SET {','.join(fields)} WHERE id=$1::uuid",
        quest_id, *vals,
    )
    return {"success": True}


# ── 멤버 관리 ──

@app.post("/quests/{quest_id}/join")
async def join_quest(quest_id: str, u: dict = Depends(get_current_user)):
    db = app.state.db
    q = await db.fetchrow("SELECT * FROM user_created_quests WHERE id=$1::uuid", quest_id)
    if not q:
        return {"success": False, "error": "Quest not found"}
    if not q["is_public"]:
        return {"success": False, "error": "비공개 퀘스트입니다. 초대가 필요합니다."}
    if q["member_count"] >= q["max_members"]:
        return {"success": False, "error": "최대 인원에 도달했습니다."}
    try:
        await db.execute("""
            INSERT INTO quest_members (quest_id, user_id, role, status)
            VALUES ($1::uuid, $2::uuid, 'member', 'accepted')
        """, quest_id, u["user_id"])
        await db.execute(
            "UPDATE user_created_quests SET member_count = member_count + 1 WHERE id=$1::uuid",
            quest_id,
        )
    except Exception:
        return {"success": False, "error": "이미 참여 중입니다."}
    return {"success": True}


@app.post("/quests/{quest_id}/invite")
async def invite_to_quest(quest_id: str, payload: dict = Body(...), u: dict = Depends(get_current_user)):
    db = app.state.db
    host = await _check_host(db, quest_id, u["user_id"])
    if not host:
        return {"success": False, "error": "권한이 없습니다"}
    usernames = payload.get("usernames", [])
    invited = []
    for uname in usernames[:20]:
        uid = await db.fetchval("SELECT id::text FROM users WHERE username=$1", uname)
        if not uid:
            continue
        try:
            await db.execute("""
                INSERT INTO quest_invitations (quest_id, inviter_id, invitee_id, status)
                VALUES ($1::uuid, $2::uuid, $3::uuid, 'pending')
                ON CONFLICT (quest_id, invitee_id) DO NOTHING
            """, quest_id, u["user_id"], uid)
            invited.append(uname)
        except Exception:
            pass
    return {"success": True, "data": {"invited": invited, "count": len(invited)}}


@app.get("/quests/{quest_id}/members")
async def quest_members(quest_id: str, u: dict = Depends(get_current_user)):
    db = app.state.db
    rows = await db.fetch("""
        SELECT qm.role, qm.status, qm.joined_at,
               u.id::text AS user_id, u.username, u.display_name, u.avatar_url,
               COALESCE(up.total_points, 0) AS total_xp,
               (SELECT COUNT(*) FROM quest_posts qp WHERE qp.user_id = u.id AND qp.quest_id = $1::uuid) AS post_count
        FROM quest_members qm
        JOIN users u ON u.id = qm.user_id
        LEFT JOIN user_points up ON up.user_id = u.id
        WHERE qm.quest_id = $1::uuid AND qm.status IN ('accepted', 'warned', 'suspended')
        ORDER BY CASE qm.role WHEN 'host' THEN 0 WHEN 'cohost' THEN 1 ELSE 2 END, qm.joined_at
    """, quest_id)
    return {"success": True, "data": [dict(r) for r in rows]}


@app.put("/quests/{quest_id}/members/{user_id}/role")
async def set_member_role(quest_id: str, user_id: str, payload: dict = Body(...), u: dict = Depends(get_current_user)):
    db = app.state.db
    host = await _check_member(db, quest_id, u["user_id"])
    if not host or host["role"] != "host":
        return {"success": False, "error": "주최자만 역할을 변경할 수 있습니다"}
    new_role = payload.get("role", "member")
    if new_role not in ("cohost", "member"):
        return {"success": False, "error": "유효하지 않은 역할입니다"}
    await db.execute(
        "UPDATE quest_members SET role=$1 WHERE quest_id=$2::uuid AND user_id=$3::uuid",
        new_role, quest_id, user_id,
    )
    return {"success": True}


@app.delete("/quests/{quest_id}/members/{user_id}")
async def remove_member(quest_id: str, user_id: str, u: dict = Depends(get_current_user)):
    db = app.state.db
    # 본인 탈퇴 or 호스트가 추방
    is_self = u["user_id"] == user_id
    host = await _check_host(db, quest_id, u["user_id"])
    if not is_self and not host:
        return {"success": False, "error": "권한이 없습니다"}
    # host는 탈퇴 불가
    target = await _check_member(db, quest_id, user_id)
    if target and target["role"] == "host":
        return {"success": False, "error": "주최자는 탈퇴할 수 없습니다. 퀘스트를 종료하세요."}
    await db.execute(
        "DELETE FROM quest_members WHERE quest_id=$1::uuid AND user_id=$2::uuid",
        quest_id, user_id,
    )
    await db.execute(
        "UPDATE user_created_quests SET member_count = GREATEST(member_count - 1, 1) WHERE id=$1::uuid",
        quest_id,
    )
    return {"success": True}


@app.post("/quests/{quest_id}/members/{user_id}/warn")
async def warn_member(quest_id: str, user_id: str, payload: dict = Body(default={}), u: dict = Depends(get_current_user)):
    """호스트 전용 — 멤버 경고."""
    db = app.state.db
    host = await _check_host(db, quest_id, u["user_id"])
    if not host:
        return {"success": False, "error": "주최자만 경고할 수 있습니다"}
    reason = (payload or {}).get("reason", "활동 규칙 위반")
    await db.execute("UPDATE quest_members SET status='warned' WHERE quest_id=$1::uuid AND user_id=$2::uuid", quest_id, user_id)
    return {"success": True, "data": {"status": "warned", "reason": reason}}


@app.post("/quests/{quest_id}/members/{user_id}/suspend")
async def suspend_member(quest_id: str, user_id: str, payload: dict = Body(default={}), u: dict = Depends(get_current_user)):
    """호스트 전용 — 멤버 활동 정지."""
    db = app.state.db
    host = await _check_host(db, quest_id, u["user_id"])
    if not host:
        return {"success": False, "error": "주최자만 활동 정지할 수 있습니다"}
    reason = (payload or {}).get("reason", "활동 정지")
    await db.execute("UPDATE quest_members SET status='suspended' WHERE quest_id=$1::uuid AND user_id=$2::uuid", quest_id, user_id)
    return {"success": True, "data": {"status": "suspended", "reason": reason}}


@app.post("/quests/{quest_id}/members/{user_id}/restore")
async def restore_member(quest_id: str, user_id: str, u: dict = Depends(get_current_user)):
    """호스트 전용 — 경고/정지 해제."""
    db = app.state.db
    host = await _check_host(db, quest_id, u["user_id"])
    if not host:
        return {"success": False, "error": "주최자만 해제할 수 있습니다"}
    await db.execute("UPDATE quest_members SET status='accepted' WHERE quest_id=$1::uuid AND user_id=$2::uuid", quest_id, user_id)
    return {"success": True}


@app.get("/invitations/me")
async def my_invitations(u: dict = Depends(get_current_user)):
    rows = await app.state.db.fetch("""
        SELECT qi.id, qi.status, qi.created_at,
               q.id AS quest_id, q.title, q.quest_type, q.member_count, q.reward_xp,
               inv.username AS inviter_username, inv.display_name AS inviter_name
        FROM quest_invitations qi
        JOIN user_created_quests q ON q.id = qi.quest_id
        JOIN users inv ON inv.id = qi.inviter_id
        WHERE qi.invitee_id = $1::uuid AND qi.status = 'pending'
        ORDER BY qi.created_at DESC
    """, u["user_id"])
    return {"success": True, "data": [dict(r) for r in rows]}


@app.post("/invitations/{invitation_id}/respond")
async def respond_invitation(invitation_id: str, payload: dict = Body(...), u: dict = Depends(get_current_user)):
    db = app.state.db
    inv = await db.fetchrow(
        "SELECT * FROM quest_invitations WHERE id=$1::uuid AND invitee_id=$2::uuid AND status='pending'",
        invitation_id, u["user_id"],
    )
    if not inv:
        return {"success": False, "error": "초대를 찾을 수 없습니다"}
    action = payload.get("action", "accept")
    if action == "accept":
        await db.execute(
            "UPDATE quest_invitations SET status='accepted' WHERE id=$1::uuid",
            invitation_id,
        )
        try:
            await db.execute("""
                INSERT INTO quest_members (quest_id, user_id, role, status)
                VALUES ($1::uuid, $2::uuid, 'member', 'accepted')
            """, str(inv["quest_id"]), u["user_id"])
            await db.execute(
                "UPDATE user_created_quests SET member_count = member_count + 1 WHERE id=$1::uuid",
                str(inv["quest_id"]),
            )
        except Exception:
            pass
    else:
        await db.execute(
            "UPDATE quest_invitations SET status='declined' WHERE id=$1::uuid",
            invitation_id,
        )
    return {"success": True, "data": {"action": action}}


# ── 퀘스트 전용 피드 ──

@app.get("/quests/{quest_id}/feed")
async def quest_feed(quest_id: str, page: int = Query(1, ge=1), size: int = Query(20, le=50),
                     u: dict = Depends(get_current_user)):
    db = app.state.db
    m = await _check_member(db, quest_id, u["user_id"])
    if not m:
        return {"success": False, "error": "멤버만 피드를 볼 수 있습니다"}
    offset = (page - 1) * size
    is_admin = u.get("role") == "admin"
    hidden_filter = "" if is_admin else " AND (qp.is_hidden IS NOT TRUE)"
    rows = await db.fetch(f"""
        SELECT qp.id, qp.content, qp.image_urls, qp.like_count, qp.created_at,
               qp.user_id, qp.is_hidden, qp.lat, qp.lon, qp.visibility,
               usr.username, usr.display_name, usr.avatar_url,
               (SELECT COUNT(*) FROM quest_post_comments c WHERE c.post_id = qp.id) AS comment_count,
               EXISTS(SELECT 1 FROM quest_post_likes l WHERE l.post_id=qp.id AND l.user_id=$3::uuid) AS liked
        FROM quest_posts qp
        JOIN users usr ON usr.id = qp.user_id
        WHERE qp.quest_id = $1::uuid{hidden_filter}
        ORDER BY qp.created_at DESC
        LIMIT $2 OFFSET $4
    """, quest_id, size, u["user_id"], offset)
    result = [dict(r) for r in rows]
    post_ids = [str(d["id"]) for d in result]
    if post_ids:
        # 배치: 리액션 요약
        all_rx = await db.fetch("""
            SELECT post_id::text, reaction_type, COUNT(*) as cnt
            FROM quest_post_likes WHERE post_id::text = ANY($1)
            GROUP BY post_id, reaction_type
        """, post_ids)
        rx_map = {}
        for rx in all_rx:
            rx_map.setdefault(rx["post_id"], {})[rx["reaction_type"]] = rx["cnt"]
        # 배치: 내 리액션
        my_rx = await db.fetch("""
            SELECT post_id::text, reaction_type FROM quest_post_likes
            WHERE user_id=$1::uuid AND post_id::text = ANY($2)
        """, u["user_id"], post_ids)
        my_map = {r["post_id"]: r["reaction_type"] for r in my_rx}
        for d in result:
            pid = str(d["id"])
            d["reactions"] = rx_map.get(pid, {})
            d["my_reaction"] = my_map.get(pid)
    return {"success": True, "data": result}


@app.post("/quests/{quest_id}/feed")
async def create_quest_post(quest_id: str, payload: dict = Body(...), u: dict = Depends(get_current_user)):
    db = app.state.db
    m = await _check_member(db, quest_id, u["user_id"])
    if not m:
        return {"success": False, "error": "멤버만 글을 쓸 수 있습니다"}

    content = payload.get("content", "")
    image_urls = payload.get("image_urls", [])
    visibility = payload.get("visibility", "quest")
    hashtags = payload.get("hashtags", [])
    lat = payload.get("lat")
    lon = payload.get("lon")

    # 비속어 필터링
    filter_result = await check_content(db, u["user_id"], content)
    if filter_result["blocked"]:
        return {"success": False, "error": filter_result["message"], "content_warning": True}

    pid = str(_uuid.uuid4())
    try:
        await db.execute("""
            INSERT INTO quest_posts (id, quest_id, user_id, content, image_urls, lat, lon, visibility)
            VALUES ($1::uuid, $2::uuid, $3::uuid, $4, $5::text[], $6, $7, $8)
        """, pid, quest_id, u["user_id"], content, image_urls, lat, lon, visibility)
    except Exception as e:
        return {"success": False, "error": f"퀘스트 게시 실패: {str(e)[:200]}"}

    # XP 보상 — 퀘스트 피드 게시 +30 XP (통합 grant_xp)
    xp_earned = 30
    await grant_xp(db, app.state.redis, u["user_id"], xp_earned, "quest_post",
                   ref_id=pid, logger=logger)

    # 마당 공개(public) — SNS 피드에도 동시 게시
    sns_post_id = None
    if visibility == "public":
        try:
            # 퀘스트 이름 조회
            quest_row = await db.fetchrow("SELECT title FROM user_created_quests WHERE id=$1::uuid", quest_id)
            quest_title = quest_row["title"] if quest_row else "퀘스트"
            # 퀘스트 태그 자동 추가
            auto_tags = [f"#{quest_title}"] if quest_title else []
            all_tags = list(set(auto_tags + [f"#{t.lstrip('#')}" for t in hashtags]))

            sns_pid = str(_uuid.uuid4())
            await db.execute("""
                INSERT INTO posts (id, user_id, content, image_urls, hashtags, entity_tags, visibility, created_at)
                VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, NOW())
            """, sns_pid, u["user_id"], content, image_urls, all_tags, [], "public")
            sns_post_id = sns_pid
        except Exception as e:
            logger.warning(f"마당 공개 SNS 동시 게시 실패: {e}")

    return {"success": True, "data": {"post_id": pid, "xp_earned": xp_earned, "sns_post_id": sns_post_id}}


@app.post("/quests/{quest_id}/feed/{post_id}/hide")
async def toggle_hide_quest_post(quest_id: str, post_id: str, u: dict = Depends(get_current_user)):
    """관리자 전용 — 퀘스트 게시글 숨김/해제 토글."""
    if u["role"] != "admin":
        return {"success": False, "error": "관리자만 숨김 처리할 수 있습니다"}
    db = app.state.db
    current = await db.fetchval("SELECT is_hidden FROM quest_posts WHERE id=$1::uuid", post_id)
    if current is None:
        return {"success": False, "error": "게시글을 찾을 수 없습니다"}
    new_val = not current
    if new_val:
        await db.execute("UPDATE quest_posts SET is_hidden=true, hidden_by=$1::uuid, hidden_at=NOW() WHERE id=$2::uuid", u["user_id"], post_id)
    else:
        await db.execute("UPDATE quest_posts SET is_hidden=false, hidden_by=NULL, hidden_at=NULL WHERE id=$1::uuid", post_id)
    return {"success": True, "data": {"is_hidden": new_val}}


@app.delete("/quests/{quest_id}/feed/{post_id}")
async def delete_quest_post(quest_id: str, post_id: str, u: dict = Depends(get_current_user)):
    """퀘스트 피드 게시글 삭제 (댓글 없는 경우만, 아카이브 백업)."""
    db = app.state.db

    # 게시글 조회
    post = await db.fetchrow("SELECT * FROM quest_posts WHERE id=$1::uuid AND quest_id=$2::uuid", post_id, quest_id)
    if not post:
        return {"success": False, "error": "게시글을 찾을 수 없습니다"}

    # 본인 또는 호스트만 삭제 가능
    is_owner = str(post["user_id"]) == u["user_id"]
    host = await _check_host(db, quest_id, u["user_id"])
    if not is_owner and not host:
        return {"success": False, "error": "본인 게시글만 삭제할 수 있습니다"}

    # 댓글이 있으면 삭제 불가
    comment_count = await db.fetchval(
        "SELECT COUNT(*) FROM quest_post_comments WHERE post_id=$1::uuid", post_id
    )
    if comment_count > 0:
        return {"success": False, "error": f"댓글이 {comment_count}개 있는 게시글은 삭제할 수 없습니다"}

    # 아카이브 백업
    try:
        await db.execute("""
            INSERT INTO quest_posts_archive (id, quest_id, user_id, content, image_urls, like_count, created_at, deleted_by, delete_reason)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8::uuid, 'user_request')
        """, post["id"], post["quest_id"], post["user_id"], post["content"],
            post["image_urls"], post["like_count"], post["created_at"], u["user_id"])
    except Exception as e:
        logger.warning(f"아카이브 백업 실패: {e}")

    # 리액션 삭제 → 게시글 삭제
    await db.execute("DELETE FROM quest_post_likes WHERE post_id=$1::uuid", post_id)
    await db.execute("DELETE FROM quest_posts WHERE id=$1::uuid", post_id)

    return {"success": True, "data": {"archived": True}}


@app.post("/quests/{quest_id}/feed/{post_id}/like")
async def like_quest_post(quest_id: str, post_id: str, payload: dict = Body(default={}), u: dict = Depends(get_current_user)):
    db = app.state.db
    m = await _check_member(db, quest_id, u["user_id"])
    if not m:
        return {"success": False, "error": "멤버만 리액션을 할 수 있습니다"}
    reaction = (payload or {}).get("reaction", "👍")
    if len(reaction) > 4:
        reaction = "👍"

    existing = await db.fetchval(
        "SELECT reaction_type FROM quest_post_likes WHERE user_id=$1::uuid AND post_id=$2::uuid AND reaction_type=$3",
        u["user_id"], post_id, reaction,
    )
    if existing:
        await db.execute("DELETE FROM quest_post_likes WHERE user_id=$1::uuid AND post_id=$2::uuid AND reaction_type=$3",
                         u["user_id"], post_id, reaction)
        await db.execute("UPDATE quest_posts SET like_count = GREATEST(like_count - 1, 0) WHERE id=$1::uuid", post_id)
        liked = False
    else:
        old = await db.fetchval("SELECT reaction_type FROM quest_post_likes WHERE user_id=$1::uuid AND post_id=$2::uuid", u["user_id"], post_id)
        if old:
            await db.execute("DELETE FROM quest_post_likes WHERE user_id=$1::uuid AND post_id=$2::uuid", u["user_id"], post_id)
            await db.execute("UPDATE quest_posts SET like_count = GREATEST(like_count - 1, 0) WHERE id=$1::uuid", post_id)
        await db.execute("INSERT INTO quest_post_likes (user_id, post_id, reaction_type) VALUES ($1::uuid,$2::uuid,$3) ON CONFLICT DO NOTHING",
                         u["user_id"], post_id, reaction)
        await db.execute("UPDATE quest_posts SET like_count = like_count + 1 WHERE id=$1::uuid", post_id)
        liked = True

    cnt = await db.fetchval("SELECT like_count FROM quest_posts WHERE id=$1::uuid", post_id)
    reactions = await db.fetch("SELECT reaction_type, COUNT(*) as cnt FROM quest_post_likes WHERE post_id=$1::uuid GROUP BY reaction_type", post_id)
    reaction_summary = {r["reaction_type"]: r["cnt"] for r in reactions}
    return {"success": True, "data": {"liked": liked, "reaction": reaction, "like_count": cnt, "reactions": reaction_summary}}


@app.put("/quests/{quest_id}/feed/{post_id}")
async def edit_quest_post(quest_id: str, post_id: str, payload: dict = Body(...), u: dict = Depends(get_current_user)):
    """퀘스트 피드 게시글 수정 (본인만)."""
    db = app.state.db
    post = await db.fetchrow("SELECT user_id FROM quest_posts WHERE id=$1::uuid AND quest_id=$2::uuid", post_id, quest_id)
    if not post:
        return {"success": False, "error": "게시글을 찾을 수 없습니다"}
    if str(post["user_id"]) != u["user_id"]:
        return {"success": False, "error": "본인 게시글만 수정할 수 있습니다"}
    content = payload.get("content", "").strip()
    if not content:
        return {"success": False, "error": "내용을 입력하세요"}
    await db.execute("UPDATE quest_posts SET content=$1 WHERE id=$2::uuid", content, post_id)
    return {"success": True}


@app.get("/quests/{quest_id}/feed/{post_id}/comments")
async def quest_post_comments(quest_id: str, post_id: str, u: dict = Depends(get_current_user)):
    db = app.state.db
    rows = await db.fetch("""
        SELECT c.id, c.content, c.created_at, c.user_id,
               usr.username, usr.display_name, usr.avatar_url
        FROM quest_post_comments c
        JOIN users usr ON usr.id = c.user_id
        WHERE c.post_id = $1::uuid
        ORDER BY c.created_at ASC
    """, post_id)
    return {"success": True, "data": {"comments": [dict(r) for r in rows]}}


@app.post("/quests/{quest_id}/feed/{post_id}/comments")
async def add_quest_comment(quest_id: str, post_id: str, payload: dict = Body(...),
                            u: dict = Depends(get_current_user)):
    db = app.state.db
    m = await _check_member(db, quest_id, u["user_id"])
    if not m:
        return {"success": False, "error": "멤버만 댓글을 쓸 수 있습니다"}
    comment_text = payload.get("content", "")
    filter_result = await check_content(db, u["user_id"], comment_text)
    if filter_result["blocked"]:
        return {"success": False, "error": filter_result["message"], "content_warning": True}
    cid = await db.fetchval("""
        INSERT INTO quest_post_comments (post_id, user_id, content)
        VALUES ($1::uuid, $2::uuid, $3) RETURNING id
    """, post_id, u["user_id"], comment_text)
    resp = {"success": True, "data": {"comment_id": str(cid)}}
    if filter_result.get("warning"):
        resp["data"]["content_warning"] = filter_result["message"]
    return resp


@app.delete("/quests/{quest_id}/feed/{post_id}/comments/{comment_id}")
async def delete_quest_comment(quest_id: str, post_id: str, comment_id: str, u: dict = Depends(get_current_user)):
    """퀘스트 댓글 삭제 (본인 또는 호스트)."""
    db = app.state.db
    comment = await db.fetchrow("SELECT user_id FROM quest_post_comments WHERE id=$1::uuid", comment_id)
    if not comment:
        return {"success": False, "error": "댓글을 찾을 수 없습니다"}
    is_owner = str(comment["user_id"]) == u["user_id"]
    host = await _check_host(db, quest_id, u["user_id"])
    if not is_owner and not host:
        return {"success": False, "error": "본인 댓글만 삭제할 수 있습니다"}
    await db.execute("DELETE FROM quest_post_comments WHERE id=$1::uuid", comment_id)
    return {"success": True}


# ── 퀘스트 배지 ──

@app.post("/quests/{quest_id}/badges")
async def create_quest_badge(quest_id: str, payload: dict = Body(...), u: dict = Depends(get_current_user)):
    db = app.state.db
    host = await _check_host(db, quest_id, u["user_id"])
    if not host:
        return {"success": False, "error": "주최자/부주최자만 배지를 만들 수 있습니다"}
    bid = str(_uuid.uuid4())
    await db.execute("""
        INSERT INTO quest_badges (id, quest_id, name, description, icon_url, rarity, created_by)
        VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7::uuid)
    """, bid, quest_id, payload.get("name", "퀘스트 배지"),
        payload.get("description"), payload.get("icon_url", "🏅"),
        payload.get("rarity", "common"), u["user_id"])
    return {"success": True, "data": {"badge_id": bid}}


@app.get("/quests/{quest_id}/badges")
async def list_quest_badges(quest_id: str, u: dict = Depends(get_current_user)):
    db = app.state.db
    rows = await db.fetch("""
        SELECT qb.id, qb.name, qb.description, qb.icon_url, qb.rarity, qb.created_at,
               EXISTS(SELECT 1 FROM quest_badge_awards a WHERE a.badge_id=qb.id AND a.user_id=$2::uuid) AS earned,
               (SELECT COUNT(*) FROM quest_badge_awards a2 WHERE a2.badge_id=qb.id) AS award_count
        FROM quest_badges qb
        WHERE qb.quest_id = $1::uuid
        ORDER BY qb.created_at
    """, quest_id, u["user_id"])
    return {"success": True, "data": [dict(r) for r in rows]}


@app.post("/quests/{quest_id}/badges/{badge_id}/award")
async def award_quest_badge(quest_id: str, badge_id: str, payload: dict = Body(...),
                            u: dict = Depends(get_current_user)):
    db = app.state.db
    host = await _check_host(db, quest_id, u["user_id"])
    if not host:
        return {"success": False, "error": "주최자/부주최자만 배지를 수여할 수 있습니다"}
    user_ids = payload.get("user_ids", [])
    awarded = []
    for uid in user_ids[:50]:
        m = await _check_member(db, quest_id, uid)
        if not m:
            continue
        try:
            await db.execute("""
                INSERT INTO quest_badge_awards (badge_id, user_id, awarded_by)
                VALUES ($1::uuid, $2::uuid, $3::uuid) ON CONFLICT (badge_id, user_id) DO NOTHING
            """, badge_id, uid, u["user_id"])
            awarded.append(uid)
        except Exception:
            pass
    return {"success": True, "data": {"awarded": awarded, "count": len(awarded)}}


# ── 관리자: 시스템 설정 ──

@app.get("/admin/config")
async def admin_get_config(u: dict = Depends(get_current_user)):
    """시스템 설정 조회 (관리자 전용)."""
    if u.get("role") != "admin":
        return {"success": False, "error": "관리자 권한 필요"}
    rows = await app.state.db.fetch("SELECT key, value, description, updated_at FROM system_config ORDER BY key")
    return {"success": True, "data": [dict(r) for r in rows]}


@app.put("/admin/config/{key}")
async def admin_set_config(key: str, payload: dict = Body(...), u: dict = Depends(get_current_user)):
    """시스템 설정 변경 (관리자 전용)."""
    if u.get("role") != "admin":
        return {"success": False, "error": "관리자 권한 필요"}
    value = payload.get("value")
    if value is None:
        return {"success": False, "error": "value 필드 필요"}
    import json as _json
    await app.state.db.execute("""
        INSERT INTO system_config (key, value, description, updated_at)
        VALUES ($1, $2, $3, NOW())
        ON CONFLICT (key) DO UPDATE SET value=$2, updated_at=NOW()
    """, key, _json.dumps(value), payload.get("description", ""))
    logger.info(f"[admin] config set: {key}={value} by {u['username']}")
    return {"success": True, "data": {"key": key, "value": value}}


# ── 관리자: 사용자별 XP 이력 ──

@app.get("/admin/xp-history/{username}")
async def admin_xp_history(username: str, u: dict = Depends(get_current_user)):
    """특정 사용자의 XP 획득 이력 (관리자 전용)."""
    if u.get("role") != "admin":
        return {"success": False, "error": "관리자 권한 필요"}
    db = app.state.db
    user = await db.fetchrow("SELECT id, username, display_name FROM users WHERE username=$1", username)
    if not user:
        return {"success": False, "error": "사용자를 찾을 수 없습니다"}
    # 이력
    rows = await db.fetch("""
        SELECT pl.delta, pl.reason, pl.created_at, pl.ref_id
        FROM point_ledger pl WHERE pl.user_id=$1
        ORDER BY pl.created_at DESC LIMIT 50
    """, user["id"])
    # 유형별 합계
    by_reason = await db.fetch("""
        SELECT reason, SUM(delta) AS total, COUNT(*) AS cnt
        FROM point_ledger WHERE user_id=$1
        GROUP BY reason ORDER BY total DESC
    """, user["id"])
    total_xp = await db.fetchval(
        "SELECT COALESCE(SUM(delta),0) FROM point_ledger WHERE user_id=$1", user["id"]
    )
    return {
        "success": True,
        "data": {
            "username": user["username"],
            "display_name": user["display_name"],
            "total_xp": int(total_xp),
            "history": [dict(r) for r in rows],
            "by_reason": [dict(r) for r in by_reason],
        },
    }


# ── 관리자: 사용자별 XP 현황 ──

@app.get("/admin/xp-stats")
async def admin_xp_stats(u: dict = Depends(get_current_user)):
    """사용자별 XP 현황 + XP 부여 이력 통계 (관리자 전용)."""
    if u.get("role") != "admin":
        return {"success": False, "error": "관리자 권한 필요"}
    db = app.state.db
    # 사용자별 XP 현황
    users = await db.fetch("""
        SELECT u.username, u.display_name, u.avatar_url,
               COALESCE(up.total_points, 0) AS total_xp,
               (SELECT COUNT(*) FROM point_ledger pl WHERE pl.user_id=u.id AND pl.delta>0) AS grant_count,
               (SELECT MAX(pl.created_at) FROM point_ledger pl WHERE pl.user_id=u.id) AS last_activity
        FROM users u
        LEFT JOIN user_points up ON up.user_id = u.id
        WHERE u.role != 'admin'
        ORDER BY COALESCE(up.total_points, 0) DESC
        LIMIT 50
    """)
    # XP 부여 유형별 통계
    by_reason = await db.fetch("""
        SELECT reason, SUM(delta) AS total, COUNT(*) AS cnt
        FROM point_ledger WHERE delta > 0
        GROUP BY reason ORDER BY total DESC
    """)
    # 최근 7일 일별 XP 부여 합계
    daily = await db.fetch("""
        SELECT created_at::date AS day, SUM(delta) AS total, COUNT(*) AS cnt
        FROM point_ledger WHERE delta > 0 AND created_at > NOW() - INTERVAL '14 days'
        GROUP BY created_at::date ORDER BY day DESC
    """)
    result = []
    for r in users:
        d = dict(r)
        xp = d["total_xp"]
        lv, lt, _ = get_level(xp)
        d["level"] = lv
        d["level_title"] = lt
        result.append(d)
    return {
        "success": True,
        "data": {
            "users": result,
            "by_reason": [dict(r) for r in by_reason],
            "daily": [dict(r) for r in daily],
        },
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("GAME_PORT", 8502))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
