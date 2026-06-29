"""
SNS Service — 인증·게시물·피드·좋아요·댓글·프로필·해시태그·중복제보방지.
Kafka Producer: plogging.report.created
포트: SNS_PORT 환경변수 (기본 8501)
"""
import os
import sys
import random

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "shared"))

from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, UploadFile, File, Form, Query, Body, Request
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional

from core.types import TrashType, Severity, DataSource, GeoPoint
from core.events import ReportCreatedEvent
from core.kafka_client import KafkaProducerClient
from core.auth import get_current_user, get_optional_user, create_access_token
from core.db import create_db_pool, create_redis
from core.logger import get_logger
from core.content_filter import check_content
from core.gamification import get_level, grant_xp, calc_xp
from core.cache_agent import CacheAgent

logger = get_logger("sns-service")


@asynccontextmanager
async def lifespan(app: FastAPI):
    import time as _time
    app.state.db = await create_db_pool(min_size=10, max_size=50)
    app.state.redis = create_redis()
    app.state.cache = CacheAgent(app.state.redis, logger)
    app.state.start_time = _time.time()
    app.state.request_count = 0
    app.state.kafka = KafkaProducerClient(
        os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093")
    )
    try:
        from minio import Minio
        app.state.minio = Minio(
            os.environ.get("MINIO_ENDPOINT", "localhost:9001"),
            access_key=os.environ.get("MINIO_ACCESS_KEY", "plogging_admin"),
            secret_key=os.environ.get("MINIO_SECRET_KEY", ""),
            secure=False,
        )
    except Exception as e:
        logger.warning(f"MinIO not available: {e}")
        app.state.minio = None

    # DM 테이블 자동 생성
    try:
        await app.state.db.execute("""
            CREATE TABLE IF NOT EXISTS direct_messages (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                sender_id UUID REFERENCES users(id) ON DELETE CASCADE,
                recipient_id UUID REFERENCES users(id) ON DELETE CASCADE,
                content TEXT NOT NULL,
                is_read BOOLEAN DEFAULT FALSE,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
        await app.state.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_dm_recipient
            ON direct_messages(recipient_id, created_at DESC)
        """)
        await app.state.db.execute("""
            CREATE INDEX IF NOT EXISTS idx_dm_sender
            ON direct_messages(sender_id, created_at DESC)
        """)
        logger.info("DM 테이블 준비 완료")
    except Exception as e:
        logger.warning(f"DM 테이블 생성 실패 (이미 존재할 수 있음): {e}")

    # post_comments 테이블 확인 (id가 UUID인지 보장)
    try:
        await app.state.db.execute("""
            CREATE TABLE IF NOT EXISTS post_comments (
                id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
                post_id UUID REFERENCES posts(id) ON DELETE CASCADE,
                user_id UUID REFERENCES users(id) ON DELETE CASCADE,
                content TEXT NOT NULL,
                created_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)
    except Exception as e:
        logger.warning(f"post_comments 테이블 확인: {e}")

    yield
    await app.state.db.close()
    await app.state.redis.aclose()
    app.state.kafka.flush()


app = FastAPI(title="SNS Service", version="1.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)


@app.middleware("http")
async def count_requests(request: Request, call_next):
    """요청 카운터 미들웨어 — 트래픽 모니터링용."""
    import time as _time
    if request.method != "OPTIONS":
        app.state.request_count = getattr(app.state, "request_count", 0) + 1
        try:
            redis = app.state.redis
            mk = f"req_count:{int(_time.time())//60}"
            hk = f"req_count_h:{int(_time.time())//3600}"
            pipe = redis.pipeline()
            pipe.incr(mk); pipe.expire(mk, 3600)
            pipe.incr(hk); pipe.expire(hk, 86400)
            await pipe.execute()
        except Exception:
            pass
    return await call_next(request)


# ── 중복 제보 방지 (Redis) ────────────────────────────────
async def _check_duplicate(redis_client, lat: float, lon: float) -> bool:
    key = f"dedup:report:{round(lat, 3)}:{round(lon, 3)}"
    if await redis_client.exists(key):
        return True
    await redis_client.setex(key, 3600, "1")
    return False


@app.get("/health")
async def health():
    return {"status": "ok", "service": "sns"}


@app.get("/health/detailed")
async def health_detailed():
    """시스템 상세 메트릭 — 메모리, DB풀, 트래픽, 가동시간."""
    import time as _time, os
    result = {"status": "ok", "service": "sns"}
    start = getattr(app.state, "start_time", _time.time())
    result["uptime_seconds"] = int(_time.time() - start)
    # 메모리 (/proc/self/status)
    try:
        with open("/proc/self/status") as pf:
            for line in pf:
                if line.startswith("VmRSS:"):
                    result["memory_mb"] = round(int(line.split()[1]) / 1024, 1)
                    break
    except Exception:
        result["memory_mb"] = -1
    # CPU
    try:
        with open("/proc/self/stat") as pf:
            parts = pf.read().split()
        utime, stime = int(parts[13]), int(parts[14])
        ticks = os.sysconf("SC_CLK_TCK")
        result["cpu_percent"] = round(((utime + stime) / ticks) / max(_time.time() - start, 1) * 100, 1)
    except Exception:
        result["cpu_percent"] = -1
    # DB 풀
    try:
        pool = app.state.db
        result["db_pool"] = {"size": pool.get_size(), "free": pool.get_idle_size(),
                             "used": pool.get_size() - pool.get_idle_size(), "max": pool.get_max_size()}
    except Exception:
        result["db_pool"] = {}
    # 트래픽
    result["total_requests"] = getattr(app.state, "request_count", 0)
    try:
        redis = app.state.redis
        now_min = int(_time.time()) // 60
        now_hour = int(_time.time()) // 3600
        cur_min = int(await redis.get(f"req_count:{now_min}") or 0)
        pipe = redis.pipeline()
        for i in range(60): pipe.get(f"req_count:{now_min - i}")
        mins = await pipe.execute()
        per_hour = sum(int(v or 0) for v in mins)
        pipe2 = redis.pipeline()
        for i in range(24): pipe2.get(f"req_count_h:{now_hour - i}")
        hours = await pipe2.execute()
        hour_vals = [int(v or 0) for v in hours]
        result["traffic"] = {"requests_this_minute": cur_min, "requests_last_hour": per_hour,
                             "peak_hour_value": max(hour_vals) if hour_vals else 0}
    except Exception:
        result["traffic"] = {}
    return result


@app.get("/config/public")
async def public_config():
    """공개 설정 조회 (인증 불필요) — 프론트에서 기능 토글에 사용."""
    try:
        rows = await app.state.db.fetch(
            "SELECT key, value FROM system_config WHERE key = ANY($1)",
            ["report_require_location"],
        )
        return {"success": True, "data": {r["key"]: r["value"] for r in rows}}
    except Exception:
        return {"success": True, "data": {}}


@app.get("/cache/stats")
async def cache_stats():
    """캐시 에이전트 성능 메트릭."""
    redis = app.state.redis
    try:
        # 활동 카운터
        posts_activity = int(await redis.get("activity:posts") or 0)
        xp_activity = int(await redis.get("activity:xp") or 0)

        # 각 캐시 키의 TTL 조회
        cache_keys = await redis.keys("cache:*")
        cache_entries = []
        for k in cache_keys:
            ttl = await redis.ttl(k)
            key_name = k.decode() if isinstance(k, bytes) else k
            cache_entries.append({"key": key_name, "ttl": ttl})

        # TTL 티어 계산 (현재 적용 중인 TTL)
        from core.cache_agent import TTL_TIERS
        current_ttls = {}
        for cat, act_key in [("trending_feeds", posts_activity), ("trending_users", xp_activity), ("ranking_global", xp_activity)]:
            for threshold, ttl in TTL_TIERS:
                if act_key >= threshold:
                    current_ttls[cat] = {"ttl": ttl, "activity": act_key, "tier": f"{threshold}+건/시간"}
                    break

        return {
            "success": True,
            "data": {
                "activity": {"posts": posts_activity, "xp": xp_activity},
                "current_ttls": current_ttls,
                "cached_keys": cache_entries,
                "total_cached": len(cache_entries),
            },
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


# ═══════════════════════════════════════════════════════════
# 인증 (Auth)
# ═══════════════════════════════════════════════════════════

@app.post("/auth/register")
async def register(payload: dict):
    """회원가입 — username/email + password."""
    import bcrypt
    db = app.state.db
    username = payload.get("username", "").strip()
    email = payload.get("email", "").strip()
    password = payload.get("password", "")
    display_name = payload.get("display_name", username)

    if not username or not email or not password:
        return {"success": False, "error": "username, email, password 필수"}
    if len(password) < 4:
        return {"success": False, "error": "비밀번호 4자 이상"}

    quest_invite_opt_in = payload.get("quest_invite_opt_in", True)
    pw_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()

    try:
        user_id = await db.fetchval("""
            INSERT INTO users (username, email, password_hash, display_name, quest_invite_opt_in)
            VALUES ($1, $2, $3, $4, $5)
            RETURNING id
        """, username, email, pw_hash, display_name, quest_invite_opt_in)
    except Exception as e:
        err = str(e)
        if "users_username_key" in err:
            return {"success": False, "error": "이미 사용 중인 아이디입니다"}
        if "users_email_key" in err:
            return {"success": False, "error": "이미 사용 중인 이메일입니다"}
        return {"success": False, "error": f"가입 실패: {err[:100]}"}

    # 기본 권한 부여
    for perm in ["feed", "ranking", "zone", "quest"]:
        await db.execute("""
            INSERT INTO user_permissions (user_id, permission)
            VALUES ($1, $2) ON CONFLICT DO NOTHING
        """, user_id, perm)

    return {
        "success": True,
        "data": {
            "user_id": str(user_id),
            "username": username,
            "message": "회원가입 완료! 로그인해주세요.",
        },
    }


@app.post("/auth/login")
async def login(payload: dict, request: Request):
    """로그인 — username 또는 email + password."""
    import bcrypt
    db = app.state.db
    login_id = payload.get("username", "").strip()
    password = payload.get("password", "")

    if not login_id or not password:
        return {"success": False, "error": "아이디와 비밀번호를 입력하세요"}

    # username 또는 email로 조회
    row = await db.fetchrow("""
        SELECT id, username, email, password_hash, display_name, role, avatar_url, bio
        FROM users WHERE username = $1 OR email = $1
    """, login_id)

    if not row:
        return {"success": False, "error": "존재하지 않는 계정입니다"}

    if not bcrypt.checkpw(password.encode(), row["password_hash"].encode()):
        return {"success": False, "error": "비밀번호가 일치하지 않습니다"}

    token = create_access_token(str(row["id"]), row["username"], row["role"])

    # 접속 로그 기록 (제3조: 3개월 보관)
    try:
        ip = request.headers.get("x-forwarded-for", request.client.host if request.client else "unknown")
        ua = request.headers.get("user-agent", "")[:500]
        await db.execute(
            "INSERT INTO login_logs (user_id, ip_address, user_agent) VALUES ($1, $2, $3)",
            row["id"], ip, ua,
        )
        # 3개월 초과 로그 자동 정리
        await db.execute("DELETE FROM login_logs WHERE login_at < NOW() - INTERVAL '3 months'")
    except Exception as e:
        logger.warning(f"접속 로그 기록 실패: {e}")

    # 권한 조회
    perms = await db.fetch(
        "SELECT permission FROM user_permissions WHERE user_id = $1", row["id"]
    )
    perm_list = [p["permission"] for p in perms]

    return {
        "success": True,
        "data": {
            "token": token,
            "user": {
                "user_id": str(row["id"]),
                "username": row["username"],
                "email": row["email"],
                "display_name": row["display_name"],
                "avatar_url": row["avatar_url"],
                "role": row["role"],
                "bio": row.get("bio", ""),
                "permissions": perm_list,
            },
        },
    }


@app.post("/auth/google")
async def google_login(payload: dict):
    """Google OAuth 로그인 — 프론트에서 받은 credential 처리."""
    db = app.state.db
    google_id = payload.get("google_id", "")
    email = payload.get("email", "")
    name = payload.get("name", "")
    avatar = payload.get("avatar", "")

    if not email:
        return {"success": False, "error": "Google 인증 정보 부족"}

    # 기존 사용자 확인
    row = await db.fetchrow(
        "SELECT id, username, role FROM users WHERE email = $1 OR google_id = $2",
        email, google_id,
    )

    if row:
        token = create_access_token(str(row["id"]), row["username"], row["role"])
        perms = await db.fetch(
            "SELECT permission FROM user_permissions WHERE user_id = $1", row["id"]
        )
        return {
            "success": True,
            "data": {
                "token": token,
                "user": {
                    "user_id": str(row["id"]),
                    "username": row["username"],
                    "role": row["role"],
                    "permissions": [p["permission"] for p in perms],
                },
            },
        }

    # 신규 가입
    username = email.split("@")[0]
    # 중복 방지
    cnt = await db.fetchval("SELECT COUNT(*) FROM users WHERE username LIKE $1||'%'", username)
    if cnt > 0:
        username = f"{username}{cnt+1}"

    user_id = await db.fetchval("""
        INSERT INTO users (username, email, password_hash, display_name, avatar_url, google_id)
        VALUES ($1, $2, 'google_oauth', $3, $4, $5) RETURNING id
    """, username, email, name or username, avatar, google_id)

    for perm in ["feed", "ranking", "zone", "quest"]:
        await db.execute(
            "INSERT INTO user_permissions (user_id, permission) VALUES ($1, $2) ON CONFLICT DO NOTHING",
            user_id, perm,
        )

    token = create_access_token(str(user_id), username, "user")
    return {
        "success": True,
        "data": {
            "token": token,
            "user": {"user_id": str(user_id), "username": username, "role": "user",
                     "permissions": ["feed", "ranking", "zone", "quest"]},
            "is_new": True,
        },
    }


# ═══════════════════════════════════════════════════════════
# 프로필 / 타임라인
# ═══════════════════════════════════════════════════════════

@app.get("/users/{username}/profile")
async def user_profile(username: str):
    """사용자 프로필 조회 (공개)."""
    db = app.state.db
    row = await db.fetchrow("""
        SELECT u.id, u.username, u.display_name, u.avatar_url, u.bio,
               u.role, u.created_at, u.quest_invite_opt_in,
               COALESCE(up.total_points, 0) AS total_xp,
               (SELECT COUNT(*) FROM posts WHERE user_id = u.id) AS post_count,
               (SELECT COUNT(*) FROM trash_reports WHERE reporter_id = u.id) AS report_count
        FROM users u
        LEFT JOIN user_points up ON up.user_id = u.id
        WHERE u.username = $1
    """, username)

    if not row:
        return {"success": False, "error": "사용자를 찾을 수 없습니다"}

    d = dict(row)
    xp = d["total_xp"]
    lv, lv_title, xp_to_next = get_level(xp)

    # 배지
    badges = await db.fetch("""
        SELECT bd.badge_key, bd.name, bd.icon_url, bd.rarity, ub.earned_at
        FROM user_badges ub JOIN badge_definitions bd ON bd.id = ub.badge_id
        WHERE ub.user_id = $1 ORDER BY ub.earned_at DESC
    """, row["id"])

    return {
        "success": True,
        "data": {
            "user_id": str(row["id"]),
            "username": row["username"],
            "display_name": row["display_name"],
            "avatar_url": row["avatar_url"],
            "bio": d.get("bio", ""),
            "role": row["role"],
            "total_xp": xp,
            "level": lv,
            "level_title": lv_title,
            "post_count": d["post_count"],
            "report_count": d["report_count"],
            "badges": [dict(b) for b in badges],
            "quest_invite_opt_in": d.get("quest_invite_opt_in", False),
            "created_at": str(row["created_at"]),
        },
    }


@app.get("/users/{username}/timeline")
async def user_timeline(
    username: str,
    page: int = Query(1, ge=1),
    size: int = Query(20, le=50),
    sort: str = Query("recent"),
    current_user: dict = Depends(get_optional_user),
):
    """사용자 타임라인. sort=recent|engagement."""
    db = app.state.db
    offset = (page - 1) * size

    target = await db.fetchval("SELECT id FROM users WHERE username = $1", username)
    viewer_id = current_user["user_id"] if current_user else None
    is_owner = viewer_id and target and viewer_id == str(target)

    base = """
        SELECT p.id, p.content, p.image_urls, p.hashtags, p.entity_tags,
               p.post_type, (SELECT COUNT(*) FROM post_likes pl WHERE pl.post_id = p.id) AS like_count, p.created_at, p.updated_at,
               p.visibility, p.is_hidden,
               u.display_name, u.avatar_url, u.username, u.id as uid, u.role AS author_role,
               gz.name AS zone_name,
               tr.trash_type, tr.severity, tr.status,
               COALESCE(up.total_points, 0) AS total_xp,
               (SELECT COUNT(*) FROM post_comments pc WHERE pc.post_id = p.id) AS comment_count
        FROM posts p
        JOIN users u ON u.id = p.user_id
        LEFT JOIN geofence_zones gz ON gz.id = p.zone_id
        LEFT JOIN trash_reports tr ON tr.id = p.report_id
        LEFT JOIN user_points up ON up.user_id = p.user_id
        WHERE u.username = $1
    """
    order = " ORDER BY (SELECT COUNT(*) FROM post_likes pl2 WHERE pl2.post_id=p.id)+(SELECT COUNT(*) FROM post_comments pc2 WHERE pc2.post_id=p.id) DESC, p.created_at DESC" if sort == "engagement" else " ORDER BY p.created_at DESC"

    if is_owner:
        rows = await db.fetch(base + order + " LIMIT $2 OFFSET $3", username, size, offset)
    elif viewer_id:
        rows = await db.fetch(base + " AND p.visibility IN ('public','friends')" + order + " LIMIT $2 OFFSET $3", username, size, offset)
    else:
        rows = await db.fetch(base + " AND p.visibility = 'public'" + order + " LIMIT $2 OFFSET $3", username, size, offset)

    result = [_with_level(dict(r)) for r in rows]
    # 리액션 배치 로딩
    post_ids = [str(d["id"]) for d in result]
    if post_ids:
        all_rx = await db.fetch("""
            SELECT post_id::text, reaction_type, COUNT(*) as cnt
            FROM post_likes WHERE post_id::text = ANY($1)
            GROUP BY post_id, reaction_type
        """, post_ids)
        rx_map = {}
        for rx in all_rx:
            rx_map.setdefault(rx["post_id"], {})[rx["reaction_type"]] = rx["cnt"]
        # 내 리액션
        if viewer_id:
            my_rx = await db.fetch("""
                SELECT post_id::text, reaction_type FROM post_likes
                WHERE post_id::text = ANY($1) AND user_id=$2::uuid
            """, post_ids, viewer_id)
            my_map = {r["post_id"]: r["reaction_type"] for r in my_rx}
        else:
            my_map = {}
        for d in result:
            pid = str(d["id"])
            d["reactions"] = rx_map.get(pid, {})
            d["liked"] = pid in my_map
            d["my_reaction"] = my_map.get(pid)
    return {"success": True, "data": result}


@app.get("/profile/me")
async def my_profile(current_user: dict = Depends(get_current_user)):
    """내 프로필."""
    db = app.state.db
    row = await db.fetchrow("""
        SELECT u.id, u.username, u.email, u.display_name, u.avatar_url, u.bio,
               u.role, u.created_at, u.quest_invite_opt_in,
               COALESCE(up.total_points, 0) AS total_xp,
               (SELECT COUNT(*) FROM posts WHERE user_id = u.id) AS post_count
        FROM users u
        LEFT JOIN user_points up ON up.user_id = u.id
        WHERE u.id = $1::uuid
    """, current_user["user_id"])

    if not row:
        return {"success": False, "error": "사용자 없음"}

    perms = await db.fetch(
        "SELECT permission FROM user_permissions WHERE user_id = $1", row["id"]
    )

    xp = row["total_xp"]
    lv, lv_title, _ = get_level(xp)

    return {
        "success": True,
        "data": {
            "user_id": str(row["id"]),
            "username": row["username"],
            "email": row["email"],
            "display_name": row["display_name"],
            "avatar_url": row["avatar_url"],
            "bio": row.get("bio", ""),
            "role": row["role"],
            "total_xp": xp,
            "level": lv,
            "level_title": lv_title,
            "post_count": row["post_count"],
            "permissions": [p["permission"] for p in perms],
            "quest_invite_opt_in": row.get("quest_invite_opt_in", False),
            "created_at": str(row["created_at"]),
        },
    }


@app.put("/profile/me")
async def update_profile(payload: dict, current_user: dict = Depends(get_current_user)):
    """프로필 수정."""
    db = app.state.db
    display_name = payload.get("display_name")
    bio = payload.get("bio")

    if display_name is not None:
        await db.execute(
            "UPDATE users SET display_name=$1, updated_at=NOW() WHERE id=$2::uuid",
            display_name, current_user["user_id"],
        )
    if bio is not None:
        await db.execute(
            "UPDATE users SET bio=$1, updated_at=NOW() WHERE id=$2::uuid",
            bio, current_user["user_id"],
        )

    return {"success": True}


@app.delete("/profile/me")
async def delete_account(current_user: dict = Depends(get_current_user)):
    """회원 탈퇴 — 모든 개인정보 및 게시물 즉시 삭제."""
    db = app.state.db
    uid = current_user["user_id"]
    try:
        async with db.acquire() as conn:
            async with conn.transaction():
                # 1. 게시글 관련 (댓글·리액션·퀘스트 게시글)
                await conn.execute("DELETE FROM post_comments WHERE user_id=$1::uuid", uid)
                await conn.execute("DELETE FROM post_likes WHERE user_id=$1::uuid", uid)
                await conn.execute("DELETE FROM quest_post_comments WHERE user_id=$1::uuid", uid)
                await conn.execute("DELETE FROM quest_post_likes WHERE user_id=$1::uuid", uid)
                await conn.execute("DELETE FROM quest_posts WHERE user_id=$1::uuid", uid)
                await conn.execute("DELETE FROM posts WHERE user_id=$1::uuid", uid)
                # 2. 소셜 (DM·알림·초대)
                await conn.execute("DELETE FROM dm_messages WHERE sender_id=$1::uuid OR recipient_id=$1::uuid", uid)
                await conn.execute("DELETE FROM notifications WHERE user_id=$1::uuid", uid)
                await conn.execute("DELETE FROM quest_invitations WHERE inviter_id=$1::uuid OR invitee_id=$1::uuid", uid)
                # 3. 퀘스트 멤버십·배지
                await conn.execute("DELETE FROM quest_members WHERE user_id=$1::uuid", uid)
                await conn.execute("DELETE FROM quest_badge_awards WHERE user_id=$1::uuid", uid)
                await conn.execute("DELETE FROM quest_badges WHERE created_by=$1::uuid", uid)
                # 4. 사용자 메타 (권한·배지·포인트)
                await conn.execute("DELETE FROM user_permissions WHERE user_id=$1::uuid", uid)
                await conn.execute("DELETE FROM user_badges WHERE user_id=$1::uuid", uid)
                await conn.execute("DELETE FROM point_ledger WHERE user_id=$1::uuid", uid)
                # 5. 제보
                await conn.execute("DELETE FROM trash_reports WHERE reporter_id=$1::uuid", uid)
                # 6. 접속 로그 (개인정보)
                await conn.execute("DELETE FROM login_logs WHERE user_id=$1::uuid", uid)
                # 7. 사용자 삭제
                await conn.execute("DELETE FROM users WHERE id=$1::uuid", uid)
                # 8. MV 갱신
                await conn.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY user_points")
        logger.info(f"Account deleted: {current_user['username']} ({uid})")
        return {"success": True, "message": "계정이 삭제되었습니다"}
    except Exception as e:
        logger.error(f"Account deletion failed: {e}")
        return {"success": False, "error": f"탈퇴 처리 실패: {str(e)[:200]}"}


@app.post("/profile/me/avatar")
async def upload_avatar(
    file: UploadFile = File(...),
    current_user: dict = Depends(get_current_user),
):
    """프로필 아바타 업로드 (ico/png/jpg/jpeg, 4MB 이하, 주 1회 제한)."""
    import io
    from datetime import datetime, timedelta

    db = app.state.db

    # 주 1회 변경 제한 (admin, jwon은 무제한)
    if current_user.get("username") not in ("admin", "jwon") and current_user.get("role") != "admin":
        last_change = await db.fetchval(
            "SELECT updated_at FROM users WHERE id=$1::uuid", current_user["user_id"]
        )
        if last_change:
            diff = datetime.utcnow() - last_change.replace(tzinfo=None)
            if diff < timedelta(days=7):
                remain = 7 - diff.days
                return {"success": False, "error": f"프로필 사진은 주 1회만 변경 가능합니다 ({remain}일 후 가능)"}

    # 확장자 검증
    ext = os.path.splitext(file.filename or "avatar.png")[1].lower()
    if ext not in (".ico", ".png", ".jpg", ".jpeg"):
        return {"success": False, "error": "ico/png/jpg 파일만 업로드 가능합니다"}

    # 크기 검증 (4MB)
    data = await file.read()
    if len(data) > 4 * 1024 * 1024:
        return {"success": False, "error": "파일 크기는 4MB 이하여야 합니다"}

    # 파일 저장 경로 설정
    avatar_dir = os.path.join(os.path.dirname(__file__), "..", "..", "apps", "web", "avatars")
    os.makedirs(avatar_dir, exist_ok=True)

    # 기존 아바타 삭제
    uid = current_user["user_id"]
    for old_ext in (".ico", ".png"):
        old_path = os.path.join(avatar_dir, f"{uid}{old_ext}")
        if os.path.exists(old_path):
            os.remove(old_path)

    # 새 파일 저장
    filename = f"{uid}{ext}"
    filepath = os.path.join(avatar_dir, filename)
    with open(filepath, "wb") as f:
        f.write(data)

    # DB 업데이트
    avatar_url = f"/avatars/{filename}"
    db = app.state.db
    await db.execute(
        "UPDATE users SET avatar_url=$1, updated_at=NOW() WHERE id=$2::uuid",
        avatar_url, uid,
    )

    return {"success": True, "data": {"avatar_url": avatar_url}}


@app.get("/users/search")
async def search_users_for_invite(
    q: str = Query(..., min_length=1),
    size: int = Query(10, le=30),
    current_user: dict = Depends(get_current_user),
):
    """퀘스트 초대용 사용자 검색 (opt-in 사용자만)."""
    db = app.state.db
    rows = await db.fetch("""
        SELECT id::text AS user_id, username, display_name, avatar_url
        FROM users
        WHERE quest_invite_opt_in = true
          AND id != $1::uuid
          AND (username ILIKE $2 OR display_name ILIKE $2)
        ORDER BY display_name
        LIMIT $3
    """, current_user["user_id"], f"%{q}%", size)
    return {"success": True, "data": [dict(r) for r in rows]}


@app.put("/profile/me/invite-opt-in")
async def toggle_invite_opt_in(payload: dict = Body(...), current_user: dict = Depends(get_current_user)):
    """퀘스트 초대 허용 설정."""
    db = app.state.db
    opt_in = payload.get("opt_in", False)
    await db.execute(
        "UPDATE users SET quest_invite_opt_in=$1 WHERE id=$2::uuid",
        opt_in, current_user["user_id"],
    )
    return {"success": True, "data": {"quest_invite_opt_in": opt_in}}


# ═══════════════════════════════════════════════════════════
# 관리자 — 사용자 관리
# ═══════════════════════════════════════════════════════════

@app.get("/admin/hidden-posts")
async def admin_hidden_posts(
    page: int = Query(1, ge=1),
    size: int = Query(50, le=100),
    current_user: dict = Depends(get_current_user),
):
    """관리자 — 숨김 처리된 게시글 목록."""
    if current_user["role"] != "admin":
        return {"success": False, "error": "관리자 권한 필요"}
    db = app.state.db
    offset = (page - 1) * size
    rows = await db.fetch("""
        SELECT p.id::text, p.content, p.image_urls, p.created_at, p.hidden_at,
               u.username, u.display_name,
               (SELECT COUNT(*) FROM post_comments pc WHERE pc.post_id = p.id) AS comment_count
        FROM posts p
        JOIN users u ON u.id = p.user_id
        WHERE p.is_hidden = true
        ORDER BY p.hidden_at DESC NULLS LAST
        LIMIT $1 OFFSET $2
    """, size, offset)
    total = await db.fetchval("SELECT COUNT(*) FROM posts WHERE is_hidden = true") or 0
    return {"success": True, "data": {"posts": [dict(r) for r in rows], "total": total}}


@app.post("/admin/hidden-posts/{post_id}/restore")
async def admin_restore_post(post_id: str, current_user: dict = Depends(get_current_user)):
    """관리자 — 숨김 게시글 복원."""
    if current_user["role"] != "admin":
        return {"success": False, "error": "관리자 권한 필요"}
    await app.state.db.execute(
        "UPDATE posts SET is_hidden=false, hidden_by=NULL, hidden_at=NULL WHERE id=$1::uuid", post_id
    )
    return {"success": True}


@app.delete("/admin/hidden-posts/{post_id}")
async def admin_delete_post_permanent(post_id: str, current_user: dict = Depends(get_current_user)):
    """관리자 — 게시글 영구 삭제."""
    if current_user["role"] != "admin":
        return {"success": False, "error": "관리자 권한 필요"}
    await app.state.db.execute("DELETE FROM posts WHERE id=$1::uuid", post_id)
    return {"success": True}


@app.get("/admin/hidden-posts/export")
async def admin_export_hidden(current_user: dict = Depends(get_current_user)):
    """관리자 — 숨김 게시글 JSON export."""
    if current_user["role"] != "admin":
        return {"success": False, "error": "관리자 권한 필요"}
    rows = await app.state.db.fetch("""
        SELECT p.id::text, p.content, p.image_urls, p.created_at, p.hidden_at,
               u.username, u.display_name
        FROM posts p JOIN users u ON u.id = p.user_id
        WHERE p.is_hidden = true ORDER BY p.hidden_at DESC
    """)
    return {"success": True, "data": [dict(r) for r in rows]}


@app.get("/admin/users")
async def admin_list_users(
    page: int = Query(1, ge=1),
    size: int = Query(50, le=200),
    current_user: dict = Depends(get_current_user),
):
    """관리자 — 사용자 목록."""
    if current_user["role"] != "admin":
        return {"success": False, "error": "관리자 권한 필요"}

    db = app.state.db
    offset = (page - 1) * size
    rows = await db.fetch("""
        SELECT u.id, u.username, u.email, u.display_name, u.avatar_url, u.role, u.created_at,
               COALESCE(up.total_points, 0) AS total_xp,
               (SELECT COUNT(*) FROM posts p WHERE p.user_id=u.id) AS post_count,
               (SELECT COUNT(*) FROM trash_reports tr WHERE tr.reporter_id=u.id) AS report_count,
               array_agg(DISTINCT upr.permission) FILTER (WHERE upr.permission IS NOT NULL) AS permissions
        FROM users u
        LEFT JOIN user_points up ON up.user_id = u.id
        LEFT JOIN user_permissions upr ON upr.user_id = u.id
        GROUP BY u.id, u.username, u.email, u.display_name, u.avatar_url, u.role, u.created_at, up.total_points
        ORDER BY COALESCE(up.total_points, 0) DESC, u.created_at DESC
        LIMIT $1 OFFSET $2
    """, size, offset)

    total = await db.fetchval("SELECT COUNT(*) FROM users")
    result = []
    for r in rows:
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
            "total": total,
            "page": page,
        },
    }


@app.get("/admin/login-logs")
async def admin_login_logs(
    size: int = Query(50, ge=1, le=200),
    current_user: dict = Depends(get_current_user),
):
    """관리자 — 접속 로그 조회 (최근 3개월)."""
    if current_user["role"] != "admin":
        return {"success": False, "error": "관리자 권한 필요"}
    db = app.state.db
    rows = await db.fetch("""
        SELECT l.login_at, l.ip_address, l.user_agent,
               u.username, u.display_name
        FROM login_logs l
        JOIN users u ON u.id = l.user_id
        ORDER BY l.login_at DESC
        LIMIT $1
    """, size)
    # 일별 통계
    daily = await db.fetch("""
        SELECT login_at::date AS day, COUNT(*) AS cnt
        FROM login_logs
        WHERE login_at > NOW() - INTERVAL '30 days'
        GROUP BY login_at::date
        ORDER BY day DESC
    """)
    return {
        "success": True,
        "data": {
            "logs": [dict(r) for r in rows],
            "daily": [dict(r) for r in daily],
            "total": await db.fetchval("SELECT COUNT(*) FROM login_logs"),
        },
    }


@app.get("/admin/content-violations")
async def admin_content_violations(
    size: int = Query(50, ge=1, le=200),
    current_user: dict = Depends(get_current_user),
):
    """관리자 — 콘텐츠 필터 위반 현황."""
    if current_user["role"] != "admin":
        return {"success": False, "error": "관리자 권한 필요"}
    db = app.state.db

    # 테이블 존재 확인
    exists = await db.fetchval("""
        SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name='content_violations')
    """)
    if not exists:
        return {"success": True, "data": {"summary": {}, "violators": [], "recent": []}}

    # 요약 통계
    summary = await db.fetchrow("""
        SELECT
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '7 days') AS week,
            COUNT(*) FILTER (WHERE created_at > NOW() - INTERVAL '1 day') AS today,
            COUNT(DISTINCT user_id) AS unique_users,
            COUNT(*) FILTER (WHERE violation_level = 2) AS severe
        FROM content_violations
    """)

    # 상습 위반자 (Top 10)
    violators = await db.fetch("""
        SELECT cv.user_id, u.username, u.display_name,
               COUNT(*) AS cnt, MAX(cv.violation_level) AS max_level,
               MAX(cv.created_at) AS last_at
        FROM content_violations cv
        JOIN users u ON u.id = cv.user_id
        GROUP BY cv.user_id, u.username, u.display_name
        ORDER BY cnt DESC LIMIT 10
    """)

    # 최근 위반 (30건)
    recent = await db.fetch("""
        SELECT cv.content_snippet, cv.detected_words, cv.violation_level,
               cv.created_at, u.username, u.display_name
        FROM content_violations cv
        JOIN users u ON u.id = cv.user_id
        ORDER BY cv.created_at DESC LIMIT $1
    """, size)

    return {
        "success": True,
        "data": {
            "summary": dict(summary) if summary else {},
            "violators": [dict(v) for v in violators],
            "recent": [dict(r) for r in recent],
        },
    }


@app.put("/admin/users/{user_id}/permissions")
async def admin_set_permissions(
    user_id: str,
    payload: dict,
    current_user: dict = Depends(get_current_user),
):
    """관리자 — 사용자 권한 설정."""
    if current_user["role"] != "admin":
        return {"success": False, "error": "관리자 권한 필요"}

    db = app.state.db
    permissions = payload.get("permissions", [])

    # 기존 권한 삭제 후 재설정
    await db.execute("DELETE FROM user_permissions WHERE user_id = $1::uuid", user_id)
    for perm in permissions:
        await db.execute("""
            INSERT INTO user_permissions (user_id, permission, granted_by)
            VALUES ($1::uuid, $2, $3::uuid) ON CONFLICT DO NOTHING
        """, user_id, perm, current_user["user_id"])

    return {"success": True, "data": {"permissions": permissions}}


@app.put("/admin/users/{user_id}/role")
async def admin_set_role(
    user_id: str,
    payload: dict,
    current_user: dict = Depends(get_current_user),
):
    """관리자 — 사용자 역할 변경."""
    if current_user["role"] != "admin":
        return {"success": False, "error": "관리자 권한 필요"}

    role = payload.get("role", "user")
    await app.state.db.execute(
        "UPDATE users SET role=$1, updated_at=NOW() WHERE id=$2::uuid", role, user_id,
    )
    return {"success": True}


# ═══════════════════════════════════════════════════════════
# 게시물 작성 / 피드
# ═══════════════════════════════════════════════════════════

@app.post("/posts/report")
async def create_report_post(
    lat: Optional[float] = Form(None),
    lon: Optional[float] = Form(None),
    trash_type: TrashType = Form(...),
    severity: Severity = Form(...),
    content: str = Form(""),
    hashtags: str = Form(""),
    files: List[UploadFile] = File(default=[]),
    current_user: dict = Depends(get_current_user),
):
    db = app.state.db
    kafka = app.state.kafka

    # 위치 필수 여부 확인 (system_config)
    require_location = False
    try:
        cfg = await db.fetchval("SELECT value FROM system_config WHERE key='report_require_location'")
        if cfg: require_location = str(cfg).strip('"').lower() == 'true'
    except Exception: pass
    if require_location and (lat is None or lon is None):
        return {"success": False, "error": "제보에는 위치 정보가 필요합니다. 브라우저 설정에서 위치를 허용해주세요."}

    if lat is not None and lon is not None and await _check_duplicate(app.state.redis, lat, lon):
        return {"success": False, "error": "이 위치에 최근 제보가 있습니다 (50m 이내)"}

    image_urls = []
    if app.state.minio and files:
        bucket = os.environ.get("MINIO_BUCKET_IMAGES", "plogging-images")
        # MinIO 외부 URL 구성
        minio_endpoint = os.environ.get("MINIO_ENDPOINT", "localhost:9001")
        server_ip = os.environ.get("SERVER_HOST", minio_endpoint.split(":")[0])
        if server_ip in ("localhost", "127.0.0.1"):
            import socket
            try: server_ip = socket.gethostbyname(socket.gethostname())
            except: server_ip = "localhost"
        minio_port = minio_endpoint.split(":")[-1]
        external_host = f"http://{server_ip}:{minio_port}"
        for fi in files:
            import uuid
            key = f"reports/{current_user['user_id']}/{uuid.uuid4()}_{fi.filename}"
            try:
                app.state.minio.put_object(
                    bucket, key, fi.file, length=-1, part_size=5 * 1024 * 1024
                )
                image_urls.append(f"{external_host}/{bucket}/{key}")
            except Exception as e:
                logger.warning(f"MinIO upload failed: {e}")
    elif files:
        image_urls = [f"dummy://{fi.filename}" for fi in files]

    has_location = lat is not None and lon is not None
    async with db.acquire() as conn:
        async with conn.transaction():
            zone = None
            if has_location:
                zone = await conn.fetchrow("""
                    SELECT id, zone_key, bonus_multiplier
                    FROM geofence_zones
                    WHERE ST_Contains(geom, ST_SetSRID(ST_MakePoint($1,$2),4326))
                      AND is_active = TRUE
                    LIMIT 1
                """, lon, lat)

            if has_location:
                report_id = await conn.fetchval("""
                    INSERT INTO trash_reports
                      (reporter_id, location, zone_id, trash_type, severity, image_urls, source)
                    VALUES
                      ($1::uuid, ST_SetSRID(ST_MakePoint($2,$3),4326), $4, $5, $6, $7, 'sns')
                    RETURNING id
                """, current_user["user_id"], lon, lat,
                     zone["id"] if zone else None,
                     trash_type.value, severity.value, image_urls)
            else:
                report_id = await conn.fetchval("""
                    INSERT INTO trash_reports
                      (reporter_id, zone_id, trash_type, severity, image_urls, source)
                    VALUES
                      ($1::uuid, $2, $3, $4, $5, 'sns')
                    RETURNING id
                """, current_user["user_id"], None,
                     trash_type.value, severity.value, image_urls)

            tags = [t.strip() for t in hashtags.split(",") if t.strip()]
            if has_location:
                await conn.execute("""
                    INSERT INTO posts
                      (user_id, report_id, content, image_urls, hashtags,
                       location, zone_id, post_type)
                    VALUES ($1::uuid,$2,$3,$4,$5,ST_SetSRID(ST_MakePoint($6,$7),4326),$8,'report')
                """, current_user["user_id"], report_id, content,
                     image_urls, tags, lon, lat, zone["id"] if zone else None)
            else:
                await conn.execute("""
                    INSERT INTO posts
                      (user_id, report_id, content, image_urls, hashtags, post_type)
                    VALUES ($1::uuid,$2,$3,$4,$5,'report')
                """, current_user["user_id"], report_id, content, image_urls, tags)

    kafka.produce(
        "plogging.report.created",
        ReportCreatedEvent(
            report_id=str(report_id),
            reporter_id=current_user["user_id"],
            location=GeoPoint(lat=lat or 0, lon=lon or 0),
            zone_id=str(zone["id"]) if zone else None,
            trash_type=trash_type,
            severity=severity,
            image_urls=image_urls,
            source=DataSource.SNS,
        ),
        key=str(report_id),
    )

    if zone:
        import json
        await app.state.redis.publish(
            f"ws:zone:{zone['zone_key']}",
            json.dumps({
                "type": "pin_added",
                "report": {
                    "id": str(report_id), "lat": lat, "lon": lon,
                    "trash_type": trash_type.value, "severity": severity.value,
                },
            }),
        )

    logger.info(f"Report created: {report_id} at ({lat},{lon})")
    # XP 보상 — 제보 +20 XP (주말 보너스 적용)
    xp_amount = calc_xp("report_created")
    total, _ = await grant_xp(db, app.state.redis, current_user["user_id"], xp_amount, "report_created",
                              ref_id=str(report_id), kafka=app.state.kafka, logger=logger)
    await app.state.cache.invalidate("trending_feeds", prefix=True)
    await app.state.cache.invalidate("trending_users")
    await app.state.cache.track_activity("trending_feeds")
    await app.state.cache.track_activity("trending_users")
    return {"success": True, "data": {"report_id": str(report_id), "xp_earned": xp_amount}}


def _with_level(d: dict) -> dict:
    xp = d.get("total_xp", 0)
    lv, lv_title, _ = get_level(xp)
    d["level"] = lv
    d["level_title"] = lv_title
    return d


@app.get("/feed")
async def get_feed(
    zone_id: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    size: int = Query(20, le=50),
    current_user: dict = Depends(get_optional_user),
):
    db = app.state.db
    offset = (page - 1) * size

    is_admin = current_user and current_user.get("role") == "admin"

    base_query = """
        SELECT p.id, p.content, p.image_urls, p.hashtags, p.entity_tags,
               p.post_type, (SELECT COUNT(*) FROM post_likes pl WHERE pl.post_id = p.id) AS like_count, p.created_at, p.updated_at,
               p.visibility, p.is_hidden,
               u.display_name, u.avatar_url, u.username, u.id as uid, u.role AS author_role,
               gz.name AS zone_name,
               tr.trash_type, tr.severity, tr.status,
               ST_Y(p.location) AS lat, ST_X(p.location) AS lon,
               COALESCE(up.total_points, 0) AS total_xp,
               (SELECT COUNT(*) FROM post_comments pc WHERE pc.post_id = p.id) AS comment_count
        FROM posts p
        JOIN users u ON u.id = p.user_id
        LEFT JOIN geofence_zones gz ON gz.id = p.zone_id
        LEFT JOIN trash_reports tr ON tr.id = p.report_id
        LEFT JOIN user_points up ON up.user_id = p.user_id
    """

    # 숨김 필터: 관리자는 숨김 포함, 일반 사용자는 숨김 제외
    hidden_filter = "" if is_admin else " AND (p.is_hidden IS NOT TRUE)"

    # visibility 필터: 비로그인=public만, 로그인=public+friends+본인private
    viewer_id = current_user["user_id"] if current_user else None
    if viewer_id:
        vis_filter = " AND (p.visibility = 'public' OR p.visibility = 'friends' OR p.user_id = $%d::uuid)" + hidden_filter
    else:
        vis_filter = " AND p.visibility = 'public'" + hidden_filter

    if zone_id:
        if viewer_id:
            rows = await db.fetch(
                base_query + " WHERE p.zone_id = $1::uuid" + (vis_filter % 4) + " ORDER BY p.created_at DESC LIMIT $2 OFFSET $3",
                zone_id, size, offset, viewer_id,
            )
        else:
            rows = await db.fetch(
                base_query + " WHERE p.zone_id = $1::uuid" + vis_filter + " ORDER BY p.created_at DESC LIMIT $2 OFFSET $3",
                zone_id, size, offset,
            )
    else:
        if viewer_id:
            rows = await db.fetch(
                base_query + " WHERE 1=1" + (vis_filter % 3) + " ORDER BY p.created_at DESC LIMIT $1 OFFSET $2",
                size, offset, viewer_id,
            )
        else:
            rows = await db.fetch(
                base_query + " WHERE 1=1" + vis_filter + " ORDER BY p.created_at DESC LIMIT $1 OFFSET $2",
                size, offset,
            )

    result = [_with_level(dict(r)) for r in rows]
    post_ids = [str(d["id"]) for d in result]
    if post_ids:
        # 배치: 리액션 요약 (N+1 → 1 쿼리)
        all_reactions = await db.fetch("""
            SELECT post_id::text, reaction_type, COUNT(*) as cnt
            FROM post_likes WHERE post_id::text = ANY($1)
            GROUP BY post_id, reaction_type
        """, post_ids)
        react_map = {}
        for r in all_reactions:
            react_map.setdefault(r["post_id"], {})[r["reaction_type"]] = r["cnt"]
        # 배치: 내 리액션 (N+1 → 1 쿼리)
        my_react_map = {}
        if viewer_id:
            my_reacts = await db.fetch("""
                SELECT post_id::text, reaction_type FROM post_likes
                WHERE user_id=$1::uuid AND post_id::text = ANY($2)
            """, viewer_id, post_ids)
            my_react_map = {r["post_id"]: r["reaction_type"] for r in my_reacts}
        for d in result:
            pid = str(d["id"])
            d["reactions"] = react_map.get(pid, {})
            d["liked"] = pid in my_react_map
            d["my_reaction"] = my_react_map.get(pid)
    return {"success": True, "data": result}


# ═══════════════════════════════════════════════════════════
# 좋아요 / 댓글 / 공유
# ═══════════════════════════════════════════════════════════

@app.post("/posts/{post_id}/like")
async def toggle_like(post_id: str, payload: dict = Body(default={}), current_user: dict = Depends(get_current_user)):
    db = app.state.db
    uid = current_user["user_id"]
    reaction = payload.get("reaction", "👍") if payload else "👍"
    # 이모지 또는 레거시 키워드 허용 (최대 4자)
    if len(reaction) > 4:
        reaction = "👍"

    existing = await db.fetchrow(
        "SELECT reaction_type FROM post_likes WHERE user_id=$1::uuid AND post_id=$2::uuid AND reaction_type=$3",
        uid, post_id, reaction,
    )

    if existing:
        await db.execute(
            "DELETE FROM post_likes WHERE user_id=$1::uuid AND post_id=$2::uuid AND reaction_type=$3",
            uid, post_id, reaction,
        )
        await db.execute(
            "UPDATE posts SET like_count = GREATEST(0, like_count - 1) WHERE id=$1::uuid", post_id,
        )
        liked = False
    else:
        # 기존 다른 리액션 제거 후 새 리액션 추가
        old = await db.fetchval(
            "SELECT reaction_type FROM post_likes WHERE user_id=$1::uuid AND post_id=$2::uuid",
            uid, post_id,
        )
        if old:
            await db.execute(
                "DELETE FROM post_likes WHERE user_id=$1::uuid AND post_id=$2::uuid",
                uid, post_id,
            )
            await db.execute(
                "UPDATE posts SET like_count = GREATEST(0, like_count - 1) WHERE id=$1::uuid", post_id,
            )
        await db.execute(
            "INSERT INTO post_likes (user_id, post_id, reaction_type) VALUES ($1::uuid, $2::uuid, $3) ON CONFLICT DO NOTHING",
            uid, post_id, reaction,
        )
        await db.execute(
            "UPDATE posts SET like_count = like_count + 1 WHERE id=$1::uuid", post_id,
        )
        liked = True

        # 좋아요 받음 알림 + XP — 자기 자신 글 제외
        try:
            post_author = await db.fetchval(
                "SELECT user_id FROM posts WHERE id=$1::uuid", post_id
            )
            if post_author and str(post_author) != uid:
                actor_name = current_user.get("display_name") or current_user.get("username", "")
                actor_username = current_user.get("username", "")
                # 같은 actor+post에 대한 reaction 알림은 1개만 유지 (토글/변경 반복 시 누적 방지)
                await db.execute(
                    "DELETE FROM notifications WHERE user_id=$1::uuid AND type='reaction' "
                    "AND ref_post_id=$2::uuid AND actor_username=$3",
                    str(post_author), post_id, actor_username,
                )
                await db.execute(
                    """
                    INSERT INTO notifications
                        (user_id, type, title, body, ref_post_id, actor_username, actor_display_name)
                    VALUES ($1::uuid, 'reaction', $2, $3, $4::uuid, $5, $6)
                    """,
                    str(post_author),
                    f"{actor_name}님이 회원님의 글을 좋아합니다",
                    reaction,
                    post_id, actor_username, actor_name,
                )
                # like_received XP — ref_id=post_id로 게시글당 1회만 부여 (어뷰징 방지)
                try:
                    await grant_xp(
                        db, app.state.redis, str(post_author), 2, "like_received",
                        ref_id=post_id, kafka=app.state.kafka, logger=logger,
                    )
                except Exception as xp_err:
                    logger.warning(f"like_received XP 부여 실패: {xp_err}")
        except Exception as e:
            logger.warning(f"좋아요 알림 생성 실패: {e}")

    cnt = await db.fetchval("SELECT COUNT(*) FROM post_likes WHERE post_id=$1::uuid", post_id)
    # 리액션 요약
    reactions = await db.fetch(
        "SELECT reaction_type, COUNT(*) as cnt FROM post_likes WHERE post_id=$1::uuid GROUP BY reaction_type",
        post_id,
    )
    reaction_summary = {r["reaction_type"]: r["cnt"] for r in reactions}
    # 리액션 변경 시 trending 캐시 무효화 + 활동 추적
    await app.state.cache.invalidate("trending_feeds", prefix=True)
    await app.state.cache.track_activity("trending_feeds")
    return {"success": True, "data": {"liked": liked, "reaction": reaction, "like_count": cnt, "reactions": reaction_summary}}


@app.post("/posts/{post_id}/hide")
async def toggle_hide_post(post_id: str, current_user: dict = Depends(get_current_user)):
    """관리자 전용 — 게시글 숨김/해제 토글."""
    if current_user["role"] != "admin":
        return {"success": False, "error": "관리자만 숨김 처리할 수 있습니다"}
    db = app.state.db
    current = await db.fetchval("SELECT is_hidden FROM posts WHERE id=$1::uuid", post_id)
    if current is None:
        return {"success": False, "error": "게시글을 찾을 수 없습니다"}
    new_val = not current
    if new_val:
        await db.execute("UPDATE posts SET is_hidden=true, hidden_by=$1::uuid, hidden_at=NOW() WHERE id=$2::uuid",
                         current_user["user_id"], post_id)
    else:
        await db.execute("UPDATE posts SET is_hidden=false, hidden_by=NULL, hidden_at=NULL WHERE id=$1::uuid", post_id)
    return {"success": True, "data": {"is_hidden": new_val}}


@app.get("/posts/{post_id}/comments")
async def get_comments(post_id: str, page: int = Query(1, ge=1), size: int = Query(30, le=100)):
    db = app.state.db
    offset = (page - 1) * size
    rows = await db.fetch("""
        SELECT pc.id::text, pc.content, pc.created_at,
               u.username, u.display_name, u.avatar_url, u.role AS author_role
        FROM post_comments pc
        JOIN users u ON u.id = pc.user_id
        WHERE pc.post_id = $1::uuid
        ORDER BY pc.created_at ASC
        LIMIT $2 OFFSET $3
    """, post_id, size, offset)
    total = await db.fetchval(
        "SELECT COUNT(*) FROM post_comments WHERE post_id=$1::uuid", post_id,
    )
    return {"success": True, "data": {"comments": [dict(r) for r in rows], "total": total}}


@app.post("/posts/{post_id}/comments")
async def add_comment(post_id: str, payload: dict, current_user: dict = Depends(get_current_user)):
    content = payload.get("content", "").strip()
    if not content:
        return {"success": False, "error": "댓글 내용을 입력하세요"}

    # 비속어 필터링
    db = app.state.db
    filter_result = await check_content(db, current_user["user_id"], content)
    if filter_result["blocked"]:
        return {"success": False, "error": filter_result["message"], "content_warning": True}

    cid = await db.fetchval("""
        INSERT INTO post_comments (post_id, user_id, content)
        VALUES ($1::uuid, $2::uuid, $3) RETURNING id
    """, post_id, current_user["user_id"], content)

    # 댓글 알림: 게시글 작성자에게 알림 전송 (본인 댓글 제외)
    actor_name = current_user.get("display_name", current_user["username"])
    preview = content[:50] + ("..." if len(content) > 50 else "")
    notified_uids = {current_user["user_id"]}
    try:
        post_author = await db.fetchval(
            "SELECT user_id FROM posts WHERE id=$1::uuid", post_id
        )
        if post_author and str(post_author) not in notified_uids:
            await db.execute("""
                INSERT INTO notifications (user_id, type, title, body, ref_post_id, ref_comment_id, actor_username, actor_display_name)
                VALUES ($1::uuid, 'comment', $2, $3, $4::uuid, $5::uuid, $6, $7)
            """, str(post_author), f'{actor_name}님이 댓글을 남겼습니다',
                preview, post_id, str(cid), current_user["username"], actor_name)
            notified_uids.add(str(post_author))
    except Exception as e:
        logger.warning(f"댓글 알림 생성 실패: {e}")

    # @멘션 알림: 언급된 사용자에게 알림 전송
    mentions = payload.get("mentions", [])
    for m_username in mentions[:5]:  # 최대 5명
        try:
            m_user = await db.fetchrow("SELECT id FROM users WHERE username=$1", m_username)
            if m_user and str(m_user["id"]) not in notified_uids:
                await db.execute("""
                    INSERT INTO notifications (user_id, type, title, body, ref_post_id, ref_comment_id, actor_username, actor_display_name)
                    VALUES ($1::uuid, 'mention', $2, $3, $4::uuid, $5::uuid, $6, $7)
                """, str(m_user["id"]), f'{actor_name}님이 회원님을 언급했습니다',
                    preview, post_id, str(cid), current_user["username"], actor_name)
                notified_uids.add(str(m_user["id"]))
        except Exception:
            pass

    # XP 부여 — 자기 글에 본인 댓글은 모두 skip
    try:
        if post_author and str(post_author) != current_user["user_id"]:
            # 작성자에게 comment_received +3 (게시글당 1회, ref_id=post_id)
            try:
                await grant_xp(
                    db, app.state.redis, str(post_author), 3, "comment_received",
                    ref_id=post_id, kafka=app.state.kafka, logger=logger,
                )
            except Exception as xp_err:
                logger.warning(f"comment_received XP 부여 실패: {xp_err}")
            # 작성자(=댓글 단 사람)에게 comment_created +3 (일일 5회 한도, ref_id=cid)
            # admin은 트렌드/랭킹에서 제외되므로 XP 부여 안 함
            if current_user.get("role") != "admin":
                try:
                    await grant_xp(
                        db, app.state.redis, current_user["user_id"], 3, "comment_created",
                        ref_id=str(cid), kafka=app.state.kafka, logger=logger,
                    )
                except Exception as xp_err:
                    logger.warning(f"comment_created XP 부여 실패: {xp_err}")
    except Exception as e:
        logger.warning(f"댓글 XP 처리 실패: {e}")

    resp = {"success": True, "data": {"comment_id": str(cid)}}
    if filter_result.get("warning"):
        resp["data"]["content_warning"] = filter_result["message"]
    return resp


@app.delete("/posts/{post_id}/comments/{comment_id}")
async def delete_comment(post_id: str, comment_id: str, current_user: dict = Depends(get_current_user)):
    """댓글 삭제 (작성자 본인만 가능)."""
    db = app.state.db
    # id가 UUID 또는 integer일 수 있음
    try:
        deleted = await db.fetchval("""
            DELETE FROM post_comments
            WHERE id::text = $1 AND post_id = $2::uuid AND user_id = $3::uuid
            RETURNING id
        """, str(comment_id), post_id, current_user["user_id"])
    except Exception as e:
        logger.error(f"댓글 삭제 오류: {e}")
        return {"success": False, "error": f"삭제 실패: {str(e)[:100]}"}
    if not deleted:
        return {"success": False, "error": "삭제 권한이 없거나 댓글을 찾을 수 없습니다"}
    return {"success": True}


@app.get("/posts/{post_id}")
async def get_post(post_id: str, current_user: dict = Depends(get_optional_user)):
    """단일 게시글 조회 (공유 링크용). visibility 필터링 적용."""
    db = app.state.db
    row = await db.fetchrow("""
        SELECT p.id, p.content, p.image_urls, p.hashtags, p.entity_tags,
               p.post_type, (SELECT COUNT(*) FROM post_likes pl WHERE pl.post_id = p.id) AS like_count, p.created_at, p.updated_at,
               p.visibility,
               u.display_name, u.avatar_url, u.username, u.id as uid, u.role AS author_role,
               gz.name AS zone_name,
               tr.trash_type, tr.severity, tr.status,
               ST_Y(p.location) AS lat, ST_X(p.location) AS lon,
               COALESCE(up.total_points, 0) AS total_xp,
               (SELECT COUNT(*) FROM post_comments pc WHERE pc.post_id = p.id) AS comment_count
        FROM posts p
        JOIN users u ON u.id = p.user_id
        LEFT JOIN geofence_zones gz ON gz.id = p.zone_id
        LEFT JOIN trash_reports tr ON tr.id = p.report_id
        LEFT JOIN user_points up ON up.user_id = p.user_id
        WHERE p.id = $1::uuid
    """, post_id)

    if not row:
        return {"success": False, "error": "게시글을 찾을 수 없습니다"}

    d = dict(row)
    vis = d.get("visibility", "public")
    owner_id = str(d.get("uid", ""))
    viewer_id = current_user["user_id"] if current_user else None

    # private: 본인만
    if vis == "private" and viewer_id != owner_id:
        return {"success": False, "error": "비공개 게시글입니다"}
    # friends: 로그인 사용자만 (간단 구현 — 추후 친구 시스템 연동)
    if vis == "friends" and not viewer_id:
        return {"success": False, "error": "로그인 후 확인 가능한 게시글입니다"}

    # 리액션 요약
    reactions = await db.fetch(
        "SELECT reaction_type, COUNT(*) as cnt FROM post_likes WHERE post_id=$1::uuid GROUP BY reaction_type",
        post_id,
    )
    d["reactions"] = {rx["reaction_type"]: rx["cnt"] for rx in reactions}
    if viewer_id:
        liked = await db.fetchval(
            "SELECT reaction_type FROM post_likes WHERE user_id=$1::uuid AND post_id=$2::uuid LIMIT 1",
            viewer_id, post_id,
        )
        d["liked"] = liked is not None
        d["my_reaction"] = liked
    else:
        d["liked"] = False
        d["my_reaction"] = None

    return {"success": True, "data": _with_level(d)}


@app.put("/posts/{post_id}")
async def edit_post(post_id: str, payload: dict, current_user: dict = Depends(get_current_user)):
    """게시글 수정 — 작성 후 1분 이내만 가능."""
    from datetime import datetime, timezone, timedelta
    db = app.state.db

    row = await db.fetchrow(
        "SELECT user_id, created_at FROM posts WHERE id = $1::uuid", post_id,
    )
    if not row:
        return {"success": False, "error": "게시글을 찾을 수 없습니다"}
    if str(row["user_id"]) != current_user["user_id"]:
        return {"success": False, "error": "본인의 게시글만 수정할 수 있습니다"}

    # 시간 제한: system_config에서 조회 (기본: 무제한 — 테스트 환경)
    # 프로덕션에서는 timedelta(minutes=1)로 복원
    # elapsed = datetime.now(timezone.utc) - row["created_at"].replace(tzinfo=timezone.utc)
    # if elapsed > timedelta(minutes=1):
    #     return {"success": False, "error": "게시글 작성 후 1분이 경과하여 수정할 수 없습니다"}

    content = payload.get("content")
    if content is None:
        return {"success": False, "error": "수정할 내용을 입력하세요"}

    hashtags = payload.get("hashtags")
    visibility = payload.get("visibility")
    # image_urls: None이면 유지, list(빈 배열 포함)면 그 값으로 교체
    image_urls = payload.get("image_urls", None)

    if image_urls is None:
        await db.execute(
            "UPDATE posts SET content=$1, hashtags=COALESCE($2,hashtags), "
            "visibility=COALESCE($3,visibility), updated_at=NOW() WHERE id=$4::uuid",
            content, hashtags, visibility, post_id,
        )
    else:
        if not isinstance(image_urls, list):
            return {"success": False, "error": "image_urls는 배열이어야 합니다"}
        await db.execute(
            "UPDATE posts SET content=$1, hashtags=COALESCE($2,hashtags), "
            "visibility=COALESCE($3,visibility), image_urls=$4, updated_at=NOW() "
            "WHERE id=$5::uuid",
            content, hashtags, visibility, image_urls, post_id,
        )
    return {"success": True, "data": {"message": "게시글이 수정되었습니다"}}


@app.delete("/posts/{post_id}")
async def delete_post(post_id: str, current_user: dict = Depends(get_current_user)):
    """게시글 삭제 — 본인 또는 관리자만 가능."""
    db = app.state.db
    row = await db.fetchrow("SELECT user_id FROM posts WHERE id = $1::uuid", post_id)
    if not row:
        return {"success": False, "error": "게시글을 찾을 수 없습니다"}
    if str(row["user_id"]) != current_user["user_id"] and current_user.get("role") != "admin":
        return {"success": False, "error": "삭제 권한이 없습니다"}
    async with db.acquire() as conn:
        async with conn.transaction():
            await conn.execute("DELETE FROM post_comments WHERE post_id=$1::uuid", post_id)
            await conn.execute("DELETE FROM post_likes WHERE post_id=$1::uuid", post_id)
            await conn.execute("DELETE FROM posts WHERE id=$1::uuid", post_id)
    logger.info(f"Post deleted: {post_id} by {current_user['username']}")
    return {"success": True, "data": {"message": "게시글이 삭제되었습니다"}}


@app.put("/posts/{post_id}/visibility")
async def change_visibility(post_id: str, payload: dict, current_user: dict = Depends(get_current_user)):
    """게시글 공개 범위 변경 — public/friends/private. 시간 제한 없음."""
    db = app.state.db

    row = await db.fetchrow(
        "SELECT user_id FROM posts WHERE id = $1::uuid", post_id,
    )
    if not row:
        return {"success": False, "error": "게시글을 찾을 수 없습니다"}
    if str(row["user_id"]) != current_user["user_id"]:
        return {"success": False, "error": "본인의 게시글만 변경할 수 있습니다"}

    visibility = payload.get("visibility", "public")
    if visibility not in ("public", "friends", "private"):
        return {"success": False, "error": "유효하지 않은 공개 범위입니다 (public/friends/private)"}

    await db.execute(
        "UPDATE posts SET visibility = $1 WHERE id = $2::uuid",
        visibility, post_id,
    )
    return {"success": True, "data": {"visibility": visibility}}


@app.get("/search")
async def search(q: str = Query(..., min_length=1)):
    rows = await app.state.db.fetch("""
        SELECT p.id, p.content, p.hashtags, p.like_count,
               p.created_at, u.display_name, u.username
        FROM posts p JOIN users u ON u.id = p.user_id
        WHERE ($1 = ANY(p.hashtags) OR p.content ILIKE '%'||$1||'%')
          AND (p.is_hidden = false OR p.is_hidden IS NULL)
        ORDER BY p.created_at DESC LIMIT 30
    """, q)
    return {"success": True, "data": [dict(r) for r in rows]}


# ═══════════════════════════════════════════════════════
# PLAYGROUND — 해시태그 클러스터링 · 트렌딩 · 워드클라우드
# ═══════════════════════════════════════════════════════

@app.get("/hashtags/trending")
async def trending_hashtags(size: int = Query(20, ge=1, le=50)):
    """인기 해시태그 — 해시태그가 들어간 글들의 받은 XP × 시간 decay 합산.

    각 게시글의 engagement_xp(좋아요·댓글 받음)를 시간 감쇄와 함께 해시태그별로 SUM.
    빈도(cnt)는 tie-breaker.
    """
    rows = await app.state.db.fetch("""
        WITH post_scores AS (
            SELECT p.id, p.hashtags,
                   p.like_count AS like_count,
                   (SELECT COUNT(*) FROM post_comments pc WHERE pc.post_id = p.id) AS comment_count,
                   COALESCE((SELECT SUM(delta) FROM point_ledger
                             WHERE ref_id = p.id
                               AND reason IN ('like_received','comment_received')), 0)
                   * 24.0 / (24.0 + EXTRACT(EPOCH FROM (NOW() - p.created_at))/3600.0)
                   AS decayed_xp
            FROM posts p
            WHERE p.created_at > NOW() - INTERVAL '7 days'
              AND p.visibility = 'public'
              AND (p.is_hidden = false OR p.is_hidden IS NULL)
        )
        SELECT REPLACE(tag, '#', '') AS tag,
               COUNT(*) AS cnt,
               COALESCE(SUM(like_count), 0) AS total_likes,
               COALESCE(SUM(comment_count), 0) AS total_comments,
               COALESCE(SUM(decayed_xp), 0)::int AS score
        FROM post_scores p, unnest(p.hashtags) AS tag
        GROUP BY REPLACE(tag, '#', '')
        ORDER BY score DESC, cnt DESC
        LIMIT $1
    """, size)
    return {"success": True, "data": [dict(r) for r in rows]}


@app.get("/hashtags/suggest")
async def suggest_hashtags(q: str = Query("", min_length=0), size: int = Query(8, ge=1, le=20)):
    """게시글 작성 시 해시태그 자동 추천. q가 비어있으면 인기순 반환."""
    if q:
        rows = await app.state.db.fetch("""
            SELECT REPLACE(tag, '#', '') AS tag, COUNT(*) AS cnt
            FROM posts p, unnest(p.hashtags) AS tag
            WHERE REPLACE(tag, '#', '') ILIKE $1||'%' AND p.visibility = 'public'
            GROUP BY REPLACE(tag, '#', '') ORDER BY cnt DESC LIMIT $2
        """, q, size)
    else:
        rows = await app.state.db.fetch("""
            SELECT REPLACE(tag, '#', '') AS tag, COUNT(*) AS cnt
            FROM posts p, unnest(p.hashtags) AS tag
            WHERE p.visibility = 'public'
            GROUP BY REPLACE(tag, '#', '') ORDER BY cnt DESC LIMIT $1
        """, size)
    return {"success": True, "data": [dict(r) for r in rows]}


@app.get("/playground/{tag}")
async def playground(
    tag: str,
    sort: str = Query("hot", regex="^(hot|recent|likes)$"),
    page: int = Query(1, ge=1),
    size: int = Query(20, ge=1, le=50),
    current_user: dict = Depends(get_optional_user),
):
    """플레이그라운드 — 특정 해시태그 클러스터 피드 + 인기 사용자 + 위치 데이터."""
    db = app.state.db
    offset = (page - 1) * size
    # DB에 #접두사로 저장되어 있으므로 양쪽 모두 검색
    if not tag.startswith("#"):
        tag = "#" + tag

    # 정렬 기준
    order_map = {
        "hot": "ORDER BY (p.like_count + (SELECT COUNT(*) FROM post_comments pc2 WHERE pc2.post_id = p.id)) DESC, p.created_at DESC",
        "recent": "ORDER BY p.created_at DESC",
        "likes": "ORDER BY p.like_count DESC, p.created_at DESC",
    }
    order = order_map.get(sort, order_map["hot"])

    # 피드 조회
    base_select = """
        SELECT p.id, p.content, p.image_urls, p.hashtags, p.entity_tags,
               p.post_type, (SELECT COUNT(*) FROM post_likes pl WHERE pl.post_id = p.id) AS like_count, p.created_at, p.updated_at,
               p.visibility,
               u.display_name, u.avatar_url, u.username, u.id as uid, u.role AS author_role,
               gz.name AS zone_name,
               tr.trash_type, tr.severity, tr.status,
               ST_Y(p.location) AS lat, ST_X(p.location) AS lon,
               COALESCE(up.total_points, 0) AS total_xp,
               (SELECT COUNT(*) FROM post_comments pc WHERE pc.post_id = p.id) AS comment_count
        FROM posts p
        JOIN users u ON u.id = p.user_id
        LEFT JOIN geofence_zones gz ON gz.id = p.zone_id
        LEFT JOIN trash_reports tr ON tr.id = p.report_id
        LEFT JOIN user_points up ON up.user_id = p.user_id
        WHERE $1 = ANY(p.hashtags) AND p.visibility = 'public'
    """
    rows = await db.fetch(
        base_select + f" {order} LIMIT $2 OFFSET $3", tag, size, offset,
    )
    feeds = [_with_level(dict(r)) for r in rows]

    # 총 게시글 수
    total = await db.fetchval(
        "SELECT COUNT(*) FROM posts WHERE $1 = ANY(hashtags) AND visibility = 'public'", tag,
    )

    # 인기 사용자 (해당 태그 활동 기준)
    top_users = await db.fetch("""
        SELECT u.username, u.display_name, u.avatar_url,
               COUNT(p.id) AS post_count,
               SUM(p.like_count) AS total_likes,
               COALESCE(up.total_points, 0) AS total_xp
        FROM posts p
        JOIN users u ON u.id = p.user_id
        LEFT JOIN user_points up ON up.user_id = p.user_id
        WHERE $1 = ANY(p.hashtags) AND p.visibility = 'public'
        GROUP BY u.username, u.display_name, u.avatar_url, up.total_points
        ORDER BY total_likes DESC, post_count DESC
        LIMIT 5
    """, tag)

    # 위치 데이터 (핀맵용)
    pins = await db.fetch("""
        SELECT p.id, ST_Y(p.location) AS lat, ST_X(p.location) AS lon,
               p.like_count, u.display_name, p.content
        FROM posts p
        JOIN users u ON u.id = p.user_id
        WHERE $1 = ANY(p.hashtags)
          AND p.visibility = 'public'
          AND p.location IS NOT NULL
        LIMIT 50
    """, tag)

    return {
        "success": True,
        "data": {
            "tag": tag,
            "total": total,
            "feeds": feeds,
            "top_users": [dict(u) for u in top_users],
            "pins": [dict(p) for p in pins],
        },
    }


@app.get("/trending/feeds")
async def trending_feeds(
    size: int = Query(5, ge=1, le=20),
    post_type: str = Query("all", regex="^(all|general|report)$"),
):
    """급상승 피드 — engagement_xp × 시간 decay. post_type=general|report|all 필터 지원."""
    cache = app.state.cache
    cache_key = f"trending_feeds_{post_type}"
    cached = await cache.get(cache_key, size=size)
    if cached:
        return cached

    # post_type 필터 (all이면 필터 없음)
    type_filter = ""
    if post_type == "general":
        type_filter = "AND p.post_type = 'general'"
    elif post_type == "report":
        type_filter = "AND p.post_type = 'report'"

    rows = await app.state.db.fetch(f"""
        SELECT p.id, p.content, p.image_urls, p.hashtags, p.entity_tags,
               p.post_type, (SELECT COUNT(*) FROM post_likes pl WHERE pl.post_id = p.id) AS like_count, p.created_at, p.updated_at,
               p.visibility,
               u.display_name, u.avatar_url, u.username, u.id as uid, u.role AS author_role,
               gz.name AS zone_name,
               tr.trash_type, tr.severity, tr.status,
               COALESCE(up.total_points, 0) AS total_xp,
               (SELECT COUNT(*) FROM post_comments pc WHERE pc.post_id = p.id) AS comment_count,
               COALESCE((SELECT SUM(delta) FROM point_ledger
                         WHERE ref_id = p.id
                           AND reason IN ('like_received','comment_received')), 0) AS engagement_xp,
               (COALESCE((SELECT SUM(delta) FROM point_ledger
                          WHERE ref_id = p.id
                            AND reason IN ('like_received','comment_received')), 0)
                * 24.0 / (24.0 + EXTRACT(EPOCH FROM (NOW() - p.created_at))/3600.0)
               )::numeric(10,2) AS trend_score,
               EXTRACT(EPOCH FROM (NOW() - p.created_at))/3600.0 AS age_hours
        FROM posts p
        JOIN users u ON u.id = p.user_id
        LEFT JOIN geofence_zones gz ON gz.id = p.zone_id
        LEFT JOIN trash_reports tr ON tr.id = p.report_id
        LEFT JOIN user_points up ON up.user_id = p.user_id
        WHERE p.created_at > NOW() - INTERVAL '7 days'
          AND p.visibility = 'public'
          AND (p.is_hidden = false OR p.is_hidden IS NULL)
          {type_filter}
        ORDER BY trend_score DESC, p.created_at DESC
        LIMIT $1
    """, size)
    result = [_with_level(dict(r)) for r in rows]
    post_ids = [str(d["id"]) for d in result]
    if post_ids:
        all_reactions = await app.state.db.fetch("""
            SELECT post_id::text, reaction_type, COUNT(*) as cnt
            FROM post_likes WHERE post_id::text = ANY($1)
            GROUP BY post_id, reaction_type
        """, post_ids)
        react_map = {}
        for rx in all_reactions:
            react_map.setdefault(rx["post_id"], {})[rx["reaction_type"]] = rx["cnt"]
        for d in result:
            d["reactions"] = react_map.get(str(d["id"]), {})
    resp = {"success": True, "data": result}
    await cache.set(cache_key, resp, size=size)
    return resp


@app.get("/trending/users")
async def trending_users(size: int = Query(5, ge=1, le=20)):
    """급상승 랭커 — 최근 7일 XP 획득 기준. (적응형 캐싱)"""
    cache = app.state.cache
    cached = await cache.get("trending_users", size=size)
    if cached:
        return cached
    rows = await app.state.db.fetch("""
        SELECT u.username, u.display_name, u.avatar_url,
               COALESCE(up.total_points, 0) AS total_xp,
               COUNT(p.id) AS recent_posts,
               COALESCE(SUM(p.like_count), 0) AS recent_likes,
               COALESCE(rxp.recent_xp, 0) AS recent_xp
        FROM users u
        LEFT JOIN user_points up ON up.user_id = u.id
        LEFT JOIN posts p ON p.user_id = u.id AND p.created_at > NOW() - INTERVAL '7 days'
        LEFT JOIN (
            SELECT user_id, SUM(delta) AS recent_xp
            FROM point_ledger
            WHERE created_at > NOW() - INTERVAL '7 days'
            GROUP BY user_id
        ) rxp ON rxp.user_id = u.id
        WHERE u.role != 'admin'
        GROUP BY u.username, u.display_name, u.avatar_url, up.total_points, rxp.recent_xp
        HAVING COALESCE(rxp.recent_xp, 0) > 0 OR COUNT(p.id) > 0
        ORDER BY COALESCE(rxp.recent_xp, 0) DESC, COUNT(p.id) DESC
        LIMIT $1
    """, size)
    result = []
    for r in rows:
        d = dict(r)
        xp = d["total_xp"]
        lv, lv_title, _ = get_level(xp)
        d["level"] = lv
        d["level_title"] = lv_title
        result.append(d)
    # 배치: 사용자별 최근 리액션 요약 (N+1 → 1 쿼리)
    usernames = [d["username"] for d in result]
    if usernames:
        user_rx = await app.state.db.fetch("""
            SELECT u.username, pl.reaction_type, COUNT(*) as cnt
            FROM post_likes pl
            JOIN posts p ON p.id = pl.post_id
            JOIN users u ON u.id = p.user_id
            WHERE u.username = ANY($1) AND p.created_at > NOW() - INTERVAL '7 days'
            GROUP BY u.username, pl.reaction_type
        """, usernames)
        urx_map = {}
        for rx in user_rx:
            urx_map.setdefault(rx["username"], {})[rx["reaction_type"]] = rx["cnt"]
        for d in result:
            d["recent_reactions"] = urx_map.get(d["username"], {})
    resp = {"success": True, "data": result}
    await cache.set("trending_users", resp, size=size)
    return resp


# ── 알림 시스템 ──

@app.get("/notifications")
async def get_notifications(size: int = Query(20, ge=1, le=50), current_user: dict = Depends(get_current_user)):
    """로그인 사용자의 읽지 않은 알림 조회."""
    db = app.state.db
    rows = await db.fetch("""
        SELECT id::text, type, title, body, ref_post_id::text, ref_comment_id::text,
               actor_username, actor_display_name, is_read, created_at
        FROM notifications
        WHERE user_id = $1::uuid
        ORDER BY created_at DESC
        LIMIT $2
    """, current_user["user_id"], size)
    unread = await db.fetchval(
        "SELECT COUNT(*) FROM notifications WHERE user_id=$1::uuid AND is_read=FALSE",
        current_user["user_id"]
    )
    return {"success": True, "data": {"notifications": [dict(r) for r in rows], "unread_count": unread}}


@app.post("/notifications/read")
async def mark_notifications_read(payload: dict = Body(...), current_user: dict = Depends(get_current_user)):
    """알림 읽음 처리. ids가 비어있으면 전체 읽음 처리."""
    db = app.state.db
    ids = payload.get("ids", [])
    if ids:
        await db.execute("""
            UPDATE notifications SET is_read = TRUE
            WHERE user_id = $1::uuid AND id::text = ANY($2)
        """, current_user["user_id"], ids)
    else:
        await db.execute(
            "UPDATE notifications SET is_read = TRUE WHERE user_id = $1::uuid AND is_read = FALSE",
            current_user["user_id"]
        )
    return {"success": True}


# ── 시스템 알림 (퀘스트 초대 등) ──
ALLOWED_SYSTEM_TYPES = {"quest_invite", "zone_alert", "system"}


@app.post("/notifications/system")
async def send_system_notification(payload: dict = Body(...), current_user: dict = Depends(get_current_user)):
    """시스템/대량 알림 전송 — 퀘스트 초대 등 사람-대-사람 대화가 아닌 메시지.

    payload: { type, title, body, ref_id?, recipients: [username, ...] }
    """
    db = app.state.db
    ntype = (payload.get("type") or "").strip()
    title = (payload.get("title") or "").strip()[:200]
    body = (payload.get("body") or "").strip()[:1000]
    ref_id = payload.get("ref_id")  # 퀘스트/게시글 UUID
    recipients = payload.get("recipients") or []

    if ntype not in ALLOWED_SYSTEM_TYPES:
        return {"success": False, "error": "허용되지 않은 알림 타입"}
    if not title:
        return {"success": False, "error": "title 필수"}
    if not recipients or not isinstance(recipients, list):
        return {"success": False, "error": "recipients 필요"}
    if len(recipients) > 50:
        return {"success": False, "error": "한 번에 최대 50명까지 전송 가능"}

    # 발신자 표시 정보
    actor = await db.fetchrow(
        "SELECT username, display_name FROM users WHERE id = $1::uuid",
        current_user["user_id"],
    )
    actor_username = actor["username"] if actor else None
    actor_display = actor["display_name"] if actor else None

    # 수신자 ID 조회 (자기 자신 제외)
    rows = await db.fetch(
        "SELECT id::text, username FROM users WHERE username = ANY($1) AND id != $2::uuid",
        recipients, current_user["user_id"],
    )

    sent = 0
    for r in rows:
        try:
            await db.execute(
                """
                INSERT INTO notifications
                    (user_id, type, title, body, ref_post_id, actor_username, actor_display_name)
                VALUES ($1::uuid, $2, $3, $4, $5, $6, $7)
                """,
                r["id"], ntype, title, body,
                ref_id if ref_id else None,
                actor_username, actor_display,
            )
            sent += 1
        except Exception as e:
            logger.warning(f"시스템 알림 전송 실패 user={r['username']}: {e}")

    return {"success": True, "data": {"sent": sent, "total": len(recipients)}}


@app.get("/my/engagement-summary")
async def engagement_summary(current_user: dict = Depends(get_current_user)):
    """내 게시글에 달린 댓글/리액션 요약 — 네비 뱃지용."""
    db = app.state.db
    uid = current_user["user_id"]
    unread = await db.fetchval(
        "SELECT COUNT(*) FROM notifications WHERE user_id=$1::uuid AND is_read=FALSE", uid
    )
    reactions = await db.fetchval("""
        SELECT COUNT(*) FROM post_likes pl
        JOIN posts p ON pl.post_id = p.id
        WHERE p.user_id = $1::uuid
    """, uid)
    return {"success": True, "data": {"unread_comments": int(unread or 0), "total_reactions": int(reactions or 0)}}


@app.post("/posts/upload")
async def upload_files(
    files: List[UploadFile] = File(...),
    current_user: dict = Depends(get_current_user),
):
    """파일 업로드 (이미지/동영상) → MinIO에 저장 후 URL 반환."""
    import uuid as _uuid
    import io

    minio = app.state.minio
    if not minio:
        return {"success": False, "error": "파일 스토리지가 사용 불가합니다"}

    bucket = "plogging-images"
    try:
        if not minio.bucket_exists(bucket):
            minio.make_bucket(bucket)
    except Exception:
        pass

    # 외부 접근용 URL: 서버 외부 IP 사용 (모바일 등에서 접근 가능)
    import socket
    external_host = os.environ.get("MINIO_PUBLIC_URL", None)
    if not external_host:
        server_ip = os.environ.get("SERVER_HOST", "")
        if not server_ip or server_ip == "0.0.0.0":
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                s.connect(("8.8.8.8", 80))
                server_ip = s.getsockname()[0]
                s.close()
            except Exception:
                server_ip = "localhost"
        minio_port = os.environ.get("MINIO_ENDPOINT", "localhost:9001").split(":")[-1]
        external_host = f"http://{server_ip}:{minio_port}"

    urls = []
    for f in files[:5]:  # 최대 5개
        ext = os.path.splitext(f.filename or "img.jpg")[1] or ".jpg"
        key = f"posts/{current_user['user_id']}/{_uuid.uuid4().hex}{ext}"
        data = await f.read()
        minio.put_object(bucket, key, io.BytesIO(data), len(data), content_type=f.content_type or "image/jpeg")
        urls.append(f"{external_host}/{bucket}/{key}")
    return {"success": True, "data": {"urls": urls}}


@app.get("/posts/{post_id}/tags")
async def get_post_tags(post_id: str):
    """게시글의 해시태그 + AI 엔티티 태그 조회 (지식 그래프용)."""
    row = await app.state.db.fetchrow("""
        SELECT p.hashtags, p.entity_tags, u.display_name, u.username
        FROM posts p JOIN users u ON u.id = p.user_id
        WHERE p.id = $1::uuid
    """, post_id)
    if not row:
        return {"success": False, "error": "게시글을 찾을 수 없습니다"}
    return {
        "success": True,
        "data": {
            "post_id": post_id,
            "hashtags": [t.lstrip("#") for t in (row["hashtags"] or [])],
            "entity_tags": row["entity_tags"] or [],
            "author": row["display_name"] or row["username"],
        },
    }


@app.post("/posts/create")
async def create_post(payload: dict, current_user: dict = Depends(get_current_user)):
    """게시글 작성."""
    import uuid
    db = app.state.db
    content = payload.get("content", "").strip()
    if not content:
        return {"success": False, "error": "내용을 입력하세요"}

    # 비속어 필터링
    filter_result = await check_content(db, current_user["user_id"], content)
    if filter_result["blocked"]:
        return {"success": False, "error": filter_result["message"], "content_warning": True,
                "violation_count": filter_result["violation_count"]}

    hashtags = [("#" + t.lstrip("#")) for t in payload.get("hashtags", [])]
    entity_tags = payload.get("entity_tags", [])
    image_urls = payload.get("image_urls", [])
    visibility = payload.get("visibility", "public")
    post_type = payload.get("post_type", "general")  # general or report
    if post_type not in ("general", "report"):
        post_type = "general"
    lat = payload.get("lat")
    lon = payload.get("lon")
    zone_id = payload.get("zone_id")

    if visibility not in ("public", "friends", "private"):
        visibility = "public"

    location = None
    if lat and lon:
        location = f"POINT({lon} {lat})"

    post_id = str(uuid.uuid4())
    try:
        if location:
            await db.execute("""
                INSERT INTO posts (id, user_id, content, image_urls, hashtags, entity_tags, visibility, location, zone_id, created_at)
                VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, ST_SetSRID(ST_GeomFromText($8),4326), $9::uuid, NOW())
            """, post_id, current_user["user_id"], content, image_urls, hashtags, entity_tags, visibility, location, zone_id)
        else:
            # zone_id가 None이면 uuid 캐스팅 제외
            if zone_id:
                await db.execute("""
                    INSERT INTO posts (id, user_id, content, image_urls, hashtags, entity_tags, visibility, zone_id, created_at)
                    VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8::uuid, NOW())
                """, post_id, current_user["user_id"], content, image_urls, hashtags, entity_tags, visibility, zone_id)
            else:
                await db.execute("""
                    INSERT INTO posts (id, user_id, content, image_urls, hashtags, entity_tags, visibility, post_type, created_at)
                    VALUES ($1::uuid, $2::uuid, $3, $4, $5, $6, $7, $8, NOW())
                """, post_id, current_user["user_id"], content, image_urls, hashtags, entity_tags, visibility, post_type)
    except Exception as e:
        logger.error(f"게시글 작성 DB 오류: {e}")
        return {"success": False, "error": f"게시글 저장 실패: {str(e)[:200]}"}

    # XP 보상 — 게시글 작성 +20 XP (통합 grant_xp)
    xp_earned = 20
    total, dup = await grant_xp(db, app.state.redis, current_user["user_id"], xp_earned, "post_create",
                                ref_id=post_id, kafka=app.state.kafka, logger=logger)

    # 캐시 무효화 + 활동 추적
    await app.state.cache.invalidate("trending_feeds", prefix=True)
    await app.state.cache.invalidate("trending_users")
    await app.state.cache.track_activity("trending_feeds")
    await app.state.cache.track_activity("trending_users")

    resp = {"success": True, "data": {"post_id": post_id, "xp_earned": 20}}
    if filter_result.get("warning"):
        resp["data"]["content_warning"] = filter_result["message"]
        resp["data"]["violation_count"] = filter_result["violation_count"]
    return resp


# ═══════════════════════════════════════════════════════════
# DM (Direct Messages)
# ═══════════════════════════════════════════════════════════

@app.post("/messages/send")
async def send_dm(payload: dict, current_user=Depends(get_current_user)):
    """DM 전송."""
    db = app.state.db
    recipient_username = payload.get("recipient_username", "").strip()
    content = payload.get("content", "").strip()

    if not recipient_username or not content:
        return {"success": False, "error": "수신자와 내용을 입력하세요"}
    if len(content) > 1000:
        return {"success": False, "error": "메시지는 1000자 이내로 입력하세요"}

    recipient = await db.fetchrow(
        "SELECT id, username, display_name FROM users WHERE username = $1",
        recipient_username,
    )
    if not recipient:
        return {"success": False, "error": "존재하지 않는 사용자입니다"}
    if str(recipient["id"]) == current_user["user_id"]:
        return {"success": False, "error": "자신에게 메시지를 보낼 수 없습니다"}

    try:
        msg_id = await db.fetchval("""
            INSERT INTO direct_messages (sender_id, recipient_id, content)
            VALUES ($1::uuid, $2::uuid, $3)
            RETURNING id
        """, current_user["user_id"], str(recipient["id"]), content)
    except Exception as e:
        logger.error(f"DM 전송 DB 오류: {e}")
        return {"success": False, "error": f"메시지 저장 실패: {str(e)[:100]}"}

    return {
        "success": True,
        "data": {
            "message_id": str(msg_id),
            "recipient": recipient_username,
        },
    }


@app.get("/messages/conversations")
async def get_conversations(current_user=Depends(get_current_user)):
    """내 대화 목록. 시스템 메시지(`[QUEST_INVITE`, `[ZONE_FIX`)는 알림으로 분리되어 제외."""
    db = app.state.db

    uid = current_user["user_id"]
    rows = await db.fetch("""
        WITH filtered AS (
            SELECT id, sender_id, recipient_id, content, is_read, created_at,
                   recipient_id AS partner_id FROM direct_messages
            WHERE sender_id = $1::uuid
              AND content NOT LIKE '[QUEST_INVITE%' AND content NOT LIKE '[ZONE_FIX%'
            UNION ALL
            SELECT id, sender_id, recipient_id, content, is_read, created_at,
                   sender_id AS partner_id FROM direct_messages
            WHERE recipient_id = $1::uuid
              AND content NOT LIKE '[QUEST_INVITE%' AND content NOT LIKE '[ZONE_FIX%'
        ),
        latest AS (
            SELECT DISTINCT ON (partner_id) *
            FROM filtered
            ORDER BY partner_id, created_at DESC
        )
        SELECT l.*, u.username AS partner_username, u.display_name AS partner_display_name,
               u.avatar_url AS partner_avatar_url,
               (SELECT COUNT(*) FROM direct_messages
                WHERE sender_id = l.partner_id AND recipient_id = $1::uuid AND is_read = FALSE
                  AND content NOT LIKE '[QUEST_INVITE%' AND content NOT LIKE '[ZONE_FIX%') AS unread_count
        FROM latest l
        JOIN users u ON u.id = l.partner_id
        ORDER BY l.created_at DESC
    """, uid)

    return {
        "success": True,
        "data": [
            {
                "partner_username": r["partner_username"],
                "partner_display_name": r["partner_display_name"],
                "partner_avatar_url": r["partner_avatar_url"],
                "last_message": r["content"][:80],
                "last_message_at": r["created_at"].isoformat(),
                "unread_count": r["unread_count"],
                "is_mine": str(r["sender_id"]) == uid,
            }
            for r in rows
        ],
    }


@app.get("/messages/with/{username}")
async def get_messages_with(username: str, current_user=Depends(get_current_user)):
    """특정 사용자와의 대화 내역."""
    db = app.state.db

    partner = await db.fetchrow("SELECT id FROM users WHERE username = $1", username)
    if not partner:
        return {"success": False, "error": "존재하지 않는 사용자"}

    uid = current_user["user_id"]
    pid = str(partner["id"])

    # 읽음 처리
    await db.execute("""
        UPDATE direct_messages SET is_read = TRUE
        WHERE sender_id = $1::uuid AND recipient_id = $2::uuid AND is_read = FALSE
    """, pid, uid)

    rows = await db.fetch("""
        SELECT dm.*, u.username AS sender_username, u.display_name AS sender_display_name
        FROM direct_messages dm
        JOIN users u ON u.id = dm.sender_id
        WHERE ((dm.sender_id = $1::uuid AND dm.recipient_id = $2::uuid)
            OR (dm.sender_id = $2::uuid AND dm.recipient_id = $1::uuid))
          AND dm.content NOT LIKE '[QUEST_INVITE%'
          AND dm.content NOT LIKE '[ZONE_FIX%'
        ORDER BY dm.created_at ASC
        LIMIT 100
    """, uid, pid)

    return {
        "success": True,
        "data": [
            {
                "id": str(r["id"]),
                "sender_username": r["sender_username"],
                "sender_display_name": r["sender_display_name"],
                "content": r["content"],
                "is_read": r["is_read"],
                "is_mine": str(r["sender_id"]) == uid,
                "created_at": r["created_at"].isoformat(),
            }
            for r in rows
        ],
    }


@app.get("/messages/unread-count")
async def get_unread_count(current_user=Depends(get_current_user)):
    """읽지 않은 DM 수."""
    db = app.state.db
    count = await db.fetchval("""
        SELECT COUNT(*) FROM direct_messages
        WHERE recipient_id = $1::uuid AND is_read = FALSE
          AND content NOT LIKE '[QUEST_INVITE%'
          AND content NOT LIKE '[ZONE_FIX%'
    """, current_user["user_id"])
    return {"success": True, "data": {"count": count or 0}}


@app.delete("/messages/{message_id}")
async def delete_message(message_id: str, current_user=Depends(get_current_user)):
    """DM 삭제 (자신이 보낸 메시지만)."""
    db = app.state.db
    deleted = await db.fetchval("""
        DELETE FROM direct_messages
        WHERE id = $1::uuid AND sender_id = $2::uuid
        RETURNING id
    """, message_id, current_user["user_id"])

    if not deleted:
        return {"success": False, "error": "삭제 권한이 없거나 메시지를 찾을 수 없습니다"}
    return {"success": True}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("SNS_PORT", 8501))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
