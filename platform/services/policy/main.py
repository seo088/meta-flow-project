"""
Policy Agent — 콘텐츠 모니터링 및 자동 제재 시스템.
포트: POLICY_PORT 환경변수 (기본 8541)

기능:
- 기존 게시글/댓글에서 비속어 스캔 (배치)
- 사용자 위반 이력 조회
- 위반 통계 대시보드
- 자동 숨김 처리 (3회 이상 위반 사용자의 게시글)
"""
import os
import sys
import asyncio

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "shared"))

from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, Query
from fastapi.middleware.cors import CORSMiddleware

from core.auth import get_current_user
from core.db import create_db_pool
from core.logger import get_logger
from core.content_filter import detect_profanity

logger = get_logger("policy-agent")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = await create_db_pool(min_size=2, max_size=10)

    # content_violations 테이블 확인
    await app.state.db.execute("""
        CREATE TABLE IF NOT EXISTS content_violations (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            user_id UUID REFERENCES users(id) ON DELETE CASCADE,
            content_snippet TEXT,
            detected_words TEXT[],
            violation_level INTEGER DEFAULT 1,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)
    logger.info("Policy Agent 시작 — 콘텐츠 모니터링 활성화")
    yield
    await app.state.db.close()


app = FastAPI(title="Policy Agent", version="1.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)


@app.get("/health")
async def health():
    return {"status": "ok", "service": "policy-agent"}


@app.get("/violations/stats")
async def violation_stats(u: dict = Depends(get_current_user)):
    """위반 통계 (관리자용)."""
    if u["role"] != "admin":
        return {"success": False, "error": "관리자 권한 필요"}
    db = app.state.db

    total = await db.fetchval("SELECT COUNT(*) FROM content_violations") or 0
    today = await db.fetchval(
        "SELECT COUNT(*) FROM content_violations WHERE created_at > CURRENT_DATE"
    ) or 0
    unique_users = await db.fetchval(
        "SELECT COUNT(DISTINCT user_id) FROM content_violations"
    ) or 0
    repeat_offenders = await db.fetchval("""
        SELECT COUNT(*) FROM (
            SELECT user_id FROM content_violations
            GROUP BY user_id HAVING COUNT(*) >= 2
        ) sub
    """) or 0

    return {
        "success": True,
        "data": {
            "total_violations": total,
            "today_violations": today,
            "unique_violators": unique_users,
            "repeat_offenders": repeat_offenders,
        },
    }


@app.get("/violations/users")
async def violation_users(u: dict = Depends(get_current_user)):
    """위반 사용자 목록 (관리자용)."""
    if u["role"] != "admin":
        return {"success": False, "error": "관리자 권한 필요"}
    db = app.state.db

    rows = await db.fetch("""
        SELECT cv.user_id::text, u.username, u.display_name,
               COUNT(*) AS violation_count,
               MAX(cv.violation_level) AS max_level,
               MAX(cv.created_at) AS last_violation
        FROM content_violations cv
        JOIN users u ON u.id = cv.user_id
        GROUP BY cv.user_id, u.username, u.display_name
        ORDER BY COUNT(*) DESC
        LIMIT 50
    """)

    return {
        "success": True,
        "data": [
            {
                "user_id": r["user_id"],
                "username": r["username"],
                "display_name": r["display_name"],
                "violation_count": r["violation_count"],
                "max_level": r["max_level"],
                "last_violation": r["last_violation"].isoformat(),
            }
            for r in rows
        ],
    }


@app.get("/violations/user/{username}")
async def user_violations(username: str, u: dict = Depends(get_current_user)):
    """특정 사용자 위반 이력."""
    if u["role"] != "admin" and u["username"] != username:
        return {"success": False, "error": "권한 없음"}
    db = app.state.db

    user = await db.fetchrow("SELECT id FROM users WHERE username=$1", username)
    if not user:
        return {"success": False, "error": "사용자 없음"}

    rows = await db.fetch("""
        SELECT content_snippet, detected_words, violation_level, created_at
        FROM content_violations WHERE user_id = $1::uuid
        ORDER BY created_at DESC LIMIT 20
    """, str(user["id"]))

    return {
        "success": True,
        "data": [
            {
                "snippet": r["content_snippet"],
                "words": r["detected_words"],
                "level": r["violation_level"],
                "created_at": r["created_at"].isoformat(),
            }
            for r in rows
        ],
    }


@app.post("/scan/posts")
async def scan_existing_posts(u: dict = Depends(get_current_user)):
    """기존 게시글 일괄 스캔 (관리자 전용)."""
    if u["role"] != "admin":
        return {"success": False, "error": "관리자 권한 필요"}
    db = app.state.db

    # 최근 7일 게시글 스캔
    posts = await db.fetch("""
        SELECT p.id, p.user_id::text, p.content
        FROM posts p
        WHERE p.created_at > NOW() - INTERVAL '7 days' AND p.is_hidden = false
    """)

    flagged = 0
    for post in posts:
        result = detect_profanity(post["content"] or "")
        if result["found"]:
            flagged += 1
            # 자동 숨김 처리
            await db.execute(
                "UPDATE posts SET is_hidden=true, hidden_at=NOW() WHERE id=$1::uuid",
                post["id"],
            )
            # 위반 기록
            await db.execute("""
                INSERT INTO content_violations (user_id, content_snippet, detected_words, violation_level)
                VALUES ($1::uuid, $2, $3, $4)
            """, post["user_id"], (post["content"] or "")[:100], result["words"], result["level"])

    # 퀘스트 게시글도 스캔
    quest_posts = await db.fetch("""
        SELECT qp.id, qp.user_id::text, qp.content
        FROM quest_posts qp
        WHERE qp.created_at > NOW() - INTERVAL '7 days'
          AND (qp.is_hidden = false OR qp.is_hidden IS NULL)
    """)

    for qp in quest_posts:
        result = detect_profanity(qp["content"] or "")
        if result["found"]:
            flagged += 1
            await db.execute(
                "UPDATE quest_posts SET is_hidden=true, hidden_at=NOW() WHERE id=$1::uuid",
                qp["id"],
            )
            await db.execute("""
                INSERT INTO content_violations (user_id, content_snippet, detected_words, violation_level)
                VALUES ($1::uuid, $2, $3, $4)
            """, qp["user_id"], (qp["content"] or "")[:100], result["words"], result["level"])

    return {
        "success": True,
        "data": {
            "scanned_posts": len(posts),
            "scanned_quest_posts": len(quest_posts),
            "flagged": flagged,
        },
    }


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("POLICY_PORT", 8541))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
