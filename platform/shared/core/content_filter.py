"""
콘텐츠 필터링 — 비속어/욕설 탐지 및 제재 시스템.
모든 서비스에서 import하여 게시글·댓글 텍스트를 검사합니다.

사용법:
    from core.content_filter import check_content, apply_penalty

    result = check_content(db, user_id, text)
    if result["blocked"]:
        return {"success": False, "warning": result["message"]}
"""
import re
from datetime import datetime

# ── 비속어 사전 (한국어 + 영어) ─────────────────────────────
# 레벨 1: 경고만 (일반 비속어)
PROFANITY_L1 = [
    # 한국어 욕설
    "시발", "씨발", "ㅅㅂ", "ㅆㅂ", "시bal", "씨bal",
    "병신", "ㅂㅅ", "븅신", "byungsin",
    "개새끼", "개색끼", "개세끼", "ㄱㅅㄲ",
    "지랄", "ㅈㄹ", "짓거리",
    "미친", "ㅁㅊ", "미쳤",
    "좆", "ㅈ같", "존나", "ㅈㄴ", "졸라",
    "꺼져", "닥쳐", "엿먹어",
    "년", "놈", "새끼",
    "게이", "창녀", "걸레",
    "바보", "멍청이", "등신",
    # 영어 욕설
    "fuck", "shit", "damn", "bitch", "asshole", "bastard",
    "dick", "pussy", "crap", "idiot", "stupid",
    # 변형 우회 패턴
    "시ㅂ", "ㅅ1발", "씌발", "쉬발", "시8", "ㅂ5", "ㅂ ㅅ",
]

# 레벨 2: 즉시 제재 (심각한 비속어/혐오 표현)
PROFANITY_L2 = [
    "자살", "죽어", "죽여", "살인",
    "테러", "폭탄", "총기",
    "인종차별", "혐오",
]

# 컴파일된 정규식 (초성 분리 대응 + 공백 무시)
def _build_pattern(word):
    """단어 사이 공백/특수문자 삽입 우회 대응."""
    escaped = re.escape(word)
    # 각 글자 사이에 선택적 공백/특수문자 매칭
    flexible = r'[\s.,_\-*]*'.join(escaped)
    return flexible


_L1_PATTERNS = [re.compile(_build_pattern(w), re.IGNORECASE) for w in PROFANITY_L1]
_L2_PATTERNS = [re.compile(_build_pattern(w), re.IGNORECASE) for w in PROFANITY_L2]


def detect_profanity(text: str) -> dict:
    """텍스트에서 비속어를 탐지합니다.

    Returns:
        {"found": bool, "level": 0|1|2, "words": list[str]}
    """
    if not text:
        return {"found": False, "level": 0, "words": []}

    found_words = []
    max_level = 0

    for pattern in _L2_PATTERNS:
        if pattern.search(text):
            found_words.append(pattern.pattern.replace(r'[\s.,_\-*]*', ''))
            max_level = 2

    for pattern in _L1_PATTERNS:
        if pattern.search(text):
            word = pattern.pattern.replace(r'[\s.,_\-*]*', '').replace('\\', '')
            if word not in found_words:
                found_words.append(word)
            if max_level < 1:
                max_level = 1

    return {"found": bool(found_words), "level": max_level, "words": found_words[:3]}


async def check_content(db, user_id: str, text: str) -> dict:
    """콘텐츠를 검사하고 사용자의 위반 이력을 확인합니다.

    Returns:
        {
            "blocked": bool,       # True면 게시 차단
            "warning": bool,       # True면 경고 표시 (게시는 허용)
            "message": str,        # 사용자에게 보여줄 메시지
            "violation_count": int, # 누적 위반 횟수
            "penalty_applied": str, # 적용된 제재 (없으면 None)
        }
    """
    result = detect_profanity(text)

    if not result["found"]:
        return {
            "blocked": False, "warning": False,
            "message": "", "violation_count": 0,
            "penalty_applied": None,
        }

    # 위반 이력 테이블 확인/생성
    await db.execute("""
        CREATE TABLE IF NOT EXISTS content_violations (
            id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
            user_id UUID REFERENCES users(id) ON DELETE CASCADE,
            content_snippet TEXT,
            detected_words TEXT[],
            violation_level INTEGER DEFAULT 1,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
    """)

    # 최근 30일 위반 횟수 조회
    violation_count = await db.fetchval("""
        SELECT COUNT(*) FROM content_violations
        WHERE user_id = $1::uuid AND created_at > NOW() - INTERVAL '30 days'
    """, user_id) or 0

    # 위반 기록 저장
    snippet = text[:100] if len(text) > 100 else text
    await db.execute("""
        INSERT INTO content_violations (user_id, content_snippet, detected_words, violation_level)
        VALUES ($1::uuid, $2, $3, $4)
    """, user_id, snippet, result["words"], result["level"])

    violation_count += 1  # 현재 건 포함
    penalty = None

    if result["level"] >= 2:
        # 레벨2 (심각): 즉시 차단
        return {
            "blocked": True, "warning": True,
            "message": "⛔ 부적절한 표현이 포함되어 있어 게시할 수 없습니다.",
            "violation_count": violation_count,
            "penalty_applied": "blocked",
        }

    if violation_count == 1:
        # 1차: 경고만 (게시는 허용)
        return {
            "blocked": False, "warning": True,
            "message": f"⚠️ 부적절한 표현이 감지되었습니다. 건전한 표현을 사용해주세요.\n(감지된 표현이 포함되어 있습니다)",
            "violation_count": violation_count,
            "penalty_applied": "warning",
        }

    if violation_count == 2:
        # 2차: XP 감점 (-50)
        penalty = "xp_penalty"
        try:
            await db.execute("""
                INSERT INTO point_ledger (id, user_id, delta, reason, created_at)
                VALUES (gen_random_uuid(), $1::uuid, -50, 'content_violation_penalty', NOW())
            """, user_id)
            await db.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY user_points")
        except Exception:
            pass

        return {
            "blocked": False, "warning": True,
            "message": "🚨 2차 경고: 부적절한 표현이 반복 감지되었습니다.\nXP -50이 차감되었습니다. 계속 위반 시 활동이 제한됩니다.",
            "violation_count": violation_count,
            "penalty_applied": penalty,
        }

    # 3차 이상: 게시 차단 + XP 감점
    penalty = "blocked_and_xp"
    try:
        xp_deduct = -100 * (violation_count - 2)
        await db.execute("""
            INSERT INTO point_ledger (id, user_id, delta, reason, created_at)
            VALUES (gen_random_uuid(), $1::uuid, $2, 'content_violation_penalty', NOW())
        """, user_id, xp_deduct)
        await db.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY user_points")
    except Exception:
        pass

    return {
        "blocked": True, "warning": True,
        "message": f"⛔ {violation_count}차 위반: 게시가 차단되었습니다.\nXP {xp_deduct}이 차감되었습니다. 관리자에게 문의하세요.",
        "violation_count": violation_count,
        "penalty_applied": penalty,
    }


async def get_user_violations(db, user_id: str) -> list:
    """사용자의 위반 이력을 조회합니다."""
    try:
        rows = await db.fetch("""
            SELECT violation_level, detected_words, created_at
            FROM content_violations
            WHERE user_id = $1::uuid
            ORDER BY created_at DESC LIMIT 10
        """, user_id)
        return [dict(r) for r in rows]
    except Exception:
        return []
