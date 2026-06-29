"""
AI Service — YOLO 탐지 · RL 경로 · 인증 검증.
AI_MODE=dummy 시 더미 모델 사용 → 실 모델 교체 시 .env만 변경.
포트: AI_PORT 환경변수 (기본 8504)
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "shared"))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from core.logger import get_logger
from models.dummy_yolo import yolo_model, verifier, rl_agent, entity_tagger

logger = get_logger("ai-service")

app = FastAPI(title="AI Service", version="1.0.0")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])


@app.get("/health")
async def health():
    return {"status": "ok", "mode": os.environ.get("AI_MODE", "dummy")}


@app.post("/predictions/yolo_plogging_v8")
async def predict_yolo(payload: dict):
    """YOLO 탐지 엔드포인트 — TorchServe 호환 인터페이스."""
    image_urls = payload.get("image_urls", [])
    result = yolo_model.predict(image_urls)
    logger.info(f"YOLO: {result['class']} ({result['confidence']:.3f})")
    return result


@app.post("/predictions/yolo_plogging_v8/batch")
async def predict_yolo_batch(payload: dict):
    """드론 영상 프레임 배치 탐지."""
    frames = payload.get("frames", [])
    result = yolo_model.predict_batch(frames)
    logger.info(f"YOLO batch: {len(result['detections'])} detections from {len(frames)} frames")
    return result


@app.post("/predictions/cleanup_verify_v1")
async def verify_cleanup(payload: dict):
    """청소 인증 (SSIM + ResNet)."""
    result = verifier.verify(payload.get("before_url", ""), payload.get("after_url", ""))
    logger.info(f"Verify: score={result['ssim_score']:.3f} verified={result['verified']}")
    return result


@app.post("/predictions/rl_route_agent")
async def optimize_route(payload: dict):
    """RL 경로 최적화."""
    result = rl_agent.optimize(
        payload.get("start", {}),
        payload.get("targets", []),
        payload.get("agent_type", "drone"),
    )
    logger.info(f"RL: {len(result['waypoints'])} waypoints, {result['total_distance_km']}km")
    return result


@app.post("/predictions/entity_tags")
async def predict_entity_tags(payload: dict):
    """이미지 장면 엔티티 태그 추출 — OpenCV/YOLO v8 더미."""
    image_urls = payload.get("image_urls", [])
    result = entity_tagger.predict_entities(image_urls)
    logger.info(f"Entity tags: {len(result['entities'])} entities detected")
    return result


# ── 이모지 추천 (Gemini API + 키워드 폴백) ──
import re
import json as _json
import httpx

_EMOJI_FALLBACK = [
    (re.compile(r"(쓰레기|플로깅|청소|봉투|치움)"), ["🧹", "🗑️", "♻️", "🌱", "💪"]),
    (re.compile(r"(공원|숲|나무|자연|꽃|풀)"), ["🌳", "🌸", "🌿", "🍃", "🌷"]),
    (re.compile(r"(바다|강|해변|물|호수)"), ["🌊", "🏖️", "🐚", "💧", "⛵"]),
    (re.compile(r"(러닝|달리기|운동|걷기|산책|뛰)"), ["🏃", "👟", "💨", "💪", "🥇"]),
    (re.compile(r"(친구|함께|같이|모임|팀)"), ["👫", "🤝", "💚", "🎉", "✨"]),
    (re.compile(r"(맛있|식사|먹|배고)"), ["🍴", "🍱", "😋", "🍵", "🍰"]),
    (re.compile(r"(고마|감사|덕분|최고)"), ["🙏", "💖", "✨", "🥰", "👍"]),
    (re.compile(r"(힘들|피곤|지쳐|어렵)"), ["😮‍💨", "💦", "🥵", "💪", "🍵"]),
    (re.compile(r"(좋아|행복|기쁘|즐거)"), ["😊", "💕", "🥰", "✨", "🎉"]),
    (re.compile(r"(슬프|아쉬|미안|속상)"), ["😢", "💧", "💔", "🥺", "😔"]),
    (re.compile(r"(놀라|와|대박|헐)"), ["😲", "🤯", "✨", "🔥", "👀"]),
    (re.compile(r"(퀘스트|챌린지|미션|도전)"), ["🎯", "🏆", "⚔️", "🔥", "✨"]),
    (re.compile(r"(인증|완료|성공|클리어)"), ["✅", "🏆", "🎉", "💯", "🥳"]),
    (re.compile(r"(아침|점심|저녁|밤)"), ["☀️", "🌤️", "🌆", "🌙", "🌃"]),
    (re.compile(r"(비|눈|날씨|하늘|구름)"), ["☔", "❄️", "☁️", "🌈", "☀️"]),
]
_EMOJI_DEFAULT = ["🌱", "♻️", "💚", "✨", "🙌"]


def _fallback_emoji_suggest(text: str, k: int = 5) -> list:
    """키워드 기반 이모지 추천 (Gemini 키 없을 때)."""
    text = (text or "").lower()
    picks: list = []
    for pat, em in _EMOJI_FALLBACK:
        if pat.search(text):
            for e in em:
                if e not in picks:
                    picks.append(e)
                if len(picks) >= k:
                    return picks
    if not picks:
        return _EMOJI_DEFAULT[:k]
    for e in _EMOJI_DEFAULT:
        if e not in picks:
            picks.append(e)
        if len(picks) >= k:
            break
    return picks[:k]


GEMINI_MODEL = os.environ.get("GEMINI_MODEL", "gemini-flash-latest")
# 이모지 한 글자(VS16/조합 포함)를 잡는 정규식. CJK·라틴은 제외.
_EMOJI_CHAR_RE = re.compile(
    r"[\U0001F300-\U0001FAFF\U00002600-\U000027BF\U0001F600-\U0001F64F"
    r"\U0001F680-\U0001F6FF\U0001F900-\U0001F9FF\U0001FA70-\U0001FAFF"
    r"✀-➿☀-⛿]"
    r"(?:️)?"
    r"(?:‍[\U0001F300-\U0001FAFF\U00002600-\U000027BF✀-➿☀-⛿])*"
)


def _extract_emojis(text: str, k: int) -> list:
    """텍스트에서 이모지만 골라 중복 제거."""
    seen = []
    for m in _EMOJI_CHAR_RE.finditer(text or ""):
        e = m.group(0)
        if e not in seen:
            seen.append(e)
        if len(seen) >= k:
            break
    return seen


async def _gemini_emoji_suggest(text: str, api_key: str, k: int = 5) -> list:
    """Gemini Flash로 이모지 추천. 실패 시 빈 리스트."""
    prompt = (
        "다음 한국어 문장에 어울리는 이모지를 정확히 "
        f"{k}개 추천해. 다른 설명 없이 이모지만 공백으로 구분해 한 줄로 출력.\n\n"
        f"문장: {text[:300]}"
    )
    url = (
        "https://generativelanguage.googleapis.com/v1beta/models/"
        f"{GEMINI_MODEL}:generateContent?key={api_key}"
    )
    body = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {
            "temperature": 0.4,
            "maxOutputTokens": 256,
            # 2.5 계열의 thinking 모드 비활성화 — 토큰 절약
            "thinkingConfig": {"thinkingBudget": 0},
        },
    }
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            r = await client.post(url, json=body)
            if r.status_code != 200:
                logger.warning(f"Gemini API status={r.status_code}: {r.text[:200]}")
                return []
            data = r.json()
            cand = (data.get("candidates") or [{}])[0]
            parts = (cand.get("content") or {}).get("parts") or []
            txt = "".join(p.get("text", "") for p in parts).strip()
            # 코드 펜스 제거
            txt = re.sub(r"```(?:json)?", "", txt).strip()
            # 이모지 직접 추출 (가장 안전)
            emojis = _extract_emojis(txt, k)
            if emojis:
                return emojis
            # JSON 배열 형태도 폴백으로 시도
            try:
                arr = _json.loads(txt)
                if isinstance(arr, list):
                    return [str(x) for x in arr if isinstance(x, str)][:k]
            except Exception:
                pass
    except Exception as e:
        logger.warning(f"Gemini emoji 추천 실패: {e}")
    return []


@app.post("/predictions/emoji_suggest")
async def emoji_suggest(payload: dict):
    """문맥에 맞는 이모지 추천. Gemini 키 있으면 Gemini, 없으면 키워드 폴백."""
    text = (payload.get("text") or "").strip()
    k = max(3, min(int(payload.get("count", 5)), 8))
    if not text:
        return {"success": True, "data": {"emojis": _EMOJI_DEFAULT[:k], "source": "default"}}

    api_key = os.environ.get("GEMINI_API_KEY", "").strip()
    used = "fallback"
    emojis: list = []
    if api_key:
        emojis = await _gemini_emoji_suggest(text, api_key, k)
        if emojis:
            used = "gemini"
    if not emojis:
        emojis = _fallback_emoji_suggest(text, k)
    logger.info(f"Emoji suggest ({used}): {''.join(emojis)} for '{text[:30]}...'")
    return {"success": True, "data": {"emojis": emojis, "source": used}}


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("AI_PORT", 8504))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
