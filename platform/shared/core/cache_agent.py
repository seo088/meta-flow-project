"""
적응형 캐시 에이전트 — 활동률 기반 동적 TTL + 이벤트 기반 무효화.

사용법:
    from core.cache_agent import CacheAgent
    cache = CacheAgent(redis_client)

    # 캐시 읽기/쓰기 (TTL 자동 계산)
    data = await cache.get("trending_feeds", size=5)
    if not data:
        data = await compute_trending()
        await cache.set("trending_feeds", data, size=5)

    # 이벤트 발생 시 무효화
    await cache.invalidate("trending_feeds")  # 새 게시글 생성 시
    await cache.invalidate("trending_users")  # XP 변경 시
    await cache.invalidate_all()              # 전체 무효화
"""

import json
import time
from typing import Optional


# TTL 정책: (활동 임계값, TTL 초)
# 최근 1시간 활동 수 기준으로 TTL 결정
TTL_TIERS = [
    (50, 15),    # 50건 이상/시간 → 15초 (매우 활발)
    (20, 30),    # 20~49건/시간  → 30초 (활발)
    (5,  60),    # 5~19건/시간   → 60초 (보통)
    (1,  120),   # 1~4건/시간    → 120초 (한산)
    (0,  300),   # 0건/시간      → 5분 (비활성)
]

# 카테고리별 활동 측정 쿼리
ACTIVITY_KEYS = {
    "trending_feeds": "activity:posts",
    "trending_users": "activity:xp",
    "ranking_global": "activity:xp",
}


class CacheAgent:
    """활동률 기반 적응형 캐시 관리자."""

    def __init__(self, redis, logger=None):
        self.redis = redis
        self.log = logger

    async def get(self, category: str, **params) -> Optional[dict]:
        """캐시에서 데이터를 읽는다. 없으면 None 반환."""
        key = self._key(category, params)
        try:
            cached = await self.redis.get(key)
            if cached:
                return json.loads(cached)
        except Exception:
            pass
        return None

    async def set(self, category: str, data, **params):
        """데이터를 캐시에 저장. TTL은 활동률 기반으로 자동 결정."""
        key = self._key(category, params)
        ttl = await self._compute_ttl(category)
        try:
            await self.redis.set(key, json.dumps(data, default=str), ex=ttl)
            if self.log:
                self.log.debug(f"[CacheAgent] SET {key} ttl={ttl}s")
        except Exception:
            pass

    async def invalidate(self, category: str, prefix: bool = False):
        """특정 카테고리의 모든 캐시를 무효화.

        prefix=True 시 `cache:{category}*` 와일드카드 (서브 카테고리 포함 일괄 무효화).
        예: invalidate("trending_feeds", prefix=True) →
            cache:trending_feeds_all:*, cache:trending_feeds_general:* 등 모두 매치
        """
        pattern = f"cache:{category}*" if prefix else f"cache:{category}:*"
        try:
            keys = await self.redis.keys(pattern)
            if keys:
                await self.redis.delete(*keys)
                if self.log:
                    self.log.info(f"[CacheAgent] INVALIDATE {pattern} ({len(keys)} keys)")
        except Exception:
            pass

    async def invalidate_all(self):
        """trending + ranking 캐시 전부 무효화."""
        for cat in ("trending_feeds", "trending_users", "ranking_global"):
            await self.invalidate(cat)

    async def track_activity(self, category: str):
        """활동 발생을 기록한다. (게시글 생성, XP 변경 등)"""
        activity_key = ACTIVITY_KEYS.get(category)
        if not activity_key:
            return
        try:
            pipe = self.redis.pipeline()
            pipe.incr(activity_key)
            pipe.expire(activity_key, 3600)  # 1시간 윈도우
            await pipe.execute()
        except Exception:
            pass

    async def _compute_ttl(self, category: str) -> int:
        """최근 1시간 활동 수를 기반으로 최적 TTL 계산."""
        activity_key = ACTIVITY_KEYS.get(category, "activity:posts")
        try:
            count = await self.redis.get(activity_key)
            count = int(count) if count else 0
        except Exception:
            count = 0

        for threshold, ttl in TTL_TIERS:
            if count >= threshold:
                return ttl
        return 300  # 기본 5분

    def _key(self, category: str, params: dict) -> str:
        """캐시 키 생성."""
        suffix = ":".join(f"{v}" for v in params.values()) if params else "default"
        return f"cache:{category}:{suffix}"
