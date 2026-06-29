"""
공유 DB 연결 팩토리 — 모든 서비스에서 동일한 패턴 사용.
Redis 연결도 REDIS_PORT 환경변수를 반드시 참조.
"""
import os
import asyncpg
import redis.asyncio as aioredis


async def create_db_pool(min_size: int = 5, max_size: int = 20) -> asyncpg.Pool:
    """PostgreSQL 연결 풀 생성. DATABASE_URL 또는 개별 환경변수 사용."""
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        host = os.environ.get("POSTGRES_HOST", "localhost")
        port = os.environ.get("POSTGRES_PORT", "5433")
        user = os.environ.get("POSTGRES_USER", "plogging")
        pw = os.environ.get("POSTGRES_PASSWORD", "")
        db = os.environ.get("POSTGRES_DB", "plogging_db")
        dsn = f"postgresql://{user}:{pw}@{host}:{port}/{db}"
    return await asyncpg.create_pool(dsn=dsn, min_size=min_size, max_size=max_size)


def create_redis(decode_responses: bool = True) -> aioredis.Redis:
    """Redis 연결 생성. REDIS_PORT 환경변수를 반드시 사용."""
    host = os.environ.get("REDIS_HOST", "localhost")
    port = os.environ.get("REDIS_PORT", "6380")
    password = os.environ.get("REDIS_PASSWORD", "")
    return aioredis.from_url(
        f"redis://:{password}@{host}:{port}",
        decode_responses=decode_responses,
    )
