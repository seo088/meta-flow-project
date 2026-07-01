#!/usr/bin/env python3
"""
데이터 활용 API 플랫폼 마이그레이션 (Phase D).

신규 테이블:
  - api_key_requests : 로그인 사용자의 데이터 활용 신청 (관리자 승인 대기)
  - api_keys         : 승인 시 발급되는 API Key (해시 저장, 평문은 발급 1회만 노출)
  - api_key_usage    : 경량 사용 감사 로그

추가형(CREATE TABLE IF NOT EXISTS)이라 기존 데이터에 영향 없음.
실행: conda run -n meta-flow python scripts/migrate_api_platform.py
"""
import os
import sys
import asyncio


def _dsn() -> str:
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("DATABASE_URL 환경변수 필요 (source .env)", file=sys.stderr)
        sys.exit(1)
    return dsn


DDL = """
CREATE TABLE IF NOT EXISTS api_key_requests (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    purpose         TEXT NOT NULL,
    organization    VARCHAR(200),
    contact         VARCHAR(200),
    requested_scopes TEXT[] NOT NULL DEFAULT ARRAY['read:reports']::text[],
    status          VARCHAR(20) NOT NULL DEFAULT 'pending',  -- pending|approved|rejected
    review_note     TEXT,
    reviewed_by     UUID REFERENCES users(id),
    reviewed_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_api_req_status ON api_key_requests(status);
CREATE INDEX IF NOT EXISTS idx_api_req_user   ON api_key_requests(user_id);

CREATE TABLE IF NOT EXISTS api_keys (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id     UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    request_id  UUID REFERENCES api_key_requests(id) ON DELETE SET NULL,
    key_prefix  VARCHAR(20) NOT NULL,           -- 식별용 접두(평문 앞부분)
    key_hash    VARCHAR(128) NOT NULL UNIQUE,   -- sha256 hex
    name        VARCHAR(120),
    scopes      TEXT[] NOT NULL DEFAULT ARRAY['read:reports']::text[],
    rate_limit  INT NOT NULL DEFAULT 1000,      -- 시간당 요청 한도
    is_active   BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_used_at TIMESTAMPTZ,
    expires_at  TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS idx_api_keys_hash ON api_keys(key_hash);
CREATE INDEX IF NOT EXISTS idx_api_keys_user ON api_keys(user_id);

CREATE TABLE IF NOT EXISTS api_key_usage (
    id          BIGSERIAL PRIMARY KEY,
    api_key_id  UUID NOT NULL REFERENCES api_keys(id) ON DELETE CASCADE,
    ts          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    path        VARCHAR(200),
    status_code INT
);
CREATE INDEX IF NOT EXISTS idx_api_usage_key ON api_key_usage(api_key_id, ts DESC);
"""


async def main():
    import asyncpg
    conn = await asyncpg.connect(_dsn())
    try:
        await conn.execute(DDL)
        tables = await conn.fetch(
            "SELECT tablename FROM pg_tables WHERE tablename = ANY($1::text[])",
            ["api_key_requests", "api_keys", "api_key_usage"])
        print("생성 확인:", sorted(r["tablename"] for r in tables))
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
