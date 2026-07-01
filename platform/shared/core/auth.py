"""JWT 인증 미들웨어 — 회원가입, 로그인, 토큰 발급. + 외부 제공용 API Key 인증."""
import os
import hashlib
import secrets
from datetime import datetime, timedelta, timezone
from fastapi import Depends, HTTPException, status, Header, Request
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials, APIKeyHeader

security = HTTPBearer(auto_error=False)
# 외부 제공 API 보안 스킴 — OpenAPI(Swagger/ReDoc)에 X-API-Key 인증으로 문서화
api_key_scheme = APIKeyHeader(name="X-API-Key", auto_error=False)

DUMMY_USER = {
    "user_id": "00000000-0000-0000-0000-000000000001",
    "username": "testuser",
    "role": "user",
    "display_name": "테스트유저",
}

JWT_SECRET = os.environ.get("JWT_SECRET_KEY", "CHANGE-ME-set-JWT_SECRET_KEY-env")
JWT_ALGO = os.environ.get("JWT_ALGORITHM", "HS256")
JWT_EXPIRE = int(os.environ.get("JWT_ACCESS_EXPIRE_MINUTES", 1440))


def create_access_token(user_id: str, username: str, role: str = "user") -> str:
    from jose import jwt
    exp = datetime.now(timezone.utc) + timedelta(minutes=JWT_EXPIRE)
    return jwt.encode(
        {"sub": user_id, "username": username, "role": role, "exp": exp},
        JWT_SECRET, algorithm=JWT_ALGO,
    )


def get_current_user(
    cred: HTTPAuthorizationCredentials = Depends(security),
) -> dict:
    if os.environ.get("AI_MODE") == "dummy" and not cred:
        return DUMMY_USER

    if not cred:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Token required")

    try:
        from jose import jwt
        payload = jwt.decode(cred.credentials, JWT_SECRET, algorithms=[JWT_ALGO])
        uid = payload.get("sub")
        if not uid:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid token")
        return {
            "user_id": uid,
            "username": payload.get("username", ""),
            "role": payload.get("role", "user"),
            "display_name": payload.get("display_name", ""),
        }
    except Exception:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Could not validate credentials")


def get_optional_user(
    cred: HTTPAuthorizationCredentials = Depends(security),
) -> dict | None:
    """토큰이 없으면 None, 있으면 검증 후 반환 (피드 등 비로그인도 가능)."""
    if not cred:
        if os.environ.get("AI_MODE") == "dummy":
            return DUMMY_USER
        return None
    try:
        from jose import jwt
        payload = jwt.decode(cred.credentials, JWT_SECRET, algorithms=[JWT_ALGO])
        return {
            "user_id": payload.get("sub"),
            "username": payload.get("username", ""),
            "role": payload.get("role", "user"),
        }
    except Exception:
        return None


def require_role(role: str):
    def _check(u: dict = Depends(get_current_user)):
        if u["role"] != role:
            raise HTTPException(status.HTTP_403_FORBIDDEN, "Insufficient permissions")
        return u
    return _check


# ═══════════════════════════════════════════════════════════
# 외부 데이터 제공 API Key 인증 (신청→관리자 승인→발급)
# 키는 평문을 저장하지 않고 sha256 해시만 보관, 평문은 발급 1회만 노출.
# ═══════════════════════════════════════════════════════════
API_KEY_ENV = os.environ.get("ENVIRONMENT", "dev")[:4]


def hash_api_key(plain: str) -> str:
    return hashlib.sha256(plain.encode()).hexdigest()


def generate_api_key():
    """(평문, 식별 접두, 해시) 반환. 평문은 호출자에게 1회만 노출."""
    plain = f"mfk_{API_KEY_ENV}_{secrets.token_hex(24)}"
    return plain, plain[:16], hash_api_key(plain)


async def get_api_key_principal(
    request: Request,
    x_api_key: str = Depends(api_key_scheme),
) -> dict:
    """X-API-Key 검증 + 만료/활성/rate-limit 확인 + 사용 로그. data 서비스 app.state.db 사용."""
    if not x_api_key:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "API Key required (X-API-Key header)")
    db = request.app.state.db
    row = await db.fetchrow("SELECT * FROM api_keys WHERE key_hash=$1", hash_api_key(x_api_key))
    if not row or not row["is_active"]:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Invalid or inactive API Key")
    if row["expires_at"] and row["expires_at"] < datetime.now(timezone.utc):
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "API Key expired")
    used = await db.fetchval(
        "SELECT count(*) FROM api_key_usage WHERE api_key_id=$1 AND ts > NOW() - INTERVAL '1 hour'",
        row["id"])
    if used >= row["rate_limit"]:
        raise HTTPException(status.HTTP_429_TOO_MANY_REQUESTS,
                            f"Rate limit exceeded ({row['rate_limit']}/hour)")
    await db.execute("UPDATE api_keys SET last_used_at=NOW() WHERE id=$1", row["id"])
    await db.execute("INSERT INTO api_key_usage(api_key_id, path) VALUES($1,$2)",
                     row["id"], str(request.url.path)[:200])
    return {"key_id": str(row["id"]), "user_id": str(row["user_id"]),
            "scopes": list(row["scopes"]), "rate_limit": row["rate_limit"]}


def require_scope(scope: str):
    """제공 API 스코프 가드. 예: Depends(require_scope('read:reports'))."""
    async def _check(principal: dict = Depends(get_api_key_principal)) -> dict:
        if scope not in principal["scopes"]:
            raise HTTPException(status.HTTP_403_FORBIDDEN, f"Scope '{scope}' required")
        return principal
    return _check
