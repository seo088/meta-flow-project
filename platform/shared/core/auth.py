"""JWT 인증 미들웨어 — 회원가입, 로그인, 토큰 발급."""
import os
from datetime import datetime, timedelta, timezone
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials

security = HTTPBearer(auto_error=False)

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
