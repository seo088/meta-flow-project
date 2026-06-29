"""
Gateway 서비스 — 정적 파일 서빙 + API 리버스 프록시
포트: GATEWAY_PORT 환경변수 (기본 8500)

- /           → apps/web/index.html (일반 사용자 웹 UI)
- /admin      → apps/admin/index.html (관리자 대시보드)
- /api/sns/*  → SNS Service (:8501)
- /api/game/* → Game Service (:8502)
- /api/gis/*  → GIS Service (:8503)
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "shared"))

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, FileResponse, JSONResponse
import httpx

PORT = int(os.environ.get("GATEWAY_PORT", 8500))
HOST = os.environ.get("SERVER_HOST", "0.0.0.0")

APPS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "apps")

SERVICE_MAP = {
    "sns": os.environ.get("SNS_SERVICE_URL", f"http://127.0.0.1:8501"),
    "game": os.environ.get("GAME_SERVICE_URL", f"http://127.0.0.1:8502"),
    "gis": os.environ.get("GIS_SERVICE_URL", f"http://127.0.0.1:8503"),
    "ai": os.environ.get("AI_SERVICE_URL", f"http://127.0.0.1:8504"),
    "data": os.environ.get("DATA_SERVICE_URL", f"http://127.0.0.1:8505"),
    "mcp": os.environ.get("MCP_SERVER_URL", f"http://127.0.0.1:8510").replace("/sse", ""),
    "ws": f"http://127.0.0.1:{os.environ.get('WS_PORT', 8520)}",
    "drone": f"http://127.0.0.1:{os.environ.get('DRONE_AGENT_PORT', 8531)}",
    "data-agent": f"http://127.0.0.1:{os.environ.get('DATA_AGENT_PORT', 8532)}",
    "mobility": f"http://127.0.0.1:{os.environ.get('MOBILITY_AGENT_PORT', 8533)}",
    "quest": f"http://127.0.0.1:{os.environ.get('QUEST_AGENT_PORT', 8540)}",
    "policy": f"http://127.0.0.1:{os.environ.get('POLICY_PORT', 8541)}",
}

app = FastAPI(title="Meta SW Plogging Gateway", version="1.0.0")
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)

_http: httpx.AsyncClient = None


@app.on_event("startup")
async def startup():
    global _http
    _http = httpx.AsyncClient(timeout=30.0)


@app.on_event("shutdown")
async def shutdown():
    if _http:
        await _http.aclose()


@app.get("/health")
async def health():
    return {"status": "ok", "service": "gateway", "port": PORT}


# ── 정적 파일 서빙 ──


@app.get("/", response_class=HTMLResponse)
async def web_index():
    path = os.path.join(APPS_DIR, "web", "index.html")
    if os.path.exists(path):
        from starlette.responses import Response
        with open(path, "r") as f:
            content = f.read()
        return Response(content, media_type="text/html", headers={
            "Cache-Control": "no-cache, no-store, must-revalidate",
            "Pragma": "no-cache",
            "Expires": "0",
        })
    return HTMLResponse("<h1>Meta SW Plogging</h1><p>apps/web/index.html not found</p>", status_code=404)


@app.get("/admin", response_class=HTMLResponse)
async def admin_index():
    path = os.path.join(APPS_DIR, "admin", "index.html")
    if os.path.exists(path):
        return FileResponse(path, media_type="text/html")
    return HTMLResponse("<h1>Admin</h1><p>apps/admin/index.html not found</p>", status_code=404)


# ── 아바타 정적 파일 ──


@app.get("/avatars/{filename}")
async def serve_avatar(filename: str):
    path = os.path.join(APPS_DIR, "web", "avatars", filename)
    if os.path.exists(path):
        ext = os.path.splitext(filename)[1].lower()
        mt = {".png": "image/png", ".ico": "image/x-icon"}.get(ext, "application/octet-stream")
        return FileResponse(path, media_type=mt)
    return JSONResponse({"error": "Avatar not found"}, status_code=404)


# ── API 리버스 프록시 ──


@app.api_route("/api/{service}/{path:path}", methods=["GET", "POST", "PUT", "DELETE", "PATCH"])
async def proxy(service: str, path: str, request: Request):
    """백엔드 서비스로 요청을 프록시합니다."""
    target_base = SERVICE_MAP.get(service)
    if not target_base:
        return JSONResponse({"error": f"Unknown service: {service}"}, status_code=404)

    url = f"{target_base}/{path}"
    if request.query_params:
        url += f"?{request.query_params}"

    try:
        body = await request.body()
        headers = {
            k: v for k, v in request.headers.items()
            if k.lower() not in ("host", "content-length", "transfer-encoding")
        }
        # 파일 업로드 시 타임아웃 연장
        timeout = 60.0 if "upload" in path else 30.0
        resp = await _http.request(
            method=request.method,
            url=url,
            content=body if body else None,
            headers=headers,
            timeout=timeout,
        )
        ct = resp.headers.get("content-type", "")
        if ct.startswith("application/json"):
            return JSONResponse(content=resp.json(), status_code=resp.status_code,
                                headers={"Cache-Control": "no-cache, no-store", "Pragma": "no-cache"})
        else:
            # JSON이 아닌 응답 (에러 HTML 등)
            try:
                return JSONResponse(content=resp.json(), status_code=resp.status_code)
            except Exception:
                return JSONResponse(
                    content={"error": resp.text[:500] if resp.text else "Backend error", "status": resp.status_code},
                    status_code=resp.status_code,
                )
    except httpx.TimeoutException:
        return JSONResponse({"error": "Backend service timeout"}, status_code=504)
    except httpx.ConnectError:
        return JSONResponse({"error": f"Service '{service}' is not available"}, status_code=503)
    except Exception as e:
        return JSONResponse({"error": str(e)}, status_code=502)


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=PORT)
