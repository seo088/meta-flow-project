"""
Data Service — Elasticsearch 집계·시계열 통계·공공데이터 API.
포트: DATA_PORT 환경변수 (기본 8505)
"""
import os
import sys
from datetime import datetime

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "shared"))

from contextlib import asynccontextmanager
from fastapi import FastAPI, Query, Depends, HTTPException, Request, Body
from fastapi.middleware.cors import CORSMiddleware
from typing import Optional

from core.db import create_db_pool
from core.logger import get_logger
from core.auth import (
    get_current_user, generate_api_key, get_api_key_principal, require_scope,
)

VALID_SCOPES = ["read:reports", "read:datasets", "read:stats"]

logger = get_logger("data-service")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = await create_db_pool(min_size=3, max_size=10)
    try:
        from elasticsearch import AsyncElasticsearch
        es_host = os.environ.get("ES_HOST", "localhost")
        es_port = os.environ.get("ES_PORT", "9201")
        app.state.es = AsyncElasticsearch(f"http://{es_host}:{es_port}")
        app.state.es_connected = True
    except Exception as e:
        logger.warning(f"Elasticsearch not available: {e}")
        app.state.es = None
        app.state.es_connected = False
    yield
    await app.state.db.close()
    if app.state.es_connected and app.state.es:
        await app.state.es.close()


_BASE = os.environ.get("PUBLIC_BASE_URL", "http://<서버주소>:8500/api/data")
API_DESCRIPTION = f"""
군산대 **메타 SW 플로깅** 플랫폼의 제보·외부 데이터셋 **공개 제공 API**입니다.
외부 기관·연구자는 아래 절차로 API Key를 발급받아 데이터를 활용할 수 있습니다.

**Base URL**: `{_BASE}`  ·  **라이선스**: `CC-BY-4.0`

---

### 1. 상태 점검 (정상 여부 확인)
인증 없이 헬스 체크로 서비스 정상 여부를 확인합니다.
```bash
curl {_BASE}/health
# → {{"status":"ok","service":"data","es_connected":true}}
```
HTTP `200` + `"status":"ok"` 이면 정상입니다.

### 2. API Key 발급받기 (신청 → 관리자 승인 → 발급)
1. 플랫폼에 **로그인** 후 프로필 화면의 **"데이터 활용 API 신청"** 폼에서 목적·소속·스코프를 제출합니다.
   (또는 `POST {_BASE}/data-access/requests`, JWT 필요)
2. **관리자 승인** 시 API Key 평문이 **단 1회** 발급됩니다. (분실 시 재발급)
3. 발급된 키는 프로필 화면 또는 `GET {_BASE}/data-access/requests/me` 에서 접두/상태 확인.

> 키는 서버에 **해시(sha256)로만 저장**되어 평문은 복구 불가합니다. 안전하게 보관하세요.

### 3. 인증 · 호출 방법
모든 제공 API(`/api/v1/*`)는 요청 헤더에 발급키를 넣습니다.
```
X-API-Key: mfk_xxx_xxxxxxxxxxxxxxxx
```
- **스코프**: `read:reports`(제보), `read:datasets`(외부 데이터셋), `read:stats`(통계)
- **rate-limit**: 키별 시간당 한도(기본 1000). 초과 시 `429`.
- **증분 수집**: `updated_since=ISO8601` 로 변경분만.

### 4. 외부 요청 샘플
**cURL — 제보 GeoJSON(증분)**
```bash
curl -H "X-API-Key: 발급키" \\
  "{_BASE}/api/v1/reports?format=geojson&updated_since=2026-06-01T00:00:00"
```
**Python (requests)**
```python
import requests
r = requests.get("{_BASE}/api/v1/reports",
    headers={{"X-API-Key": "발급키"}},
    params={{"format": "json", "zone": "KU_CAMPUS", "size": 500}})
print(r.status_code, r.json()["count"])
```
**JavaScript (fetch)**
```javascript
const r = await fetch("{_BASE}/api/v1/datasets/2026-06-01-di-full?format=geojson",
  {{ headers: {{ "X-API-Key": "발급키" }} }});
const geo = await r.json();
```
**응답 코드**: `200` 성공 · `401` 키 없음/무효 · `403` 스코프 부족 · `429` 한도 초과
""".strip()

tags_metadata = [
    {"name": "상태", "description": "서비스 정상 여부 점검 (인증 불필요)"},
    {"name": "통계·공개데이터", "description": "집계·시계열·GeoJSON 내보내기 (기존 공개)"},
    {"name": "API Key 신청·승인", "description": "로그인(JWT) 후 신청, 관리자 승인 시 키 발급"},
    {"name": "제공 API (v1)", "description": "발급 API Key(X-API-Key) + 스코프로 호출하는 외부 제공 엔드포인트"},
]

app = FastAPI(
    title="Meta SW 플로깅 — 데이터 제공 API",
    description=API_DESCRIPTION,
    version="1.0.0",
    lifespan=lifespan,
    openapi_tags=tags_metadata,
    root_path=os.environ.get("ROOT_PATH", ""),  # 게이트웨이 경유 시 /api/data (Swagger 자산 경로 정상화)
)
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"]
)


@app.get("/health", tags=["상태"], summary="서비스 상태 점검 (인증 불필요)")
async def health():
    return {"status": "ok", "service": "data",
            "es_connected": getattr(app.state, "es_connected", False)}


@app.get("/statistics/zones")
async def zone_stats():
    """구역별 집계 — ES 사용 가능 시 ES, 아니면 PostgreSQL 폴백."""
    if app.state.es_connected:
        try:
            body = {
                "size": 0,
                "aggs": {
                    "by_zone": {
                        "terms": {"field": "zone_key", "size": 10},
                        "aggs": {
                            "by_severity": {"terms": {"field": "severity"}},
                            "by_status": {"terms": {"field": "status"}},
                        },
                    }
                },
            }
            res = await app.state.es.search(index="plogging-reports", body=body)
            buckets = res["aggregations"]["by_zone"]["buckets"]
            return {"success": True, "source": "elasticsearch", "data": [
                {
                    "zone_key": b["key"],
                    "total": b["doc_count"],
                    "severity": {s["key"]: s["doc_count"] for s in b["by_severity"]["buckets"]},
                    "status": {s["key"]: s["doc_count"] for s in b["by_status"]["buckets"]},
                }
                for b in buckets
            ]}
        except Exception as e:
            logger.warning(f"ES query failed, using PG fallback: {e}")

    # PostgreSQL 폴백
    rows = await app.state.db.fetch("""
        SELECT gz.zone_key,
            COUNT(tr.id) AS total,
            COUNT(tr.id) FILTER (WHERE tr.severity='boss') AS boss,
            COUNT(tr.id) FILTER (WHERE tr.severity='high') AS high,
            COUNT(tr.id) FILTER (WHERE tr.status='pending') AS pending,
            COUNT(tr.id) FILTER (WHERE tr.status='completed') AS completed
        FROM geofence_zones gz
        LEFT JOIN trash_reports tr ON tr.zone_id = gz.id
        WHERE gz.is_active = TRUE
        GROUP BY gz.zone_key
    """)
    return {"success": True, "source": "postgresql", "data": [dict(r) for r in rows]}


@app.get("/statistics/timeseries")
async def timeseries(
    zone_key: Optional[str] = Query(None),
    interval: str = Query("1d", pattern="^(1h|1d|1w)$"),
    days: int = Query(30, ge=1, le=365),
):
    """시계열 집계 — ES 사용 가능 시 ES, 아니면 PG 폴백."""
    if app.state.es_connected:
        try:
            must = [{"range": {"created_at": {"gte": f"now-{days}d/d"}}}]
            if zone_key:
                must.append({"term": {"zone_key": zone_key}})
            body = {
                "size": 0,
                "query": {"bool": {"must": must}},
                "aggs": {
                    "over_time": {
                        "date_histogram": {"field": "created_at", "calendar_interval": interval},
                        "aggs": {
                            "by_type": {"terms": {"field": "trash_type"}},
                            "completed": {"filter": {"term": {"status": "completed"}}},
                        },
                    }
                },
            }
            res = await app.state.es.search(index="plogging-reports", body=body)
            return {"success": True, "data": res["aggregations"]["over_time"]["buckets"]}
        except Exception as e:
            logger.warning(f"ES timeseries failed: {e}")

    # PG 폴백: 일별 집계
    if zone_key:
        rows = await app.state.db.fetch("""
            SELECT DATE(tr.created_at) AS date, COUNT(*) AS count,
                   COUNT(*) FILTER (WHERE tr.status='completed') AS completed
            FROM trash_reports tr
            JOIN geofence_zones gz ON gz.id = tr.zone_id
            WHERE gz.zone_key = $1 AND tr.created_at > NOW() - ($2 || ' days')::interval
            GROUP BY DATE(tr.created_at) ORDER BY date
        """, zone_key, str(days))
    else:
        rows = await app.state.db.fetch("""
            SELECT DATE(created_at) AS date, COUNT(*) AS count,
                   COUNT(*) FILTER (WHERE status='completed') AS completed
            FROM trash_reports
            WHERE created_at > NOW() - ($1 || ' days')::interval
            GROUP BY DATE(created_at) ORDER BY date
        """, str(days))
    return {"success": True, "data": [dict(r) for r in rows]}


@app.get("/opendata/list")
async def opendata_list():
    rows = await app.state.db.fetch("""
        SELECT export_type, file_url, record_count, date_from, date_to, created_at
        FROM opendata_exports ORDER BY created_at DESC LIMIT 50
    """)
    return {"success": True, "data": [dict(r) for r in rows]}


@app.get("/opendata/export/geojson")
async def direct_geojson(
    zone_key: Optional[str] = Query(None),
    limit: int = Query(1000, le=5000),
):
    """실시간 GeoJSON 스트리밍 (소량 요청용)."""
    if zone_key:
        rows = await app.state.db.fetch("""
            SELECT tr.id, tr.trash_type, tr.severity, tr.status,
                   ST_X(tr.location::geometry) AS lon,
                   ST_Y(tr.location::geometry) AS lat,
                   tr.created_at::text, gz.zone_key
            FROM trash_reports tr
            LEFT JOIN geofence_zones gz ON gz.id = tr.zone_id
            WHERE tr.source != 'unity_sim' AND gz.zone_key = $1
            ORDER BY tr.created_at DESC LIMIT $2
        """, zone_key, limit)
    else:
        rows = await app.state.db.fetch("""
            SELECT tr.id, tr.trash_type, tr.severity, tr.status,
                   ST_X(tr.location::geometry) AS lon,
                   ST_Y(tr.location::geometry) AS lat,
                   tr.created_at::text, gz.zone_key
            FROM trash_reports tr
            LEFT JOIN geofence_zones gz ON gz.id = tr.zone_id
            WHERE tr.source != 'unity_sim'
            ORDER BY tr.created_at DESC LIMIT $1
        """, limit)

    return {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [r["lon"], r["lat"]]},
                "properties": {k: v for k, v in dict(r).items() if k not in ("lat", "lon")},
            }
            for r in rows
        ],
        "metadata": {
            "count": len(rows),
            "license": "CC-BY-4.0",
            "source": "meta-plogging-gunsan",
            "generated_at": datetime.utcnow().isoformat(),
        },
    }


# ═══════════════════════════════════════════════════════════
# 데이터 활용 신청 → 관리자 승인 → API Key 발급
# ═══════════════════════════════════════════════════════════

@app.post("/data-access/requests", tags=["API Key 신청·승인"], summary="데이터 활용 신청 (로그인 필요)")
async def create_access_request(payload: dict = Body(...), user: dict = Depends(get_current_user)):
    """로그인 사용자의 데이터 활용 신청."""
    purpose = (payload.get("purpose") or "").strip()
    if not purpose:
        raise HTTPException(400, "purpose(활용 목적)는 필수입니다")
    scopes = [s for s in (payload.get("scopes") or ["read:reports"]) if s in VALID_SCOPES] or ["read:reports"]
    row = await app.state.db.fetchrow("""
        INSERT INTO api_key_requests (user_id, purpose, organization, contact, requested_scopes)
        VALUES ($1,$2,$3,$4,$5::text[]) RETURNING id, status, created_at
    """, user["user_id"], purpose, payload.get("organization"), payload.get("contact"), scopes)
    return {"success": True, "data": dict(row)}


@app.get("/data-access/requests/me", tags=["API Key 신청·승인"], summary="내 신청·발급 키 조회")
async def my_access_requests(user: dict = Depends(get_current_user)):
    """내 신청 내역 + 발급된 키(평문 제외, 접두만)."""
    db = app.state.db
    reqs = await db.fetch("""
        SELECT id, purpose, organization, requested_scopes, status, review_note, created_at, reviewed_at
        FROM api_key_requests WHERE user_id=$1 ORDER BY created_at DESC
    """, user["user_id"])
    keys = await db.fetch("""
        SELECT id, key_prefix, name, scopes, rate_limit, is_active, created_at, last_used_at, expires_at
        FROM api_keys WHERE user_id=$1 ORDER BY created_at DESC
    """, user["user_id"])
    return {"success": True, "data": {"requests": [dict(r) for r in reqs], "keys": [dict(k) for k in keys]}}


def _require_admin(user: dict):
    if user.get("role") != "admin":
        raise HTTPException(403, "관리자 권한 필요")


@app.get("/admin/data-access/requests")
async def admin_list_requests(status: str = Query("pending"), user: dict = Depends(get_current_user)):
    _require_admin(user)
    rows = await app.state.db.fetch("""
        SELECT r.id, r.user_id, u.username, u.email, r.purpose, r.organization, r.contact,
               r.requested_scopes, r.status, r.created_at
        FROM api_key_requests r JOIN users u ON u.id=r.user_id
        WHERE ($1='all' OR r.status=$1) ORDER BY r.created_at DESC
    """, status)
    return {"success": True, "data": [dict(r) for r in rows]}


@app.post("/admin/data-access/requests/{req_id}/approve")
async def admin_approve_request(req_id: str, payload: dict = Body(default={}), user: dict = Depends(get_current_user)):
    """승인 → API Key 발급. 평문 키는 이 응답에서 1회만 반환."""
    _require_admin(user)
    db = app.state.db
    req = await db.fetchrow("SELECT * FROM api_key_requests WHERE id=$1", req_id)
    if not req:
        raise HTTPException(404, "신청을 찾을 수 없습니다")
    if req["status"] != "pending":
        raise HTTPException(400, f"이미 처리된 신청({req['status']})")
    scopes = [s for s in (payload.get("scopes") or list(req["requested_scopes"])) if s in VALID_SCOPES] or ["read:reports"]
    rate_limit = int(payload.get("rate_limit") or 1000)
    plain, prefix, key_hash = generate_api_key()
    async with db.acquire() as conn:
        async with conn.transaction():
            key = await conn.fetchrow("""
                INSERT INTO api_keys (user_id, request_id, key_prefix, key_hash, name, scopes, rate_limit)
                VALUES ($1,$2,$3,$4,$5,$6::text[],$7) RETURNING id, key_prefix, scopes, rate_limit, created_at
            """, req["user_id"], req_id, prefix, key_hash, payload.get("name") or "data-access",
                 scopes, rate_limit)
            await conn.execute("""
                UPDATE api_key_requests SET status='approved', reviewed_by=$1, reviewed_at=NOW(), review_note=$2
                WHERE id=$3
            """, user["user_id"], payload.get("note"), req_id)
    return {"success": True, "data": {"key": dict(key), "api_key_plaintext": plain,
            "note": "이 평문 키는 다시 표시되지 않습니다. 안전하게 보관하세요."}}


@app.post("/admin/data-access/requests/{req_id}/reject")
async def admin_reject_request(req_id: str, payload: dict = Body(default={}), user: dict = Depends(get_current_user)):
    _require_admin(user)
    r = await app.state.db.execute("""
        UPDATE api_key_requests SET status='rejected', reviewed_by=$1, reviewed_at=NOW(), review_note=$2
        WHERE id=$3 AND status='pending'
    """, user["user_id"], payload.get("note"), req_id)
    return {"success": r.endswith("1"), "data": {"req_id": req_id}}


@app.delete("/admin/data-access/keys/{key_id}")
async def admin_revoke_key(key_id: str, user: dict = Depends(get_current_user)):
    _require_admin(user)
    r = await app.state.db.execute("UPDATE api_keys SET is_active=FALSE WHERE id=$1", key_id)
    return {"success": r.endswith("1"), "data": {"key_id": key_id}}


@app.get("/admin/data-access/stats", tags=["API Key 신청·승인"], summary="API 플랫폼 통계 (관리자)")
async def admin_data_access_stats(user: dict = Depends(get_current_user)):
    """신청 현황 + 발급 현황 + 사용 내역 집계."""
    _require_admin(user)
    db = app.state.db
    req = await db.fetchrow("""SELECT count(*) total,
        count(*) FILTER(WHERE status='pending') pending,
        count(*) FILTER(WHERE status='approved') approved,
        count(*) FILTER(WHERE status='rejected') rejected FROM api_key_requests""")
    keys = await db.fetchrow("""SELECT count(*) total,
        count(*) FILTER(WHERE is_active) active,
        count(*) FILTER(WHERE NOT is_active) revoked FROM api_keys""")
    usage = await db.fetchrow("""SELECT count(*) total,
        count(*) FILTER(WHERE ts > NOW()-INTERVAL '24 hours') last_24h,
        count(*) FILTER(WHERE ts > NOW()-INTERVAL '1 hour') last_1h FROM api_key_usage""")
    top_ep = await db.fetch("""SELECT path, count(*) cnt FROM api_key_usage
        GROUP BY path ORDER BY cnt DESC LIMIT 8""")
    top_keys = await db.fetch("""SELECT k.key_prefix, u.username,
        count(g.id) cnt, max(g.ts) last_used
        FROM api_keys k LEFT JOIN api_key_usage g ON g.api_key_id=k.id
        LEFT JOIN users u ON u.id=k.user_id
        GROUP BY k.id, k.key_prefix, u.username ORDER BY cnt DESC LIMIT 10""")
    return {"success": True, "data": {
        "requests": dict(req), "keys": dict(keys), "usage": dict(usage),
        "top_endpoints": [dict(r) for r in top_ep],
        "top_keys": [dict(r) for r in top_keys],
    }}


@app.get("/admin/data-access/keys", tags=["API Key 신청·승인"], summary="발급된 키 목록 (관리자)")
async def admin_list_all_keys(user: dict = Depends(get_current_user)):
    _require_admin(user)
    rows = await app.state.db.fetch("""
        SELECT k.id, k.key_prefix, k.name, k.scopes, k.rate_limit, k.is_active,
               k.created_at, k.last_used_at, u.username,
               (SELECT count(*) FROM api_key_usage g WHERE g.api_key_id=k.id) AS usage_count
        FROM api_keys k LEFT JOIN users u ON u.id=k.user_id
        ORDER BY k.created_at DESC
    """)
    return {"success": True, "data": [dict(r) for r in rows]}


# ═══════════════════════════════════════════════════════════
# 외부 제공 API (X-API-Key + scope) — /api/v1/*
# ═══════════════════════════════════════════════════════════

@app.get("/api/v1/catalog", tags=["제공 API (v1)"], summary="API 카탈로그 (엔드포인트·내 스코프)")
async def api_catalog(principal: dict = Depends(get_api_key_principal)):
    """API 현황 — 엔드포인트·파라미터·요청자 키 스코프 요약. 상세는 /docs(OpenAPI)."""
    return {"success": True, "data": {
        "your_scopes": principal["scopes"], "rate_limit_per_hour": principal["rate_limit"],
        "endpoints": [
            {"path": "/api/v1/reports", "scope": "read:reports",
             "params": {"format": "geojson|json", "zone": "zone_key", "bbox": "minLon,minLat,maxLon,maxLat",
                        "updated_since": "ISO8601 (증분)", "page": "int", "size": "int<=1000"}},
            {"path": "/api/v1/datasets/{batch_id}", "scope": "read:datasets",
             "params": {"format": "geojson|json"}},
        ],
        "auth": "X-API-Key 헤더", "license": "CC-BY-4.0", "openapi": "/docs",
    }}


def _reports_to_payload(rows, fmt):
    if fmt == "geojson":
        return {"type": "FeatureCollection",
                "features": [{"type": "Feature",
                              "geometry": {"type": "Point", "coordinates": [r["lon"], r["lat"]]},
                              "properties": {k: v for k, v in dict(r).items() if k not in ("lat", "lon")}}
                             for r in rows if r["lon"] is not None],
                "metadata": {"count": len(rows), "license": "CC-BY-4.0", "source": "meta-plogging-gunsan",
                             "generated_at": datetime.utcnow().isoformat()}}
    return {"success": True, "count": len(rows), "data": [dict(r) for r in rows]}


@app.get("/api/v1/reports", tags=["제공 API (v1)"], summary="제보 데이터 제공 (전량/증분, GeoJSON·JSON)")
async def api_reports(
    request: Request,
    format: str = Query("geojson"),
    zone: Optional[str] = Query(None),
    bbox: Optional[str] = Query(None),
    updated_since: Optional[str] = Query(None),
    page: int = Query(1, ge=1),
    size: int = Query(500, le=1000),
    principal: dict = Depends(require_scope("read:reports")),
):
    """제보 데이터 제공 — 전량+증분(updated_since), zone/bbox 필터, GeoJSON/JSON."""
    conds = ["tr.source != 'unity_sim'", "tr.location IS NOT NULL"]
    args = []
    if zone:
        args.append(zone); conds.append(f"gz.zone_key = ${len(args)}")
    if updated_since:
        try:
            args.append(datetime.fromisoformat(updated_since)); conds.append(f"tr.created_at >= ${len(args)}")
        except ValueError:
            raise HTTPException(400, "updated_since 는 ISO8601 형식")
    if bbox:
        try:
            mnx, mny, mxx, mxy = [float(x) for x in bbox.split(",")]
        except Exception:
            raise HTTPException(400, "bbox 는 minLon,minLat,maxLon,maxLat")
        args.extend([mnx, mny, mxx, mxy])
        i = len(args)
        conds.append(f"ST_Within(tr.location::geometry, ST_MakeEnvelope(${i-3},${i-2},${i-1},${i},4326))")
    args.extend([size, (page - 1) * size])
    rows = await app.state.db.fetch(f"""
        SELECT tr.id, tr.trash_type, tr.severity, tr.status, tr.source,
               ST_X(tr.location::geometry) AS lon, ST_Y(tr.location::geometry) AS lat,
               tr.created_at::text, gz.zone_key
        FROM trash_reports tr LEFT JOIN geofence_zones gz ON gz.id = tr.zone_id
        WHERE {' AND '.join(conds)}
        ORDER BY tr.created_at DESC LIMIT ${len(args)-1} OFFSET ${len(args)}
    """, *args)
    return _reports_to_payload(rows, format)


@app.get("/api/v1/datasets/{batch_id}", tags=["제공 API (v1)"], summary="외부 데이터셋 배치 단위 제공")
async def api_dataset(
    batch_id: str,
    format: str = Query("geojson"),
    principal: dict = Depends(require_scope("read:datasets")),
):
    """외부 데이터셋 배치 단위 제공."""
    rows = await app.state.db.fetch("""
        SELECT tr.id, tr.trash_type, tr.severity, tr.handling, tr.status,
               ST_X(tr.location::geometry) AS lon, ST_Y(tr.location::geometry) AS lat,
               tr.created_at::text, tr.gt_label
        FROM trash_reports tr
        WHERE tr.source='external_di' AND tr.gt_label->>'ingested_batch' = $1
        ORDER BY tr.created_at DESC
    """, batch_id)
    if not rows:
        raise HTTPException(404, "배치를 찾을 수 없습니다")
    return _reports_to_payload(rows, format)


if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("DATA_PORT", 8505))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
