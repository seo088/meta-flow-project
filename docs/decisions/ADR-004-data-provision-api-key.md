# ADR-004: 데이터 활용 제공 API + API Key (신청→승인→발급)

**상태**: 채택
**일자**: 2026-06-24
**관련**: [ADR-003 외부 데이터셋 적재](ADR-003-external-dataset-ingest.md), `shared/core/auth.py`, `services/data/main.py`

## 맥락
외부 기관·연구자에게 플랫폼 데이터(제보·외부 데이터셋)를 안전하게 제공해야 한다.
기존 export(`/opendata/export/geojson`, `/statistics/*`)는 전부 무인증 공개였고, 인증은 JWT뿐이라
서버-대-서버 외부 소비자를 위한 키 기반 인증·승인·rate-limit 체계가 없었다.

## 결정

### 1. 신청 → 관리자 승인 → API Key 발급 워크플로
- 로그인 사용자가 `POST /data-access/requests`로 활용 목적·스코프를 신청.
- 관리자가 `/admin/data-access/requests/{id}/approve`로 승인 시 키 발급.
- 셀프서비스 발급 금지 → 데이터 오남용 게이트.

### 2. 키는 해시만 저장, 평문은 1회만 노출
- 포맷 `mfk_<env>_<32hex>`. 저장은 `sha256` 해시(`api_keys.key_hash`), 평문은 승인 응답에서 단 1회 반환.
- 분실 시 재발급(폐기 후 재신청). `key_prefix`로 식별만.

### 3. 스코프 + 시간당 rate-limit + 사용 로그
- 스코프: `read:reports` / `read:datasets` / `read:stats`. `require_scope()` dependency로 가드.
- rate-limit: `api_key_usage` 1시간 카운트 ≥ `rate_limit` → 429 (Redis 비의존, DB 자족).
- 모든 제공 호출은 `api_key_usage`에 경량 감사 기록 + `last_used_at` 갱신.

### 4. 제공 API는 data 서비스(:8505) `/api/v1/*`
- `GET /api/v1/reports` — 전량 + **증분(`updated_since`)**, `zone`/`bbox` 필터, GeoJSON/JSON.
- `GET /api/v1/datasets/{batch_id}` — 외부 데이터셋 배치 단위.
- `GET /api/v1/catalog` — 엔드포인트·파라미터·요청자 스코프 요약(+ FastAPI `/docs` OpenAPI).
- 인증 헤더 `X-API-Key`. 라이선스 `CC-BY-4.0` 메타 동봉.

### 5. 기존 무인증 공개 통계는 유지
- `/statistics/*`·`/opendata/export/geojson`는 공개 그대로. **신규 `/api/v1/*` 대량 제공만** 키 필수.

## 결과
- DB 추가: `api_key_requests`, `api_keys`, `api_key_usage` (추가형, 기존 무영향).
- 인증 헬퍼: `generate_api_key`/`get_api_key_principal`/`require_scope` (`shared/core/auth.py`).
- 프론트: 프로필 신청 폼 + 관리자 승인 패널.
- e2e 검증: 신청→승인→키→제공API(증분/배치/catalog)→무키 401 전 경로 통과.

## 대안 (기각)
- **공개 export 확장**: 오남용·rate 제어 불가 → 기각.
- **JWT만 사용**: 만료 짧고 사용자 세션용, 서버-대-서버 부적합 → 기각.
- **Redis rate-limit**: data 서비스에 redis 미연결 → DB 카운트 방식으로 자족(저트래픽 적합). 트래픽 증가 시 Redis 전환 여지.

## 주의
- `AI_MODE=dummy`면 `get_current_user`가 무토큰을 testuser로 처리(개발 편의) → **프로덕션/NAS는 AI_MODE를 dummy로 두지 말 것**.
- 외부 소비자 base URL: 게이트웨이 경유 시 `/api/data/api/v1/*`, 직결 시 `:8505/api/v1/*`. 배포 시 문서화.
