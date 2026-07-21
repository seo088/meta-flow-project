# 🌿 메타 SW 플로깅 플랫폼 (Meta SW Plogging Platform)

> **프로젝트**: 메타 SW 플로깅 플랫폼 — TrashHunt
> **기관**: 군산대학교 / 전북특별자치도 군산시
> **버전**: v0.2.0
> **대상 구역**: 군산대학교 캠퍼스 · 은파유원지 · 새만금 · 금강하구둑

---

## 1. 프로젝트 개요

메타 SW 플로깅 플랫폼은 **플로깅(Plogging: 달리기 + 줍기)** 활동을 SNS 콘텐츠 생산, 게미피케이션, AI 드론/모빌리티 데이터 수집, 공공데이터 파이프라인으로 확장하는 **지역 생태 서비스 플랫폼**입니다.

### 핵심 목표

| 목표 | 내용 | 지표 |
|------|------|------|
| **데이터 통합** | SNS · 드론 · 모빌리티를 단일 클라우드로 수집 | Kafka 토픽 7종 |
| **AI 자동화** | YOLO 탐지 · RL 경로 · 인증 AI 파이프라인 | 탐지 정확도 ≥ 90% |
| **게미피케이션** | 포켓몬고형 퀘스트 · 배지 · 랭킹으로 시민 참여 유도 | DAU 200명+ |
| **공공데이터** | AI-Ready GeoJSON · COCO 포맷 배포 | 월 1회 갱신 |
| **외부 데이터 연동** | AIHub 새만금 쓰레기 데이터 수집 · 분석 | Dataset #71594 |

---

## 2. 6-레이어 마이크로서비스 아키텍처

```
┌────────────────────────────────────────────────────────────────┐
│ Layer 6: Frontend                                              │
│   Web Portal (8-tab SPA) · Admin Dashboard · Mobile (예정)     │
├────────────────────────────────────────────────────────────────┤
│ Layer 5: Backend Microservices (FastAPI)                        │
│   Gateway :8500 · SNS :8501 · Game :8502 · GIS :8503           │
│   AI :8504 · Data :8505 · MCP :8510 · WebSocket :8520          │
├────────────────────────────────────────────────────────────────┤
│ Layer 4: AI/ML Engine  [NVIDIA A6000 48GB VRAM]                │
│   YOLO v8 탐지 · PPO/SAC RL · ResNet 인증 · CLIP 분류         │
├────────────────────────────────────────────────────────────────┤
│ Layer 3: MCP Servers & AI Agents                               │
│   FastMCP (7 Tools) · Drone :8531 · Data :8532                 │
│   Mobility :8533 · Quest :8540 · Scheduler (cron)              │
├────────────────────────────────────────────────────────────────┤
│ Layer 2: Cloud Infra (Docker)                                  │
│   PostgreSQL+PostGIS :5433 · Redis :6380 · Kafka :9093         │
│   MinIO :9001 · Elasticsearch :9201 · Zookeeper :2181          │
├────────────────────────────────────────────────────────────────┤
│ Layer 1: Data Sources                                          │
│   SNS 앱 · 드론 SDK · 자율주행 모빌리티 · AIHub Open Data      │
└────────────────────────────────────────────────────────────────┘
```

---

## 3. 기술 스택

| 영역 | 기술 |
|------|------|
| **Language** | Python 3.11 |
| **Backend** | FastAPI, Uvicorn, asyncpg |
| **Message Queue** | Apache Kafka 3.7 (confluent-kafka) |
| **Database** | PostgreSQL 16 + PostGIS 3.4 |
| **Cache** | Redis 7.2 (ZSET 랭킹, Pub/Sub 실시간) |
| **Search** | Elasticsearch 8.x (비동기 인덱싱) |
| **Object Storage** | MinIO (S3 호환) |
| **AI/ML** | PyTorch 2.1+, YOLO v8 (Ultralytics), Stable-Baselines3 |
| **Agents** | FastMCP 3.x (Model Context Protocol) |
| **Infra** | Docker Compose |
| **Frontend** | Vanilla HTML/JS SPA (모바일 반응형, 다크/라이트 테마) |
| **Map** | OpenStreetMap + Leaflet.js (핀맵/구역 시각화) |
| **Auth** | JWT (python-jose) + bcrypt + Google OAuth 준비 |
| **Env Manager** | Conda (meta-flow 환경) |

---

## 4. 프로젝트 구조

```
meta-flow/
├── 00_overview.md ~ 07_deployment.md   # 설계 문서 (8편)
├── BUILD_PLAN.md                        # 빌드 계획
├── viz_*.html                           # 아키텍처/데이터플로우 시각화
├── data/                                # 수집 데이터 저장소
│
└── platform/                            # 메인 플랫폼 코드
    ├── .env                             # 환경 변수 설정
    ├── environment.yml                  # Conda 환경 정의
    │
    ├── services/                        # Layer 5: 백엔드 마이크로서비스
    │   ├── gateway/main.py              #   API 게이트웨이 + 정적 파일 (:8500)
    │   ├── sns/main.py                  #   SNS 피드/게시물/좋아요 (:8501)
    │   ├── game/main.py                 #   랭킹/레벨/퀘스트/배지 (:8502)
    │   ├── gis/main.py                  #   GIS 지오펜싱/핀맵 (:8503)
    │   ├── ai/main.py                   #   AI 탐지/경로최적화 (:8504)
    │   ├── ai/models/dummy_yolo.py      #   YOLO 더미 모델
    │   ├── data/main.py                 #   데이터 분석/ES 집계 (:8505)
    │   ├── data/es_sync.py              #   PG→ES 실시간 동기화
    │   ├── mcp/server.py                #   FastMCP 7도구 허브 (:8510)
    │   ├── ws/main.py                   #   WebSocket 실시간 (:8520)
    │   └── policy/main.py              #   콘텐츠 정책 에이전트 (:8541)
    │
    ├── agents/                          # Layer 3: AI 에이전트
    │   ├── drone_agent/
    │   │   ├── agent.py                 #   드론 에이전트 (:8531)
    │   │   └── video_processor.py       #   영상 프레임 추출 + AI 탐지
    │   ├── data_agent/
    │   │   ├── agent.py                 #   데이터 에이전트 (:8532)
    │   │   ├── quality_check.py         #   공공데이터 품질 검증
    │   │   └── aihub_collector.py       #   AIHub 새만금 쓰레기 데이터 수집
    │   ├── mobility_agent/agent.py      #   자율주행 모빌리티 (:8533)
    │   ├── quest_agent/agent.py         #   퀘스트 진행 관리 (:8540)
    │   └── scheduler_agent/agent.py     #   스케줄러 (cron 기반)
    │
    ├── apps/                            # Layer 6: 프론트엔드
    │   ├── web/index.html               #   8탭 웹 포털 (SPA)
    │   └── admin/index.html             #   관리자 대시보드
    │
    ├── shared/core/                     # 공유 모듈
    │   ├── db.py                        #   DB 커넥션 팩토리
    │   ├── kafka_client.py              #   Kafka Producer/Consumer
    │   ├── auth.py                      #   JWT 인증 미들웨어
    │   ├── events.py                    #   이벤트 스키마 (Pydantic)
    │   ├── types.py                     #   공유 타입 정의
    │   ├── logger.py                    #   통합 로거
    │   ├── mcp_client.py                #   MCP 클라이언트 헬퍼
    │   ├── gamification.py              #   포인트/레벨/배지 로직
    │   ├── content_filter.py            #   비속어 필터 + 제재 시스템
    │   └── dummy/                       #   더미 데이터 생성기
    │       ├── generators.py            #     군산 좌표 기반 데이터 생성
    │       └── scenarios.py             #     시나리오 시뮬레이터
    │
    ├── infra/                           # Layer 2: 인프라 설정
    │   ├── docker/docker-compose.yml    #   Docker 스택 정의
    │   ├── postgres/init.sql            #   DB 스키마 (12테이블)
    │   ├── postgres/seed.sql            #   초기 시드 데이터
    │   ├── kafka/create_topics.sh       #   Kafka 토픽 생성
    │   ├── elasticsearch/               #   ES 인덱스 매핑
    │   └── nginx/                       #   리버스 프록시 설정
    │
    └── scripts/                         # 운영 스크립트
        ├── init.sh                      #   전체 초기화
        ├── start_all.sh                 #   서비스 일괄 시작
        ├── stop_all.sh                  #   서비스 일괄 중지
        ├── seed_dummy.py                #   더미 데이터 적재
        └── run_scenario.py              #   시나리오 시뮬레이션
```

---

## 5. 서비스 포트 맵

| 서비스 | 포트 | 설명 |
|--------|------|------|
| Gateway | 8500 | API 리버스 프록시 + 웹 UI 서빙 |
| SNS Service | 8501 | 피드, 게시물, 좋아요, 해시태그 |
| Game Service | 8502 | 랭킹, 레벨, 퀘스트, 배지, XP |
| GIS Service | 8503 | 지오펜싱, 핀맵, 구역 통계 |
| AI Service | 8504 | YOLO 탐지, RL 경로 최적화 |
| Data Service | 8505 | ES 집계, 타임시리즈, 공공데이터 |
| MCP Server | 8510 | FastMCP 7개 도구 (에이전트 허브) |
| WebSocket | 8520 | 실시간 알림, 핀맵, 랭킹 |
| Drone Agent | 8531 | 드론 영상 처리 + AI 탐지 |
| Data Agent | 8532 | GeoJSON/COCO 내보내기, AIHub 연동 |
| Mobility Agent | 8533 | 자율주행 로봇 경로 최적화 |
| Quest Agent | 8540 | 퀘스트 진행 추적 + 배지 수여 |
| Policy Agent | 8541 | 콘텐츠 모니터링 + 비속어 제재 |

### 인프라 포트

| 서비스 | 포트 | 비고 |
|--------|------|------|
| PostgreSQL + PostGIS | 5433 | 비표준 포트 |
| Redis | 6380 | 비표준 포트 |
| Kafka | 9093 | Zookeeper: 2181 |
| MinIO | 9001/9002 | API/Console |
| Elasticsearch | 9201 | 비표준 포트 |

---

## 6. 빠른 시작 가이드

### 6.1 사전 요구사항

- Ubuntu 22.04+ / Linux
- Docker & Docker Compose
- Anaconda 또는 Miniconda
- NVIDIA GPU (선택, AI_MODE=dummy로 대체 가능)

### 6.2 설치 및 실행

```bash
# 1) 저장소 클론
git clone https://203.234.62.175:5443/system/meta-flow.git
cd meta-flow/platform

# 2) Conda 환경 생성
conda env create -f environment.yml
conda activate meta-flow

# 3) 환경 변수 설정
cp .env.example .env  # 또는 기존 .env 확인
vi .env               # SERVER_HOST, DB 비밀번호 등 수정

# 4) Docker 인프라 시작
cd infra/docker
docker-compose up -d
cd ../..

# 5) DB 초기화
psql -h localhost -p 5433 -U plogging -d plogging_db -f infra/postgres/init.sql
psql -h localhost -p 5433 -U plogging -d plogging_db -f infra/postgres/seed.sql

# 6) Kafka 토픽 생성
bash infra/kafka/create_topics.sh

# 7) 더미 데이터 적재
python scripts/seed_dummy.py

# 8) 전체 서비스 시작
bash scripts/start_all.sh

# 9) 웹 UI 접속
# http://<SERVER_HOST>:8500/         (사용자 포털)
# http://<SERVER_HOST>:8500/admin    (관리자 대시보드)
```

### 6.3 개별 서비스 시작 (개발 모드)

```bash
source .env
export PYTHONPATH=$(pwd)
export AI_MODE=dummy

# 게이트웨이
python services/gateway/main.py

# SNS 서비스
python services/sns/main.py

# 드론 에이전트
python agents/drone_agent/agent.py
```

---

## 7. Kafka 토픽

| 토픽 | 용도 |
|------|------|
| `plogging.report.created` | 새 쓰레기 제보 생성 이벤트 |
| `plogging.ai.detected` | AI 탐지 완료 이벤트 → ES 동기화 |
| `plogging.mission.completed` | 미션 완료 → 포인트 지급 |
| `drone.stream.uploaded` | 드론 영상 업로드 완료 |
| `mobility.route.completed` | 자율주행 경로 완료 |
| `user.points.updated` | 사용자 포인트 변동 |
| `data.export.completed` | 공공데이터 내보내기 완료 |

---

## 8. MCP 도구 (7개)

| 도구 | 설명 |
|------|------|
| `detect_trash` | YOLO v8 쓰레기 탐지 |
| `optimize_route` | RL 기반 수거 경로 최적화 |
| `verify_cleanup` | 청소 전/후 비교 인증 |
| `get_zone_stats` | 구역별 통계 조회 |
| `create_report` | 쓰레기 제보 생성 |
| `award_points` | 포인트/배지 수여 |
| `export_data` | GeoJSON/COCO 내보내기 |

---

## 9. PostGIS 지오펜싱 구역

| 구역 | zone_key | 보너스 배율 |
|------|----------|-------------|
| 군산대학교 캠퍼스 | `KU_CAMPUS` | x1.0 |
| 은파유원지 | `EUNPA` | x1.5 |
| 새만금 | `SAEMANGEUM` | x2.0 |
| 금강하구둑 | `GEUMGANG` | x1.5 |

---

## 10. AIHub 데이터 연동

[AIHub Dataset #71594](https://aihub.or.kr/aihubdata/data/view.do?dataSetSn=71594) — 전북 새만금 방조제 유입 하천 쓰레기 데이터

- **규모**: 207,586 이미지 (1.87TB), 9대 쓰레기 카테고리
- **카테고리**: 플라스틱, 스티로폼, 섬유, 비닐/필름, 목재, 금속, 유리, 고무, 종이
- **수집 지점**: 만경강 하류, 동진강 하류, 새만금 방조제 (북/중/남), 군산 내항, 비응도, 야미도
- **연동 API**:
  - `GET /api/data-agent/aihub/collect?count=50` — 데이터 수집 (시뮬레이션)
  - `POST /api/data-agent/aihub/ingest` — 플랫폼 DB 적재

---

## 11. 웹 UI 탭 구성

| 탭 | 내용 |
|----|------|
| **홈** | 프로필 요약 (XP/레벨), 인기 키워드 Playground, 군산 피드 |
| **피드** | 인스타그램 스타일 SNS 피드 (좋아요/댓글/공유, 이미지 갤러리) |
| **랭킹** | 글로벌 Top 20 + 구역별 랭킹 (사용자 클릭 → 프로필) |
| **퀘스트** | 퀘스트 탐색/참여, 퀘스트 피드(+30XP), 통계, 배지 컬렉션 |
| **구역** | OpenStreetMap 지도 (Leaflet) + 핀맵 + 구역-퀘스트 연동 |
| **프로필** | 사용자 프로필 (XP/레벨/배지) + 타임라인 (공개) |
| **드론/모빌리티** | 드론 영상 업로드, 프레임 분석, 순찰 |
| **공공데이터/AIHub** | GeoJSON/COCO 내보내기, 품질 검증, AIHub 새만금 수집/적재 |
| **아키텍처** | 실시간 헬스체크, Kafka 토픽 (관리자 전용) + 사용자 권한 관리 |

---

## 12. 인증 및 소셜 기능 (v0.0.2)

### 인증

| 엔드포인트 | 설명 |
|-----------|------|
| `POST /api/sns/auth/register` | 회원가입 (username/email/password) |
| `POST /api/sns/auth/login` | 로그인 (JWT 토큰 발급) |
| `POST /api/sns/auth/google` | Google OAuth 로그인 |
| `GET /api/sns/profile/me` | 내 프로필 (토큰 필요) |

- 가입 후 로그인 폼에 아이디 자동 추천
- 관리자 계정: `.env` 파일의 `ADMIN_USERNAME` / `ADMIN_PASSWORD` 참조
- JWT 토큰 유효기간: `.env` 파일의 `JWT_EXPIRY_HOURS` 참조

### 프로필 / 타임라인

| 엔드포인트 | 설명 |
|-----------|------|
| `GET /api/sns/users/{username}/profile` | 사용자 프로필 (공개) |
| `GET /api/sns/users/{username}/timeline` | 사용자 게시물 (공개) |

### 소셜 기능

| 엔드포인트 | 설명 |
|-----------|------|
| `POST /api/sns/posts/{id}/like` | 좋아요 토글 (로그인 필요) |
| `GET /api/sns/posts/{id}/comments` | 댓글 조회 |
| `POST /api/sns/posts/{id}/comments` | 댓글 작성 (로그인 필요) |

### 권한 시스템

- **기본 권한** (가입 시 자동): `feed`, `ranking`, `zone`
- **확장 권한** (관리자 부여): `quest`, `drone`, `data`
- **관리자 전용**: `arch` (아키텍처 탭)
- 관리자 페이지에서 체크박스로 실시간 권한 관리

| 엔드포인트 | 설명 |
|-----------|------|
| `GET /api/sns/admin/users` | 사용자 목록 (관리자) |
| `PUT /api/sns/admin/users/{id}/permissions` | 권한 설정 (관리자) |
| `PUT /api/sns/admin/users/{id}/role` | 역할 변경 (관리자) |

---

## 13. v0.0.3 신규 기능

### 모바일 반응형 UI
- 모바일 하단 네비게이션 바 (홈/피드/글쓰기/퀘스트/더보기)
- `viewport-fit=cover` + Safe Area 대응
- FAB(+) 글쓰기 버튼 (z-index 1100, 모바일 네비 위 고정)
- 좌우 여백 최소화, 각진 border-radius 스타일

### 다크/라이트 테마
- CSS Custom Properties 기반 테마 전환 (`[data-theme="light"]`)
- `localStorage` 저장으로 새로고침 시 유지
- 상단 + 모바일 메뉴 토글 버튼

### 퀘스트 피드 시스템
- 퀘스트 내 게시글 작성 (+30 XP, 일반 피드 +20 XP보다 높은 보상)
- 3단계 공개 범위:
  - 🌐 **마당 공개**: 퀘스트 피드 + SNS 전체 피드 동시 게시
  - 🎯 **퀘스트 공개**: 퀘스트 멤버에게만 공개
  - 🔒 **비공개**: 본인만 확인
- 퀘스트 활동 미니맵 (Leaflet)
- 퀘스트-구역 키워드 매칭 연동

### XP/포인트 시스템 개선
- `point_ledger` → `user_points` (Materialized View) 정상 연동
- 게시글 등록 시 즉시 XP 갱신 (REFRESH MATERIALIZED VIEW CONCURRENTLY)
- 레벨 계산: `level = min(10, xp ÷ 150 + 1)`

### 내비게이션 스택
- `_navStack` 기반 뒤로가기 (브라우저 히스토리 미의존)
- 강제 새로고침 시 항상 홈으로 이동
- 모든 뒤로가기 버튼 `goBack()` 통합

### 게이트웨이 프록시 개선
- 파일 업로드 타임아웃 60초 (일반 30초)
- 서비스별 에러 메시지 세분화 (503/504/502)
- multipart/form-data 프록시 안정화

---

## 14. 데이터베이스 스키마

17개 테이블 + 1 Materialized View:

| 테이블 | 설명 |
|--------|------|
| `users` | 사용자 (username, email, password_hash, google_id, bio, role) |
| `user_badges` | 획득 배지 |
| `user_permissions` | 사용자별 접근 권한 |
| `geofence_zones` | PostGIS 지오펜싱 구역 |
| `trash_reports` | 쓰레기 제보 (위치, 종류, 심각도) |
| `posts` | SNS 게시물 |
| `post_likes` | 좋아요 (user_id + post_id 복합키) |
| `post_comments` | 댓글 |
| `quest_definitions` | 퀘스트 정의 |
| `user_quests` | 퀘스트 진행 상태 |
| `badge_definitions` | 배지 정의 (13종) |
| `point_ledger` | 포인트 이력 (중복방지 UNIQUE) |
| `cleanups` | 청소 인증 |
| `drone_events` | 드론/모빌리티 이벤트 |
| `opendata_exports` | 공공데이터 배포 이력 |
| `direct_messages` | DM 다이렉트 메시지 |
| `content_violations` | 비속어 위반 이력 |
| `user_points` (MV) | 사용자별 총 XP (Materialized View) |

---

## 15. 환경 변수 (.env)

주요 환경 변수 (상세는 `platform/.env` 참조):

```bash
# 서버
SERVER_HOST=203.234.62.176
ENVIRONMENT=development

# 데이터베이스
POSTGRES_PORT=5433
POSTGRES_DB=plogging_db

# 캐시/메시지
REDIS_PORT=6380
KAFKA_BOOTSTRAP_SERVERS=203.234.62.176:9093

# AI 모드 (dummy = 더미 응답, gpu = 실제 모델)
AI_MODE=dummy

# 서비스 포트 (85xx 체계)
GATEWAY_PORT=8500
SNS_PORT=8501
# ... (전체 포트 매핑은 §5 참조)
```

---

## 16. 변경 이력

| 버전 | 날짜 | 내용 |
|------|------|------|
| v0.0.1 | 2026-03-16 | 초기 릴리스: 6-레이어 아키텍처, 12서비스, 8탭 웹 UI |
| v0.0.2 | 2026-03-17 | 인증/프로필/소셜/지도/권한 시스템 + AIHub 연동 |
| v0.0.3 | 2026-04-10 | 모바일 반응형 UI, 퀘스트 피드/XP 시스템, 다크/라이트 테마 |
| v0.0.4 | 2026-04-11 | DM 메시지, 비속어 필터, 랭킹 분석, 도움말, UI 전면 개선 |
| v0.0.4a | 2026-04-11 | 리액션/댓글 수정, 비공개 퀘스트, 카운트다운, 제보 연동, UI 개선 |
| v0.0.5 | 2026-04-13 | 홈 레이아웃 개편, 이모지 리액션 시스템, N+1 최적화+Redis 캐싱, AI 아바타 추천, 구역 크라우드소싱 제보, 제보 유형별 그룹화+지도 핀 팝업, 댓글 알림, 퀘스트 초대 자동완성, 개인정보 처리방침, 회원 탈퇴 |
| v0.0.6 | 2026-05-13 | XP 시스템 전면 개편, 적응형 캐시 에이전트, 시스템 모니터링 대시보드, 모바일 제보 UX 개선, 서비스 헬스체크 자동 복구, UI/폰트 현대화 |
| v0.0.6a | 2026-05-13 | 관리자 사용자 패널 개편(검색+카드), XP 이력 조회, 이미지 풀스크린 뷰어, 탐색→트렌드 명칭 변경, 트렌드 선정 기준 도움말 |
| v0.0.7 | 2026-05-14 | 프로필 Instagram 스타일, 모바일 반응형 근본 수정, 이미지 갤러리 뷰어, 시스템 모니터링 강화, 댓글→게시글 복귀 네비게이션 |
| v0.0.8 | 2026-05-21 | 시스템 메시지 DM/알림 분리(퀘스트 초대→notifications), Gemini API 이모지 추천(gemini-flash-latest+키워드 폴백), 모니터링 차트 SVG 개선(그리드/평균선/툴팁), 타임라인 댓글 ID 충돌 수정, 알림 뱃지 갱신 버그 수정, 알림 목록 페이지 신설, 글쓰기 통합(플로그/퀘스트 위치 선택+참여 퀘스트 드롭다운) |
| v0.0.9 | 2026-05-21 | 스크롤 맨 위로(활성 탭/로고 재클릭+우측 하단 ↑ FAB), 알림 진입점 영구화(상단 종 SVG 아이콘+빨간 점/9+ 카운트, 인스타 ❤️ 스타일), 알림 페이지 인스타화(진입 시 자동 일괄 읽음, 날짜별 그룹화: 오늘/어제/이번 주/이전, 타입별 컬러 아이콘 뱃지, 모바일 더보기 메뉴에 알림 항목), 좋아요 받음 알림(reaction 타입+like_received +2 XP, 게시글당 1회·어뷰징 방지) |
| v0.0.10 | 2026-05-22 | 멘션 드롭다운 키보드 네비(↑↓/Enter/Tab/Esc + scrollIntoView), XP 시스템 종합 보완(comment_created +3 일일 5회한도/comment_received +3 게시글당 1회/zone_fix ref_id 어뷰징차단/admin 댓글 XP 면제/데드 reason 정리), 트렌드 차트 4종 XP 기반 통합(받은 engagement_xp×24/(24+h) 시간 decay·idx_ledger_ref_reason 인덱스), 게시글 수정 시 이미지 추가/삭제 지원(PUT image_urls·❌ 미리보기 삭제 버튼·업로드 실패 명시 알림), admin 게시글 트렌드 노출(🛡️ 운영 배지·랭커는 제외 유지), 트렌드 점수 투명화(섹션 헤더 ℹ️ 공식·각 카드 🔥 점수 칩 호버 분해), 트렌드 segmented 필터(전체/💬일반/📢제보 캐시 키 분리), UI modernization(크림슨 통일·테두리/그림자 제거 flat·메달+점수 가로 배치·border-radius 14px 통일·hover 미세 톤만) |
| v0.0.10a | 2026-05-28 | 컴포즈 모달 스크롤 수정(max-height:90vh;overflow-y:auto — 제보 시 제출 버튼 접근 불가 해결), DB 자동 백업 스크립트(asyncpg+gzip, 매일 새벽 2시 cron, 30일 보관) |
| v0.0.11a | 2026-06-01 | 지오펜스 좌표 정합성 보정 — KU_CAMPUS(군산대학교) 폴리곤이 실제 캠퍼스(대학로 558, 미룡동 ≈126.682,35.945)에서 약 5km 북동(126.737)의 임의 공간에 찍혀 있던 오류 수정, EUNPA 원위치 복원(비겹침), 전체 제보 zone 재매칭(DI 1033건→군산대학교), seed.sql 좌표 영구 수정 |
| v0.2.0 | 2026-06-30 | **게이미피케이션 완성** — ① 개인 수거 인증 루프(핀 "내가 치웠어요"→after 사진 인증→제보 완료 처리+cleanup_verified +50XP, 구역·주말 보너스, 관리자 사후검수 철회/XP회수), ② 수거 퀘스트(구역별 대기 제보 임계초과 시 6h 스케줄러 자동 생성, 제한시간·진행도·마감 전 빠를수록 시간보너스 cleanup_quest_bonus, 탐색 탭 '수거 작전' 카드 카운트다운/진행바/치우러가기), ③ 퀘스트 완료 보상(POST /quests/{id}/complete → 멤버 전원 quest_complete +reward_xp, 수거 퀘스트 완료 시 기여자 분배), ④ 시스템 배지 자동수여(업적 — 제보수/청소수/구역별/레벨 기준 충족 시 user_badges INSERT + 희귀도별 badge_earn XP), ⑤ EXIF GPS 자동 추출(업로드 사진 GPS로 위치 보완, HTTP 위치 누락 대응). **지도 개편** — 핀 자동 줌(fitBounds)+'제보 위치로' 컨트롤, 핀 상한 UI 입력(200→최대 5000), 외부 데이터셋 표시 토글(include_external) + 전용 보기 모달(전체 사용자), 핀 팝업 폭/링크 버튼 정리(오버플로 수정), 상태 색상 배지, **loadZones TDZ 크래시 수정**(typeNames 선언순서 — 현황/제보/지도 핀 전부 미표시 해결). **API 개발자 도구** — 인터랙티브 API 콘솔(/api-console, 엔드포인트 검색 자동완성·실시간 테스터·curl 생성·구역키/배치ID 자동완성), FastAPI Swagger/ReDoc 노출(게이트웨이 비-JSON 패스스루+root_path, OpenAPI 가이드·X-API-Key 보안스킴·태그), 관리자 'API' 통계 탭(신청/발급/사용 현황·키 폐기). **관리자** — 전체 퀘스트 보기(/admin/quests, 멤버십 무관). **운영** — systemd 영구 구동(9개 서비스 유저 유닛, linger·자동재시작·세션 독립), 쓰레기 분류 사전 '로딩중' 고착 수정. **문서** — 산학협력 활용방안(docs/industry), ADR, 세션 26~29 |
| v0.1.0 | 2026-06-24 | 데이터 활용 API Key 플랫폼(신청→관리자 승인→발급, sha256 해시 저장·평문 1회·스코프·시간당 rate-limit·사용 감사 — api_key_requests/api_keys/api_key_usage), 외부 제공 API /api/v1/reports(전량+증분 updated_since·zone/bbox·GeoJSON/JSON)·/datasets/{batch_id}·/catalog(+OpenAPI), 지도 핀→연결 게시글 이동, 중복 핀 제거(report/post 이중표시) + 더미 핀(aihub/picsum) 119건 하드삭제, 외부데이터셋 관리자 페이지 강화(배치 상세·썸네일·분포·롤백·데이터셋 목록), 미구현 기능 폐기(드론/데이터에이전트/AIHub시뮬·죽은 라우트), ADR-003/004 (외부데이터셋 적재·데이터 제공 API). NAS 배포 Docker 1단계(data/ws/policy 서비스 추가+프록시 배선) |
| v0.0.12 | 2026-06-24 | Docker 컨테이너화 인프라 추가 — 6-Layer 전체 스택 docker-compose(gateway/sns/game/gis/ai/mcp + postgres/redis/kafka/zookeeper/minio + elasticsearch profile=search), python:3.11-slim 멀티스테이지 Dockerfile(서비스별 CMD 분기), .env.docker.example 환경변수 템플릿+통합 requirements.txt+.dockerignore, DOCKER.md 빌드/배포 가이드, .gitignore 대용량 데이터셋·db_backup·avatars·worktrees 제외 규칙. 외부 데이터셋 적재 ADR-003 문서화 + dataset-ingest 서브에이전트 정의 추가. 서비스 코드 변경 없음(인프라/문서 전용) |
| v0.0.11 | 2026-06-01 | 외부 DI 데이터셋 배치 적재(1035건, merged_records.json 정답라벨+좌표, SHA-256 멱등), 다축 쓰레기 분류체계(품목 9종/크기/수거구분 handling/긴급성 자동산출 — trash_taxonomy.py SSOT), 신규 카테고리 6종(담배꽁초·종이·비닐·캔금속·유리·스티로폼)+report_categories 테이블+gt_label jsonb, 내부 적재 API(/admin/reports/ingest·ingest-batch, 업로드/URL 참조 모드, shared/core/ingest.py), dataset_bot 시스템계정, 배치 제보 메인피드 제외+전용 탭(/feed?source=external_di), 현황 카테고리 동적화+제보 사진 썸네일(GIS pins image_urls), 관리자 데이터셋 탭(배치 진행/통계), 도움말 쓰레기 분류 사전, 댓글 멘션 작성자 자동완성 수정, EUNPA 구역 서편 확장(은파호수 일대 포함) |

### 데이터 제공 API 응답 확장

- `GET /api/v1/reports`는 외부 요청 대응을 위해 `trash_size`, `handling`, `image_urls`, `primary_image_url`, `location`, `photo_location`, `items`를 제공한다.
- 추가 입력 조건: `trash_type`, `trash_size`, `handling`, `has_image`, `has_location`, `source`, `include_items`.
- `GET /api/v1/datasets/{batch_id}`도 동일한 사진/위치/크기 분류 응답 구조를 사용하며, `include_items=false`로 세부 라벨 목록을 생략할 수 있다.

---

## 17. v0.0.6 주요 변경 사항

### 17.1 XP 시스템 전면 개편
- **통합 `grant_xp()` 함수**: point_ledger INSERT + MV 갱신 + Redis zadd + Kafka 발행을 한 곳에서 처리
- **제보 XP 부여 수정**: 기존 데드코드 → 실제 +20 XP 부여 (주말 ×1.5 보너스)
- **XP 소급 적용**: 기존 게시글 35건 + 제보 274건의 누락 XP 소급 반영
- **퀘스트 생성 XP 임계값**: 최소 XP 충족 시에만 퀘스트 생성 가능 (관리자 설정)
- **레벨 시스템 일관성**: 모든 서비스에서 `gamification.py`의 비선형 테이블 사용으로 통일

### 17.2 적응형 캐시 에이전트 (`cache_agent.py`)
- 최근 1시간 활동 수 기반 동적 TTL: 50+건→15초, 20+→30초, 5+→60초, 1+→120초, 0→5분
- 이벤트 기반 캐시 무효화: 게시글/제보 생성, 리액션 변경 시 즉시 무효화
- 활동 추적 카운터로 실시간 활동률 측정

### 17.3 관리자 대시보드 강화
- **시스템 모니터링**: 서비스 헬스체크 카드, 캐시 에이전트 TTL 현황, Kafka 토픽
- **통계 대시보드**: 일별 접속 차트 + 콘텐츠 필터 현황 + 접속/위반 로그 (3단 레이아웃)
- **XP 관리**: 사용자별 XP 현황 테이블, 유형별 통계, 일별 XP 부여 차트
- **시스템 설정**: 퀘스트 최소 XP, 위치 필수 여부 등 관리자 설정 API + UI
- **접속 로그**: login_logs 테이블 + 3개월 자동 정리 (통신비밀보호법 준수)
- **아키텍처 탭 → 관리자 통합**: 네비 간결화

### 17.4 모바일 UX 개선
- **해시태그 입력**: Enter 외에 쉼표/스페이스/"추가" 버튼/onblur 지원
- **위치 권한 흐름**: 로그인 시 자동 확인, Permissions API 활용, 브라우저 차단 시 안내
- **제보 위치 선택 토글**: HTTP 환경에서 위치 없이 제보 가능 (관리자 설정)
- **게시글 수정**: prompt() → 컴포즈 모달 재활용 (내용/해시태그/이미지/공개범위 프리필)
- **게시글 삭제**: DELETE API 추가, 시간 제한 해제 (테스트 환경)
- **이미지 풀스크린 뷰어**: 터치/클릭으로 확대, 다시 터치로 닫기 (블러 배경)

### 17.5 UI/UX 현대화
- **전체 폰트**: Gowun Batang(궁서체) → Pretendard(산세리프, 토스/카카오 스타일)
- **로그인/회원가입 모달**: 세그먼트 컨트롤 탭, 그라데이션 버튼, 포커스 글로우
- **급상승 피드**: Instagram 스타일 카드 (헤더→이미지→본문→액션바 분리)
- **급상승 랭커**: 🥇🥈🥉 메달 아이콘, 간결한 레이아웃
- **구역 서브탭**: iOS 세그먼트 컨트롤 스타일
- **탭 네비**: 이모지 제거, 텍스트만 표시, 기본 탭을 플로그로 변경
- **홈 → 탐색**: 피드가 메인이므로 기존 홈을 "탐색"으로 명칭 변경
- **라이트 모드**: text-muted 대비 강화 (WCAG AA 충족)
- **피드 카드**: 본문/액션바 border-top 구분, 인기 키워드 페이드 그라데이션
- **더보기(⋯) 메뉴**: body 레벨 팝업 + requestAnimationFrame 닫기 (overflow:hidden 해결)

### 17.6 개인정보 처리방침 준수 보완
- **회원 탈퇴**: 누락 테이블 7개 추가 삭제 (user_permissions, user_badges 등)
- **접속 로그**: login_logs 테이블 생성, 로그인 시 IP/UA 기록, 3개월 초과 자동 정리
- **구글 로그인**: HTTPS 전환 전까지 비활성화

### 17.7 인프라/운영
- **서비스 헬스체크 에이전트**: 3분마다 cron 실행, 중지 서비스 자동 재시작
- **구역 데이터 좌표 기반 연동**: 키워드 하드코딩 제거, zone_id 기반 피드/퀘스트 매칭
- **Worktree 동기화 규칙**: CLAUDE.md에 게이트웨이↔worktree 동기화 절차 명시

### 17.8 v0.0.6a 패치 (2026-05-13)

#### 관리자 사용자 패널 개편
- **사용자 검색**: 이름/아이디/이메일 실시간 필터링
- **카드 그리드 레이아웃**: 아바타 + 이름 + 역할 뱃지 + 4칸 스탯(XP/게시글/제보/레벨) + 권한 체크박스
- **XP 기준 정렬**: 활동 많은 사용자 우선 표시
- **API 확장**: avatar_url, post_count, report_count, level, level_title 추가 반환

#### 사용자별 XP 이력 조회
- **API**: `GET /admin/xp-history/{username}` — 최근 50건 이력 + 유형별 합계
- **UI**: 사용자 카드의 "XP ▸" 클릭 → XP 이력 모달 (유형별 카드 + 이력 테이블)
- **XP 대시보드**: 테이블의 XP 숫자 클릭으로도 이력 확인 가능

#### 이미지 풀스크린 뷰어
- **Fullscreen API**: 이미지 터치 시 브라우저 주소창 숨김 (Android Chrome)
- **블러 배경**: 여백에 `backdrop-filter: blur(20px)` + 반투명 블랙
- **원본 비율 유지**: `object-fit: contain`으로 잘림 없이 표시

#### 탐색 → 트렌드
- **명칭 변경**: "탐색" → "트렌드" (급상승 피드/랭커가 핵심 콘텐츠이므로)
- **모바일 아이콘**: 돋보기 → 트렌드 차트 아이콘
- **도움말 추가**: 트렌드 선정 기준 상세 설명
  - 급상승 피드: 최근 7일 내 (리액션 + 댓글) 합산 내림차순
  - 급상승 랭커: 최근 7일 내 XP 획득량 내림차순, 동점 시 게시글 수
  - 관리자 계정은 랭킹에서 자동 제외

### 17.9 v0.0.7 (2026-05-14)

#### 프로필 UI Instagram 스타일 재설계
- **레이아웃**: 세로 중앙 → 아바타(좌) + 스탯 3칸(우) 가로 배치
- **설정 모달**: 체크박스/테마/탈퇴를 ⚙️ 아이콘 모달로 분리
- **여백 40% 감소**, XP 바 컴팩트 (4px)
- **댓글 아바타 클릭**: 해당 사용자 프로필로 이동
- **댓글→프로필→뒤로**: 댓글이 달린 게시글로 스크롤 복귀 (scrollIntoView)

#### 모바일 반응형 근본 수정
- **근본 원인**: `max-width:640px`가 모바일 뷰포트 초과 → `max-width:min(640px,100%)`
- **`-webkit-overflow-scrolling:touch`**: iOS 스크롤 레이어 문제 → 전면 제거 (8곳)
- **글로벌 img**: `max-width:100%;height:auto` 적용
- **3중 overflow 방어**: html + body + .panel 모두 `overflow-x:hidden`
- **갤러리 이미지**: `flex:0 0 calc(50%-1px)` (flex-shrink:0 충돌 해결)
- **그리드**: `repeat(auto-fit,minmax())` 패턴 통일
- **규칙 문서화**: `docs/skills/frontend-spa.md`에 모바일 반응형 필수 규칙 7개

#### 이미지 갤러리 뷰어
- **좌우 네비게이션**: 화살표 버튼 + 위치 표시 (1/3)
- **모바일 스와이프**: touchstart/touchend (50px 임계)
- **마지막 이미지 → 자동 닫기**: 우측 이동 시 뷰어 종료
- **키보드 지원**: ArrowLeft/Right, Escape
- **Fullscreen API**: Android Chrome 주소창 숨김

#### 모바일 하단 네비 개편
- **+ 버튼**: 주황→크림슨 레드, 테두리 3px→1.5px
- **아이콘 변경**: 피드(🏠) / 트렌드(↑) / 퀘스트(★) / 메뉴(☰)
- **활성 인디케이터**: 하단 2px 바
- **스크롤 자동 숨김**: 스크롤 시 슬라이드 아웃, 멈추면 슬라이드 인

#### 시스템 모니터링 강화
- **`/health/detailed` API**: 메모리, CPU, DB풀, 트래픽 메트릭
- **요청 카운터 미들웨어**: Redis 분/시간 버킷 자동 집계
- **병렬 헬스체크**: Promise.all (순차 → 병렬)
- **개별 재체크 버튼**: 서비스별 "체크" 버튼 + 응답시간(ms)
- **트래픽 카드**: 요청/분, 요청/시간, 피크 최대

#### 기타 개선
- **네비 알림 뱃지**: 💬 미읽은 댓글 수 표시
- **타임라인 인기순**: 🔥인기순/🕐최신순 토글
- **제보 2단 레이아웃**: 좌측 분류 + 우측 최신 타임라인
- **제보 타임라인**: 위치 없는 제보도 표시 (`/pins/recent` API)
- **프로필 네트워크 재시도**: null 응답 시 1회 재시도 + "다시 시도" 버튼
- **AI 아바타**: 다시 추천 버튼, 적용 후 즉시 반영, jpg 허용
- **관리자 사용자 카드**: 권한 폴딩(details/summary)
- **더미 데이터 정리**: 테스트용 사용자/제보 삭제

---

## 18. v0.0.4 신규 기능

### DM (다이렉트 메시지) 시스템
- `POST /api/sns/messages/send` — 사용자 간 1:1 메시지 전송
- `GET /api/sns/messages/conversations` — 대화 목록
- `GET /api/sns/messages/with/{username}` — 대화 내역 (자동 읽음 처리)
- `GET /api/sns/messages/unread-count` — 읽지 않은 메시지 수
- `DELETE /api/sns/messages/{id}` — 메시지 삭제
- 로그인 시 **읽지 않은 메시지 팝업** (세션당 1회)
- 프로필 페이지 **메시지함 탭** (타임라인/메시지 전환)
- 퀘스트 멤버 프로필에서 바로 DM 전송
- DB 테이블: `direct_messages` (서비스 시작 시 자동 생성)

### 비속어/욕설 필터링 시스템
- **`shared/core/content_filter.py`** — 공유 필터 모듈
  - 한국어 비속어 40여종 + 영어 11종 + 변형 우회 패턴 대응
  - 레벨1 (일반 비속어) / 레벨2 (심각한 혐오 표현) 분류
- **단계별 제재**:
  - 1차: 경고 팝업 (게시 허용)
  - 2차: XP -50 감점 + 경고
  - 3차+: 게시 차단 + XP -100×n 감점
  - 심각 표현: 즉시 차단
- 적용 범위: SNS 게시글, 댓글, 퀘스트 피드, 퀘스트 댓글 (4곳 실시간 검사)
- **위반 이력 테이블**: `content_violations` (30일 기준 누적)

### Policy Agent (콘텐츠 모니터링 서비스, :8541)
- `GET /api/policy/violations/stats` — 위반 통계
- `GET /api/policy/violations/users` — 위반자 목록
- `GET /api/policy/violations/user/{username}` — 개인 이력
- `POST /api/policy/scan/posts` — 기존 게시글 일괄 스캔 + 자동 숨김

### 플로거 랭킹 전면 재설계 (폴딩 아코디언)
- 테이블 → 폴딩 카드 (Top 3 메달, 아바타, XP)
- `GET /api/game/ranking/user/{username}/activity` — 사용자 활동 분석 API
  - XP 사유별 막대 그래프
  - 해시태그 키워드 클라우드
  - 14일 XP 추이 바 차트
  - 최근 게시글 5건
  - XP 적립 내역 15건

### 댓글 삭제 기능
- `DELETE /api/sns/posts/{post_id}/comments/{comment_id}` — 작성자 본인만 삭제
- 댓글이 있는 게시글은 삭제 불가 (데이터 보호)

### 관리자 페이지 재설계
- 서브탭: **사용자** / **숨김 콘텐츠** / **통계**
- 사용자 목록: 테이블 → 폴딩 카드 (랭킹과 동일 스타일)
- **숨김 콘텐츠 관리**:
  - `GET /api/sns/admin/hidden-posts` — 숨김 목록
  - `POST /api/sns/admin/hidden-posts/{id}/restore` — 복원
  - `DELETE /api/sns/admin/hidden-posts/{id}` — 영구 삭제
  - `GET /api/sns/admin/hidden-posts/export` — JSON 내보내기
- 숨김 게시글이 급상승 피드, 검색, 해시태그에 노출되지 않도록 수정

### 아바타 시스템 개선
- 기본 아바타: 이모지 → **이니셜 + 따뜻한 배경색** (16색 팔레트)
- 한글 이름 첫 글자 / 영문 대문자 자동 표시
- 업로드: `.ico`, `.png` (4MB 이하) → `apps/web/avatars/` 저장

### UI/UX 전면 개선
- **따뜻한 색상 체계**: 차가운 블루 → 앰버/골드 (#f59e0b) 톤
  - 다크 테마: 따뜻한 다크 (#111827, #1a2332)
  - 라이트 테마: 크림/베이지 (#faf8f5, #f3efe8)
- **퀘스트 상세 2-컬럼 레이아웃**: 사이드바(220px) + 메인 피드
  - 사이드바 트리 네비게이션 (피드 > 타임라인/활동지도, 멤버, 배지, 통계)
  - 스크롤바 없는 컴팩트 디자인
- **리액션 팝업**: hover → 클릭 토글 방식 (26px 대형 이모지, 화면 밖 자동 보정)
- 카드 border-radius 12~16px (부드러운 곡선)
- 중복 글쓰기 버튼 제거 (FAB만 유지)

### 도움말 페이지 (11섹션 폴딩 가이드)
- 개요/목적, 시작하기, 플로그, 퀘스트, XP/등급(12단계 표), 랭킹, 구역 지도, 메시지, 프로필, 콘텐츠 정책, FAQ
- SVG 다이어그램 2개 (시스템 구성도, 퀘스트 흐름도)
- 초등학생~노약자 가독성 고려 (큰 글씨, 충분한 행간)

### v0.0.4a 패치

#### 리액션 시스템 수정
- 리액션(😮🥰😂 등) 선택 후 즉시 UI 반영되도록 수정
- 피드/타임라인 조회 시 `liked`, `my_reaction` 필드 추가 — 페이지 로드 시 정확한 이모지 표시
- `like_count`를 `post_likes` 테이블 실시간 COUNT로 변경 (캐시값 불일치 해소)

#### 댓글 시스템 수정
- 댓글 중복 생성 방지 (`_commentSubmitting` 플래그 + `event.preventDefault()`)
- 댓글 삭제 실패 수정: `id::text` 비교로 UUID/integer 타입 모두 대응
- `post_comments` 테이블 `init.sql` 정의 추가 + 서비스 시작 시 자동 생성

#### 비공개 퀘스트
- XP 300 이상 (Lv.3) 생성 조건 — 프론트엔드 + 백엔드 양쪽 검증
- 카드: 회색 배경 + 점선 테두리 + 🔒 아이콘
- 관리자는 탐색 탭에서 비공개 포함 전체 조회
- `my-quests` API에 `is_public` 필드 추가

#### 퀘스트 카운트다운 타이머
- 종료일 기준 실시간 카운트다운 (`⏱ 종료까지 2일 05:12:30`)
- 시작 전/진행 중/종료 상태 자동 전환
- 퀘스트 카드에 D-DAY 캘린더 배지 (남은 일수 / 진행 일째)
- Google 캘린더 추가 / 초대 메시지 전송 버튼

#### 제보 연동
- GIS `/pins/nearby` 수정: `trash_reports` + SNS 제보 게시글 통합 조회
- 플로그에서 위치 포함 제보 작성 → 구역 제보 목록 자동 반영

#### UI 개선
- 구역 현황: 테이블 → 폴딩 카드 (통계 + 진행률 바 + 관련 퀘스트)
- 제보 목록: 구역별 > 심각도별 2단계 그룹핑 + unknown 제거
- 피드 카드: Figma 스타일 (이미지 상단, 14px 라운드, hover 효과)
- 게시 전 "1분 이내 수정·삭제" 안내 표시
- 관리자 숨김 버튼 퀘스트 피드 통일
- 브랜딩: `메타` → `Meta`, 네비 폰트 800

---

## 18. 라이선스

본 프로젝트는 군산대학교 연구 과제로 개발되었습니다.

---

## 18. 문의

- **연구실**: 군산대학교
- **GitLab**: https://203.234.62.175:5443/system/meta-flow
