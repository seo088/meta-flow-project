# 메타 플로깅 SW 플랫폼 — 구축 계획서

> **서버**: spark-a6 (NVIDIA RTX A6000 48GB · RAM 32GB · 32 cores · 536GB 여유)
> **방식**: conda 가상환경 + Docker Compose 하이브리드
> **전략**: 더미 데이터 기반 전체 파이프라인 → 추후 실데이터 즉시 교체 가능

---

## 1. A6000 서버 한 대로 충분한가?

### 리소스 분석

```
┌──────────────────────────────────────────────────────────────┐
│                  A6000 서버 리소스 현황                         │
├────────────┬────────────┬──────────┬──────────────────────────┤
│ 리소스      │ 전체        │ 현재 사용 │ 메타 플로깅 필요량        │
├────────────┼────────────┼──────────┼──────────────────────────┤
│ GPU VRAM   │ 48GB       │ 2.7GB    │ ~16GB (YOLO8+RL+ResNet  │
│            │            │          │  +CLIP = 8+4+2+2)        │
│ RAM        │ 32GB       │ 6.9GB    │ ~18GB (Kafka2+ES2+PG2   │
│            │            │          │  +Redis4+서비스8)         │
│ CPU        │ 32 cores   │ 낮음      │ ~20 cores 피크           │
│ Disk       │ 536GB 여유  │ —        │ ~50GB (Docker+모델+데이터)│
│ 포트        │ 충분        │ 3개 사용  │ ~25개 필요               │
└────────────┴────────────┴──────────┴──────────────────────────┘
```

### 결론: ✅ 충분하지만 전략적 분리 필요

**GPU 48GB** → YOLO+RL+ResNet+CLIP 합계 16GB, 여유 32GB. **충분**.
**RAM 32GB** → 모든 인프라+서비스 합산 ~18GB. 피크 시 25GB. **빠듯하지만 가능**.
  - 핵심: ES/Kafka JVM 힙을 각 1GB로 제한 (기본 2GB에서 절감)
  - Redis maxmemory를 2GB로 조정 (4GB → 2GB)
**CPU 32코어** → 동시 1,000명 기준 충분.
**디스크** → 이미지/영상 축적 시 NAS 확장 검토.

### ⚠️ 포트 충돌 해결

현재 사용 중인 포트:
- :5432 PostgreSQL (lyric_db) → **meta-plogging용 별도 PG 컨테이너는 :5433으로 변경**
- :9200 Elasticsearch → **meta-plogging용 ES는 :9201로 변경**
- :5601 Kibana → 공유 가능

---

## 2. MCP Server와 Agents 운영 구조

### 왜 하이브리드(conda + Docker)인가?

```
┌─ Docker 영역 (인프라 + 서비스) ──────────────────────────────┐
│                                                               │
│  PostgreSQL  Redis  Kafka  MinIO  ES  Nginx  Kong             │
│  SNS-svc  Game-svc  GIS-svc  Data-svc  WS-svc                │
│  (컨테이너 간 plogging-net 브릿지 네트워크)                    │
│                                                               │
└───────────────────────────────────────────────────────────────┘
         ▲ 포트 노출: 5433, 6380, 9093, 9001, 9201, 8000-8200
         │
┌─ conda 영역 (GPU 필요 + 개발/디버깅) ───────────────────────┐
│                                                               │
│  conda env: meta-plogging (Python 3.11)                       │
│                                                               │
│  ┌─ MCP Server (:8100) ─────────────────────────────────┐    │
│  │  FastMCP SSE 허브                                     │    │
│  │  7개 도구: detect, verify, pins, route, points...     │    │
│  │  → Docker 인프라 연결: localhost:5433, 6380, 9093     │    │
│  └──────────────────────────────────────────────────────┘    │
│                                                               │
│  ┌─ AI Service (:8004) ─────────────────────────────────┐    │
│  │  TorchServe 또는 직접 GPU 추론                        │    │
│  │  YOLO v8 + RL + ResNet + CLIP                        │    │
│  │  → CUDA:0 직접 접근 (Docker GPU 패스스루 불필요)       │    │
│  └──────────────────────────────────────────────────────┘    │
│                                                               │
│  ┌─ Agents (프로세스) ──────────────────────────────────┐    │
│  │  DroneAgent     → MCP SSE 클라이언트 (:8301)         │    │
│  │  MobilityAgent  → MCP SSE 클라이언트                  │    │
│  │  QuestAgent     → Kafka Consumer + MCP 호출           │    │
│  │  DataAgent      → Kafka Consumer + MinIO (:8302)      │    │
│  │  SchedulerAgent → APScheduler cron + MCP 호출         │    │
│  └──────────────────────────────────────────────────────┘    │
│                                                               │
│  → GPU 직접 접근, 빠른 디버깅, import 공유                    │
└───────────────────────────────────────────────────────────────┘
```

### MCP ↔ Agent 통신 흐름

```
1. 에이전트 시작 시 MCP Server(:8100/sse)에 SSE 연결
2. 에이전트는 MCP 도구를 tool_call로 호출
3. MCP Server가 실제 작업 수행 (DB, Kafka, TorchServe)
4. 결과를 에이전트에 SSE로 반환

예시: DroneAgent가 드론 영상 처리

  DroneAgent                MCP Server              AI Service
     │                         │                       │
     ├──SSE connect────────────▶                       │
     │                         │                       │
     ├──call: detect_trash────▶│                       │
     │                         ├──POST /predictions──▶│
     │                         │◀──{class,conf}───────┤
     │                         ├──UPDATE DB            │
     │                         ├──Kafka produce        │
     │◀──{trash_type,sev}──────┤                       │
     │                         │                       │
     ├──call: optimize_route──▶│                       │
     │                         ├──POST RL agent──────▶│
     │◀──{waypoints}──────────┤                       │
```

### 프로세스 관리

```bash
# conda 환경에서 supervisord 또는 개별 터미널로 관리
# 개발 단계: 각각 터미널에서 실행
# 운영 단계: systemd 유닛 또는 PM2/supervisord

# 터미널 1: MCP Server
python services/mcp/server.py

# 터미널 2: AI Service (GPU)
python services/ai/main.py

# 터미널 3-7: Agents
python agents/drone_agent/agent.py
python agents/quest_agent/agent.py
python agents/data_agent/agent.py
python agents/scheduler_agent/agent.py
```

---

## 3. 프로젝트 디렉토리 구조 (실제 구축)

```
/home/spark/research/meta-flow/
├── docs/                          # 설계 문서 (기존 md 파일들)
│   ├── 00_overview.md ~ 07_deployment.md
│   └── viz_*.html
│
├── BUILD_PLAN.md                  # ← 이 문서
│
├── platform/                      # ★ 실제 코드 루트
│   │
│   ├── shared/core/               # 공통 라이브러리 (conda 환경)
│   │   ├── __init__.py
│   │   ├── types.py               # Pydantic 타입
│   │   ├── events.py              # Kafka 이벤트 스키마
│   │   ├── kafka_client.py        # Producer/Consumer
│   │   ├── auth.py                # JWT
│   │   ├── gamification.py        # XP/레벨
│   │   ├── logger.py              # 통합 로거
│   │   └── dummy/                 # ★ 더미 데이터 생성기
│   │       ├── __init__.py
│   │       ├── generators.py      # 위치/이미지/영상 더미
│   │       ├── scenarios.py       # 시나리오 시뮬레이터
│   │       └── seed_data.py       # DB 시드
│   │
│   ├── services/                  # Docker 컨테이너 서비스
│   │   ├── sns/                   # :8001
│   │   │   ├── Dockerfile
│   │   │   ├── requirements.txt
│   │   │   └── main.py
│   │   ├── game/                  # :8002
│   │   ├── gis/                   # :8003
│   │   ├── ai/                    # :8004 (conda에서 직접 실행)
│   │   │   ├── main.py
│   │   │   ├── models/            # ★ 더미 모델 (실제 모델 교체 지점)
│   │   │   │   ├── dummy_yolo.py
│   │   │   │   ├── dummy_rl.py
│   │   │   │   └── dummy_verify.py
│   │   │   └── requirements.gpu.txt
│   │   ├── data/                  # :8005
│   │   ├── mcp/                   # :8100 (conda에서 직접 실행)
│   │   │   ├── server.py
│   │   │   └── requirements.txt
│   │   ├── ws/                    # :8200
│   │   └── gateway/
│   │       └── kong.yml
│   │
│   ├── agents/                    # conda에서 직접 실행
│   │   ├── drone_agent/
│   │   │   ├── agent.py
│   │   │   └── simulator.py       # ★ 드론 더미 시뮬레이터
│   │   ├── quest_agent/
│   │   ├── data_agent/
│   │   ├── mobility_agent/
│   │   │   ├── agent.py
│   │   │   └── simulator.py       # ★ 모빌리티 더미 시뮬레이터
│   │   └── scheduler_agent/
│   │
│   ├── infra/                     # Docker 인프라 설정
│   │   ├── docker/
│   │   │   ├── docker-compose.yml
│   │   │   └── docker-compose.override.yml
│   │   ├── postgres/
│   │   │   ├── init.sql
│   │   │   └── seed.sql
│   │   ├── kafka/
│   │   │   └── create_topics.sh
│   │   ├── elasticsearch/
│   │   │   └── reports_mapping.json
│   │   ├── nginx/
│   │   │   └── nginx.conf
│   │   └── monitoring/
│   │       ├── prometheus.yml
│   │       └── alert_rules.yml
│   │
│   ├── apps/                      # 프론트엔드
│   │   ├── web/                   # Next.js 14
│   │   ├── admin/                 # Vite + React
│   │   └── mobile/                # React Native (별도)
│   │
│   ├── scripts/
│   │   ├── init.sh                # 전체 초기화
│   │   ├── start_conda.sh         # conda 서비스 일괄 시작
│   │   ├── stop_all.sh
│   │   ├── seed_dummy.py          # 더미 데이터 주입
│   │   └── run_scenario.py        # 시나리오 시뮬레이션
│   │
│   ├── .env.example
│   ├── environment.yml            # conda 환경 정의
│   └── pyproject.toml             # Python 프로젝트 설정
```

---

## 4. 단계별 구축 계획

### Phase 0: 환경 준비 (1일)

```bash
# 1. conda 환경 생성
conda create -n meta-plogging python=3.11 -y
conda activate meta-plogging
pip install fastapi uvicorn asyncpg confluent-kafka redis
pip install pydantic python-jose httpx minio
pip install fastmcp mcp apscheduler
pip install torch torchvision ultralytics  # GPU
pip install stable-baselines3              # RL

# 2. Docker 인프라 기동 (기존 서비스와 포트 분리)
# PostgreSQL :5433, Redis :6380, Kafka :9093, MinIO :9001, ES :9201
docker compose -f infra/docker/docker-compose.yml up -d

# 3. DB 초기화 + 더미 시드 데이터
python scripts/seed_dummy.py
```

### Phase 1: 인프라 + 공통 라이브러리 (2일)

| 작업 | 산출물 | 더미 전략 |
|------|--------|----------|
| Docker Compose 작성 | docker-compose.yml | 포트 충돌 회피 |
| PostgreSQL + PostGIS init | init.sql, seed.sql | 지오펜싱 4구역 + 사용자 50명 + 제보 200건 |
| Kafka 토픽 생성 | create_topics.sh | 7개 토픽 |
| Redis 키 초기화 | seed_dummy.py | 랭킹 50명 + 캐시 |
| shared/core/ 구현 | types, events, kafka, auth, gamification | — |
| 더미 생성기 | shared/core/dummy/ | 좌표·이미지URL·영상URL 생성 |

### Phase 2: 백엔드 마이크로서비스 (3일)

| 서비스 | 포트 | 더미 전략 |
|--------|------|----------|
| SNS Service | :8001 | 이미지 URL만 저장 (실제 업로드 생략), GPS 더미 좌표 |
| Game Service | :8002 | Redis ZADD로 랭킹 시뮬레이션 |
| GIS Service | :8003 | PostGIS 실제 쿼리, 더미 좌표 |
| AI Service | :8004 | **더미 모델** (random confidence 반환) → 실제 YOLO 교체 지점 |
| Data Service | :8005 | ES 집계 실동작 |
| WebSocket | :8200 | Redis PubSub 실동작 |

### Phase 3: MCP Server + Agents (2일)

| 컴포넌트 | 더미 전략 |
|----------|----------|
| MCP Server | 7개 도구 모두 구현, AI는 더미 모델 호출 |
| DroneAgent | 시뮬레이터에서 가상 RTSP 이벤트 생성 |
| MobilityAgent | 가상 수거 경로 완료 이벤트 생성 |
| QuestAgent | 실동작 (Kafka Consumer) |
| DataAgent | GeoJSON/COCO 실제 생성 (더미 데이터 기반) |
| SchedulerAgent | APScheduler 실동작 |

### Phase 4: 프론트엔드 (2일)

| 앱 | 전략 |
|----|------|
| Admin Dashboard (Vite) | 관리자 대시보드 먼저 구축, API 연동 |
| Web Portal (Next.js) | 공개 핀맵 + 통계 페이지 |
| Mobile (React Native) | 별도 일정, 웹 우선 |

### Phase 5: 시나리오 시뮬레이션 (1일)

```python
# scripts/run_scenario.py
# 군산시 가상 시나리오 3가지를 자동으로 실행

시나리오 1: "학생 플로깅 데이"
  → 50명이 군산대 캠퍼스에서 동시 플로깅
  → 100건 제보 → AI 탐지 → 게미피케이션 전체 플로우

시나리오 2: "드론 순찰 + 모빌리티 수거"
  → 은파유원지 드론 3대 순찰
  → YOLO 탐지 → 최적 경로 → 모빌리티 디스패치

시나리오 3: "보스 레이드 이벤트"
  → 새만금 대형 폐기물 발견
  → 보스 퀘스트 자동 생성 → 3인 팀 참여 → 완료
```

---

## 5. 더미 → 실데이터 교체 지점

```
★ = 더미에서 실제로 교체할 때 변경할 파일 (1개씩만)

┌─────────────────────┬──────────────────┬───────────────────────┐
│ 컴포넌트             │ 더미 상태         │ 실데이터 교체 방법      │
├─────────────────────┼──────────────────┼───────────────────────┤
│ ★ YOLO 탐지          │ random conf 반환  │ models/dummy_yolo.py  │
│                     │                  │ → ultralytics YOLO    │
│                     │                  │ 가중치 교체만 하면 끝   │
├─────────────────────┼──────────────────┼───────────────────────┤
│ ★ RL 경로            │ 단순 거리순 정렬   │ models/dummy_rl.py    │
│                     │                  │ → SB3 PPO/SAC 모델    │
├─────────────────────┼──────────────────┼───────────────────────┤
│ ★ ResNet 인증        │ random score 반환 │ models/dummy_verify.py│
│                     │                  │ → torchvision ResNet  │
├─────────────────────┼──────────────────┼───────────────────────┤
│ ★ 이미지 업로드       │ URL 문자열만 저장  │ MinIO put_object      │
│                     │                  │ 활성화만 하면 끝       │
├─────────────────────┼──────────────────┼───────────────────────┤
│ ★ 드론 RTSP          │ 시뮬레이터 이벤트  │ RTSP URL 교체         │
│                     │                  │ OpenCV cap 동일 코드   │
├─────────────────────┼──────────────────┼───────────────────────┤
│ ★ 모빌리티 연동       │ HTTP webhook 시뮬 │ 실 모빌리티 SDK 연동   │
├─────────────────────┼──────────────────┼───────────────────────┤
│ ★ GPS 좌표           │ 군산시 범위 랜덤   │ 실제 GPS 수신으로 교체 │
├─────────────────────┼──────────────────┼───────────────────────┤
│ ★ 사용자 인증         │ 고정 JWT 토큰     │ 실제 회원가입/로그인   │
└─────────────────────┴──────────────────┴───────────────────────┘

핵심: 모든 더미 컴포넌트는 실제 컴포넌트와 동일한 인터페이스(입출력)를 사용.
      .env 변수 + 1개 파일 교체만으로 전환 가능.
```

---

## 6. 환경 설정 파일

### `environment.yml` (conda)

```yaml
name: meta-plogging
channels:
  - pytorch
  - nvidia
  - conda-forge
  - defaults
dependencies:
  - python=3.11
  - pip
  - nodejs=20
  - pip:
    # 백엔드
    - fastapi==0.111.0
    - uvicorn[standard]==0.30.0
    - asyncpg==0.29.0
    - confluent-kafka==2.5.0
    - redis[hiredis]==5.0.7
    - pydantic==2.8.0
    - python-jose[cryptography]==3.3.0
    - httpx==0.27.0
    - minio==7.2.7
    - apscheduler==3.10.4
    # MCP
    - fastmcp>=2.0.0
    - mcp>=1.0.0
    # AI/ML
    - torch==2.3.0
    - torchvision==0.18.0
    - ultralytics==8.2.0
    - stable-baselines3==2.3.0
    - opencv-python-headless==4.10.0.84
    - scikit-image==0.23.2
    # 유틸
    - faker==25.0.0
    - rich==13.7.0
    - typer==0.12.0
```

### `.env.example` (포트 충돌 회피 버전)

```bash
# === 프로젝트 ===
COMPOSE_PROJECT_NAME=meta-plogging
ENVIRONMENT=development

# === PostgreSQL (기존 :5432 사용 중 → :5433) ===
POSTGRES_HOST=localhost
POSTGRES_PORT=5433
POSTGRES_USER=plogging
POSTGRES_PASSWORD=<YOUR_DB_PASSWORD>
POSTGRES_DB=plogging_db
DATABASE_URL=postgresql://plogging:<YOUR_DB_PASSWORD>@localhost:5433/plogging_db

# === Redis (기존 미사용 → :6380) ===
REDIS_HOST=localhost
REDIS_PORT=6380
REDIS_PASSWORD=<YOUR_REDIS_PASSWORD>

# === Kafka (→ :9093) ===
KAFKA_BOOTSTRAP_SERVERS=localhost:9093

# === MinIO (→ :9001/:9002) ===
MINIO_ENDPOINT=localhost:9001
MINIO_ACCESS_KEY=plogging_admin
MINIO_SECRET_KEY=<YOUR_MINIO_SECRET>
MINIO_BUCKET_IMAGES=plogging-images
MINIO_BUCKET_VIDEOS=plogging-videos

# === Elasticsearch (기존 :9200 사용 중 → :9201) ===
ES_HOST=localhost
ES_PORT=9201

# === JWT ===
JWT_SECRET_KEY=<YOUR_JWT_SECRET>
JWT_ALGORITHM=HS256
JWT_ACCESS_EXPIRE_MINUTES=1440

# === AI (로컬 conda 직접 실행) ===
AI_MODE=dummy
TORCHSERVE_HOST=localhost
TORCHSERVE_PORT=7070
YOLO_MODEL_NAME=yolo_plogging_v8
VERIFY_MODEL_NAME=cleanup_verify_v1
GPU_DEVICE=cuda:0

# === MCP ===
MCP_SERVER_URL=http://localhost:8100/sse

# === 모니터링 ===
GRAFANA_ADMIN_PASSWORD=admin
```

---

## 7. Docker Compose (인프라 전용, 포트 분리)

```yaml
version: "3.9"

networks:
  plogging-net:
    driver: bridge

volumes:
  pg_plogging_data:
  redis_plogging_data:
  kafka_plogging_data:
  zk_plogging_data:
  minio_plogging_data:
  es_plogging_data:

services:

  postgres-plogging:
    image: postgis/postgis:16-3.4
    restart: unless-stopped
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
    command: >
      postgres
      -c max_connections=200
      -c shared_buffers=1GB
      -c effective_cache_size=3GB
      -c work_mem=8MB
    volumes:
      - pg_plogging_data:/var/lib/postgresql/data
      - ./infra/postgres/init.sql:/docker-entrypoint-initdb.d/01_init.sql
      - ./infra/postgres/seed.sql:/docker-entrypoint-initdb.d/02_seed.sql
    ports:
      - "5433:5432"
    networks: [plogging-net]
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER}"]
      interval: 10s
      retries: 5

  redis-plogging:
    image: redis:7.2-alpine
    restart: unless-stopped
    command: >
      redis-server
      --requirepass ${REDIS_PASSWORD}
      --maxmemory 2gb
      --maxmemory-policy allkeys-lru
    volumes:
      - redis_plogging_data:/data
    ports:
      - "6380:6379"
    networks: [plogging-net]

  zookeeper-plogging:
    image: confluentinc/cp-zookeeper:7.5.0
    restart: unless-stopped
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
    volumes:
      - zk_plogging_data:/var/lib/zookeeper
    networks: [plogging-net]

  kafka-plogging:
    image: confluentinc/cp-kafka:7.5.0
    restart: unless-stopped
    depends_on: [zookeeper-plogging]
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper-plogging:2181
      KAFKA_ADVERTISED_LISTENERS: INSIDE://kafka-plogging:9092,OUTSIDE://localhost:9093
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: INSIDE:PLAINTEXT,OUTSIDE:PLAINTEXT
      KAFKA_INTER_BROKER_LISTENER_NAME: INSIDE
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_NUM_PARTITIONS: 8
      KAFKA_LOG_RETENTION_HOURS: 168
      KAFKA_HEAP_OPTS: "-Xmx1G -Xms1G"
    volumes:
      - kafka_plogging_data:/var/lib/kafka/data
    ports:
      - "9093:9093"
    networks: [plogging-net]

  minio-plogging:
    image: minio/minio:latest
    restart: unless-stopped
    command: server /data --console-address ":9002"
    environment:
      MINIO_ROOT_USER: ${MINIO_ACCESS_KEY}
      MINIO_ROOT_PASSWORD: ${MINIO_SECRET_KEY}
    volumes:
      - minio_plogging_data:/data
    ports:
      - "9001:9000"
      - "9002:9002"
    networks: [plogging-net]

  es-plogging:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.14.1
    restart: unless-stopped
    environment:
      discovery.type: single-node
      xpack.security.enabled: "false"
      ES_JAVA_OPTS: "-Xms1g -Xmx1g"
    ulimits:
      memlock: { soft: -1, hard: -1 }
    volumes:
      - es_plogging_data:/usr/share/elasticsearch/data
    ports:
      - "9201:9200"
    networks: [plogging-net]
```

---

## 8. 구축 실행 순서

```
Week 1
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Day 1  ▸ conda 환경 + Docker 인프라 기동 + DB 초기화
Day 2  ▸ shared/core/ 전체 + 더미 생성기
Day 3  ▸ SNS Service + GIS Service (PostGIS 연동)
Day 4  ▸ Game Service + WebSocket Server
Day 5  ▸ AI Service (더미 모델) + Data Service

Week 2
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Day 6  ▸ MCP Server 7개 도구 구현
Day 7  ▸ DroneAgent + MobilityAgent + 시뮬레이터
Day 8  ▸ QuestAgent + DataAgent + SchedulerAgent
Day 9  ▸ Admin Dashboard (Vite) + 핀맵 웹
Day 10 ▸ 시나리오 시뮬레이션 + 통합 테스트

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
이후   ▸ 실제 YOLO 학습/배포, React Native 앱, 공공데이터 연계
```

---

## 9. RAM 32GB 최적화 전략

```
┌────────────────────┬──────────┬──────────────────────┐
│ 서비스              │ 할당 RAM │ 최적화 설정            │
├────────────────────┼──────────┼──────────────────────┤
│ PostgreSQL         │ 2 GB     │ shared_buffers=1GB   │
│ Elasticsearch      │ 2 GB     │ -Xms1g -Xmx1g       │
│ Kafka              │ 1.5 GB   │ -Xmx1G              │
│ Redis              │ 2 GB     │ maxmemory 2gb        │
│ MinIO              │ 0.5 GB   │ 기본                  │
│ ZooKeeper          │ 0.3 GB   │ 기본                  │
├────────────────────┼──────────┼──────────────────────┤
│ FastAPI 서비스 ×5   │ 3 GB     │ workers=2 (절반)     │
│ MCP Server         │ 0.5 GB   │ —                    │
│ AI Service (CPU)   │ 2 GB     │ GPU VRAM 별도         │
│ Agents ×5          │ 1.5 GB   │ 각 ~300MB            │
│ Next.js / Vite     │ 0.5 GB   │ dev 서버             │
├────────────────────┼──────────┼──────────────────────┤
│ OS + 기타          │ 3 GB     │ —                    │
├────────────────────┼──────────┼──────────────────────┤
│ 합계               │ ~19 GB   │ 여유: ~12GB          │
│ GPU VRAM 별도       │ ~16 GB   │ 여유: ~32GB          │
└────────────────────┴──────────┴──────────────────────┘
```
