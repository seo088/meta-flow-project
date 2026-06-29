# 메타 플로깅 — 인프라 환경 설계

> **참조**: `00_overview.md` §7 포트 맵  
> **환경**: Ubuntu 22.04 LTS · Docker Compose v2 · NVIDIA RTX A6000  
> **목표 동시접속**: 1,000명

---

## 1. 디렉터리 구조

```
/opt/meta-plogging/
├── docker-compose.yml          # 인프라 + 서비스
├── docker-compose.gpu.yml      # AI 서비스 GPU 오버라이드
├── docker-compose.agents.yml   # 에이전트 컴포즈
├── .env                        # 실제 환경변수 (Git 제외)
├── .env.example                # 환경변수 템플릿
├── services/                   # 마이크로서비스 (02_backend.md)
├── agents/                     # 에이전트 (03_agents_mcp.md)
├── shared/core/                # 공통 라이브러리 — 여기서만 정의
│   ├── types.py
│   ├── events.py
│   ├── kafka_client.py
│   ├── auth.py
│   ├── logger.py
│   └── gamification.py
├── infra/
│   ├── postgres/init.sql
│   ├── kafka/
│   ├── elasticsearch/
│   ├── nginx/nginx.conf
│   ├── torchserve/config.properties
│   └── monitoring/
├── models/                     # YOLO·RL·검증 모델 가중치
└── scripts/
    ├── init.sh
    ├── create_kafka_topics.sh
    └── backup.sh
```

---

## 2. `.env.example`

```bash
# === 기본 ===
COMPOSE_PROJECT_NAME=meta-plogging
ENVIRONMENT=development

# === PostgreSQL ===
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_USER=plogging
POSTGRES_PASSWORD=CHANGE_ME_STRONG_PW
POSTGRES_DB=plogging_db
DATABASE_URL=postgresql://plogging:CHANGE_ME_STRONG_PW@postgres:5432/plogging_db

# === Redis ===
REDIS_HOST=redis
REDIS_PORT=6379
REDIS_PASSWORD=CHANGE_ME_REDIS_PW

# === Kafka ===
KAFKA_BOOTSTRAP_SERVERS=kafka:9092

# === MinIO ===
MINIO_ENDPOINT=minio:9000
MINIO_ACCESS_KEY=CHANGE_ME_ACCESS
MINIO_SECRET_KEY=CHANGE_ME_SECRET
MINIO_BUCKET_IMAGES=plogging-images
MINIO_BUCKET_VIDEOS=plogging-videos

# === Elasticsearch ===
ES_HOST=elasticsearch
ES_PORT=9200

# === JWT ===
JWT_SECRET_KEY=CHANGE_ME_256BIT_SECRET
JWT_ALGORITHM=HS256
JWT_ACCESS_EXPIRE_MINUTES=30
JWT_REFRESH_EXPIRE_DAYS=30

# === AI (TorchServe) ===
TORCHSERVE_HOST=torchserve
TORCHSERVE_PORT=7070
YOLO_MODEL_NAME=yolo_plogging_v8
VERIFY_MODEL_NAME=cleanup_verify_v1
GPU_DEVICE=cuda:0

# === MCP ===
MCP_SERVER_URL=http://mcp-server:8100/sse

# === 모니터링 ===
GRAFANA_ADMIN_PASSWORD=CHANGE_ME
```

---

## 3. `docker-compose.yml` — 인프라 + 서비스

```yaml
version: "3.9"

x-restart: &restart
  restart: unless-stopped

x-logging: &logging
  logging:
    driver: json-file
    options: { max-size: "50m", max-file: "5" }

x-service-base: &service-base
  restart: unless-stopped
  logging:
    driver: json-file
    options: { max-size: "50m", max-file: "5" }

networks:
  plogging-net:
    driver: bridge

volumes:
  postgres_data:
  redis_data:
  kafka_data:
  zookeeper_data:
  minio_data:
  es_data:
  prometheus_data:
  grafana_data:
  torchserve_logs:

services:

  # ── 인프라 ──────────────────────────────────────────────

  postgres:
    image: postgis/postgis:15-3.3
    <<: *service-base
    environment:
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      POSTGRES_DB: ${POSTGRES_DB}
    command: >
      postgres
      -c max_connections=200
      -c shared_buffers=2GB
      -c effective_cache_size=6GB
      -c work_mem=16MB
      -c maintenance_work_mem=512MB
    volumes:
      - postgres_data:/var/lib/postgresql/data
      - ./infra/postgres/init.sql:/docker-entrypoint-initdb.d/01_init.sql
      - ./infra/postgres/seed.sql:/docker-entrypoint-initdb.d/02_seed.sql
    ports:
      - "5432:5432"
    networks:
      - plogging-net
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${POSTGRES_USER}"]
      interval: 10s
      timeout: 5s
      retries: 5

  redis:
    image: redis:7.2-alpine
    <<: *service-base
    command: >
      redis-server
      --requirepass ${REDIS_PASSWORD}
      --maxmemory 4gb
      --maxmemory-policy allkeys-lru
      --save 900 1 300 10
    volumes:
      - redis_data:/data
    ports:
      - "6379:6379"
    networks:
      - plogging-net
    healthcheck:
      test: ["CMD", "redis-cli", "-a", "${REDIS_PASSWORD}", "ping"]
      interval: 10s
      retries: 5

  zookeeper:
    image: confluentinc/cp-zookeeper:7.5.0
    <<: *service-base
    environment:
      ZOOKEEPER_CLIENT_PORT: 2181
      ZOOKEEPER_TICK_TIME: 2000
    volumes:
      - zookeeper_data:/var/lib/zookeeper
    networks:
      - plogging-net

  kafka:
    image: confluentinc/cp-kafka:7.5.0
    <<: *service-base
    depends_on:
      - zookeeper
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_NUM_PARTITIONS: 12
      KAFKA_LOG_RETENTION_HOURS: 168
      KAFKA_MESSAGE_MAX_BYTES: 10485760       # 10MB — 드론 키프레임
      KAFKA_REPLICA_FETCH_MAX_BYTES: 10485760
      KAFKA_HEAP_OPTS: "-Xmx2G -Xms2G"
    volumes:
      - kafka_data:/var/lib/kafka/data
    ports:
      - "9092:9092"
    networks:
      - plogging-net
    healthcheck:
      test: ["CMD", "kafka-broker-api-versions", "--bootstrap-server", "kafka:9092"]
      interval: 15s
      timeout: 10s
      retries: 5

  minio:
    image: minio/minio:RELEASE.2024-01-16T16-07-38Z
    <<: *service-base
    command: server /data --console-address ":9001"
    environment:
      MINIO_ROOT_USER: ${MINIO_ACCESS_KEY}
      MINIO_ROOT_PASSWORD: ${MINIO_SECRET_KEY}
    volumes:
      - minio_data:/data
    ports:
      - "9000:9000"
      - "9001:9001"
    networks:
      - plogging-net
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9000/minio/health/live"]
      interval: 30s
      retries: 3

  elasticsearch:
    image: docker.elastic.co/elasticsearch/elasticsearch:8.11.0
    <<: *service-base
    environment:
      discovery.type: single-node
      xpack.security.enabled: "false"
      ES_JAVA_OPTS: "-Xms2g -Xmx2g"
    ulimits:
      memlock: { soft: -1, hard: -1 }
    volumes:
      - es_data:/usr/share/elasticsearch/data
    ports:
      - "9200:9200"
    networks:
      - plogging-net
    healthcheck:
      test: ["CMD-SHELL", "curl -sf http://localhost:9200/_cluster/health"]
      interval: 20s
      retries: 5

  # ── API Gateway ─────────────────────────────────────────

  kong:
    image: kong:3.5
    <<: *service-base
    environment:
      KONG_DATABASE: "off"
      KONG_DECLARATIVE_CONFIG: /kong/declarative/kong.yml
      KONG_PROXY_LISTEN: "0.0.0.0:8000"
      KONG_ADMIN_LISTEN: "0.0.0.0:8001"
    volumes:
      - ./services/gateway/kong.yml:/kong/declarative/kong.yml
    ports:
      - "8000:8000"
      - "8001:8001"
    networks:
      - plogging-net

  nginx:
    image: nginx:1.25-alpine
    <<: *service-base
    volumes:
      - ./infra/nginx/nginx.conf:/etc/nginx/nginx.conf:ro
    ports:
      - "80:80"
      - "443:443"
    depends_on:
      - kong
    networks:
      - plogging-net

  # ── 마이크로서비스 ──────────────────────────────────────

  sns-service:
    build: { context: ., dockerfile: services/sns/Dockerfile }
    <<: *service-base
    env_file: .env
    environment: { SERVICE_NAME: sns-service, PORT: "8001" }
    ports: ["8001:8001"]
    depends_on:
      postgres: { condition: service_healthy }
      kafka:    { condition: service_healthy }
      redis:    { condition: service_healthy }
    networks: [plogging-net]

  game-service:
    build: { context: ., dockerfile: services/game/Dockerfile }
    <<: *service-base
    env_file: .env
    environment: { SERVICE_NAME: game-service, PORT: "8002" }
    ports: ["8002:8002"]
    depends_on:
      postgres: { condition: service_healthy }
      kafka:    { condition: service_healthy }
      redis:    { condition: service_healthy }
    networks: [plogging-net]

  gis-service:
    build: { context: ., dockerfile: services/gis/Dockerfile }
    <<: *service-base
    env_file: .env
    environment: { SERVICE_NAME: gis-service, PORT: "8003" }
    ports: ["8003:8003"]
    depends_on:
      postgres: { condition: service_healthy }
      kafka:    { condition: service_healthy }
    networks: [plogging-net]

  data-service:
    build: { context: ., dockerfile: services/data/Dockerfile }
    <<: *service-base
    env_file: .env
    environment: { SERVICE_NAME: data-service, PORT: "8005" }
    ports: ["8005:8005"]
    depends_on:
      postgres:      { condition: service_healthy }
      elasticsearch: { condition: service_healthy }
    networks: [plogging-net]

  mcp-server:
    build: { context: ., dockerfile: services/mcp/Dockerfile }
    <<: *service-base
    env_file: .env
    environment: { SERVICE_NAME: mcp-server, PORT: "8100" }
    ports: ["8100:8100"]
    depends_on:
      postgres: { condition: service_healthy }
      kafka:    { condition: service_healthy }
      redis:    { condition: service_healthy }
    networks: [plogging-net]

  ws-server:
    build: { context: ., dockerfile: services/ws/Dockerfile }
    <<: *service-base
    env_file: .env
    environment: { SERVICE_NAME: ws-server, PORT: "8200" }
    ports: ["8200:8200"]
    depends_on: [redis, kafka]
    networks: [plogging-net]

  # ── 모니터링 ────────────────────────────────────────────

  prometheus:
    image: prom/prometheus:v2.48.0
    <<: *restart
    volumes:
      - ./infra/monitoring/prometheus.yml:/etc/prometheus/prometheus.yml
      - prometheus_data:/prometheus
    ports: ["9090:9090"]
    networks: [plogging-net]

  grafana:
    image: grafana/grafana:10.2.0
    <<: *restart
    environment:
      GF_SECURITY_ADMIN_PASSWORD: ${GRAFANA_ADMIN_PASSWORD}
    volumes:
      - grafana_data:/var/lib/grafana
    ports: ["3000:3000"]
    networks: [plogging-net]
```

---

## 4. `docker-compose.gpu.yml` — AI 오버라이드

```yaml
version: "3.9"

services:
  ai-service:
    build: { context: ., dockerfile: services/ai/Dockerfile.gpu }
    restart: unless-stopped
    env_file: .env
    environment: { SERVICE_NAME: ai-service, PORT: "8004" }
    ports: ["8004:8004"]
    deploy:
      resources:
        reservations:
          devices:
            - driver: nvidia
              count: 1
              capabilities: [gpu]
    depends_on:
      kafka: { condition: service_healthy }
    networks: [plogging-net]

  torchserve:
    image: pytorch/torchserve:0.9.0-gpu
    restart: unless-stopped
    volumes:
      - ./models:/home/model-server/model-store
      - ./infra/torchserve/config.properties:/home/model-server/config.properties
      - torchserve_logs:/home/model-server/logs
    ports: ["7070:7070", "7071:7071"]
    deploy:
      resources:
        reservations:
          devices:
            - driver: nvidia
              count: 1
              capabilities: [gpu]
    networks: [plogging-net]
```

---

## 5. PostgreSQL 초기화 (`infra/postgres/init.sql`)

```sql
-- PostGIS 및 확장
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS postgis_topology;
CREATE EXTENSION IF NOT EXISTS pg_trgm;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ── 사용자 ──────────────────────────────────────────────
CREATE TABLE users (
    id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    username      VARCHAR(50)  UNIQUE NOT NULL,
    email         VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    display_name  VARCHAR(100),
    avatar_url    TEXT,
    role          VARCHAR(20) DEFAULT 'user',   -- user|admin|drone_operator
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    updated_at    TIMESTAMPTZ DEFAULT NOW()
);

-- ── 지오펜싱 구역 ────────────────────────────────────────
CREATE TABLE geofence_zones (
    id               UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    zone_key         VARCHAR(50) UNIQUE NOT NULL,
    name             VARCHAR(100) NOT NULL,
    geom             GEOMETRY(POLYGON, 4326) NOT NULL,
    phase            INTEGER DEFAULT 1,
    bonus_multiplier DECIMAL(4,2) DEFAULT 1.0,
    is_active        BOOLEAN DEFAULT TRUE,
    created_at       TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_zones_geom ON geofence_zones USING GIST(geom);

-- ── 쓰레기 제보 ─────────────────────────────────────────
CREATE TABLE trash_reports (
    id            UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    reporter_id   UUID REFERENCES users(id) ON DELETE SET NULL,
    location      GEOMETRY(POINT, 4326) NOT NULL,
    zone_id       UUID REFERENCES geofence_zones(id),
    trash_type    VARCHAR(50),
    severity      VARCHAR(20),
    status        VARCHAR(30) DEFAULT 'pending',
    image_urls    TEXT[],
    video_url     TEXT,
    source        VARCHAR(30),           -- sns|drone|mobility|unity_sim
    ai_confidence DECIMAL(5,4),
    ai_label      JSONB,
    created_at    TIMESTAMPTZ DEFAULT NOW(),
    updated_at    TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_reports_location ON trash_reports USING GIST(location);
CREATE INDEX idx_reports_zone_status ON trash_reports(zone_id, status)
    WHERE status != 'completed';
CREATE INDEX idx_reports_created ON trash_reports(created_at DESC);

-- ── 청소 인증 ────────────────────────────────────────────
CREATE TABLE cleanups (
    id             UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    report_id      UUID REFERENCES trash_reports(id) ON DELETE CASCADE,
    cleaner_id     UUID REFERENCES users(id) ON DELETE SET NULL,
    cleaner_type   VARCHAR(20),          -- human|drone|mobility
    before_image   TEXT,
    after_image    TEXT,
    verify_score   DECIMAL(5,4),
    verified       BOOLEAN DEFAULT FALSE,
    points_awarded INTEGER DEFAULT 0,
    completed_at   TIMESTAMPTZ DEFAULT NOW()
);

-- ── 드론 이벤트 ─────────────────────────────────────────
CREATE TABLE drone_events (
    id             UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    drone_id       VARCHAR(50) NOT NULL,
    event_type     VARCHAR(30),
    location       GEOMETRY(POINT, 4326),
    zone_id        UUID REFERENCES geofence_zones(id),
    video_url      TEXT,
    detected_count INTEGER DEFAULT 0,
    payload        JSONB,
    created_at     TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_drone_location ON drone_events USING GIST(location);
CREATE INDEX idx_drone_created  ON drone_events(created_at DESC);

-- ── 포인트 원장 ──────────────────────────────────────────
CREATE TABLE point_ledger (
    id         UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id    UUID REFERENCES users(id) ON DELETE CASCADE,
    delta      INTEGER NOT NULL,
    reason     VARCHAR(100) NOT NULL,
    ref_id     UUID,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (user_id, ref_id, reason)    -- 중복 지급 방지
);
CREATE INDEX idx_ledger_user    ON point_ledger(user_id);
CREATE INDEX idx_ledger_created ON point_ledger(created_at DESC);

-- 포인트 집계 Materialized View
CREATE MATERIALIZED VIEW user_points AS
SELECT user_id, SUM(delta) AS total_points
FROM point_ledger GROUP BY user_id;
CREATE UNIQUE INDEX ON user_points(user_id);

CREATE OR REPLACE FUNCTION refresh_user_points() RETURNS void AS $$
BEGIN REFRESH MATERIALIZED VIEW CONCURRENTLY user_points; END;
$$ LANGUAGE plpgsql;

-- ── 배지 정의 ────────────────────────────────────────────
CREATE TABLE badge_definitions (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    badge_key   VARCHAR(100) UNIQUE NOT NULL,
    name        VARCHAR(200) NOT NULL,
    description TEXT,
    icon_url    TEXT,
    rarity      VARCHAR(20) DEFAULT 'common'
);

CREATE TABLE user_badges (
    user_id   UUID REFERENCES users(id) ON DELETE CASCADE,
    badge_id  UUID REFERENCES badge_definitions(id),
    earned_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (user_id, badge_id)
);

-- ── 퀘스트 ──────────────────────────────────────────────
CREATE TABLE quest_definitions (
    id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    quest_key       VARCHAR(100) UNIQUE NOT NULL,
    title           VARCHAR(200) NOT NULL,
    description     TEXT,
    quest_type      VARCHAR(30),
    zone_id         UUID REFERENCES geofence_zones(id),
    target_count    INTEGER NOT NULL,
    reward_xp       INTEGER NOT NULL,
    reward_badge_id UUID REFERENCES badge_definitions(id),
    is_active       BOOLEAN DEFAULT TRUE,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE user_quests (
    id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id      UUID REFERENCES users(id) ON DELETE CASCADE,
    quest_id     UUID REFERENCES quest_definitions(id),
    progress     INTEGER DEFAULT 0,
    is_completed BOOLEAN DEFAULT FALSE,
    completed_at TIMESTAMPTZ,
    started_at   TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(user_id, quest_id)
);

-- ── SNS 게시물 ───────────────────────────────────────────
CREATE TABLE posts (
    id          UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id     UUID REFERENCES users(id) ON DELETE CASCADE,
    report_id   UUID REFERENCES trash_reports(id),
    content     TEXT,
    image_urls  TEXT[],
    hashtags    TEXT[],
    location    GEOMETRY(POINT, 4326),
    zone_id     UUID REFERENCES geofence_zones(id),
    post_type   VARCHAR(30) DEFAULT 'report',
    like_count  INTEGER DEFAULT 0,
    created_at  TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX idx_posts_user    ON posts(user_id);
CREATE INDEX idx_posts_created ON posts(created_at DESC);
CREATE INDEX idx_posts_hashtag ON posts USING GIN(hashtags);
CREATE INDEX idx_posts_zone    ON posts(zone_id, created_at DESC);

-- ── 공공데이터 배포 이력 ─────────────────────────────────
CREATE TABLE opendata_exports (
    id           UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    export_type  VARCHAR(50),
    file_url     TEXT NOT NULL,
    record_count INTEGER,
    date_from    DATE,
    date_to      DATE,
    created_at   TIMESTAMPTZ DEFAULT NOW()
);
```

---

## 6. 초기화 스크립트 (`scripts/init.sh`)

```bash
#!/bin/bash
set -euo pipefail

echo "=== 메타 플로깅 플랫폼 초기화 ==="

[ ! -f ".env" ] && { cp .env.example .env; echo "[!] .env 생성 완료. 비밀번호 설정 후 재실행하세요."; exit 1; }

# GPU 감지
GPU_COMPOSE=""
if command -v nvidia-smi &>/dev/null; then
    echo "[✓] GPU: $(nvidia-smi --query-gpu=name --format=csv,noheader)"
    GPU_COMPOSE="-f docker-compose.gpu.yml"
fi

# 1. 인프라 기동
echo "[*] 인프라 시작..."
docker compose up -d postgres redis zookeeper kafka minio elasticsearch

# 2. 헬스체크 대기
echo "[*] DB 준비 대기..."
until docker compose exec -T postgres pg_isready -U "${POSTGRES_USER}" &>/dev/null; do sleep 2; done
echo "[✓] PostgreSQL 준비"

until docker compose exec -T kafka kafka-broker-api-versions \
  --bootstrap-server kafka:9092 &>/dev/null; do sleep 3; done
echo "[✓] Kafka 준비"

# 3. Kafka 토픽 생성
bash scripts/create_kafka_topics.sh

# 4. MinIO 버킷 초기화
echo "[*] MinIO 버킷 생성..."
sleep 5
docker run --rm --network meta-plogging_plogging-net \
  minio/mc:latest sh -c "
    mc alias set local http://minio:9000 ${MINIO_ACCESS_KEY} ${MINIO_SECRET_KEY} &&
    mc mb --ignore-existing local/plogging-images &&
    mc mb --ignore-existing local/plogging-videos &&
    mc mb --ignore-existing local/plogging-opendata
  "

# 5. Elasticsearch 인덱스 생성
echo "[*] Elasticsearch 인덱스 생성..."
until curl -sf http://localhost:9200/_cluster/health &>/dev/null; do sleep 2; done
curl -sX PUT http://localhost:9200/plogging-reports \
     -H "Content-Type: application/json" \
     -d @infra/elasticsearch/reports_mapping.json > /dev/null
curl -sX PUT http://localhost:9200/plogging-posts \
     -H "Content-Type: application/json" \
     -d @infra/elasticsearch/posts_mapping.json > /dev/null

# 6. 전체 서비스 기동
echo "[*] 전체 서비스 기동..."
docker compose -f docker-compose.yml ${GPU_COMPOSE} -f docker-compose.agents.yml up -d

echo ""
echo "[✓] 초기화 완료!"
echo "  API Gateway : http://localhost:8000"
echo "  MCP Server  : http://localhost:8100"
echo "  Grafana     : http://localhost:3000"
echo "  MinIO       : http://localhost:9001"
```

---

## 7. Redis 키 설계

```
# 랭킹 (Sorted Set)
ZADD ranking:global            {xp}   {user_id}
ZADD ranking:zone:{zone_key}   {xp}   {user_id}
ZADD ranking:weekly:{YYYYWW}   {xp}   {user_id}

# 세션 (Hash, TTL 30분)
HSET session:{user_id}  token {jwt}  expires {ts}
EXPIRE session:{user_id} 1800

# API 캐시 (String, TTL 10-60초)
SET cache:zone_stats     {json}  EX 60
SET cache:leaderboard    {json}  EX 10

# 드론 실시간 위치 (GEO, TTL 30초)
GEOADD drones:live {lon} {lat} {drone_id}

# 중복 제보 방지 (String, TTL 1시간)
SET dedup:report:{lat3}:{lon3}  1  EX 3600

# 퀘스트 진행 캐시 (Hash, TTL 1일)
HSET quest:{user_id}:{quest_id}  count {n}
EXPIRE quest:{user_id}:{quest_id} 86400
```

---

## 8. GPU 환경 설정

```bash
# scripts/setup_gpu.sh
distribution=$(. /etc/os-release; echo $ID$VERSION_ID)
curl -fsSL https://nvidia.github.io/libnvidia-container/gpgkey | \
  sudo gpg --dearmor -o /usr/share/keyrings/nvidia-container-toolkit-keyring.gpg

curl -s -L https://nvidia.github.io/libnvidia-container/$distribution/libnvidia-container.list | \
  sed 's#deb https://#deb [signed-by=/usr/share/keyrings/nvidia-container-toolkit-keyring.gpg] https://#g' | \
  sudo tee /etc/apt/sources.list.d/nvidia-container-toolkit.list

sudo apt-get update && sudo apt-get install -y nvidia-container-toolkit
sudo nvidia-ctk runtime configure --runtime=docker
sudo systemctl restart docker

# 검증
docker run --rm --gpus all nvidia/cuda:12.1-base nvidia-smi
```
