# 메타 플로깅 — 배포 전략 & 모니터링

> **참조**: `01_infra.md` §3 Docker Compose, `03_agents_mcp.md` §7 에이전트 Compose  
> **목표**: 코드 push → 자동 빌드·테스트 → 무중단 배포 → 이상 감지·경보

---

## 1. CI/CD 파이프라인 (`.github/workflows/ci.yml`)

```yaml
name: CI/CD

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

env:
  REGISTRY:      ghcr.io
  IMAGE_PREFIX:  ${{ github.repository_owner }}/meta-plogging

jobs:
  # ── 1단계: 린트·타입·테스트 ────────────────────────────
  quality:
    runs-on: ubuntu-22.04
    steps:
      - uses: actions/checkout@v4

      - uses: actions/setup-python@v5
        with: { python-version: "3.11", cache: pip }

      - name: 공통 의존성 설치
        run: pip install -r shared/requirements.txt

      - name: Python 린트 (ruff)
        run: ruff check services/ agents/ shared/

      - name: Python 타입 (mypy)
        run: mypy services/ agents/ shared/ --ignore-missing-imports

      - name: 단위 테스트
        run: pytest tests/ -v --cov=shared --cov-report=xml

      - uses: actions/setup-node@v4
        with: { node-version: 20, cache: npm }

      - name: 프론트엔드 타입 검사
        run: npm ci && npm run type-check

  # ── 2단계: Docker 빌드 & 푸시 ──────────────────────────
  build:
    needs: quality
    runs-on: ubuntu-22.04
    if: github.ref == 'refs/heads/main'
    permissions:
      contents: read
      packages: write

    strategy:
      matrix:
        service: [sns, game, gis, ai, data, mcp, ws]

    steps:
      - uses: actions/checkout@v4

      - uses: docker/login-action@v3
        with:
          registry: ${{ env.REGISTRY }}
          username: ${{ github.actor }}
          password: ${{ secrets.GITHUB_TOKEN }}

      - uses: docker/build-push-action@v5
        with:
          context: .
          file: services/${{ matrix.service }}/Dockerfile
          push: true
          tags: |
            ${{ env.REGISTRY }}/${{ env.IMAGE_PREFIX }}-${{ matrix.service }}:latest
            ${{ env.REGISTRY }}/${{ env.IMAGE_PREFIX }}-${{ matrix.service }}:${{ github.sha }}
          cache-from: type=gha
          cache-to:   type=gha,mode=max

  # ── 3단계: spark-a6 서버 배포 ──────────────────────────
  deploy:
    needs: build
    runs-on: ubuntu-22.04
    if: github.ref == 'refs/heads/main'
    steps:
      - uses: appleboy/ssh-action@v1
        with:
          host:     ${{ secrets.SERVER_HOST }}
          username: ${{ secrets.SERVER_USER }}
          key:      ${{ secrets.SERVER_SSH_KEY }}
          script: |
            cd /opt/meta-plogging
            git pull origin main
            docker compose pull
            docker compose \
              -f docker-compose.yml \
              -f docker-compose.gpu.yml \
              -f docker-compose.agents.yml \
              up -d --no-deps --remove-orphans
            echo "[✓] 배포 완료: $(date)"
```

---

## 2. Nginx 설정 (`infra/nginx/nginx.conf`)

```nginx
worker_processes auto;
worker_rlimit_nofile 65535;

events {
    worker_connections 4096;
    use epoll;
    multi_accept on;
}

http {
    include      /etc/nginx/mime.types;
    sendfile     on;
    tcp_nopush   on;
    gzip         on;
    gzip_types   application/json application/javascript text/css application/geo+json;

    limit_req_zone $binary_remote_addr zone=api:10m    rate=30r/s;
    limit_req_zone $binary_remote_addr zone=upload:10m rate=5r/s;

    upstream kong    { server kong:8000;       keepalive 64; }
    upstream ws_srv  { server ws-server:8200;  keepalive 32; }

    server {
        listen 80;
        return 301 https://$host$request_uri;
    }

    server {
        listen 443 ssl http2;
        server_name meta-plogging.kunsan.ac.kr;

        ssl_certificate     /etc/nginx/ssl/cert.pem;
        ssl_certificate_key /etc/nginx/ssl/key.pem;
        ssl_protocols       TLSv1.2 TLSv1.3;
        ssl_session_cache   shared:SSL:10m;

        # API 요청
        location /api/ {
            limit_req zone=api burst=50 nodelay;
            proxy_pass         http://kong;
            proxy_http_version 1.1;
            proxy_set_header   Host             $host;
            proxy_set_header   X-Real-IP        $remote_addr;
            proxy_set_header   X-Forwarded-For  $proxy_add_x_forwarded_for;
            proxy_read_timeout 30s;
        }

        # 파일 업로드 (이미지·영상)
        location /api/v1/posts/report {
            limit_req             zone=upload burst=10 nodelay;
            client_max_body_size  50M;
            proxy_pass            http://kong;
            proxy_read_timeout    120s;
        }

        # WebSocket
        location /ws/ {
            proxy_pass         http://ws_srv;
            proxy_http_version 1.1;
            proxy_set_header   Upgrade    $http_upgrade;
            proxy_set_header   Connection "upgrade";
            proxy_set_header   Host       $host;
            proxy_read_timeout 3600s;
        }

        # MinIO 이미지 CDN
        location /media/ {
            proxy_pass          http://minio:9000/plogging-images/;
            add_header          Cache-Control "public, max-age=604800";
        }

        # 공공데이터 직접 다운로드
        location /opendata/ {
            proxy_pass          http://minio:9000/plogging-opendata/;
            add_header          Cache-Control "public, max-age=3600";
            add_header          Access-Control-Allow-Origin "*";
        }
    }
}
```

---

## 3. Prometheus 스크레이프 설정 (`infra/monitoring/prometheus.yml`)

```yaml
global:
  scrape_interval:     15s
  evaluation_interval: 15s

rule_files:
  - alert_rules.yml

scrape_configs:
  - job_name: kong
    static_configs:
      - targets: [kong:8001]
    metrics_path: /metrics

  - job_name: services
    static_configs:
      - targets:
          - sns-service:8001
          - game-service:8002
          - gis-service:8003
          - ai-service:8004
          - data-service:8005

  - job_name: mcp-server
    static_configs:
      - targets: [mcp-server:8100]

  - job_name: postgres
    static_configs:
      - targets: [postgres-exporter:9187]

  - job_name: redis
    static_configs:
      - targets: [redis-exporter:9121]

  - job_name: kafka
    static_configs:
      - targets: [kafka-exporter:9308]

  - job_name: node
    static_configs:
      - targets: [node-exporter:9100]

  - job_name: nvidia_gpu
    static_configs:
      - targets: [dcgm-exporter:9400]
```

---

## 4. 알림 규칙 (`infra/monitoring/alert_rules.yml`)

```yaml
groups:
  - name: meta_plogging
    rules:

      - alert: HighAPILatency
        expr: |
          histogram_quantile(0.99,
            rate(http_request_duration_seconds_bucket[5m])) > 0.3
        for: 5m
        labels: { severity: warning }
        annotations:
          summary: "API P99 응답 > 300ms"

      - alert: PostgresConnectionHigh
        expr: pg_stat_database_numbackends > 180
        for: 2m
        labels: { severity: critical }
        annotations:
          summary: "PostgreSQL 연결 180 초과"

      - alert: KafkaConsumerLag
        expr: kafka_consumer_group_lag > 1000
        for: 5m
        labels: { severity: warning }
        annotations:
          summary: "Kafka Consumer Lag > 1000"

      - alert: GPUMemoryHigh
        expr: DCGM_FI_DEV_MEM_COPY_UTIL > 90
        for: 3m
        labels: { severity: warning }
        annotations:
          summary: "A6000 GPU 메모리 90% 초과"

      - alert: ServiceDown
        expr: up == 0
        for: 1m
        labels: { severity: critical }
        annotations:
          summary: "서비스 다운: {{ $labels.job }}"

      - alert: RedisMemoryHigh
        expr: redis_memory_used_bytes / redis_memory_max_bytes > 0.9
        for: 5m
        labels: { severity: warning }
        annotations:
          summary: "Redis 메모리 90% 초과"
```

---

## 5. 백업 전략 (`scripts/backup.sh`)

```bash
#!/bin/bash
set -euo pipefail

DIR="/opt/backups/meta-plogging/$(date +%Y%m%d)"
mkdir -p "$DIR"

# PostgreSQL
echo "[*] PostgreSQL 백업..."
docker compose exec -T postgres pg_dump \
  -U "${POSTGRES_USER}" "${POSTGRES_DB}" \
  | gzip > "${DIR}/postgres_$(date +%H%M).sql.gz"

# Redis RDB
echo "[*] Redis 백업..."
docker compose exec -T redis redis-cli -a "${REDIS_PASSWORD}" --no-auth-warning BGSAVE
sleep 5
docker cp "$(docker compose ps -q redis)":/data/dump.rdb "${DIR}/redis.rdb"

# 7일 이상 오래된 백업 삭제
find /opt/backups/meta-plogging -type d -mtime +7 -exec rm -rf {} + 2>/dev/null || true

echo "[✓] 백업 완료: ${DIR}"
```

```bash
# crontab -e 에 추가
0 3 * * * /opt/meta-plogging/scripts/backup.sh >> /var/log/plogging-backup.log 2>&1
```

---

## 6. 서비스 기동 순서 (전체 체크리스트)

```bash
# 1. 인프라 기동
docker compose up -d postgres redis zookeeper kafka minio elasticsearch

# 2. 헬스체크 대기 (최대 120초)
timeout 120 bash -c 'until pg_isready -h localhost -p 5432; do sleep 2; done'
timeout 120 bash -c 'until kafka-broker-api-versions --bootstrap-server localhost:9092; do sleep 2; done'

# 3. Kafka 토픽 생성
bash scripts/create_kafka_topics.sh

# 4. Elasticsearch 인덱스 생성
curl -sX PUT http://localhost:9200/plogging-reports -H "Content-Type: application/json" \
     -d @infra/elasticsearch/reports_mapping.json
curl -sX PUT http://localhost:9200/plogging-posts -H "Content-Type: application/json" \
     -d @infra/elasticsearch/posts_mapping.json

# 5. 마이크로서비스 기동
docker compose up -d sns-service game-service gis-service data-service

# 6. GPU 서비스 기동
docker compose -f docker-compose.gpu.yml up -d ai-service torchserve

# 7. MCP + WebSocket + Gateway
docker compose up -d mcp-server ws-server kong nginx

# 8. 에이전트 기동
docker compose -f docker-compose.agents.yml up -d \
  drone-agent quest-agent data-agent scheduler-agent

# 9. 모니터링
docker compose up -d prometheus grafana

# 10. 검증
curl -sf http://localhost:8000/api/v1/zones/stats | python3 -m json.tool
echo "[✓] 플랫폼 기동 완료"
```

---

## 7. 로컬 → 클라우드 전환 체크리스트

| 컴포넌트 | 로컬 (현재) | 클라우드 (목표) | `.env` 변경 |
|----------|------------|----------------|------------|
| 오브젝트 스토리지 | MinIO | AWS S3 | `MINIO_ENDPOINT` → S3 endpoint |
| 메시지 큐 | 로컬 Kafka | AWS MSK | `KAFKA_BOOTSTRAP_SERVERS` |
| 데이터베이스 | 로컬 PostgreSQL | AWS RDS PostGIS | `DATABASE_URL` |
| 검색 엔진 | 로컬 ES | AWS OpenSearch | `ES_HOST` |
| AI 추론 | A6000 TorchServe | SageMaker / EC2 G5 | `TORCHSERVE_HOST` |
| 이미지 레지스트리 | GHCR | GHCR 유지 | — |

**코드 변경 없이 `.env`만 교체하면 전환 완료** — 12-Factor App 원칙 준수.

---

## 8. 동시 1,000명 처리 근거

| 구성 요소 | 설정 | 근거 |
|-----------|------|------|
| FastAPI uvicorn | workers=4, uvloop | 코어당 1 worker, async I/O |
| DB 커넥션 풀 | min=10, max=50 per service | 5서비스 × 50 = 250 총 연결 (max_connections=200 이내) |
| Redis | maxmemory=4GB, LRU | 랭킹·캐시 핫데이터 |
| Kafka | partitions=12 | 서비스당 2~3 파티션 소비 |
| WebSocket | asyncio broadcast | 1,000 연결 = 1,000 coroutine |
| Kong | 60 req/min/user | 남용 방지 rate limit |
| Elasticsearch | shards=3 | 병렬 집계 처리 |
| Nginx | worker_connections=4096 | 동시 4,096 연결 처리 |
