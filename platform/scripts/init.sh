#!/bin/bash
# 메타 플로깅 플랫폼 — 전체 초기화 스크립트
set -euo pipefail

PROJ_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJ_DIR"

echo "============================================"
echo " 메타 플로깅 SW 플랫폼 초기화"
echo " 서버: spark-a6 (A6000 48GB)"
echo "============================================"

# 1. 환경변수 로드
if [ -f .env ]; then
    export $(grep -v '^#' .env | xargs)
else
    echo "[!] .env 파일이 없습니다. .env.example을 복사하세요."
    exit 1
fi

# 2. Docker 인프라 기동
echo ""
echo "[1/6] Docker 인프라 기동..."
cd infra/docker
docker compose --env-file ../../.env up -d
cd "$PROJ_DIR"

# 3. 헬스체크 대기
echo ""
echo "[2/6] 인프라 준비 대기..."
echo -n "  PostgreSQL..."
until docker exec meta-plogging-postgres-plogging-1 pg_isready -U plogging &>/dev/null 2>&1; do
    sleep 2; echo -n "."
done
echo " ready"

echo -n "  Kafka..."
for i in $(seq 1 30); do
    docker exec meta-plogging-kafka-plogging-1 kafka-broker-api-versions \
        --bootstrap-server kafka-plogging:9092 &>/dev/null 2>&1 && break
    sleep 3; echo -n "."
done
echo " ready"

echo -n "  Elasticsearch..."
until curl -sf http://localhost:9201/_cluster/health &>/dev/null; do
    sleep 3; echo -n "."
done
echo " ready"

# 4. Kafka 토픽 생성
echo ""
echo "[3/6] Kafka 토픽 생성..."
bash infra/kafka/create_topics.sh

# 5. Elasticsearch 인덱스
echo ""
echo "[4/6] Elasticsearch 인덱스 생성..."
curl -sX PUT http://localhost:9201/plogging-reports \
     -H "Content-Type: application/json" \
     -d '{
  "mappings": {
    "properties": {
      "id":           {"type":"keyword"},
      "zone_key":     {"type":"keyword"},
      "zone_name":    {"type":"keyword"},
      "trash_type":   {"type":"keyword"},
      "severity":     {"type":"keyword"},
      "status":       {"type":"keyword"},
      "source":       {"type":"keyword"},
      "location":     {"type":"geo_point"},
      "ai_confidence":{"type":"float"},
      "created_at":   {"type":"date"}
    }
  },
  "settings": {"number_of_shards":3,"number_of_replicas":0,"refresh_interval":"5s"}
}' > /dev/null 2>&1 && echo "  [✓] plogging-reports" || echo "  [!] plogging-reports (exists)"

curl -sX PUT http://localhost:9201/plogging-posts \
     -H "Content-Type: application/json" \
     -d '{
  "mappings": {
    "properties": {
      "id":         {"type":"keyword"},
      "user_id":    {"type":"keyword"},
      "content":    {"type":"text","analyzer":"standard"},
      "hashtags":   {"type":"keyword"},
      "zone_key":   {"type":"keyword"},
      "location":   {"type":"geo_point"},
      "like_count": {"type":"integer"},
      "created_at": {"type":"date"}
    }
  },
  "settings": {"number_of_shards":2,"number_of_replicas":0}
}' > /dev/null 2>&1 && echo "  [✓] plogging-posts" || echo "  [!] plogging-posts (exists)"

# 6. MinIO 버킷
echo ""
echo "[5/6] MinIO 버킷 생성..."
sleep 3
docker run --rm --network meta-plogging_plogging-net \
  minio/mc:latest sh -c "
    mc alias set local http://minio-plogging:9000 ${MINIO_ACCESS_KEY} ${MINIO_SECRET_KEY} &&
    mc mb --ignore-existing local/plogging-images &&
    mc mb --ignore-existing local/plogging-videos &&
    mc mb --ignore-existing local/plogging-opendata
  " 2>/dev/null && echo "  [✓] 버킷 생성 완료" || echo "  [!] 버킷 생성 (이미 존재하거나 오류)"

# 7. 더미 데이터 시드
echo ""
echo "[6/6] 더미 데이터 시드..."
cd "$PROJ_DIR"
python scripts/seed_dummy.py

echo ""
echo "============================================"
echo " 초기화 완료!"
echo ""
echo " 인프라 포트:"
echo "   PostgreSQL  : localhost:5433"
echo "   Redis       : localhost:6380"
echo "   Kafka       : localhost:9093"
echo "   MinIO       : localhost:9001 (Console :9002)"
echo "   Elasticsearch: localhost:9201"
echo ""
echo " 다음 단계: bash scripts/start_conda.sh"
echo "============================================"
