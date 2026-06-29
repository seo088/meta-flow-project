#!/bin/bash
# Kafka 토픽 생성 스크립트
set -euo pipefail

BROKER="localhost:9093"

echo "[*] Kafka 토픽 생성..."

topics=(
  "plogging.report.created:8"
  "plogging.ai.detected:8"
  "plogging.mission.completed:4"
  "drone.stream.uploaded:4"
  "mobility.route.completed:4"
  "user.points.updated:8"
  "opendata.export.triggered:1"
)

for entry in "${topics[@]}"; do
  IFS=':' read -r topic partitions <<< "$entry"
  docker exec meta-plogging-kafka-plogging-1 \
    kafka-topics --create \
    --bootstrap-server kafka-plogging:9092 \
    --topic "$topic" \
    --partitions "$partitions" \
    --replication-factor 1 \
    --if-not-exists \
    2>/dev/null && echo "  [✓] $topic ($partitions partitions)" \
              || echo "  [!] $topic (already exists or error)"
done

echo "[✓] 토픽 생성 완료"
