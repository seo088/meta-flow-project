"""
PostgreSQL → Elasticsearch 실시간 동기화.
Kafka Consumer: plogging.ai.detected (탐지 완료 후 ES 인덱싱)
Usage: python services/data/es_sync.py
"""
import os
import sys
import asyncio

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "shared"))

from core.events import AIDetectedEvent
from core.kafka_client import KafkaConsumerClient
from core.db import create_db_pool
from core.logger import get_logger

logger = get_logger("es-sync")


async def run_sync():
    db = await create_db_pool(min_size=3, max_size=10)

    es_host = os.environ.get("ES_HOST", "localhost")
    es_port = os.environ.get("ES_PORT", "9201")

    try:
        from elasticsearch import AsyncElasticsearch
        es = AsyncElasticsearch(f"http://{es_host}:{es_port}")
    except Exception as e:
        logger.error(f"Elasticsearch connection failed: {e}")
        return

    stop = asyncio.Event()
    consumer = KafkaConsumerClient(
        os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9093"),
        "es-sync-worker",
        ["plogging.ai.detected"],
    )

    async def handler(raw: dict):
        event = AIDetectedEvent(**raw)
        row = await db.fetchrow("""
            SELECT tr.id, tr.trash_type, tr.severity, tr.status, tr.source,
                   ST_Y(tr.location::geometry) AS lat,
                   ST_X(tr.location::geometry) AS lon,
                   tr.ai_confidence, tr.created_at,
                   gz.zone_key, gz.name AS zone_name
            FROM trash_reports tr
            LEFT JOIN geofence_zones gz ON gz.id = tr.zone_id
            WHERE tr.id = $1::uuid
        """, event.report_id)

        if not row:
            return

        doc = {
            "id": str(row["id"]),
            "zone_key": row["zone_key"],
            "zone_name": row["zone_name"],
            "trash_type": row["trash_type"],
            "severity": row["severity"],
            "status": row["status"],
            "source": row["source"],
            "location": {"lat": row["lat"], "lon": row["lon"]},
            "ai_confidence": float(row["ai_confidence"] or 0),
            "created_at": row["created_at"].isoformat(),
        }
        await es.index(index="plogging-reports", id=str(row["id"]), document=doc)
        logger.debug(f"ES indexed: {row['id']}")

    logger.info("Starting ES sync consumer...")
    await consumer.consume_loop(handler, dict, stop)


if __name__ == "__main__":
    asyncio.run(run_sync())
