"""Kafka Producer/Consumer 팩토리 — confluent_kafka 직접 import 금지."""
import json
import asyncio
import logging
from typing import Callable, Type
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class KafkaProducerClient:
    def __init__(self, bootstrap_servers: str):
        try:
            from confluent_kafka import Producer
            self._p = Producer({
                "bootstrap.servers": bootstrap_servers,
                "acks": "all",
                "retries": 3,
                "linger.ms": 10,
                "compression.type": "lz4",
            })
            self._connected = True
        except Exception as e:
            logger.warning(f"Kafka not available, using stub: {e}")
            self._p = None
            self._connected = False

    def produce(self, topic: str, event: BaseModel, key: str = None):
        if not self._connected:
            logger.info(f"[STUB] Kafka produce → {topic}: {event.model_dump_json()[:100]}...")
            return
        self._p.produce(
            topic=topic,
            value=event.model_dump_json().encode(),
            key=key.encode() if key else None,
            on_delivery=lambda e, m: logger.error(f"Kafka err: {e}") if e else None,
        )
        self._p.poll(0)

    def flush(self):
        if self._connected:
            self._p.flush()


class KafkaConsumerClient:
    def __init__(self, servers: str, group_id: str, topics: list):
        try:
            from confluent_kafka import Consumer
            self._c = Consumer({
                "bootstrap.servers": servers,
                "group.id": group_id,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            })
            self._c.subscribe(topics)
            self._connected = True
        except Exception as e:
            logger.warning(f"Kafka consumer not available: {e}")
            self._c = None
            self._connected = False

    async def consume_loop(self, handler: Callable, event_class: Type, stop: asyncio.Event):
        if not self._connected:
            logger.info("[STUB] Kafka consumer loop (idle)")
            await stop.wait()
            return

        loop = asyncio.get_event_loop()
        while not stop.is_set():
            msg = await loop.run_in_executor(None, lambda: self._c.poll(1.0))
            if msg is None or msg.error():
                continue
            try:
                data = json.loads(msg.value().decode())
                await handler(data if event_class is dict else event_class(**data))
                self._c.commit(msg)
            except Exception as e:
                logger.error(f"Event error: {e}", exc_info=True)

    def close(self):
        if self._connected and self._c:
            self._c.close()
