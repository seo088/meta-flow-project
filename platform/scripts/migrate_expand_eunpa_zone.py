"""
EUNPA(은파유원지) 구역 폴리곤을 서편으로 확장 — 외부 DI 데이터셋 좌표(은파호수 일대,
경도 126.678~126.685 / 위도 35.941~35.950)가 기존 EUNPA 경계(서편 126.6852) 밖이라
zone 매칭이 0이던 문제 해결. 확장 후 external_di 제보의 zone_id 재매칭.

idempotent. 실행: conda run -n meta-flow python scripts/migrate_expand_eunpa_zone.py
"""
import os
import asyncio

# 확장 후 EUNPA 박스 (기존 126.6852~126.7072 / 35.9392~35.9612 + 데이터 영역 포함)
NEW_BOX = (126.6770, 35.9385, 126.7072, 35.9612)  # (minlon, minlat, maxlon, maxlat)


def _dsn():
    return (f"postgresql://{os.environ.get('POSTGRES_USER','plogging')}:"
            f"{os.environ.get('POSTGRES_PASSWORD','plogging_dev_2026')}@"
            f"{os.environ.get('POSTGRES_HOST','203.234.62.176')}:"
            f"{os.environ.get('POSTGRES_PORT','5433')}/{os.environ.get('POSTGRES_DB','plogging')}")


async def main():
    import asyncpg
    conn = await asyncpg.connect(_dsn())
    try:
        before = await conn.fetchval(
            "SELECT COUNT(*) FROM trash_reports WHERE source='external_di' AND zone_id IS NOT NULL") or 0

        await conn.execute("""
            UPDATE geofence_zones
            SET geom = ST_MakeEnvelope($1,$2,$3,$4,4326)
            WHERE zone_key='EUNPA'
        """, *NEW_BOX)
        print(f"[1/3] EUNPA 폴리곤 확장: {NEW_BOX}")

        # external_di 제보 zone 재매칭 (trash_reports + posts)
        r1 = await conn.execute("""
            UPDATE trash_reports tr SET zone_id = gz.id
            FROM geofence_zones gz
            WHERE tr.source='external_di' AND tr.location IS NOT NULL
              AND gz.is_active AND ST_Contains(gz.geom, tr.location::geometry)
              AND (tr.zone_id IS NULL OR tr.zone_id <> gz.id)
        """)
        r2 = await conn.execute("""
            UPDATE posts p SET zone_id = tr.zone_id
            FROM trash_reports tr
            WHERE p.report_id = tr.id AND tr.source='external_di' AND tr.zone_id IS NOT NULL
              AND (p.zone_id IS DISTINCT FROM tr.zone_id)
        """)
        print(f"[2/3] zone 재매칭 — trash_reports: {r1}, posts: {r2}")

        after = await conn.fetchval(
            "SELECT COUNT(*) FROM trash_reports WHERE source='external_di' AND zone_id IS NOT NULL") or 0
        total = await conn.fetchval("SELECT COUNT(*) FROM trash_reports WHERE source='external_di'") or 0
        print(f"[3/3] external_di zone 매칭: {before} → {after} / {total}")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
