"""
geofence_zones 좌표 정합성 보정.

문제: KU_CAMPUS(군산대학교) 폴리곤이 실제 군산대(대학로 558, 미룡동 ≈ 126.682,35.945)에서
약 5km 북동쪽(126.737,35.969)의 임의 공간에 찍혀 있어, 실제 군산대 좌표가 엉뚱하게 매칭되거나
미매칭됨. (앞선 migrate_expand_eunpa_zone.py 의 EUNPA 서편 확장은 이 버그를 우회한 잘못된 보정이라 함께 되돌림)

수정:
  1) KU_CAMPUS → 실제 군산대 캠퍼스 영역으로 이동
  2) EUNPA → 원래 은파호수 영역으로 복원(KU_CAMPUS와 겹치지 않게 서편 경계 조정)
  3) 전체 trash_reports + posts zone_id 재매칭 (현재 폴리곤 기준)

idempotent. 실행: conda run -n meta-flow python scripts/migrate_fix_zone_coords.py
"""
import os
import asyncio

# (minlon, minlat, maxlon, maxlat)
KU_CAMPUS_BOX = (126.6740, 35.9385, 126.6880, 35.9520)   # 실제 군산대 캠퍼스 (DI 데이터 전체 포함)
EUNPA_BOX     = (126.6880, 35.9392, 126.7072, 35.9612)   # 은파호수 — KU_CAMPUS 동편, 비겹침


def _dsn():
    return (f"postgresql://{os.environ.get('POSTGRES_USER','plogging')}:"
            f"{os.environ.get('POSTGRES_PASSWORD','plogging_dev_2026')}@"
            f"{os.environ.get('POSTGRES_HOST','203.234.62.176')}:"
            f"{os.environ.get('POSTGRES_PORT','5433')}/{os.environ.get('POSTGRES_DB','plogging')}")


async def main():
    import asyncpg
    conn = await asyncpg.connect(_dsn())
    try:
        await conn.execute(
            "UPDATE geofence_zones SET geom=ST_MakeEnvelope($1,$2,$3,$4,4326) WHERE zone_key='KU_CAMPUS'",
            *KU_CAMPUS_BOX)
        await conn.execute(
            "UPDATE geofence_zones SET geom=ST_MakeEnvelope($1,$2,$3,$4,4326) WHERE zone_key='EUNPA'",
            *EUNPA_BOX)
        print(f"[1/3] KU_CAMPUS={KU_CAMPUS_BOX}, EUNPA={EUNPA_BOX} 적용")

        # 전체 재매칭 (현재 폴리곤 기준). 겹침 없으므로 LIMIT 1 결정적.
        r1 = await conn.execute("""
            UPDATE trash_reports tr SET zone_id = sub.zid
            FROM (
                SELECT t.id, (SELECT gz.id FROM geofence_zones gz
                              WHERE gz.is_active AND ST_Contains(gz.geom, t.location::geometry) LIMIT 1) AS zid
                FROM trash_reports t WHERE t.location IS NOT NULL
            ) sub
            WHERE tr.id = sub.id AND tr.zone_id IS DISTINCT FROM sub.zid
        """)
        r2 = await conn.execute("""
            UPDATE posts p SET zone_id = sub.zid
            FROM (
                SELECT pp.id, (SELECT gz.id FROM geofence_zones gz
                               WHERE gz.is_active AND ST_Contains(gz.geom, pp.location::geometry) LIMIT 1) AS zid
                FROM posts pp WHERE pp.location IS NOT NULL
            ) sub
            WHERE p.id = sub.id AND p.zone_id IS DISTINCT FROM sub.zid
        """)
        print(f"[2/3] 재매칭 — trash_reports: {r1}, posts: {r2}")

        print("[3/3] 구역별 제보 매칭 결과:")
        for z in await conn.fetch("""
            SELECT gz.zone_key, gz.name, COUNT(tr.id) AS cnt
            FROM geofence_zones gz LEFT JOIN trash_reports tr ON tr.zone_id=gz.id
            GROUP BY 1,2 ORDER BY 3 DESC
        """):
            print(f"    [{z['zone_key']}] {z['name']}: {z['cnt']}건")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
