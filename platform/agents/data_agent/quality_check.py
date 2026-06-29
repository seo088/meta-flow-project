"""
공공데이터 배포 전 품질 검증 — 좌표 범위 · 분류 유효성 · 필수값 확인.
04_data_pipeline.md §5 설계 구현.
"""
import os
import sys
from typing import Tuple, List

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "shared"))
from core.logger import get_logger

logger = get_logger("quality-check")

# 군산시 관할 범위 (WGS84 BBOX)
GUNSAN_BBOX = {
    "lat_min": 35.70,
    "lat_max": 36.05,
    "lon_min": 126.50,
    "lon_max": 126.90,
}

VALID_TRASH_TYPES = {"plastic", "food", "general", "large", "unknown"}
VALID_SOURCES = {"sns", "drone", "mobility", "unity_sim"}


async def validate(db) -> Tuple[int, List[str]]:
    """
    공공데이터 품질 검증을 수행합니다.
    Returns:
        (valid_count, error_messages)
    """
    errors = []

    # 1. 좌표 범위 이탈
    n = await db.fetchval("""
        SELECT COUNT(*) FROM trash_reports
        WHERE ST_Y(location::geometry) NOT BETWEEN $1 AND $2
           OR ST_X(location::geometry) NOT BETWEEN $3 AND $4
    """, GUNSAN_BBOX["lat_min"], GUNSAN_BBOX["lat_max"],
         GUNSAN_BBOX["lon_min"], GUNSAN_BBOX["lon_max"])
    if n:
        errors.append(f"좌표 범위 이탈: {n}건")

    # 2. 유효하지 않은 trash_type
    n = await db.fetchval("""
        SELECT COUNT(*) FROM trash_reports
        WHERE trash_type NOT IN ('plastic','food','general','large','unknown')
    """)
    if n:
        errors.append(f"비유효 trash_type: {n}건")

    # 3. AI 신뢰도 누락 (시뮬 제외)
    n = await db.fetchval("""
        SELECT COUNT(*) FROM trash_reports
        WHERE ai_confidence IS NULL AND source != 'unity_sim'
          AND status NOT IN ('pending')
    """)
    if n:
        errors.append(f"AI 신뢰도 누락: {n}건 (경고)")

    # 4. 유효하지 않은 source
    n = await db.fetchval("""
        SELECT COUNT(*) FROM trash_reports
        WHERE source NOT IN ('sns','drone','mobility','unity_sim')
    """)
    if n:
        errors.append(f"비유효 source: {n}건")

    # 5. 이미지 없는 제보
    n = await db.fetchval("""
        SELECT COUNT(*) FROM trash_reports
        WHERE (image_urls IS NULL OR array_length(image_urls, 1) IS NULL)
          AND video_url IS NULL
    """)
    if n:
        errors.append(f"미디어 누락: {n}건 (경고)")

    # 6. zone_id 미매핑
    n = await db.fetchval("""
        SELECT COUNT(*) FROM trash_reports WHERE zone_id IS NULL
    """)
    if n:
        errors.append(f"구역 미매핑: {n}건 (경고)")

    valid = await db.fetchval(
        "SELECT COUNT(*) FROM trash_reports WHERE status != 'pending'"
    )
    total = await db.fetchval("SELECT COUNT(*) FROM trash_reports")

    logger.info(
        f"품질 검증 완료: 총 {total}건, 검증 통과 {valid}건, "
        f"오류 {len(errors)}건"
    )
    return valid, errors


async def generate_report(db) -> dict:
    """품질 검증 리포트를 JSON으로 반환합니다."""
    valid_count, errors = await validate(db)
    total = await db.fetchval("SELECT COUNT(*) FROM trash_reports")

    # 소스별 통계
    source_stats = await db.fetch("""
        SELECT source, COUNT(*) as cnt,
               AVG(ai_confidence)::numeric(5,3) as avg_conf
        FROM trash_reports
        GROUP BY source ORDER BY cnt DESC
    """)

    # 구역별 통계
    zone_stats = await db.fetch("""
        SELECT gz.zone_key, gz.name, COUNT(tr.id) as cnt
        FROM geofence_zones gz
        LEFT JOIN trash_reports tr ON tr.zone_id = gz.id
        GROUP BY gz.zone_key, gz.name ORDER BY cnt DESC
    """)

    return {
        "total_records": total,
        "valid_records": valid_count,
        "quality_score": round(valid_count / max(total, 1) * 100, 1),
        "errors": errors,
        "by_source": [dict(r) for r in source_stats],
        "by_zone": [dict(r) for r in zone_stats],
    }
