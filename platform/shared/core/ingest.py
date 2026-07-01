"""
제보 적재 코어 (SSOT) — API·배치 스크립트·에이전트가 공용으로 호출.

한 장의 이미지 + 정답 라벨(items) → trash_reports + posts 적재.
  · 입력 모드 : 업로드(image_bytes) 또는 참조(source_url, HTTP(S))
  · 멱등성    : 파일 SHA-256 을 gt_label.file_sha256 으로 기록, 중복 시 skip
  · 다축 분류 : trash_taxonomy.map_items 로 category/handling/severity 산출
  · 위치      : lat/lon 있으면 PostGIS geofence_zones 매칭
  · 정답 보존 : gt_label(jsonb) 에 items 전체 + 좌표/시각/출처/배치 기록
"""
from __future__ import annotations

import hashlib
import json
import socket
import uuid
from typing import Optional
from urllib.request import urlopen

from core.trash_taxonomy import map_items


def compute_minio_external_host(minio_endpoint: str, server_host: Optional[str] = None) -> str:
    """/posts/report 과 동일한 규칙으로 MinIO 외부 URL 호스트 구성."""
    server_ip = server_host or minio_endpoint.split(":")[0]
    if server_ip in ("localhost", "127.0.0.1"):
        try:
            server_ip = socket.gethostbyname(socket.gethostname())
        except Exception:
            server_ip = "localhost"
    minio_port = minio_endpoint.split(":")[-1]
    return f"http://{server_ip}:{minio_port}"


def _fetch_url_bytes(url: str, timeout: int = 30) -> bytes:
    with urlopen(url, timeout=timeout) as resp:  # noqa: S310 (신뢰된 내부/지정 URL만)
        return resp.read()


async def ingest_report(
    conn,
    minio,
    *,
    reporter_id: str,
    filename: str,
    items: list[dict],
    image_bytes: Optional[bytes] = None,
    source_url: Optional[str] = None,
    rehost: bool = True,
    lat: Optional[float] = None,
    lon: Optional[float] = None,
    content: str = "",
    hashtags: Optional[list[str]] = None,
    captured_at: Optional[str] = None,
    record_source: Optional[str] = None,
    source_dataset: Optional[str] = None,
    batch_id: Optional[str] = None,
    minio_bucket: str = "plogging-images",
    minio_external_host: str = "",
    report_source: str = "external_di",
    kafka=None,
    emit_events: bool = False,
    dry_run: bool = False,
) -> dict:
    """
    이미지 1장 적재. 반환: {status: created|skipped|failed, ...}.
    - image_bytes (업로드 모드) 또는 source_url (참조 모드) 중 하나 필수.
    - rehost=True 면 참조 모드에서도 MinIO로 재호스팅, False면 원본 URL 그대로 사용.
    """
    hashtags = hashtags or []

    # 1) 이미지 바이트 확보
    raw: Optional[bytes] = image_bytes
    if raw is None and source_url:
        try:
            raw = _fetch_url_bytes(source_url)
        except Exception as e:
            return {"status": "failed", "filename": filename, "reason": f"fetch 실패: {e}"}
    if raw is None:
        return {"status": "failed", "filename": filename, "reason": "image_bytes 또는 source_url 필요"}

    # 2) SHA-256 (멱등 키)
    sha = hashlib.sha256(raw).hexdigest()

    # 3) 멱등성: 동일 sha 이미 적재되었으면 skip
    dup = await conn.fetchval(
        "SELECT id FROM trash_reports WHERE gt_label->>'file_sha256' = $1 LIMIT 1", sha
    )
    if dup:
        return {"status": "skipped", "filename": filename, "reason": "이미 적재됨(sha 중복)",
                "report_id": str(dup), "file_sha256": sha}

    # 4) 다축 라벨 매핑
    mapped, primary, severity, handling = map_items(items)
    primary_category = primary["category"] if primary else "unknown"

    # 5) 이미지 URL 확보 (업로드 or 참조)
    if source_url and not rehost:
        image_url = source_url
    else:
        key = f"reports/{reporter_id}/{uuid.uuid4()}_{filename}"
        image_url = f"{minio_external_host}/{minio_bucket}/{key}"
        if not dry_run:
            if minio is None:
                return {"status": "failed", "filename": filename, "reason": "MinIO 미가용"}
            import io
            try:
                minio.put_object(minio_bucket, key, io.BytesIO(raw), length=len(raw),
                                 part_size=5 * 1024 * 1024)
            except Exception as e:
                return {"status": "failed", "filename": filename, "reason": f"MinIO 업로드 실패: {e}"}
    image_urls = [image_url]

    # 6) gt_label 구성 (정답 + 출처 + 멱등키)
    gt_label = {
        "items": mapped,
        "primary_category": primary_category,
        "derived_severity": severity,
        "handling": handling,
        "source_dataset": source_dataset,
        "original_filename": filename,
        "file_sha256": sha,
        "geo": ({"lat": lat, "lon": lon, "from": "manifest_json"} if lat is not None and lon is not None else None),
        "captured_at": captured_at,
        "record_source": record_source,
        "ingested_batch": batch_id,
    }
    gt_json = json.dumps(gt_label, ensure_ascii=False)

    if dry_run:
        return {"status": "created", "filename": filename, "dry_run": True,
                "trash_type": primary_category, "severity": severity, "handling": handling,
                "has_location": lat is not None and lon is not None, "image_url": image_url,
                "file_sha256": sha, "gt_label": gt_label}

    # 7) zone 매칭 + INSERT (트랜잭션)
    has_location = lat is not None and lon is not None
    async with conn.transaction():
        zone_id = None
        if has_location:
            zone_id = await conn.fetchval("""
                SELECT id FROM geofence_zones
                WHERE ST_Contains(geom, ST_SetSRID(ST_MakePoint($1,$2),4326)) AND is_active = TRUE
                LIMIT 1
            """, lon, lat)

        if has_location:
            report_id = await conn.fetchval("""
                INSERT INTO trash_reports
                    (reporter_id, location, zone_id, trash_type, severity, handling, image_urls, source, gt_label)
                VALUES ($1::uuid, ST_SetSRID(ST_MakePoint($2,$3),4326), $4,$5,$6,$7,$8,$9,$10::jsonb)
                RETURNING id
            """, reporter_id, lon, lat, zone_id, primary_category, severity, handling,
                 image_urls, report_source, gt_json)
            await conn.execute("""
                INSERT INTO posts (user_id, report_id, content, image_urls, hashtags, location, zone_id, post_type)
                VALUES ($1::uuid,$2,$3,$4,$5,ST_SetSRID(ST_MakePoint($6,$7),4326),$8,'report')
            """, reporter_id, report_id, content, image_urls, hashtags, lon, lat, zone_id)
        else:
            report_id = await conn.fetchval("""
                INSERT INTO trash_reports
                    (reporter_id, trash_type, severity, handling, image_urls, source, gt_label)
                VALUES ($1::uuid,$2,$3,$4,$5,$6,$7::jsonb)
                RETURNING id
            """, reporter_id, primary_category, severity, handling, image_urls, report_source, gt_json)
            await conn.execute("""
                INSERT INTO posts (user_id, report_id, content, image_urls, hashtags, post_type)
                VALUES ($1::uuid,$2,$3,$4,$5,'report')
            """, reporter_id, report_id, content, image_urls, hashtags)

    # 8) (선택) Kafka 이벤트
    if emit_events and kafka is not None:
        try:
            from core.types import TrashType, Severity, DataSource, GeoPoint
            from core.events import ReportCreatedEvent
            kafka.produce("plogging.report.created", ReportCreatedEvent(
                report_id=str(report_id), reporter_id=reporter_id,
                location=GeoPoint(lat=lat or 0, lon=lon or 0), zone_id=str(zone_id) if zone_id else None,
                trash_type=TrashType(primary_category) if primary_category in TrashType._value2member_map_ else TrashType.UNKNOWN,
                severity=Severity(severity), image_urls=image_urls, source=DataSource.SNS,
            ), key=str(report_id))
        except Exception:
            pass  # 이벤트 발행 실패는 적재를 막지 않음

    return {"status": "created", "filename": filename, "report_id": str(report_id),
            "trash_type": primary_category, "severity": severity, "handling": handling,
            "has_location": has_location, "zone_id": str(zone_id) if zone_id else None,
            "image_url": image_url, "file_sha256": sha}
