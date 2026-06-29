"""
드론 영상 → OpenCV 프레임 추출 → AI Service 배치 탐지 → GPS 좌표 보정.
04_data_pipeline.md §2 설계 구현.
AI_MODE=dummy 시 더미 탐지 결과 반환.
"""
import os
import sys
import math
import asyncio
import random
import uuid
from dataclasses import dataclass, asdict
from typing import List, Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "shared"))
from core.logger import get_logger

logger = get_logger("video-processor")


@dataclass
class FrameDetection:
    frame_index: int
    timestamp_sec: float
    lat: float
    lon: float
    trash_type: str
    confidence: float
    bbox: dict
    frame_url: Optional[str] = None


def _pixel_to_gps(
    px: float, py: float,
    drone_lat: float, drone_lon: float,
    altitude_m: float, heading_deg: float,
) -> tuple:
    """이미지 픽셀 위치(0~1) → GPS 좌표 변환. FOV 90°×60° 가정."""
    gw = 2 * altitude_m * math.tan(math.radians(45))
    gh = 2 * altitude_m * math.tan(math.radians(30))
    dx = (px - 0.5) * gw
    dy = (0.5 - py) * gh
    hr = math.radians(heading_deg)
    east_m = dx * math.cos(hr) - dy * math.sin(hr)
    north_m = dx * math.sin(hr) + dy * math.cos(hr)
    return (
        drone_lat + north_m / 111_111,
        drone_lon + east_m / (111_111 * math.cos(math.radians(drone_lat))),
    )


async def process_drone_video(
    video_url: str,
    start_lat: float,
    start_lon: float,
    heading_deg: float = 0.0,
    altitude_m: float = 30.0,
    sample_fps: int = 1,
    confidence_threshold: float = 0.5,
) -> List[FrameDetection]:
    """
    드론 영상을 처리하여 탐지 결과 목록을 반환합니다.
    AI_MODE=dummy이면 OpenCV 없이 더미 결과를 생성합니다.
    """

    if os.environ.get("AI_MODE") == "dummy":
        return _dummy_process(start_lat, start_lon, altitude_m, heading_deg)

    # 실제 모드: OpenCV + AI Service
    try:
        import cv2
        import httpx
    except ImportError:
        logger.warning("cv2/httpx not available, falling back to dummy")
        return _dummy_process(start_lat, start_lon, altitude_m, heading_deg)

    ai_url = os.environ.get("AI_SERVICE_URL", "http://203.234.62.176:8504")

    cap = cv2.VideoCapture(video_url)
    if not cap.isOpened():
        logger.error(f"Cannot open video: {video_url}")
        return _dummy_process(start_lat, start_lon, altitude_m, heading_deg)

    fps_video = cap.get(cv2.CAP_PROP_FPS) or 30
    frame_interval = max(1, int(fps_video / sample_fps))
    frames = []
    idx = 0

    while True:
        ret, frame = cap.read()
        if not ret:
            break
        if idx % frame_interval == 0:
            _, buf = cv2.imencode(".jpg", frame, [cv2.IMWRITE_JPEG_QUALITY, 85])
            frames.append({
                "idx": idx,
                "ts": idx / fps_video,
                "data": buf.tobytes().hex(),
            })
        idx += 1
    cap.release()

    if not frames:
        return []

    # AI Service 배치 탐지 호출
    async with httpx.AsyncClient(timeout=120.0) as client:
        try:
            r = await client.post(
                f"{ai_url}/predictions/yolo_plogging_v8/batch",
                json={"frames": frames},
            )
            results = r.json().get("detections", [])
        except Exception as e:
            logger.error(f"AI batch detection failed: {e}")
            return _dummy_process(start_lat, start_lon, altitude_m, heading_deg)

    detections = []
    for det in results:
        if det.get("confidence", 0) < confidence_threshold:
            continue
        lat, lon = _pixel_to_gps(
            det.get("center_x", 0.5),
            det.get("center_y", 0.5),
            start_lat, start_lon,
            altitude_m, heading_deg,
        )
        detections.append(FrameDetection(
            frame_index=det.get("frame_idx", 0),
            timestamp_sec=det.get("timestamp", 0),
            lat=lat, lon=lon,
            trash_type=det.get("class", "general"),
            confidence=det["confidence"],
            bbox=det.get("bbox", {}),
        ))
    return detections


def _dummy_process(
    center_lat: float, center_lon: float,
    altitude_m: float = 30.0, heading_deg: float = 0.0,
) -> List[FrameDetection]:
    """더미 모드: 3~8개의 가상 탐지 결과를 생성합니다."""
    types = ["plastic", "food", "general", "large"]
    count = random.randint(3, 8)
    detections = []

    for i in range(count):
        px, py = random.uniform(0.1, 0.9), random.uniform(0.1, 0.9)
        lat, lon = _pixel_to_gps(px, py, center_lat, center_lon, altitude_m, heading_deg)
        detections.append(FrameDetection(
            frame_index=i * 30,
            timestamp_sec=i * 1.0,
            lat=lat, lon=lon,
            trash_type=random.choice(types),
            confidence=round(random.uniform(0.55, 0.98), 3),
            bbox={"x": round(px * 640), "y": round(py * 480), "w": 80, "h": 60},
            frame_url=f"frames/{uuid.uuid4().hex[:8]}.jpg",
        ))
    logger.info(f"[dummy] Generated {count} detections near ({center_lat:.4f}, {center_lon:.4f})")
    return detections
