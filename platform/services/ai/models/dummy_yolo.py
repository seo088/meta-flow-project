"""
더미 YOLO 탐지 모델 — 실제 YOLO v8 교체 지점.

교체 방법:
  1. ultralytics YOLO 모델 가중치 다운로드
  2. 이 파일의 DummyYOLO → 실제 YOLO 래퍼로 교체
  3. predict() 인터페이스는 동일하게 유지

입출력 인터페이스:
  Input:  image_urls: List[str]
  Output: {"class": str, "confidence": float, "boxes": List[dict]}
"""
import random
from typing import List, Dict

TRASH_CLASSES = ["plastic", "food", "general", "large", "unknown"]
SEVERITY_MAP = {
    "large": "boss",
    "plastic": "mid",
    "food": "low",
    "general": "mid",
    "unknown": "low",
}


class DummyYOLO:
    """YOLO v8 더미 — random confidence + class 반환."""

    def __init__(self, model_path: str = None):
        self.model_name = "dummy_yolo_v8"
        self.version = "dummy_v1"

    def predict(self, image_urls: List[str]) -> Dict:
        """
        더미 탐지 결과 반환.
        실제 YOLO에서는 이미지를 다운로드하여 추론.
        """
        detected_class = random.choices(
            TRASH_CLASSES,
            weights=[35, 20, 25, 10, 10],
            k=1,
        )[0]
        confidence = round(random.uniform(0.55, 0.98), 4)

        # 더미 바운딩 박스
        x = random.randint(50, 500)
        y = random.randint(50, 400)
        w = random.randint(30, 150)
        h = random.randint(30, 150)

        severity = SEVERITY_MAP.get(detected_class, "mid")
        if confidence > 0.85:
            severity = "high" if detected_class != "large" else "boss"

        return {
            "class": detected_class,
            "confidence": confidence,
            "severity": severity,
            "boxes": [
                {
                    "x": x, "y": y, "w": w, "h": h,
                    "center_x": round((x + w / 2) / 640, 3),
                    "center_y": round((y + h / 2) / 480, 3),
                    "class": detected_class,
                    "confidence": confidence,
                }
            ],
            "model_version": self.version,
            "width": 640,
            "height": 480,
        }

    def predict_batch(self, frames: List[dict]) -> Dict:
        """드론 영상 프레임 배치 탐지."""
        detections = []
        for frame in frames:
            if random.random() > 0.3:  # 70% 확률로 탐지
                det = self.predict([])
                det["frame_idx"] = frame.get("idx", 0)
                det["timestamp"] = frame.get("ts", 0.0)
                detections.append(det)
        return {"detections": detections}


class DummyVerifier:
    """청소 인증 더미 — random SSIM score 반환."""

    def verify(self, before_url: str, after_url: str) -> Dict:
        """
        전후 이미지 비교. 실제에서는 SSIM + ResNet.
        교체 시: torchvision ResNet + skimage.metrics.structural_similarity
        """
        score = round(random.uniform(0.5, 0.98), 4)
        return {
            "ssim_score": score,
            "verified": score > 0.6,
            "model_version": "dummy_verify_v1",
        }


class DummyRL:
    """RL 경로 최적화 더미 — 거리순 정렬 반환."""

    def optimize(self, start: Dict, targets: List[Dict], agent_type: str = "drone") -> Dict:
        """
        최적 경로 생성. 실제에서는 SB3 PPO/SAC.
        교체 시: stable_baselines3 + ONNX 모델
        """
        import math

        def dist(a, b):
            return math.sqrt((a["lat"] - b["lat"]) ** 2 + (a["lon"] - b["lon"]) ** 2)

        # 단순 최근접 이웃 (Nearest Neighbor)
        remaining = list(targets)
        route = []
        current = start

        while remaining:
            nearest = min(remaining, key=lambda t: dist(current, t))
            route.append(nearest)
            remaining.remove(nearest)
            current = nearest

        return {
            "waypoints": route,
            "total_distance_km": round(sum(
                dist(route[i], route[i + 1]) * 111
                for i in range(len(route) - 1)
            ), 2) if len(route) > 1 else 0,
            "estimated_time_min": len(route) * random.randint(3, 8),
            "agent_type": agent_type,
            "model_version": "dummy_rl_v1",
        }


class DummyEntityTagger:
    """이미지 장면 엔티티 태깅 더미 — YOLO v8 + OpenCV 교체 지점.

    실제 교체 시: ultralytics YOLO("yolov8n.pt") + 커스텀 클래스 매핑
    """

    ENVIRONMENT = ["river", "bench", "park", "road", "bridge", "sidewalk", "beach", "drain", "grass", "tree", "sky"]
    OBJECTS = ["plastic_bag", "bottle", "can", "cigarette_butt", "food_wrapper", "mask", "cardboard", "tire", "styrofoam"]
    LIVING = ["person", "dog", "bird", "cat", "flower", "fish"]

    def predict_entities(self, image_urls: List[str] = None) -> Dict:
        """이미지에서 엔티티 태그 추출 (더미).

        Returns:
            {"entities": [{"tag": str, "confidence": float, "category": str}]}
        """
        n_env = random.randint(1, 3)
        n_obj = random.randint(1, 3)
        n_live = random.randint(0, 2)

        entities = []
        for tag in random.sample(self.ENVIRONMENT, n_env):
            entities.append({"tag": tag, "confidence": round(random.uniform(0.60, 0.95), 3), "category": "environment"})
        for tag in random.sample(self.OBJECTS, n_obj):
            entities.append({"tag": tag, "confidence": round(random.uniform(0.55, 0.92), 3), "category": "object"})
        for tag in random.sample(self.LIVING, n_live):
            entities.append({"tag": tag, "confidence": round(random.uniform(0.50, 0.90), 3), "category": "living"})

        entities.sort(key=lambda e: e["confidence"], reverse=True)
        return {"entities": entities, "model_version": "dummy_entity_v1"}


# 싱글턴 인스턴스
yolo_model = DummyYOLO()
verifier = DummyVerifier()
rl_agent = DummyRL()
entity_tagger = DummyEntityTagger()
