"""
더미 데이터 생성기 — 군산시 실 좌표 기반.
실데이터 교체 시 이 파일만 제거하면 됨.
"""
import random
import uuid
from datetime import datetime, timedelta
from typing import List, Dict, Optional

# 군산시 지오펜싱 구역 실 좌표
ZONES = {
    "KU_CAMPUS": {
        "name": "군산대학교",
        "center": (35.9693, 126.7369),
        "radius_deg": 0.007,  # ~800m
        "bonus": 1.0,
        "phase": 1,
    },
    "EUNPA": {
        "name": "은파유원지",
        "center": (35.9861, 126.7222),
        "radius_deg": 0.011,  # ~1.2km
        "bonus": 1.5,
        "phase": 2,
    },
    "SAEMANGEUM": {
        "name": "새만금",
        "center": (35.8000, 126.6000),
        "radius_deg": 0.09,  # ~10km
        "bonus": 2.0,
        "phase": 2,
    },
    "GEUMGANG": {
        "name": "금강하구둑",
        "center": (35.9600, 126.7100),
        "radius_deg": 0.018,  # ~2km
        "bonus": 1.8,
        "phase": 3,
    },
}

TRASH_TYPES = ["plastic", "food", "general", "large", "unknown"]
SEVERITIES = ["low", "mid", "high", "boss"]
SOURCES = ["sns", "drone", "mobility"]
HASHTAGS = [
    "#플로깅", "#환경보호", "#군산", "#클린업", "#군산대",
    "#은파유원지", "#새만금", "#금강", "#에코", "#제로웨이스트",
]

KOREAN_NAMES = [
    "김민준", "이서연", "박지호", "최예진", "정우성",
    "한수빈", "송민서", "윤지훈", "장서윤", "강하늘",
    "임도현", "조예은", "류준혁", "신하윤", "오태양",
    "권지은", "홍세준", "문소율", "배준서", "전예림",
    "양성우", "노아린", "하태민", "구지아", "남서진",
    "서유진", "차은호", "방수아", "공준혁", "진하린",
    "손민재", "안서하", "유도윤", "설예진", "탁진우",
    "고수민", "마시온", "길라온", "추하율", "엄지안",
    "봉서연", "맹준호", "두하은", "범지오", "감나윤",
    "독서준", "국예서", "빈하진", "란시아", "담서율",
]


def random_point_in_zone(zone_key: str) -> tuple:
    """구역 내 랜덤 GPS 좌표 생성."""
    z = ZONES[zone_key]
    lat = z["center"][0] + random.uniform(-z["radius_deg"], z["radius_deg"])
    lon = z["center"][1] + random.uniform(-z["radius_deg"], z["radius_deg"])
    return round(lat, 6), round(lon, 6)


def random_zone() -> str:
    """가중치 기반 랜덤 구역 선택 (군산대 빈도 높게)."""
    return random.choices(
        list(ZONES.keys()),
        weights=[40, 25, 20, 15],
        k=1,
    )[0]


def gen_user_id() -> str:
    return str(uuid.uuid4())


def gen_users(n: int = 50) -> List[Dict]:
    """더미 사용자 n명 생성."""
    users = []
    for i in range(n):
        name = KOREAN_NAMES[i % len(KOREAN_NAMES)]
        uid = gen_user_id()
        users.append({
            "id": uid,
            "username": f"user_{i+1:03d}",
            "email": f"user{i+1}@kunsan.ac.kr",
            "password_hash": f"$dummy$hash${i}",
            "display_name": name,
            "avatar_url": f"https://api.dicebear.com/8.x/thumbs/svg?seed={uid[:8]}",
            "role": "admin" if i == 0 else "user",
        })
    return users


def gen_report(
    reporter_id: str,
    zone_key: Optional[str] = None,
) -> Dict:
    """더미 쓰레기 제보 1건 생성."""
    zk = zone_key or random_zone()
    lat, lon = random_point_in_zone(zk)
    trash_type = random.choice(TRASH_TYPES)
    severity = "boss" if trash_type == "large" and random.random() > 0.7 else random.choice(SEVERITIES[:3])
    source = random.choices(SOURCES, weights=[60, 25, 15], k=1)[0]
    conf = round(random.uniform(0.5, 0.99), 4) if random.random() > 0.2 else None
    status = random.choices(
        ["pending", "ai_verified", "completed"],
        weights=[30, 40, 30],
        k=1,
    )[0]

    return {
        "id": str(uuid.uuid4()),
        "reporter_id": reporter_id,
        "lat": lat,
        "lon": lon,
        "zone_key": zk,
        "trash_type": trash_type,
        "severity": severity,
        "status": status,
        "source": source,
        "ai_confidence": conf,
        "image_urls": [f"https://picsum.photos/seed/{uid.hex[:8]}-{trash_type}/640/480"],
        "created_at": datetime.utcnow() - timedelta(
            hours=random.randint(0, 168),
            minutes=random.randint(0, 59),
        ),
    }


def gen_post(user: Dict, report: Dict) -> Dict:
    """더미 SNS 게시물 생성."""
    tags = random.sample(HASHTAGS, k=random.randint(2, 4))
    contents = [
        f"{ZONES[report['zone_key']]['name']} 앞 {report['trash_type']} 발견!",
        f"오늘도 플로깅 완료 🌿 {report['severity']}급 쓰레기 처리!",
        f"AI가 탐지한 쓰레기 직접 확인하고 청소했습니다 ✨",
        f"산책하다 발견! 바로 제보합니다 📸",
        f"드론이 찾은 쓰레기 수거 완료 🚁",
    ]
    return {
        "id": str(uuid.uuid4()),
        "user_id": user["id"],
        "report_id": report["id"],
        "content": random.choice(contents),
        "image_urls": report["image_urls"],
        "hashtags": tags,
        "lat": report["lat"],
        "lon": report["lon"],
        "zone_key": report["zone_key"],
        "post_type": "report",
        "like_count": random.randint(0, 150),
        "created_at": report["created_at"],
    }


def gen_drone_event(zone_key: str) -> Dict:
    """더미 드론 이벤트 생성."""
    lat, lon = random_point_in_zone(zone_key)
    return {
        "id": str(uuid.uuid4()),
        "drone_id": f"DRONE-{random.randint(1,5):02d}",
        "event_type": "patrol",
        "lat": lat,
        "lon": lon,
        "zone_key": zone_key,
        "video_url": f"https://example.com/drone/{uuid.uuid4()}.mp4",
        "detected_count": random.randint(0, 8),
        "created_at": datetime.utcnow() - timedelta(hours=random.randint(0, 48)),
    }


def gen_point_entry(user_id: str, xp: int, reason: str) -> Dict:
    """더미 포인트 원장 엔트리."""
    return {
        "id": str(uuid.uuid4()),
        "user_id": user_id,
        "delta": xp,
        "reason": reason,
        "ref_id": str(uuid.uuid4()),
        "created_at": datetime.utcnow() - timedelta(hours=random.randint(0, 168)),
    }
