"""
AIHub 새만금 방조제 유입 하천 쓰레기 데이터 수집 에이전트
Dataset: https://aihub.or.kr/aihubdata/data/view.do?dataSetSn=71594

실제 AIHub API 연동 시 API 키가 필요하나, 현재는 실제 데이터 스키마 기반
시뮬레이션 모드로 동작합니다.

9대 쓰레기 카테고리: 플라스틱, 스티로폼, 섬유, 비닐/필름, 목재, 금속, 유리, 고무, 종이
"""
import random
import uuid
from datetime import datetime, timedelta
from typing import List, Dict

AIHUB_CATEGORIES = {
    "플라스틱": {"class": "plastic", "color": "#38bdf8", "details": ["페트병", "용기", "뚜껑", "파편"]},
    "스티로폼": {"class": "plastic", "color": "#f472b6", "details": ["부표", "상자", "파편"]},
    "섬유": {"class": "general", "color": "#a78bfa", "details": ["의류", "로프", "그물"]},
    "비닐/필름": {"class": "plastic", "color": "#fb923c", "details": ["비닐봉지", "포장재", "농업용"]},
    "목재": {"class": "large", "color": "#a3e635", "details": ["판자", "가지", "가구파편"]},
    "금속": {"class": "large", "color": "#94a3b8", "details": ["캔", "철근", "파이프"]},
    "유리": {"class": "general", "color": "#22d3ee", "details": ["병", "파편"]},
    "고무": {"class": "general", "color": "#f43f5e", "details": ["타이어", "고무호스", "파편"]},
    "종이": {"class": "food", "color": "#fbbf24", "details": ["박스", "종이컵", "기타"]},
}

SAEMANGEUM_POINTS = [
    {"name": "만경강 하류", "lat": 35.8234, "lon": 126.7123},
    {"name": "동진강 하류", "lat": 35.7856, "lon": 126.6789},
    {"name": "새만금 방조제 북단", "lat": 35.8412, "lon": 126.6234},
    {"name": "새만금 방조제 중앙", "lat": 35.8102, "lon": 126.5856},
    {"name": "새만금 방조제 남단", "lat": 35.7789, "lon": 126.5543},
    {"name": "군산 내항", "lat": 35.9756, "lon": 126.7089},
    {"name": "비응도 해안", "lat": 35.9423, "lon": 126.5912},
    {"name": "야미도 인근", "lat": 35.9012, "lon": 126.6345},
]

EQUIPMENT = [
    {"equipment_name": "DJI Phantom 4 Pro", "model_name": "FC6310"},
    {"equipment_name": "DJI Mavic 3", "model_name": "L2D-20c"},
    {"equipment_name": "Canon EOS R5", "model_name": "EOS R5"},
    {"equipment_name": "Sony A7R IV", "model_name": "ILCE-7RM4"},
]


def generate_aihub_annotation() -> Dict:
    category_name = random.choice(list(AIHUB_CATEGORIES.keys()))
    cat = AIHUB_CATEGORIES[category_name]
    detail = random.choice(cat["details"])
    point = random.choice(SAEMANGEUM_POINTS)
    equip = random.choice(EQUIPMENT)

    img_w, img_h = 4000, 3000
    bx = random.randint(100, img_w - 500)
    by = random.randint(100, img_h - 500)
    bw = random.randint(80, 400)
    bh = random.randint(80, 400)

    file_id = uuid.uuid4().hex[:12]
    date = datetime.now() - timedelta(days=random.randint(0, 365), hours=random.randint(6, 18))

    return {
        "id": str(uuid.uuid4()),
        "shoot_info": {
            "equipment_name": equip["equipment_name"],
            "model_name": equip["model_name"],
            "file_name": f"SM_{date.strftime('%Y%m%d')}_{file_id}.jpg",
            "width": img_w, "height": img_h,
            "date": date.strftime("%Y-%m-%d %H:%M:%S"),
            "region_name": "전북특별자치도 군산시",
            "location_name": point["name"],
            "latitude": point["lat"] + random.uniform(-0.005, 0.005),
            "longitude": point["lon"] + random.uniform(-0.005, 0.005),
        },
        "annotations": [
            {
                "category": category_name, "class": cat["class"], "detail": detail,
                "damage": random.choice(["Y", "N"]),
                "labeling": random.choice(["rect", "polygon"]),
                "x": bx, "y": by, "width": bw, "height": bh,
                "confidence": round(random.uniform(0.7, 0.99), 3),
            }
            for _ in range(random.randint(1, 5))
        ],
        "meta": {
            "source": "aihub", "dataset_id": 71594,
            "dataset_name": "전북 새만금 방조제 유입 하천 쓰레기 데이터",
            "license": "AI Hub 이용약관",
        },
    }


def generate_batch(count: int = 50) -> List[Dict]:
    return [generate_aihub_annotation() for _ in range(count)]


def get_category_stats(data: List[Dict]) -> Dict:
    stats = {}
    for item in data:
        for ann in item["annotations"]:
            cat = ann["category"]
            stats[cat] = stats.get(cat, 0) + 1
    return dict(sorted(stats.items(), key=lambda x: -x[1]))


def get_location_stats(data: List[Dict]) -> List[Dict]:
    loc_map = {}
    for item in data:
        loc = item["shoot_info"]["location_name"]
        loc_map[loc] = loc_map.get(loc, 0) + 1
    return [{"location": k, "count": v} for k, v in sorted(loc_map.items(), key=lambda x: -x[1])]


def to_platform_reports(data: List[Dict]) -> List[Dict]:
    reports = []
    for item in data:
        si = item["shoot_info"]
        for ann in item["annotations"]:
            reports.append({
                "lat": si["latitude"], "lon": si["longitude"],
                "trash_type": ann["class"],
                "severity": "high" if ann.get("damage") == "Y" else "mid",
                "source": "aihub", "ai_confidence": ann.get("confidence", 0.85),
                "zone_key": "SAEMANGEUM", "location_name": si["location_name"],
                "category_detail": f"{ann['category']}/{ann['detail']}",
                "image_file": si["file_name"], "captured_at": si["date"],
            })
    return reports
