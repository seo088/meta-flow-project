"""
쓰레기 분류체계 (SSOT) — 외부 DI 데이터셋 라벨과 앱 카테고리의 다축 매핑.

축:
  1) category (trash_type) : 품목 9종 (기존 5종 + 신규 6종 중 일부)
  2) size                  : small / medium / large (원본 보존)
  3) handling              : normal(일반수거) / recycle(분리배출) / bulk(대형·수거불가)
  4) severity              : size + handling 으로 자동 산출 (보조축)

merged_records.json 의 items[].label(한글) → category key 매핑 + 카테고리 속성 정의.
ingest 코어, DB 마이그레이션(report_categories 시드), 프론트 색상 매핑이 모두 이 파일을 참조한다.
"""
from __future__ import annotations
from typing import Optional

# category key -> 속성. is_builtin=True 는 기존 TrashType enum(plastic/food/general/large/unknown).
CATEGORIES: dict[str, dict] = {
    # ── 기존 빌트인 ──
    "general":   {"label_ko": "일반쓰레기",   "recycle_group": "일반",   "handling": "normal",  "recyclable": False, "color": "#9ca3af", "default_severity": "low",  "is_builtin": True},
    "plastic":   {"label_ko": "플라스틱류",   "recycle_group": "재활용", "handling": "recycle", "recyclable": True,  "color": "#38bdf8", "default_severity": "low",  "is_builtin": True},
    "large":     {"label_ko": "대형/수거불가", "recycle_group": "대형",   "handling": "bulk",    "recyclable": False, "color": "#ef4444", "default_severity": "high", "is_builtin": True},
    "food":      {"label_ko": "음식물",       "recycle_group": "일반",   "handling": "normal",  "recyclable": False, "color": "#f97316", "default_severity": "mid",  "is_builtin": True},
    "unknown":   {"label_ko": "미분류",       "recycle_group": "일반",   "handling": "normal",  "recyclable": False, "color": "#6b7280", "default_severity": "mid",  "is_builtin": True},
    # ── 신규 (외부 DI 데이터셋) ──
    "cigarette": {"label_ko": "담배꽁초",     "recycle_group": "일반",   "handling": "normal",  "recyclable": False, "color": "#a16207", "default_severity": "low",  "is_builtin": False},
    "paper":     {"label_ko": "종이류",       "recycle_group": "재활용", "handling": "recycle", "recyclable": True,  "color": "#f59e0b", "default_severity": "low",  "is_builtin": False},
    "vinyl":     {"label_ko": "비닐류",       "recycle_group": "재활용", "handling": "recycle", "recyclable": True,  "color": "#22c55e", "default_severity": "low",  "is_builtin": False},
    "metal":     {"label_ko": "캔/금속류",    "recycle_group": "재활용", "handling": "recycle", "recyclable": True,  "color": "#64748b", "default_severity": "low",  "is_builtin": False},
    "glass":     {"label_ko": "유리류",       "recycle_group": "재활용", "handling": "recycle", "recyclable": True,  "color": "#14b8a6", "default_severity": "mid",  "is_builtin": False},
    "styrofoam": {"label_ko": "스티로폼",     "recycle_group": "재활용", "handling": "recycle", "recyclable": True,  "color": "#e879f9", "default_severity": "low",  "is_builtin": False},
}

# merged_records.json items[].label (한글) → category key
LABEL_KO_TO_KEY: dict[str, str] = {
    "담배꽁초": "cigarette",
    "일반쓰레기": "general",
    "비닐류": "vinyl",
    "플라스틱류": "plastic",
    "종이류": "paper",
    "캔/금속류": "metal",
    "스티로폼": "styrofoam",
    "대형/수거불가": "large",
    "유리류": "glass",
}

SIZE_TO_SEVERITY = {"small": "low", "medium": "mid", "large": "high"}
_SEV_RANK = {"low": 1, "mid": 2, "high": 3, "boss": 4}
_SIZE_RANK = {"small": 1, "medium": 2, "large": 3}


def category_key(label_ko: str) -> str:
    """한글 라벨 → category key (미지의 라벨은 'unknown')."""
    return LABEL_KO_TO_KEY.get((label_ko or "").strip(), "unknown")


def derive_severity(size: Optional[str], handling: Optional[str], category_default: Optional[str] = None) -> str:
    """size + handling 으로 긴급성 자동 산출 (대형·수거불가 → high)."""
    if handling == "bulk":
        return "high"
    if size in SIZE_TO_SEVERITY:
        return SIZE_TO_SEVERITY[size]
    return category_default or "mid"


def map_items(items: list[dict]) -> tuple[list[dict], Optional[dict], str, Optional[str]]:
    """
    merged_records.json 의 items[] → 다축 매핑.
    반환: (mapped_items, primary_item, report_severity, report_handling)
      - mapped_items : 각 item에 category/handling/severity 부여
      - primary_item : 대표 item (severity → size → quantity 내림차순)
      - report_severity : 대표 severity (= primary)
      - report_handling : 대표 handling (= primary)
    """
    mapped: list[dict] = []
    for it in items or []:
        key = category_key(it.get("label"))
        cat = CATEGORIES.get(key, CATEGORIES["unknown"])
        size = it.get("size")
        handling = cat.get("handling")
        sev = derive_severity(size, handling, cat.get("default_severity"))
        mapped.append({
            "label_ko": it.get("label"),
            "category": key,
            "size": size,
            "quantity": it.get("quantity", 1),
            "handling": handling,
            "severity": sev,
        })
    if not mapped:
        return [], None, "mid", "normal"
    primary = max(
        mapped,
        key=lambda m: (_SEV_RANK.get(m["severity"], 0), _SIZE_RANK.get(m["size"], 0), m.get("quantity", 1)),
    )
    return mapped, primary, primary["severity"], primary["handling"]
