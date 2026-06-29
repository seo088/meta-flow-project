# 메타 플로깅 — 게미피케이션 엔진 설계

> **참조**: `02_backend.md` §1.5 `gamification.py`, `03_agents_mcp.md` §2 `award_points` 도구  
> **원칙**: 포인트 지급은 MCP `award_points` 단 하나의 경로만 허용. DB UNIQUE 제약으로 중복 지급 차단.

---

## 1. 보상 구조

### 1.1 행동별 기본 XP

| 행동 | 기본 XP | 구역 배율 | 주말 보너스 |
|------|---------|-----------|------------|
| 쓰레기 제보 (SNS) | 20 | ×zone.bonus | ×1.5 |
| 드론 영상 업로드 | 30 | ×1.0 | — |
| 청소 완료 인증 | 50 | ×zone.bonus | ×1.5 |
| 퀘스트 완료 | quest.reward_xp | — | — |
| 보스 레이드 | 100 + 퀘스트 | ×2.0 | — |
| 첫 구역 제보 | 100 | — | — |

### 1.2 구역 보너스 배율

| 구역 | 배율 | 이유 |
|------|------|------|
| 군산대 캠퍼스 | ×1.0 | 기준값 |
| 은파유원지 | ×1.5 | Phase 2 확장 |
| 새만금 | ×2.0 | 광역 관리 어려움 |
| 금강하구둑 | ×1.8 | Phase 3 확장 |

---

## 2. 레벨 시스템 (`shared/core/gamification.py`)

`02_backend.md §1.5`에 정의됨. 요약:

| 레벨 | XP | 칭호 |
|------|----|------|
| 1 | 0 | 새싹 플로거 |
| 4 | 600 | 열정 플로거 |
| 8 | 3,000 | 환경 수호자 |
| 10 | 5,500 | 마스터 플로거 |
| 12 | 10,000 | 군산 레전드 |

---

## 3. Game Service API (`services/game/main.py`)

```python
"""
Game Service — 랭킹·레벨·퀘스트 현황·배지 조회.
포인트 지급은 MCP award_points에 위임 (직접 수행 금지).
Kafka Consumer: user.points.updated → Redis 랭킹 갱신
"""
import os, sys, asyncio, json
from contextlib import asynccontextmanager
from fastapi import FastAPI, Depends, Query
from typing import Optional
import asyncpg
import redis.asyncio as aioredis

sys.path.insert(0, "/app/shared")
from core.auth import get_current_user
from core.kafka_client import KafkaConsumerClient
from core.events import PointsUpdatedEvent
from core.gamification import get_level
from core.logger import get_logger

logger = get_logger("game-service")

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db    = await asyncpg.create_pool(
        dsn=os.environ["DATABASE_URL"], min_size=5, max_size=30
    )
    app.state.redis = aioredis.from_url(
        f"redis://:{os.environ['REDIS_PASSWORD']}@{os.environ['REDIS_HOST']}:6379",
        decode_responses=True
    )
    stop     = asyncio.Event()
    consumer = KafkaConsumerClient(
        os.environ["KAFKA_BOOTSTRAP_SERVERS"],
        "game-service-ranking",
        ["user.points.updated"]
    )

    async def sync_ranking(raw: dict):
        e = PointsUpdatedEvent(**raw)
        await app.state.redis.zadd("ranking:global", {e.user_id: e.new_total})
        # WebSocket 랭킹 업데이트 브로드캐스트
        await app.state.redis.publish("ws:ranking", json.dumps({
            "type": "ranking_updated", "user_id": e.user_id, "new_total": e.new_total
        }))

    task = asyncio.create_task(consumer.consume_loop(sync_ranking, dict, stop))
    app.state.stop = stop; app.state.task = task
    yield
    stop.set(); await task; consumer.close()
    await app.state.db.close(); await app.state.redis.aclose()

app = FastAPI(title="Game Service", lifespan=lifespan)

# ── 내 프로필 ──────────────────────────────────────────────
@app.get("/profile/me")
async def my_profile(u: dict = Depends(get_current_user)):
    db, redis = app.state.db, app.state.redis

    cached = await redis.zscore("ranking:global", u["user_id"])
    xp = int(cached) if cached is not None else (
        await db.fetchval(
            "SELECT COALESCE(SUM(delta),0) FROM point_ledger WHERE user_id=$1",
            u["user_id"]
        ) or 0
    )
    level, title, xp_next = get_level(xp)
    rank = await redis.zrevrank("ranking:global", u["user_id"])

    badges = await db.fetch("""
        SELECT bd.badge_key, bd.name, bd.icon_url, bd.rarity, ub.earned_at
        FROM user_badges ub JOIN badge_definitions bd ON bd.id = ub.badge_id
        WHERE ub.user_id = $1 ORDER BY ub.earned_at DESC
    """, u["user_id"])

    return {"success": True, "data": {
        "user_id": u["user_id"], "total_xp": xp,
        "level": level, "level_title": title, "xp_to_next": xp_next,
        "global_rank": (rank + 1) if rank is not None else None,
        "badges": [dict(b) for b in badges],
    }}

# ── 글로벌 랭킹 (캐시 10초) ──────────────────────────────
@app.get("/ranking/global")
async def global_ranking(page: int = Query(1, ge=1), size: int = Query(50, le=100)):
    redis = app.state.redis; db = app.state.db
    cache_key = f"cache:leaderboard:{page}:{size}"

    cached = await redis.get(cache_key)
    if cached:
        return {"success": True, "data": json.loads(cached), "cached": True}

    start    = (page - 1) * size
    entries  = await redis.zrevrange("ranking:global", start, start+size-1, withscores=True)
    if not entries:
        return {"success": True, "data": [], "cached": False}

    user_ids = [e[0] for e in entries]
    urows    = await db.fetch("""
        SELECT id::text, display_name, avatar_url
        FROM users WHERE id = ANY($1::uuid[])
    """, user_ids)
    umap = {r["id"]: dict(r) for r in urows}

    result = [
        {"rank": start+i+1, "user_id": uid, "total_xp": int(sc),
         "display_name": umap.get(uid,{}).get("display_name","Unknown"),
         "avatar_url":   umap.get(uid,{}).get("avatar_url"),
         "level": get_level(int(sc))[0], "level_title": get_level(int(sc))[1]}
        for i,(uid,sc) in enumerate(entries)
    ]
    await redis.set(cache_key, json.dumps(result), ex=10)
    return {"success": True, "data": result, "cached": False}

# ── 구역 랭킹 ──────────────────────────────────────────────
@app.get("/ranking/zone/{zone_key}")
async def zone_ranking(zone_key: str, size: int = Query(20, le=50)):
    entries = await app.state.redis.zrevrange(
        f"ranking:zone:{zone_key}", 0, size-1, withscores=True
    )
    return {"success": True, "data": [
        {"rank": i+1, "user_id": uid, "xp": int(sc)}
        for i,(uid,sc) in enumerate(entries)
    ]}

# ── 나의 퀘스트 ───────────────────────────────────────────
@app.get("/quests/me")
async def my_quests(u: dict = Depends(get_current_user)):
    rows = await app.state.db.fetch("""
        SELECT uq.progress, uq.is_completed, uq.completed_at,
               qd.quest_key, qd.title, qd.description, qd.quest_type,
               qd.target_count, qd.reward_xp,
               gz.name AS zone_name,
               bd.name AS badge_name, bd.icon_url AS badge_icon
        FROM user_quests uq
        JOIN quest_definitions qd ON qd.id = uq.quest_id
        LEFT JOIN geofence_zones gz ON gz.id = qd.zone_id
        LEFT JOIN badge_definitions bd ON bd.id = qd.reward_badge_id
        WHERE uq.user_id=$1 AND qd.is_active=TRUE
        ORDER BY uq.is_completed, qd.reward_xp DESC
    """, u["user_id"])
    return {"success": True, "data": [dict(r) for r in rows]}

# ── 퀘스트 수락 ───────────────────────────────────────────
@app.post("/quests/{quest_key}/accept")
async def accept_quest(quest_key: str, u: dict = Depends(get_current_user)):
    db = app.state.db
    qid = await db.fetchval(
        "SELECT id FROM quest_definitions WHERE quest_key=$1 AND is_active=TRUE", quest_key
    )
    if not qid:
        return {"success": False, "error": "Quest not found"}
    await db.execute("""
        INSERT INTO user_quests (user_id, quest_id)
        VALUES ($1,$2) ON CONFLICT (user_id, quest_id) DO NOTHING
    """, u["user_id"], qid)
    return {"success": True, "data": {"quest_id": str(qid)}}

# ── 포인트 이력 ───────────────────────────────────────────
@app.get("/points/history")
async def point_history(page: int = Query(1,ge=1), size: int = Query(20,le=50),
                         u: dict = Depends(get_current_user)):
    offset = (page - 1) * size
    rows = await app.state.db.fetch("""
        SELECT delta, reason, ref_id, created_at
        FROM point_ledger WHERE user_id=$1
        ORDER BY created_at DESC LIMIT $2 OFFSET $3
    """, u["user_id"], size, offset)
    return {"success": True, "data": [dict(r) for r in rows]}
```

---

## 4. 배지 및 퀘스트 시드 데이터 (`infra/postgres/seed.sql`)

```sql
-- ── 배지 정의 ────────────────────────────────────────────
INSERT INTO badge_definitions (badge_key, name, description, rarity) VALUES
('first_report',       '🌱 첫 발걸음',     '첫 번째 제보',            'common'),
('report_5',           '🔍 탐정 Lv.1',     '5건 제보',                'common'),
('report_20',          '🔎 탐정 Lv.2',     '20건 제보',               'rare'),
('cleanup_first',      '🧹 첫 청소',       '첫 번째 청소 완료',         'common'),
('cleanup_10',         '✨ 청소왕',        '10번 청소 완료',            'rare'),
('eunpa_guardian',     '💧 은파 수호자',   '은파유원지 5건 처리',        'rare'),
('saemangeum_warrior', '🌊 새만금 워리어', '새만금 구역 미션 완료',      'epic'),
('geumgang_ranger',    '🌿 금강 레인저',   '금강하구둑 미션 완료',       'epic'),
('boss_killer',        '💀 보스 킬러',     '보스 레이드 성공',           'legendary'),
('drone_hero',         '🚁 드론 영웅',     '드론 영상 5건 업로드',       'rare'),
('ku_champion',        '🎓 군산대 챔피언', '군산대 구역 1위',            'epic'),
('eco_legend',         '🌍 에코 레전드',   'Lv.12 달성',               'legendary'),
('weekly_streak_4',    '📅 4주 연속',      '주간 퀘스트 4주 연속',       'epic')
ON CONFLICT (badge_key) DO NOTHING;

-- ── 주간 퀘스트 ──────────────────────────────────────────
INSERT INTO quest_definitions
  (quest_key, title, description, quest_type, target_count, reward_xp)
VALUES
  ('weekly_plogging_3', '🌱 주간 플로깅 챌린지', '이번 주 3건 이상 플로깅 완료',
   'weekly', 3, 150),
  ('weekly_report_5',   '📍 탐정 미션',          '이번 주 5건 쓰레기 제보',
   'weekly', 5, 100),
  ('weekly_drone_1',    '🚁 드론 협력',           '드론 영상 1건 업로드',
   'weekly', 1, 80)
ON CONFLICT (quest_key) DO NOTHING;

-- ── 구역별 퀘스트 ──────────────────────────────────────
INSERT INTO quest_definitions
  (quest_key, title, description, quest_type, zone_id, target_count, reward_xp)
SELECT 'eunpa_guardian_5', '💧 은파 수호자', '은파유원지에서 5건 이상 처리',
       'zone', id, 5, 200
FROM geofence_zones WHERE zone_key = 'EUNPA'
ON CONFLICT (quest_key) DO NOTHING;

INSERT INTO quest_definitions
  (quest_key, title, description, quest_type, zone_id, target_count, reward_xp)
SELECT 'saemangeum_mission_3', '🌊 새만금 미션', '새만금 구역에서 3건 처리',
       'zone', id, 3, 250
FROM geofence_zones WHERE zone_key = 'SAEMANGEUM'
ON CONFLICT (quest_key) DO NOTHING;

INSERT INTO quest_definitions
  (quest_key, title, description, quest_type, zone_id, target_count, reward_xp)
SELECT 'geumgang_mission_3', '🌿 금강 레인저', '금강하구둑에서 3건 처리',
       'zone', id, 3, 220
FROM geofence_zones WHERE zone_key = 'GEUMGANG'
ON CONFLICT (quest_key) DO NOTHING;
```

---

## 5. 실시간 알림 WebSocket 메시지 스펙

```python
# 모든 ws.broadcast.* 메시지 페이로드 타입

# 포인트 적립 (개인)
{"type":"points_earned","user_id":"uuid","delta":50,"new_total":1870,
 "reason":"cleanup_verified",
 "level_up":{"from":8,"to":9,"new_title":"군산 레인저"}}  # 레벨업 시만 포함

# 배지 획득 (개인)
{"type":"badge_earned","user_id":"uuid",
 "badge":{"badge_key":"eunpa_guardian","name":"💧 은파 수호자","rarity":"rare"}}

# 퀘스트 완료 (개인)
{"type":"quest_completed","user_id":"uuid","quest_key":"weekly_plogging_3",
 "title":"주간 플로깅 챌린지","xp_reward":150}

# 랭킹 업데이트 (구역 브로드캐스트)
{"type":"ranking_updated",
 "top3":[{"rank":1,"display_name":"박민준","xp":5240},
          {"rank":2,"display_name":"이수빈","xp":4120}]}

# 보스 핀 등장 (구역 브로드캐스트)
{"type":"boss_appeared","zone_key":"SAEMANGEUM","zone_name":"새만금",
 "location":{"lat":35.80,"lon":126.65},"quest_key":"boss_raid_xxx","xp_reward":400}

# 새 핀 등록 (구역 브로드캐스트)
{"type":"pin_added","report":{"id":"uuid","lat":35.97,"lon":126.69,
                               "trash_type":"large","severity":"boss"}}
```

---

## 6. 중복 제보 방지 (Redis)

```python
# services/sns/main.py — create_report_post() 내 추가 로직

async def check_duplicate(redis, lat: float, lon: float) -> bool:
    """
    50m 이내 중복 제보 방지.
    소수점 3자리 반올림 ≈ 111m × 0.001 = 111m 격자.
    실제 운영 시 PostGIS ST_DWithin으로 정밀 검사 추가 가능.
    """
    key = f"dedup:report:{round(lat,3)}:{round(lon,3)}"
    if await redis.exists(key):
        return True           # 중복
    await redis.setex(key, 3600, "1")
    return False
```

---

## 7. 성능 최적화 체크리스트

### 랭킹 처리 (1,000명 동시접속)

```
Redis ZADD ranking:global → O(log N)  실시간 갱신
Redis ZREVRANGE           → O(log N + M)  상위 M명 조회
API 캐시 10초             → 동시 1000명 × 60req/min = 1000TPS
                             → 캐시 적중률 99% 목표
```

### 퀘스트 진행 캐시 전략

```
PostgreSQL user_quests  → 원천 (정확성)
Redis HSET quest:{uid}:{qid}  → 캐시 (속도)
캐시 TTL: 86400초 (1일)
캐시 무효화: 퀘스트 완료 시 DEL
```

### PostgreSQL 인덱스 전략

```sql
-- 랭킹 조회
CREATE INDEX idx_ledger_user_created ON point_ledger(user_id, created_at DESC);

-- 퀘스트 진행 조회
CREATE INDEX idx_uq_user_active ON user_quests(user_id)
WHERE is_completed = FALSE;

-- 피드 조회
CREATE INDEX idx_posts_zone_created ON posts(zone_id, created_at DESC);
```
