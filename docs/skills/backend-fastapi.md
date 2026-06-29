# Skill: 백엔드 FastAPI 서비스 수정 가이드

> SNS(:8501), Game(:8502), GIS(:8503) 서비스 수정 시 참조

## 서비스 구조

```
platform/services/
├── sns/main.py      # 피드, 게시글, 인증, 프로필, DM, 알림
├── game/main.py     # 퀘스트, 랭킹, 배지, XP, 멤버 관리
├── gis/main.py      # 지도, 핀맵, 구역 통계, GeoJSON
├── gateway/main.py  # 정적 파일 + API 프록시
└── ai/main.py       # AI 예측, 엔티티 태깅
```

## 수정 절차

1. **코드 수정**: 해당 서비스 파일 수정
2. **서비스 재시작** (필수):
   ```bash
   # 예: SNS 서비스
   kill $(ps aux | grep "python services/sns/main.py" | grep -v grep | grep -v conda | awk '{print $2}') 2>/dev/null
   sleep 2
   cd /home/spark/research/meta-flow/platform
   export $(grep -v '^#' .env | grep -v '^$' | xargs) 2>/dev/null
   conda run -n meta-flow python services/sns/main.py &
   ```
3. **환경변수**: `.env`는 `platform/.env`에 위치 — 워크트리에서는 절대 경로 사용

## 공통 패턴

### DB 연결
```python
db = app.state.db  # asyncpg pool (create_db_pool)
row = await db.fetchrow("SELECT ... WHERE id=$1::uuid", some_id)
rows = await db.fetch("SELECT ... LIMIT $1", size)
val = await db.fetchval("SELECT COUNT(*) ...", some_id)
await db.execute("INSERT INTO ... VALUES ($1, $2)", val1, val2)
```

### 트랜잭션
```python
async with db.acquire() as conn:
    async with conn.transaction():
        await conn.execute(...)
        await conn.execute(...)
```

### 인증
```python
from core.auth import get_current_user, get_optional_user
# 필수 인증
@app.get("/endpoint")
async def handler(u: dict = Depends(get_current_user)):
    uid = u["user_id"]
    username = u["username"]

# 선택 인증 (비로그인 허용)
@app.get("/endpoint")
async def handler(u: dict | None = Depends(get_optional_user)):
```

### 레벨 계산
```python
lv = min(10, max(1, xp // 150 + 1))
titles = {1:'새싹', 2:'풀잎', 3:'나무', 4:'숲', 5:'탐정', ...}
```

## 서비스별 핵심 엔드포인트

### SNS (:8501)
| 메서드 | 경로 | 용도 |
|--------|------|------|
| POST | /auth/register | 회원가입 (bcrypt) |
| POST | /auth/login | 로그인 (JWT) |
| GET | /feed | 피드 조회 (liked/my_reaction 포함) |
| POST | /posts/create | 일반 게시 |
| POST | /posts/report | 제보 게시 (FormData) |
| POST | /posts/{id}/like | 리액션 |
| GET/POST | /notifications | 알림 조회/읽음 |
| DELETE | /profile/me | 회원 탈퇴 |

### Game (:8502)
| 메서드 | 경로 | 용도 |
|--------|------|------|
| GET | /quests/popular | 인기 퀘스트 (top_image_url 포함) |
| GET | /quests/{id}/feed | 퀘스트 피드 (visibility 포함) |
| POST | /quests/{id}/feed | 퀘스트 게시 (SNS 동시 게시) |
| GET | /ranking/global | 글로벌 랭킹 |

## 주의사항

- `user_points`는 Materialized View — INSERT 불가
- `trash_reports.reporter_id` (user_id 아님)
- XP 기록: `point_ledger`에 INSERT → `REFRESH MATERIALIZED VIEW CONCURRENTLY user_points`
- 비속어 필터: `check_content(db, user_id, content)` 호출 필수
