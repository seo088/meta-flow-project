# Skill: DB 스키마 변경 가이드

> PostgreSQL+PostGIS 스키마 변경 시 참조

## 연결 정보

```
Host: 203.234.62.176:5433
DB: plogging_db
User: plogging
Password: (환경변수 POSTGRES_PASSWORD)
```

## 스키마 변경 절차

1. **asyncpg로 직접 실행**:
   ```python
   conda run -n meta-flow python -c "
   import asyncio, asyncpg, os
   os.environ['DATABASE_URL']='postgresql://plogging:plogging_dev_2026@203.234.62.176:5433/plogging_db'
   async def main():
       db = await asyncpg.connect(os.environ['DATABASE_URL'])
       # 컬럼 존재 확인
       r = await db.fetchval(\"SELECT column_name FROM information_schema.columns WHERE table_name='테이블명' AND column_name='컬럼명'\")
       if not r:
           await db.execute('ALTER TABLE 테이블명 ADD COLUMN 컬럼명 타입 DEFAULT 값')
           print('Added')
       await db.close()
   asyncio.run(main())
   "
   ```
2. **세션 파일에 변경사항 기록** (`## DB 스키마 변경` 섹션)
3. **관련 서비스 재시작**

## 주요 테이블

| 테이블 | 핵심 컬럼 | 비고 |
|--------|----------|------|
| users | id(UUID), username, email, password_hash(bcrypt), display_name, bio, avatar_url, role, quest_invite_opt_in | |
| posts | id, user_id, content, image_urls[], hashtags[], visibility, post_type, report_id, like_count | |
| post_comments | id, post_id, user_id, content | |
| post_likes | id, post_id, user_id, reaction_type | |
| trash_reports | id, **reporter_id**, location(PostGIS), zone_id, trash_type, severity, image_urls[], source | reporter_id 주의 |
| user_created_quests | id, title, description, quest_type, creator_id, status, member_count, reward_xp, is_public, cover_image_url | |
| quest_posts | id, quest_id, user_id, content, image_urls[], like_count, visibility | |
| quest_members | quest_id, user_id, role, status | status: accepted/warned/suspended |
| point_ledger | id, user_id, delta, reason, ref_id | XP 적립 원장 |
| user_points | user_id, total_points | **Materialized View** — INSERT 불가 |
| notifications | id, user_id, type, title, body, ref_post_id, actor_username, is_read | |
| dm_messages | id, sender_id, recipient_id, content, is_read | |
| geofence_zones | id, zone_key, name, geom(PostGIS), bonus_multiplier | |

## MV 갱신

```sql
REFRESH MATERIALIZED VIEW CONCURRENTLY user_points;
```

## PostGIS 패턴

```sql
-- 포인트 생성
ST_SetSRID(ST_MakePoint(lon, lat), 4326)

-- 영역 포함 확인
ST_Contains(geom, ST_SetSRID(ST_MakePoint(lon, lat), 4326))
```
