#!/usr/bin/env python3
"""
수거 퀘스트(Phase 2) 마이그레이션.
- user_created_quests에 is_cleanup / auto_generated 컬럼 추가
- quest_target_reports: 퀘스트가 대상으로 하는 제보 묶음 + 수거 진행 추적
추가형(IF NOT EXISTS). 실행: conda run -n meta-flow python scripts/migrate_cleanup_quests.py
"""
import os
import sys
import asyncio


def _dsn():
    d = os.environ.get("DATABASE_URL")
    if not d:
        print("DATABASE_URL 필요", file=sys.stderr); sys.exit(1)
    return d


DDL = """
ALTER TABLE user_created_quests ADD COLUMN IF NOT EXISTS is_cleanup BOOLEAN DEFAULT FALSE;
ALTER TABLE user_created_quests ADD COLUMN IF NOT EXISTS auto_generated BOOLEAN DEFAULT FALSE;

CREATE TABLE IF NOT EXISTS quest_target_reports (
    quest_id   UUID NOT NULL REFERENCES user_created_quests(id) ON DELETE CASCADE,
    report_id  UUID NOT NULL REFERENCES trash_reports(id) ON DELETE CASCADE,
    cleaned_by UUID REFERENCES users(id),
    cleaned_at TIMESTAMPTZ,
    PRIMARY KEY (quest_id, report_id)
);
CREATE INDEX IF NOT EXISTS idx_qtr_report ON quest_target_reports(report_id);
CREATE INDEX IF NOT EXISTS idx_qtr_quest  ON quest_target_reports(quest_id);
"""


async def main():
    import asyncpg
    conn = await asyncpg.connect(_dsn())
    try:
        await conn.execute(DDL)
        cols = await conn.fetch("""SELECT column_name FROM information_schema.columns
            WHERE table_name='user_created_quests' AND column_name IN ('is_cleanup','auto_generated')""")
        tbl = await conn.fetchval("SELECT to_regclass('quest_target_reports')")
        print("컬럼:", sorted(r["column_name"] for r in cols), "| quest_target_reports:", tbl)
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
