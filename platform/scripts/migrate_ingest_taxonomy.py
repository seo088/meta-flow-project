"""
배치 적재용 DB 마이그레이션 (idempotent, additive):
  1) trash_reports.gt_label jsonb  추가
  2) trash_reports.handling varchar 추가
  3) report_categories 테이블 생성 + 분류체계 9종 시드 (trash_taxonomy.CATEGORIES)
  4) dataset_bot 시스템 계정 생성

실행: conda run -n meta-flow python scripts/migrate_ingest_taxonomy.py
"""
import os
import sys
import asyncio

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))
from core.trash_taxonomy import CATEGORIES  # noqa: E402

DATASET_BOT_USERNAME = "dataset_bot"
DATASET_BOT_EMAIL = "dataset_bot@meta-flow.local"


def _dsn() -> str:
    return (
        f"postgresql://{os.environ.get('POSTGRES_USER', 'plogging')}:"
        f"{os.environ.get('POSTGRES_PASSWORD', 'plogging_dev_2026')}@"
        f"{os.environ.get('POSTGRES_HOST', '203.234.62.176')}:"
        f"{os.environ.get('POSTGRES_PORT', '5433')}/"
        f"{os.environ.get('POSTGRES_DB', 'plogging')}"
    )


async def main():
    import asyncpg
    import bcrypt

    conn = await asyncpg.connect(_dsn())
    try:
        # 1) + 2) trash_reports 컬럼 추가
        await conn.execute("ALTER TABLE trash_reports ADD COLUMN IF NOT EXISTS gt_label jsonb")
        await conn.execute("ALTER TABLE trash_reports ADD COLUMN IF NOT EXISTS handling varchar")
        print("[1/4] trash_reports.gt_label, handling 컬럼 확인/추가 완료")

        # 3) report_categories 테이블 + 시드
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS report_categories (
                key              varchar PRIMARY KEY,
                label_ko         varchar NOT NULL,
                recycle_group    varchar,
                handling         varchar,
                recyclable       boolean DEFAULT false,
                color            varchar,
                default_severity varchar,
                is_builtin       boolean DEFAULT false,
                created_at       timestamptz DEFAULT now()
            )
        """)
        for key, c in CATEGORIES.items():
            await conn.execute("""
                INSERT INTO report_categories
                    (key, label_ko, recycle_group, handling, recyclable, color, default_severity, is_builtin)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8)
                ON CONFLICT (key) DO UPDATE SET
                    label_ko=EXCLUDED.label_ko, recycle_group=EXCLUDED.recycle_group,
                    handling=EXCLUDED.handling, recyclable=EXCLUDED.recyclable,
                    color=EXCLUDED.color, default_severity=EXCLUDED.default_severity,
                    is_builtin=EXCLUDED.is_builtin
            """, key, c["label_ko"], c["recycle_group"], c["handling"],
                 c["recyclable"], c["color"], c["default_severity"], c["is_builtin"])
        total = await conn.fetchval("SELECT COUNT(*) FROM report_categories")
        print(f"[2/4] report_categories 테이블 + {len(CATEGORIES)}종 시드 완료 (총 {total}행)")

        # 4) dataset_bot 계정
        existing = await conn.fetchrow("SELECT id FROM users WHERE username=$1", DATASET_BOT_USERNAME)
        if existing:
            bot_id = existing["id"]
            print(f"[3/4] dataset_bot 이미 존재 (id={bot_id})")
        else:
            pw_hash = bcrypt.hashpw(os.urandom(24).hex().encode(), bcrypt.gensalt()).decode()
            bot_id = await conn.fetchval("""
                INSERT INTO users (username, email, password_hash, display_name, role, quest_invite_opt_in)
                VALUES ($1,$2,$3,$4,'system',false)
                RETURNING id
            """, DATASET_BOT_USERNAME, DATASET_BOT_EMAIL, pw_hash, "외부 데이터셋")
            print(f"[3/4] dataset_bot 생성 완료 (id={bot_id}, role=system)")

        print(f"[4/4] 마이그레이션 완료. dataset_bot id = {bot_id}")
    finally:
        await conn.close()


if __name__ == "__main__":
    asyncio.run(main())
