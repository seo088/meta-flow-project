#!/usr/bin/env python3
"""
더미/테스트 제보(핀) 데이터 하드삭제 스크립트.

보존:  실제 사용자 업로드(source='sns') + 일괄적재 외부데이터셋(source='external_di').
삭제 대상(더미):
  1) source='aihub'  — picsum.photos placeholder 이미지의 테스트 핀(게시글 미연결)
  2) source IN ('unity_sim')  — 시뮬레이터 테스트 데이터(있으면)
  3) 시드 더미 계정(user_NNN, seed_dummy.py)과 그 소유 제보/게시글/포인트/배지/퀘스트
     — generators.py: username ~ '^user_[0-9]{3}$' AND email LIKE 'user%@kunsan.ac.kr'
     (신선한 시드 환경/NAS 대비. 현 DB엔 0건일 수 있음)

기본은 dry-run(집계만). 실제 삭제는 --apply 필요.
사용:
  conda run -n meta-flow python scripts/purge_dummy_data.py            # dry-run
  conda run -n meta-flow python scripts/purge_dummy_data.py --apply    # 실삭제
"""
import asyncio
import os
import sys
import asyncpg

DUMMY_SOURCES = ("aihub", "unity_sim")
SEED_USER_RE = r"^user_[0-9]{3}$"


async def gather(c):
    """삭제 대상 식별 + 집계."""
    # 1) 더미 source 제보
    src_reports = await c.fetchval(
        "SELECT count(*) FROM trash_reports WHERE source = ANY($1::text[])", list(DUMMY_SOURCES)
    )
    src_posts = await c.fetchval(
        "SELECT count(*) FROM posts p JOIN trash_reports tr ON tr.id=p.report_id "
        "WHERE tr.source = ANY($1::text[])", list(DUMMY_SOURCES)
    )
    src_ledger = await c.fetchval(
        "SELECT count(*) FROM point_ledger WHERE ref_id::text IN "
        "(SELECT id::text FROM trash_reports WHERE source = ANY($1::text[]))", list(DUMMY_SOURCES)
    )
    # 2) 시드 더미 계정
    seed_users = await c.fetchval(
        "SELECT count(*) FROM users WHERE username ~ $1 AND email LIKE 'user%@kunsan.ac.kr'",
        SEED_USER_RE,
    )
    seed_reports = await c.fetchval(
        "SELECT count(*) FROM trash_reports WHERE reporter_id IN "
        "(SELECT id FROM users WHERE username ~ $1 AND email LIKE 'user%@kunsan.ac.kr')",
        SEED_USER_RE,
    )
    return {
        "dummy_sources": list(DUMMY_SOURCES),
        "src_reports": src_reports,
        "src_posts": src_posts,
        "src_ledger": src_ledger,
        "seed_users": seed_users,
        "seed_reports": seed_reports,
    }


async def purge(c):
    """FK 안전 순서로 단일 트랜잭션 삭제."""
    async with c.transaction():
        # ── 1) 더미 source 제보 (+ 연결 posts/댓글/좋아요/ledger) ──
        await c.execute(
            "DELETE FROM post_likes WHERE post_id IN (SELECT p.id FROM posts p "
            "JOIN trash_reports tr ON tr.id=p.report_id WHERE tr.source = ANY($1::text[]))",
            list(DUMMY_SOURCES))
        await c.execute(
            "DELETE FROM post_comments WHERE post_id IN (SELECT p.id FROM posts p "
            "JOIN trash_reports tr ON tr.id=p.report_id WHERE tr.source = ANY($1::text[]))",
            list(DUMMY_SOURCES))
        await c.execute(
            "DELETE FROM posts WHERE report_id IN "
            "(SELECT id FROM trash_reports WHERE source = ANY($1::text[]))", list(DUMMY_SOURCES))
        await c.execute(
            "DELETE FROM point_ledger WHERE ref_id::text IN "
            "(SELECT id::text FROM trash_reports WHERE source = ANY($1::text[]))", list(DUMMY_SOURCES))
        await c.execute(
            "DELETE FROM trash_reports WHERE source = ANY($1::text[])", list(DUMMY_SOURCES))

        # ── 2) 시드 더미 계정 및 소유 데이터 ──
        seed_filter = "username ~ $1 AND email LIKE 'user%@kunsan.ac.kr'"
        dummy_uids = [r["id"] for r in await c.fetch(
            f"SELECT id FROM users WHERE {seed_filter}", SEED_USER_RE)]
        if dummy_uids:
            rep_ids = [r["id"] for r in await c.fetch(
                "SELECT id FROM trash_reports WHERE reporter_id = ANY($1::uuid[])", dummy_uids)]
            post_ids = [r["id"] for r in await c.fetch(
                "SELECT id FROM posts WHERE user_id = ANY($1::uuid[]) "
                "OR report_id = ANY($2::uuid[])", dummy_uids, rep_ids or [None])]
            if post_ids:
                await c.execute("DELETE FROM post_likes WHERE post_id = ANY($1::uuid[])", post_ids)
                await c.execute("DELETE FROM post_comments WHERE post_id = ANY($1::uuid[])", post_ids)
                await c.execute("DELETE FROM posts WHERE id = ANY($1::uuid[])", post_ids)
            await c.execute("DELETE FROM point_ledger WHERE user_id = ANY($1::uuid[])", dummy_uids)
            await c.execute("DELETE FROM user_badges WHERE user_id = ANY($1::uuid[])", dummy_uids)
            await c.execute("DELETE FROM user_quests WHERE user_id = ANY($1::uuid[])", dummy_uids)
            await c.execute("DELETE FROM drone_events WHERE TRUE")  # 전부 시드
            if rep_ids:
                await c.execute("DELETE FROM trash_reports WHERE id = ANY($1::uuid[])", rep_ids)
            await c.execute(f"DELETE FROM users WHERE {seed_filter}", SEED_USER_RE)

    # MV 갱신 (user_points)
    try:
        await c.execute("REFRESH MATERIALIZED VIEW CONCURRENTLY user_points")
    except Exception:
        await c.execute("REFRESH MATERIALIZED VIEW user_points")


async def main():
    apply = "--apply" in sys.argv
    dsn = os.environ.get("DATABASE_URL")
    if not dsn:
        print("DATABASE_URL 환경변수 필요 (source .env)", file=sys.stderr)
        sys.exit(1)
    c = await asyncpg.connect(dsn)
    try:
        before = await gather(c)
        print("=" * 56)
        print("삭제 대상 집계 (dry-run)" if not apply else "삭제 실행")
        print("=" * 56)
        print(f"  더미 source {before['dummy_sources']}:")
        print(f"    - trash_reports : {before['src_reports']}")
        print(f"    - 연결 posts    : {before['src_posts']}")
        print(f"    - point_ledger  : {before['src_ledger']}")
        print(f"  시드 더미 계정(user_NNN):")
        print(f"    - users         : {before['seed_users']}")
        print(f"    - 소유 reports  : {before['seed_reports']}")
        print("  보존: source IN ('sns','external_di') + 실제 계정")
        if not apply:
            print("\n  ⚠️  dry-run 입니다. 실제 삭제하려면 --apply 를 붙이세요.")
            return
        print("\n  삭제 진행 중...")
        await purge(c)
        after = await gather(c)
        print(f"  완료. 잔여 더미 reports: {after['src_reports']}, "
              f"시드 계정: {after['seed_users']}")
    finally:
        await c.close()


if __name__ == "__main__":
    asyncio.run(main())
