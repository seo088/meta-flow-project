"""
배치 제보 적재 CLI — 내부 ingest API(SNS)의 얇은 클라이언트.

merged_records.json(정답 라벨 + 좌표) + 이미지 폴더 → /admin/reports/ingest-batch 호출.
dataset_bot 으로 인증(JWT 발급), SHA-256 멱등이므로 재실행 안전. 감사 로그(jsonl) 기록.

예)
  conda run -n meta-flow python scripts/batch_report_ingest.py \
    --manifest data/03.External_data/extracted/메타플로깅_DI_데이터/merged_records.json \
    --image-dir data/03.External_data/extracted/메타플로깅_DI_데이터/image \
    --batch-id 2026-06-01-di-001 --limit 10 --dry-run
"""
import os
import sys
import json
import time
import argparse
from urllib.request import Request, urlopen

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "shared"))
from core.auth import create_access_token  # noqa: E402
from core.trash_taxonomy import CATEGORIES, category_key  # noqa: E402

BOT_USERNAME = "dataset_bot"


def _dsn():
    return (f"postgresql://{os.environ.get('POSTGRES_USER','plogging')}:"
            f"{os.environ.get('POSTGRES_PASSWORD','plogging_dev_2026')}@"
            f"{os.environ.get('POSTGRES_HOST','203.234.62.176')}:"
            f"{os.environ.get('POSTGRES_PORT','5433')}/{os.environ.get('POSTGRES_DB','plogging')}")


def get_bot_token():
    """dataset_bot user_id 조회 후 system 권한 JWT 발급."""
    import asyncio, asyncpg

    async def _fetch():
        conn = await asyncpg.connect(_dsn())
        try:
            row = await conn.fetchrow("SELECT id, role FROM users WHERE username=$1", BOT_USERNAME)
            return (str(row["id"]), row["role"]) if row else (None, None)
        finally:
            await conn.close()

    uid, role = asyncio.get_event_loop().run_until_complete(_fetch())
    if not uid:
        sys.exit(f"오류: {BOT_USERNAME} 계정 없음. migrate_ingest_taxonomy.py 를 먼저 실행하세요.")
    return create_access_token(uid, BOT_USERNAME, role or "system")


def build_content(labels):
    """라벨 items → 한 줄 본문 + 해시태그 생성."""
    parts, tags = [], []
    for it in labels:
        ko = (it.get("label") or "").strip()
        if not ko:
            continue
        parts.append(f"{ko} {it.get('quantity', 1)}개({it.get('size', '')})")
        key = category_key(ko)
        tag = CATEGORIES.get(key, {}).get("label_ko", ko).replace("/", "")
        if tag not in tags:
            tags.append(tag)
    return "외부 데이터셋 제보 — " + ", ".join(parts), tags


def post_batch(api, token, payload):
    req = Request(api.rstrip("/") + "/admin/reports/ingest-batch",
                  data=json.dumps(payload, ensure_ascii=False).encode("utf-8"),
                  headers={"Content-Type": "application/json",
                           "Authorization": f"Bearer {token}"}, method="POST")
    with urlopen(req, timeout=600) as resp:
        return json.loads(resp.read())


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--manifest", required=True, help="merged_records.json 경로")
    ap.add_argument("--image-dir", required=True, help="이미지 폴더 경로")
    ap.add_argument("--api", default="http://localhost:8501")
    ap.add_argument("--batch-id", required=True)
    ap.add_argument("--source-dataset", default="메타플로깅_DI_데이터(260512)")
    ap.add_argument("--chunk", type=int, default=50, help="요청당 항목 수")
    ap.add_argument("--limit", type=int, default=0, help="처리 상한(0=전체)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--emit-events", action="store_true")
    ap.add_argument("--log-dir", default="logs")
    args = ap.parse_args()

    manifest = os.path.realpath(args.manifest)
    image_dir = os.path.realpath(args.image_dir)
    records = json.load(open(manifest, encoding="utf-8"))
    if args.limit:
        records = records[: args.limit]

    # 매니페스트 레코드 → API item
    items, missing = [], 0
    for r in records:
        fn = r.get("imagePath")
        if not fn:
            continue
        path = os.path.join(image_dir, fn)
        if not os.path.exists(path):
            missing += 1
            continue
        labels = r.get("items") or []
        content, tags = build_content(labels)
        items.append({
            "filename": fn, "local_path": os.path.realpath(path), "labels": labels,
            "lat": r.get("latitude"), "lon": r.get("longitude"),
            "content": content, "hashtags": tags,
            "captured_at": r.get("timestamp"), "record_source": r.get("source"),
        })

    print(f"매니페스트 {len(records)}건 → 적재 대상 {len(items)}건 (이미지 누락 {missing}건), "
          f"dry_run={args.dry_run}")

    token = get_bot_token()
    os.makedirs(args.log_dir, exist_ok=True)
    log_path = os.path.join(args.log_dir, f"ingest-{args.batch_id}.jsonl")
    agg = {"total": 0, "created": 0, "skipped": 0, "failed": 0}

    with open(log_path, "a", encoding="utf-8") as logf:
        for i in range(0, len(items), args.chunk):
            chunk = items[i: i + args.chunk]
            payload = {"batch_id": args.batch_id, "source_dataset": args.source_dataset,
                       "dry_run": args.dry_run, "emit_events": args.emit_events,
                       "base_dir": image_dir, "items": chunk}
            try:
                resp = post_batch(args.api, token, payload)
            except Exception as e:
                print(f"  청크 {i}-{i+len(chunk)} 요청 실패: {e}")
                continue
            data = resp.get("data", {})
            for r in data.get("results", []):
                logf.write(json.dumps(r, ensure_ascii=False) + "\n")
            s = data.get("summary", {})
            for k in agg:
                agg[k] += s.get(k, 0)
            print(f"  청크 {i}-{i+len(chunk)}: {s}")
            time.sleep(0.1)

    print(f"\n=== 완료: {agg} ===")
    print(f"감사 로그: {log_path}")


if __name__ == "__main__":
    main()
