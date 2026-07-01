"""Meta SW Plogging — DB 백업 (asyncpg 기반)"""
import asyncio
import asyncpg
import gzip
import sys
import os
from datetime import datetime

DB_URL = "postgresql://plogging:plogging_dev_2026@203.234.62.176:5433/plogging_db"


async def main():
    backup_dir = sys.argv[1] if len(sys.argv) > 1 else "/home/spark/research/meta-flow/platform/db_backup"
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    filepath = os.path.join(backup_dir, f"plogging_db_{timestamp}.sql.gz")
    os.makedirs(backup_dir, exist_ok=True)

    conn = await asyncpg.connect(DB_URL)

    tables = await conn.fetch(
        "SELECT tablename FROM pg_tables WHERE schemaname='public' ORDER BY tablename"
    )

    out = []
    out.append(f"-- Meta SW Plogging DB Backup")
    out.append(f"-- Timestamp: {timestamp}")
    out.append(f"-- Tables: {len(tables)}")
    out.append("")

    total_rows = 0
    for t in tables:
        tn = t["tablename"]
        rows = await conn.fetch(f"SELECT * FROM {tn}")
        if not rows:
            out.append(f"-- Table {tn}: 0 rows")
            continue

        cols = list(rows[0].keys())
        out.append(f"-- Table: {tn} ({len(rows)} rows)")
        out.append(f"DELETE FROM {tn};")
        total_rows += len(rows)

        for r in rows:
            vals = []
            for c in cols:
                v = r[c]
                if v is None:
                    vals.append("NULL")
                elif isinstance(v, bool):
                    vals.append("TRUE" if v else "FALSE")
                elif isinstance(v, (int, float)):
                    vals.append(str(v))
                elif isinstance(v, list):
                    arr = "{" + ",".join(str(x) for x in v) + "}"
                    vals.append("'" + arr.replace("'", "''") + "'")
                else:
                    vals.append("'" + str(v).replace("'", "''") + "'")
            col_str = ", ".join(cols)
            val_str = ", ".join(vals)
            out.append(f"INSERT INTO {tn} ({col_str}) VALUES ({val_str});")
        out.append("")

    data = "\n".join(out).encode("utf-8")
    with gzip.open(filepath, "wb") as f:
        f.write(data)

    await conn.close()
    size_kb = os.path.getsize(filepath) / 1024
    print(f"OK|{len(tables)} tables|{total_rows} rows|{size_kb:.1f}KB|{filepath}")


if __name__ == "__main__":
    asyncio.run(main())
