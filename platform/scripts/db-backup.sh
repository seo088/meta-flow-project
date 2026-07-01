#!/bin/bash
#
# Meta SW Plogging — DB 백업 스크립트
# crontab: 0 2 * * * /home/spark/research/meta-flow/platform/scripts/db-backup.sh
#

BACKUP_DIR="/home/spark/research/meta-flow/platform/db_backup"
SCRIPT_DIR="/home/spark/research/meta-flow/platform/scripts"
LOG_FILE="$BACKUP_DIR/backup.log"

mkdir -p "$BACKUP_DIR"

log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "$LOG_FILE"
}

log "백업 시작"

RESULT=$(cd /home/spark/research/meta-flow/platform && conda run --no-capture-output -n meta-flow python "$SCRIPT_DIR/db_backup.py" "$BACKUP_DIR" 2>> "$LOG_FILE")

if echo "$RESULT" | grep -q "^OK|"; then
    log "백업 완료: $RESULT"
    echo "✅ $RESULT"
else
    log "백업 실패: $RESULT"
    echo "❌ 백업 실패"
    exit 1
fi

# 30일 초과 백업 삭제
DELETED=$(find "$BACKUP_DIR" -name "plogging_db_*.sql.gz" -mtime +30 -delete -print | wc -l)
if [ "$DELETED" -gt 0 ]; then log "오래된 백업 ${DELETED}건 삭제"; fi

# 로그 롤링 (1MB 초과 시)
if [ -f "$LOG_FILE" ] && [ $(stat -c%s "$LOG_FILE" 2>/dev/null || echo 0) -gt 1048576 ]; then
    tail -200 "$LOG_FILE" > "${LOG_FILE}.tmp" && mv "${LOG_FILE}.tmp" "$LOG_FILE"
fi
