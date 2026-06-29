#!/bin/bash
#
# Meta SW Plogging — 서비스 헬스체크 에이전트
# 주기적으로 서비스 상태를 확인하고, 중지된 서비스를 자동 재시작합니다.
# crontab: */3 * * * * /home/spark/research/meta-flow/platform/scripts/health-agent.sh
#

PLATFORM_DIR="/home/spark/research/meta-flow/platform"
LOG_FILE="/home/spark/research/meta-flow/platform/logs/health-agent.log"
CONDA_ENV="meta-flow"

# 로그 함수
log() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" >> "$LOG_FILE"
}

# 서비스 정의: 이름|포트|스크립트경로|헬스URL
SERVICES=(
    "gateway|8500|services/gateway/main.py|http://127.0.0.1:8500/health"
    "sns|8501|services/sns/main.py|http://127.0.0.1:8501/health"
    "game|8502|services/game/main.py|http://127.0.0.1:8502/health"
    "gis|8503|services/gis/main.py|http://127.0.0.1:8503/health"
)

# 환경변수 로드
cd "$PLATFORM_DIR" || exit 1
export $(grep -v '^#' .env | grep -v '^$' | xargs) 2>/dev/null

# 로그 디렉토리 확인
mkdir -p "$(dirname "$LOG_FILE")"

RESTARTED=0

for svc in "${SERVICES[@]}"; do
    IFS='|' read -r name port script health_url <<< "$svc"

    # 1차: 프로세스 존재 확인
    pid=$(ps aux | grep "python $script" | grep -v grep | grep -v conda | awk '{print $2}' | head -1)

    if [ -z "$pid" ]; then
        log "[$name] 프로세스 없음 — 재시작 시도"
        nohup conda run -n "$CONDA_ENV" python "$script" >> "/tmp/${name}-service.log" 2>&1 &
        sleep 3
        RESTARTED=$((RESTARTED + 1))

        # 재시작 확인
        new_pid=$(ps aux | grep "python $script" | grep -v grep | grep -v conda | awk '{print $2}' | head -1)
        if [ -n "$new_pid" ]; then
            log "[$name] 재시작 성공 (PID: $new_pid)"
        else
            log "[$name] 재시작 실패!"
        fi
        continue
    fi

    # 2차: HTTP 헬스체크 (프로세스는 있지만 응답 안 하는 경우)
    response=$(curl -s -o /dev/null -w "%{http_code}" --max-time 5 "$health_url" 2>/dev/null)
    if [ "$response" != "200" ]; then
        log "[$name] 응답 없음 (HTTP $response, PID: $pid) — 재시작"
        kill "$pid" 2>/dev/null
        sleep 2
        nohup conda run -n "$CONDA_ENV" python "$script" >> "/tmp/${name}-service.log" 2>&1 &
        sleep 3
        RESTARTED=$((RESTARTED + 1))

        new_pid=$(ps aux | grep "python $script" | grep -v grep | grep -v conda | awk '{print $2}' | head -1)
        if [ -n "$new_pid" ]; then
            log "[$name] 재시작 성공 (PID: $new_pid)"
        else
            log "[$name] 재시작 실패!"
        fi
    fi
done

# 전부 정상이면 로그 안 남김 (로그 파일 비대 방지)
# 재시작이 있었을 때만 요약 로그
if [ $RESTARTED -gt 0 ]; then
    log "[AGENT] ${RESTARTED}개 서비스 재시작 완료"
fi

# 로그 파일 크기 관리 (1MB 초과 시 롤링)
if [ -f "$LOG_FILE" ] && [ $(stat -f%z "$LOG_FILE" 2>/dev/null || stat -c%s "$LOG_FILE" 2>/dev/null) -gt 1048576 ]; then
    tail -500 "$LOG_FILE" > "${LOG_FILE}.tmp"
    mv "${LOG_FILE}.tmp" "$LOG_FILE"
    log "[AGENT] 로그 파일 롤링 완료"
fi
