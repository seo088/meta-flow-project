#!/bin/bash
# 전체 서비스 + 에이전트 시작 (conda meta-flow 환경)
set -euo pipefail

PROJ_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJ_DIR"

# .env 로드
set -a; source .env; set +a
export PYTHONPATH="$PROJ_DIR/shared:$PROJ_DIR"

PY="/home/spark/anaconda3/envs/meta-flow/bin/python"

# 기존 프로세스 종료
pkill -f "services/gateway/main.py" 2>/dev/null || true
pkill -f "services/ai/main.py" 2>/dev/null || true
pkill -f "services/sns/main.py" 2>/dev/null || true
pkill -f "services/game/main.py" 2>/dev/null || true
pkill -f "services/gis/main.py" 2>/dev/null || true
pkill -f "services/data/main.py" 2>/dev/null || true
pkill -f "services/mcp/server.py" 2>/dev/null || true
pkill -f "services/ws/main.py" 2>/dev/null || true
pkill -f "agents/drone_agent" 2>/dev/null || true
pkill -f "agents/mobility_agent" 2>/dev/null || true
pkill -f "agents/quest_agent" 2>/dev/null || true
pkill -f "agents/data_agent" 2>/dev/null || true
pkill -f "agents/scheduler_agent" 2>/dev/null || true
sleep 1

mkdir -p "$PROJ_DIR/logs"

echo "============================================"
echo " 메타 플로깅 — 전체 서비스 시작"
echo " conda env: meta-flow"
echo " 서버 IP: ${SERVER_HOST}"
echo "============================================"

echo "[0/8] Gateway (:${GATEWAY_PORT})..."
nohup $PY "$PROJ_DIR/services/gateway/main.py" > "$PROJ_DIR/logs/gateway.log" 2>&1 &
echo "  PID: $!"

echo "[1/8] AI Service (:${AI_PORT})..."
nohup $PY "$PROJ_DIR/services/ai/main.py" > "$PROJ_DIR/logs/ai-service.log" 2>&1 &
echo "  PID: $!"

echo "[2/7] SNS Service (:${SNS_PORT})..."
nohup $PY "$PROJ_DIR/services/sns/main.py" > "$PROJ_DIR/logs/sns-service.log" 2>&1 &
echo "  PID: $!"

echo "[3/7] Game Service (:${GAME_PORT})..."
nohup $PY "$PROJ_DIR/services/game/main.py" > "$PROJ_DIR/logs/game-service.log" 2>&1 &
echo "  PID: $!"

echo "[4/7] GIS Service (:${GIS_PORT})..."
nohup $PY "$PROJ_DIR/services/gis/main.py" > "$PROJ_DIR/logs/gis-service.log" 2>&1 &
echo "  PID: $!"

echo "[5/7] Data Service (:${DATA_PORT})..."
nohup $PY "$PROJ_DIR/services/data/main.py" > "$PROJ_DIR/logs/data-service.log" 2>&1 &
echo "  PID: $!"

echo "[6/7] MCP Server (:${MCP_PORT})..."
nohup $PY "$PROJ_DIR/services/mcp/server.py" > "$PROJ_DIR/logs/mcp-server.log" 2>&1 &
echo "  PID: $!"

echo "[7/7] WS Server (:${WS_PORT})..."
nohup $PY "$PROJ_DIR/services/ws/main.py" > "$PROJ_DIR/logs/ws-service.log" 2>&1 &
echo "  PID: $!"

sleep 1

echo ""
echo "=== 에이전트 ==="
echo "[A1] Drone Agent (:${DRONE_AGENT_PORT})..."
nohup $PY "$PROJ_DIR/agents/drone_agent/agent.py" > "$PROJ_DIR/logs/drone-agent.log" 2>&1 &
echo "  PID: $!"

echo "[A2] Mobility Agent (:${MOBILITY_AGENT_PORT})..."
nohup $PY "$PROJ_DIR/agents/mobility_agent/agent.py" > "$PROJ_DIR/logs/mobility-agent.log" 2>&1 &
echo "  PID: $!"

echo "[A3] Quest Agent (Kafka consumer)..."
nohup $PY "$PROJ_DIR/agents/quest_agent/agent.py" > "$PROJ_DIR/logs/quest-agent.log" 2>&1 &
echo "  PID: $!"

echo "[A4] Data Agent (:${DATA_AGENT_PORT})..."
nohup $PY "$PROJ_DIR/agents/data_agent/agent.py" > "$PROJ_DIR/logs/data-agent.log" 2>&1 &
echo "  PID: $!"

echo "[A5] Scheduler Agent (cron)..."
nohup $PY "$PROJ_DIR/agents/scheduler_agent/agent.py" > "$PROJ_DIR/logs/scheduler-agent.log" 2>&1 &
echo "  PID: $!"

echo ""
echo "============================================"
echo " 5초 대기 후 헬스체크..."
echo "============================================"
sleep 5

echo ""
echo "--- Docker 인프라 ---"
docker ps --format "  {{.Names}}: {{.Status}}" 2>/dev/null | grep plogging || echo "  Docker 미실행"

echo ""
echo "--- FastAPI 서비스 ---"
for svc in "GATEWAY:${GATEWAY_PORT}" "AI:${AI_PORT}" "SNS:${SNS_PORT}" "GAME:${GAME_PORT}" "GIS:${GIS_PORT}" "DATA:${DATA_PORT}" "MCP:${MCP_PORT}" "WS:${WS_PORT}"; do
  IFS=':' read name port <<< "$svc"
  result=$(curl -s --max-time 2 "http://${SERVER_HOST}:${port}/health" 2>/dev/null)
  if [ -n "$result" ]; then
    printf "  ✅ %-15s :%-5s %s\n" "$name" "$port" "$result"
  else
    printf "  ❌ %-15s :%-5s 응답없음\n" "$name" "$port"
  fi
done

echo ""
echo "--- 에이전트 ---"
for svc in "DRONE:${DRONE_AGENT_PORT}" "MOBILITY:${MOBILITY_AGENT_PORT}" "DATA_AGENT:${DATA_AGENT_PORT}"; do
  IFS=':' read name port <<< "$svc"
  result=$(curl -s --max-time 2 "http://${SERVER_HOST}:${port}/health" 2>/dev/null)
  if [ -n "$result" ]; then
    printf "  ✅ %-15s :%-5s %s\n" "$name" "$port" "$result"
  else
    printf "  ❌ %-15s :%-5s 응답없음\n" "$name" "$port"
  fi
done

echo ""
echo "============================================"
echo " 웹 UI 접속:"
echo "   웹 포털:       http://${SERVER_HOST}:${GATEWAY_PORT}/"
echo "   관리자 대시보드: http://${SERVER_HOST}:${GATEWAY_PORT}/admin"
echo ""
echo " 로그: tail -f $PROJ_DIR/logs/*.log"
echo " 종료: bash $PROJ_DIR/scripts/stop_all.sh"
echo "============================================"
