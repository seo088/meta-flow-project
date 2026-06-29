#!/bin/bash
# conda 환경에서 전체 서비스 + 에이전트 일괄 시작
# 모든 포트는 .env에서 읽음 (85xx 대역)
set -euo pipefail

PROJ_DIR="$(cd "$(dirname "$0")/.." && pwd)"
cd "$PROJ_DIR"

# .env 환경변수 로드
export $(grep -v '^#' .env | grep -v '^$' | xargs)
export PYTHONPATH="$PROJ_DIR/shared:$PROJ_DIR"

echo "============================================"
echo " 메타 플로깅 — conda 서비스 시작"
echo " conda env: meta-flow"
echo "============================================"

# 로그 디렉토리
mkdir -p logs

# ── 백엔드 마이크로서비스 ──
echo ""
echo "[1/7] SNS Service (:${SNS_PORT:-8501})..."
nohup python services/sns/main.py > logs/sns-service.log 2>&1 &
echo "  PID: $!"

echo "[2/7] Game Service (:${GAME_PORT:-8502})..."
nohup python services/game/main.py > logs/game-service.log 2>&1 &
echo "  PID: $!"

echo "[3/7] GIS Service (:${GIS_PORT:-8503})..."
nohup python services/gis/main.py > logs/gis-service.log 2>&1 &
echo "  PID: $!"

echo "[4/7] AI Service (:${AI_PORT:-8504})..."
nohup python services/ai/main.py > logs/ai-service.log 2>&1 &
echo "  PID: $!"

echo "[5/7] Data Service (:${DATA_PORT:-8505})..."
nohup python services/data/main.py > logs/data-service.log 2>&1 &
echo "  PID: $!"

sleep 1

echo "[6/7] MCP Server (:${MCP_PORT:-8510})..."
nohup python services/mcp/server.py > logs/mcp-server.log 2>&1 &
echo "  PID: $!"

echo "[7/7] WebSocket Server (:${WS_PORT:-8520})..."
nohup python services/ws/main.py > logs/ws-server.log 2>&1 &
echo "  PID: $!"

sleep 1

# ── AI 에이전트 ──
echo ""
echo "=== AI Agents ==="
echo "[A1] Drone Agent (:${DRONE_AGENT_PORT:-8531})..."
nohup python agents/drone_agent/agent.py > logs/drone-agent.log 2>&1 &
echo "  PID: $!"

echo "[A2] Quest Agent..."
nohup python agents/quest_agent/agent.py > logs/quest-agent.log 2>&1 &
echo "  PID: $!"

echo "[A3] Data Agent (:${DATA_AGENT_PORT:-8532})..."
nohup python agents/data_agent/agent.py > logs/data-agent.log 2>&1 &
echo "  PID: $!"

echo "[A4] Mobility Agent (:${MOBILITY_AGENT_PORT:-8533})..."
nohup python agents/mobility_agent/agent.py > logs/mobility-agent.log 2>&1 &
echo "  PID: $!"

echo "[A5] Scheduler Agent..."
nohup python agents/scheduler_agent/agent.py > logs/scheduler-agent.log 2>&1 &
echo "  PID: $!"

echo ""
echo "============================================"
echo " 실행 중인 서비스 (85xx 포트):"
echo "   SNS Service    : http://localhost:${SNS_PORT:-8501}"
echo "   Game Service   : http://localhost:${GAME_PORT:-8502}"
echo "   GIS Service    : http://localhost:${GIS_PORT:-8503}"
echo "   AI Service     : http://localhost:${AI_PORT:-8504}"
echo "   Data Service   : http://localhost:${DATA_PORT:-8505}"
echo "   MCP Server     : http://localhost:${MCP_PORT:-8510}"
echo "   WS Server      : http://localhost:${WS_PORT:-8520}"
echo ""
echo " 에이전트:"
echo "   Drone Agent    : http://localhost:${DRONE_AGENT_PORT:-8531}"
echo "   Data Agent     : http://localhost:${DATA_AGENT_PORT:-8532}"
echo "   Mobility Agent : http://localhost:${MOBILITY_AGENT_PORT:-8533}"
echo "   Quest Agent    : (Kafka Consumer)"
echo "   Scheduler      : (cron loop)"
echo ""
echo " 로그 확인: tail -f logs/*.log"
echo " 전체 종료: bash scripts/stop_all.sh"
echo "============================================"
