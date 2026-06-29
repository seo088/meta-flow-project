#!/bin/bash
# 모든 서비스 종료
echo "[*] conda 서비스 종료..."
pkill -f "services/gateway/main.py" 2>/dev/null && echo "  Gateway stopped" || true
pkill -f "services/ai/main.py" 2>/dev/null && echo "  AI Service stopped" || true
pkill -f "services/sns/main.py" 2>/dev/null && echo "  SNS Service stopped" || true
pkill -f "services/game/main.py" 2>/dev/null && echo "  Game Service stopped" || true
pkill -f "services/gis/main.py" 2>/dev/null && echo "  GIS Service stopped" || true
pkill -f "services/data/main.py" 2>/dev/null && echo "  Data Service stopped" || true
pkill -f "services/mcp/server.py" 2>/dev/null && echo "  MCP Server stopped" || true
pkill -f "services/ws/main.py" 2>/dev/null && echo "  WS Server stopped" || true

echo ""
echo "[*] 에이전트 종료..."
pkill -f "agents/drone_agent" 2>/dev/null && echo "  Drone Agent stopped" || true
pkill -f "agents/quest_agent" 2>/dev/null && echo "  Quest Agent stopped" || true
pkill -f "agents/data_agent" 2>/dev/null && echo "  Data Agent stopped" || true
pkill -f "agents/mobility_agent" 2>/dev/null && echo "  Mobility Agent stopped" || true
pkill -f "agents/scheduler_agent" 2>/dev/null && echo "  Scheduler Agent stopped" || true

echo ""
echo "[*] Docker 인프라 종료..."
cd "$(dirname "$0")/../infra/docker"
docker compose down

echo "[✓] 전체 종료 완료"
