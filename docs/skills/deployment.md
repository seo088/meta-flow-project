# Skill: 배포 및 서비스 관리 가이드

## 서비스 포트 맵

| 서비스 | 포트 | 프로세스 |
|--------|------|---------|
| Gateway | :8500 | `python services/gateway/main.py` |
| SNS | :8501 | `python services/sns/main.py` |
| Game | :8502 | `python services/game/main.py` |
| GIS | :8503 | `python services/gis/main.py` |
| AI | :8504 | `python services/ai/main.py` |
| Data | :8505 | `python services/data/main.py` |
| MCP | :8510 | `python services/mcp/server.py` |

## 환경변수

- `.env` 위치: `/home/spark/research/meta-flow/platform/.env`
- 워크트리에서는 반드시 **절대 경로** 사용
- 로드 방법: `export $(grep -v '^#' /home/spark/research/meta-flow/platform/.env | grep -v '^$' | xargs)`

## 서비스 재시작 패턴

```bash
# 1. 프로세스 종료
kill $(ps aux | grep "python services/SERVICE/main.py" | grep -v grep | grep -v conda | awk '{print $2}') 2>/dev/null

# 2. 환경변수 로드 + 재시작
sleep 2
cd /home/spark/research/meta-flow/platform
export $(grep -v '^#' .env | grep -v '^$' | xargs) 2>/dev/null
conda run -n meta-flow python services/SERVICE/main.py &
```

## 전체 서비스 상태 확인

```bash
ps aux | grep "python services/" | grep -v grep | grep -v conda
```

## GitLab Push

```bash
cd /home/spark/research/meta-flow
git push origin main --tags
```

## 캐시 관리

- 게이트웨이에 `Cache-Control: no-cache, no-store, must-revalidate` 헤더 적용됨
- 브라우저에서 Ctrl+Shift+R로 강제 새로고침

## 주의사항

- `conda run`에 `--no-banner` 플래그 사용 금지 (지원 안 됨)
- 서비스 시작 시 DB 연결 실패 → `.env` 환경변수 미로드가 원인
- 워크트리에서 실행 시 cwd가 자동 리셋됨 → 절대 경로 사용
