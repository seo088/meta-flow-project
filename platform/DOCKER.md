# 🐳 Meta SW 플로깅 — Docker 배포 가이드

리눅스 운영 환경을 **맥(Apple Silicon · Intel)**, **Windows WSL2**, **다른 리눅스 서버**에 동일하게 옮길 수 있는 풀스택 도커 배포 패키지입니다.

---

## 📦 포함 구성

| 컴포넌트 | 이미지 | 포트 | 역할 |
|---|---|---|---|
| **Gateway** | meta-plogging:latest | 8500 | 정적 파일 + API 프록시 |
| **SNS** | meta-plogging:latest | 8501 | 피드/게시글/인증/프로필/알림 |
| **Game** | meta-plogging:latest | 8502 | 퀘스트/랭킹/배지/XP |
| **GIS** | meta-plogging:latest | 8503 | 지도/구역/제보 |
| **AI** | meta-plogging:latest | 8504 | YOLO 더미 + Gemini 이모지 |
| **MCP** | meta-plogging:latest | 8510 | Claude/LLM 통합 |
| **PostgreSQL+PostGIS** | postgis/postgis:16-3.4 | 5433 | 메인 DB |
| **Redis** | redis:7.2-alpine | 6380 | 캐시/세션/랭킹 |
| **Kafka + ZooKeeper** | confluentinc/cp-kafka:7.5.0 | 9093 | 이벤트 메시지 |
| **MinIO** | minio/minio:latest | 9001 / 9002 | S3 호환 객체 스토리지 |
| **Elasticsearch** (선택) | elasticsearch:8.14.1 | 9201 | 검색 인덱스 (`--profile search`) |

> ✅ 모든 이미지는 **linux/amd64 + linux/arm64 멀티 아키텍처** 지원 → Apple Silicon에서도 에뮬레이션 없이 네이티브 실행.

---

## 🚀 빠른 시작 (3분)

### 0. 사전 요구사항
- **Docker Desktop 4.20+** (맥/윈도우) 또는 **Docker Engine 20.10+** (리눅스)
- 디스크 ~8GB, RAM 4GB+ 권장 (ES 사용 시 +1GB)
- 포트 5433 / 6380 / 8500-8510 / 9001-9002 / 9093 사용 가능

### 1. 저장소 받기
```bash
git clone https://203.234.62.175:5443/system/meta-flow.git
cd meta-flow/platform
```

### 2. 환경 변수 설정
```bash
cp .env.docker.example .env
# .env 파일 편집 — 최소 4개 [REQUIRED] 항목 채우기:
#   POSTGRES_PASSWORD, REDIS_PASSWORD, MINIO_SECRET_KEY, JWT_SECRET_KEY

# JWT 시크릿 자동 생성:
openssl rand -hex 32
# 결과를 JWT_SECRET_KEY에 붙여넣기
```

### 3. 빌드 + 실행
```bash
# 모든 서비스 빌드 + 백그라운드 실행
docker compose up -d --build

# 또는 검색 기능 포함:
docker compose --profile search up -d --build
```

### 4. 접속
```
http://localhost:8500     # 메인 웹 (Gateway)
http://localhost:9002     # MinIO 콘솔 (계정: MINIO_ACCESS_KEY/SECRET_KEY)
```

### 5. 상태 확인
```bash
docker compose ps
docker compose logs -f gateway sns game gis ai
```

---

## 🍎 맥 포팅 시나리오

### Apple Silicon (M1/M2/M3) — 네이티브 ARM64
별도 설정 불필요. 모든 이미지가 ARM64 빌드를 자동 선택합니다.

```bash
docker compose up -d --build
# 자동으로 linux/arm64 이미지 pull
```

### Intel 맥 — AMD64
동일하게 동작 (`linux/amd64` 자동 선택).

### 기존 리눅스 데이터 마이그레이션
1. **DB 덤프 → 복원**:
   ```bash
   # 리눅스 서버에서
   docker exec meta-postgres pg_dump -U plogging plogging > backup.sql

   # 맥으로 전송 후
   docker compose up -d postgres   # DB만 먼저 띄움
   docker exec -i meta-postgres psql -U plogging plogging < backup.sql

   # 나머지 서비스 기동
   docker compose up -d
   ```

2. **MinIO 객체 복사**:
   ```bash
   # 리눅스에서 mc(MinIO Client) 또는 rclone으로
   mc mirror plogging-images/ ./minio-backup/
   # 맥에서
   mc mirror ./minio-backup/ plogging-images/
   ```

---

## 🔧 운영 명령

| 작업 | 명령 |
|---|---|
| 전체 시작 | `docker compose up -d` |
| 전체 중지 | `docker compose down` |
| 데이터 포함 삭제 | `docker compose down -v` ⚠️ |
| 특정 서비스 재시작 | `docker compose restart sns` |
| 특정 서비스만 재빌드 | `docker compose up -d --build sns` |
| 로그 실시간 | `docker compose logs -f sns` |
| 컨테이너 쉘 | `docker compose exec sns bash` |
| DB 쉘 | `docker compose exec postgres psql -U plogging plogging` |
| 상태/포트 | `docker compose ps` |
| 리소스 사용량 | `docker stats` |

---

## 🐛 트러블슈팅

### 포트 충돌
이미 다른 프로세스가 5433/6380/8500 등을 점유 중일 때:
```bash
# .env에서 호스트 포트만 변경 (컨테이너 내부 포트는 그대로)
POSTGRES_PORT=15433
SNS_PORT=18501
```

### 맥에서 빌드 느림
첫 빌드는 멀티 아키텍처 + pip 다운로드로 5~10분. 이후는 캐시 활용:
```bash
# pip 캐시 + 레이어 캐시 활용
docker compose build --pull   # 베이스 이미지만 최신화
```

### DB 초기화가 안 됨
`pg_data` 볼륨이 이미 있으면 init.sql이 재실행 안 됩니다. 완전 초기화:
```bash
docker compose down -v
docker compose up -d
```

### `MINIO_PUBLIC_URL` 이미지 깨짐
브라우저가 MinIO에 직접 연결할 URL을 정확히 지정해야 합니다.
- 로컬 맥: `http://localhost:9001`
- 원격 서버: `http://<server-ip>:9001` 또는 도메인

### Gemini API 키 없음
이모지 추천은 자동으로 키워드 폴백 모드 동작. 키 입력 원하면 `.env`의 `GEMINI_API_KEY` 채우기.

---

## 📊 아키텍처

```
┌─────────────────────────────────────────────────────────────┐
│                    Docker Compose Network                   │
│                                                             │
│  ┌──────────┐    ┌─────────────────────────────────────┐    │
│  │ Gateway  │───▶│  SNS │ Game │ GIS │ AI │ MCP        │    │
│  │  :8500   │    │ 8501 │ 8502 │8503 │8504│8510         │    │
│  └────┬─────┘    └──────────┬──────────────────────────┘    │
│       │                     │                               │
│       │ 정적/프록시         │                               │
│       ▼                     ▼                               │
│  ┌──────────┐    ┌────────┬────────┬────────┬────────┐      │
│  │  웹 UI   │    │Postgres│ Redis  │ Kafka  │ MinIO  │      │
│  │ (브라우저)│    │  5432  │  6379  │  9092  │  9000  │      │
│  └──────────┘    └────────┴────────┴────────┴────────┘      │
│                                                             │
└─────────────────────────────────────────────────────────────┘
       Host 노출:  8500, 8501-8510, 5433, 6380, 9001-9002, 9093
```

---

## 🔐 프로덕션 권장

1. **시크릿 외부 관리**: `.env` 대신 Docker Secrets / Vault / SOPS
2. **HTTPS 추가**: Gateway 앞에 nginx/Caddy 리버스 프록시
3. **백업 cron**:
   ```bash
   # crontab -e
   0 3 * * * docker exec meta-postgres pg_dump -U plogging plogging | gzip > /backup/pg-$(date +\%F).sql.gz
   ```
4. **리소스 제한** (compose에 `deploy.resources` 추가):
   ```yaml
   sns:
     deploy:
       resources:
         limits: { cpus: '1.0', memory: 512M }
   ```
5. **로그 회전**: `--log-opt max-size=10m --log-opt max-file=3`

---

## 📝 변경 이력

- v0.0.10 (2026-05-22) — 첫 도커 풀스택 배포 (맥 포팅 지원)
