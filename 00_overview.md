# 메타 플로깅 SW 플랫폼 — 00. 시스템 개요 및 전체 아키텍처

> **프로젝트**: 메타 플로깅 SW 플랫폼 (가칭: TrashHunt)  
> **기반**: 군산대학교 / 전북특별자치도 군산시  
> **확장 대상**: 은파유원지 · 새만금 · 금강하구둑  
> **버전**: v1.0.0

---

## 1. 플랫폼 정의

메타 플로깅 SW 플랫폼은 **플로깅(Plogging: 달리기+줍기)** 행위를  
SNS 콘텐츠 생산 · 게미피케이션 · AI 드론/모빌리티 데이터 수집 · 공공데이터 파이프라인으로  
확장하는 **지역 생태 서비스 플랫폼**이다.

---

## 2. 핵심 목표

| 목표 | 내용 | 지표 |
|------|------|------|
| **데이터 통합** | SNS·드론·모빌리티를 단일 클라우드로 수집 | Kafka 토픽 4종 |
| **AI 자동화** | YOLO 탐지·RL 경로·인증 AI 파이프라인 | 탐지 정확도 ≥90% |
| **게미피케이션** | 포켓몬고형 퀘스트·배지·랭킹으로 시민 참여 유도 | DAU 200명+ |
| **공공데이터** | AI-Ready GeoJSON·COCO 배포 | 월 1회 갱신 |
| **확장성** | 캠퍼스→관광지→지역주민 3단계 | 동시 1,000명 |

---

## 3. 6-레이어 아키텍처

```
┌──────────────────────────────────────────────────────────────────┐
│ Layer 6: Frontend                                                 │
│ React Native (iOS·Android) · Next.js (웹) · Admin Dashboard      │
├──────────────────────────────────────────────────────────────────┤
│ Layer 5: Backend Microservices (FastAPI)                          │
│ api-gateway · sns-service · gamification · gis · public-api      │
├──────────────────────────────────────────────────────────────────┤
│ Layer 4: AI/ML Engine  [NVIDIA A6000 48GB VRAM]                  │
│ YOLO v8 탐지 · PPO/SAC RL 에이전트 · ResNet 인증 · CLIP 분류   │
├──────────────────────────────────────────────────────────────────┤
│ Layer 3: MCP Servers & AI Agents                                  │
│ 6 MCP Servers · 5 AI Agents · Trigger Orchestrator               │
├──────────────────────────────────────────────────────────────────┤
│ Layer 2: Cloud Infra  (현재: 로컬 spark-a6 서버)                 │
│ Kafka 3.7 · API Gateway · MinIO(→S3) · Redis 7.2 · nginx         │
├──────────────────────────────────────────────────────────────────┤
│ Layer 1: Data Sources                                             │
│ SNS 앱 · 드론 SDK · 자율주행 모빌리티 · Unity 시뮬레이터        │
└──────────────────────────────────────────────────────────────────┘
```

---

## 4. 물리 서버 컨테이너 포트 맵

| 서비스 | 포트 | 설명 |
|--------|------|------|
| zookeeper | 2181 | Kafka 코디네이터 |
| kafka | 9092 | 이벤트 스트림 브로커 |
| redis | 6379 | 캐시 · 실시간 랭킹 |
| postgres | 5432 | 주 DB (PostGIS) |
| elasticsearch | 9200 | 검색 · 집계 |
| kibana | 5601 | ES 모니터링 |
| minio | 9000 | Object Storage |
| nginx | 80/443 | 리버스 프록시 |
| api-gateway | 8000 | FastAPI 진입점 |
| sns-service | 8001 | SNS 도메인 |
| gamification | 8002 | 게미피케이션 엔진 |
| gis-service | 8003 | GIS + 지오펜싱 |
| ai-service | 8004 | AI/ML 추론 (GPU) |
| public-data-api | 8005 | 공공데이터 API |
| mcp-plogging | 8010 | MCP: 플로깅 |
| mcp-drone | 8011 | MCP: 드론 |
| mcp-mobility | 8012 | MCP: 모빌리티 |
| mcp-gis | 8013 | MCP: GIS |
| mcp-gamification | 8014 | MCP: 게미피케이션 |
| mcp-public-data | 8015 | MCP: 공공데이터 |
| frontend | 3000 | Next.js 웹 |
| prometheus | 9090 | 메트릭 수집 |
| grafana | 3100 | 모니터링 |

---

## 5. 핵심 데이터 흐름

### 5.1 제보 → 탐지 → 게미피케이션

```
플로거 SNS 업로드
  → [POST /api/v1/posts] image + GPS
  → S3 저장 + Kafka(plogging.raw.sns) 발행
  → TrashDetectionAgent: YOLO v8 추론
  → PostgreSQL(trash_reports) 저장
  → GeofenceAgent: PostGIS 구역 판정
  → GamificationEngine: XP 적립 + 랭킹 갱신
  → WebSocket push: 핀맵 + 알림
```

### 5.2 드론 → 탐지 → 경로 최적화

```
드론 RTSP 스트림
  → DroneStreamAgent: 1fps 프레임 버퍼
  → YOLO v8 배치 추론 (GPU)
  → Kafka(plogging.raw.drone) 발행
  → RLAgent(PPO/SAC): 최적 수거 경로
  → 모빌리티 디스패치 (MCP 모빌리티)
  → 수거 완료 → 인증 이벤트
```

### 5.3 인증 → 공공데이터

```
플로거 청소 후 사진 업로드
  → VerificationAgent: SSIM + ResNet 비교
  → 확률 ≥ 0.85 → 인증 성공
  → XP +50~100 + 퀘스트 완료
  → GeoJSON 변환 → 공공데이터 API 갱신
  → Elasticsearch 인덱스 업데이트
```

---

## 6. 지오펜싱 구역 (WGS84)

| 구역 | 중심 좌표 | 반경 | 특성 |
|------|----------|------|------|
| 군산대학교 | 35.9693°N, 126.7369°E | 800m | 도보 플로깅 기점 |
| 은파유원지 | 35.9861°N, 126.7222°E | 1.2km | 수변·드론 허용 |
| 새만금 | 35.8000°N, 126.6000°E | 10km | 군집드론 광역 순찰 |
| 금강하구둑 | 35.9600°N, 126.7100°E | 2km | 자율주행 모빌리티 |

구역 진입 시: 퀘스트 활성화 + 푸시 알림  
복수 구역 동시 활동: 보너스 XP +20%

---

## 7. 동시 접속 1,000명 처리 전략

| 항목 | 설정 |
|------|------|
| nginx worker_connections | 4096 |
| upstream 전략 | least_conn |
| api-gateway replicas | 2 |
| sns-service replicas | 2 |
| PostgreSQL max_connections | 200 |
| SQLAlchemy pool_size | 20 (서비스당) |
| Redis max_clients | 1000 |
| Kafka partitions | 8 (sns), 4 (drone/mobility) |
| WebSocket 메모리 | 2KB/conn × 1,000 = 2MB |

---

## 8. 기술 스택 요약

### 백엔드
| 구성 요소 | 기술 | 버전 |
|-----------|------|------|
| API Framework | FastAPI + Uvicorn | 0.111+ |
| ORM | SQLAlchemy + Alembic | 2.0+ |
| 유효성 검사 | Pydantic v2 | 2.7+ |
| 메시지 큐 | Apache Kafka | 3.7 |
| 캐시 | Redis | 7.2 |
| 주 DB | PostgreSQL 16 + PostGIS 3.4 | - |
| 검색 | Elasticsearch | 8.13 |
| Object Storage | MinIO (→ S3) | latest |
| MCP Framework | FastMCP (Python) | 2.x |

### AI/ML
| 모델 | 기술 | VRAM |
|------|------|------|
| 객체 탐지 | YOLO v8x (Ultralytics) | 8GB |
| RL 경로 | Stable-Baselines3 PPO/SAC | 4GB |
| 인증 | ResNet-50 (torchvision) | 2GB |
| 분류 | CLIP (OpenAI) | 2GB |

### 프론트엔드
| 구성 요소 | 기술 |
|-----------|------|
| 모바일 | React Native 0.74 + Expo |
| 웹 | Next.js 14 (App Router) |
| 상태 관리 | Zustand + TanStack Query |
| 지도 | React Native Maps + MapLibre |
| 실시간 | Socket.io-client 4.x |

---

## 9. 프로젝트 디렉토리 구조

```
meta-plogging/
├── packages/
│   ├── shared/           # 공유 유틸리티 (DRY 원칙)
│   │   ├── types/        # 공통 TypeScript 타입
│   │   ├── utils/        # 공통 함수
│   │   └── errors/       # 공통 에러 클래스
│   └── config/           # 환경 설정
│
├── services/             # 백엔드 마이크로서비스
│   ├── api-gateway/
│   ├── sns-service/
│   ├── gamification/
│   ├── gis-service/
│   ├── ai-service/
│   └── public-data-api/
│
├── mcp-servers/          # MCP 서버 (6개)
│   ├── mcp-plogging/
│   ├── mcp-drone/
│   ├── mcp-mobility/
│   ├── mcp-gis/
│   ├── mcp-gamification/
│   └── mcp-public-data/
│
├── agents/               # AI 에이전트 (5개)
│   ├── trash-detection/
│   ├── drone-stream/
│   ├── mobility/
│   ├── verification/
│   └── geofence/
│
├── apps/
│   ├── mobile/           # React Native
│   ├── web/              # Next.js
│   └── admin/            # 관리자 대시보드
│
├── infra/
│   ├── docker/
│   ├── compose/
│   ├── nginx/
│   └── monitoring/
│
├── data/
│   ├── migrations/
│   ├── seeds/
│   └── schemas/
│
└── docs/
    ├── 00_overview.md       ← 이 문서
    ├── 01_infrastructure.md
    ├── 02_backend_services.md
    ├── 03_mcp_agents.md
    ├── 04_frontend.md
    └── 05_data_pipeline.md
```

---

## 10. 단계별 구현 로드맵

### Phase 1 – 파일럿 (군산대학교, 3개월)

| 주차 | 목표 |
|------|------|
| 1~2 | Docker Compose 인프라 + PostgreSQL/Redis/Kafka/MinIO |
| 3~4 | SNS 서비스 + GIS 지오펜싱 + 기본 게미피케이션 |
| 5~6 | YOLO v8 배포 + 인증 AI + MCP 서버 3개 |
| 7~8 | React Native MVP + 지도 핀맵 + 퀘스트 UI |
| 9~12 | 부하 테스트 200명 + AI 정확도 검증 + 피드백 반영 |

### Phase 2 – 지역 확장 (군산시, +3개월)

- 드론 SDK 연동 (MCP 드론 서버)
- 자율주행 모빌리티 연동
- 은파·새만금·금강 지오펜싱 추가
- 지역주민 계정 오픈
- 동시 1,000명 확장

### Phase 3 – 공공데이터 생태계 (+6개월)

- 전북특별자치도 공공데이터 포털 연계
- AI-Hub 데이터셋 등록
- Unity RL 시뮬레이터 고도화
- AWS/NCP 클라우드 마이그레이션
