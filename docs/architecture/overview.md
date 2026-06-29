# 아키텍처 개요

## 시스템 구성

```
[브라우저] ←→ [Gateway :8500] ←→ [SNS :8501]
                   ↕                [Game :8502]
              [정적 파일]           [GIS :8503]
              index.html            [AI :8504]
                                    [Data :8505]
                                    [MCP :8510]
                   ↕
              [PostgreSQL+PostGIS :5433]
              [Redis :6380]
              [Kafka :9093]
              [MinIO :9001]
```

## 설계 원칙

1. **단일 파일 SPA**: `index.html` 하나에 HTML+CSS+JS 통합 — 빌드 도구 없음
2. **마이크로서비스**: 각 서비스 독립 FastAPI 인스턴스, 공유 DB
3. **게이트웨이 프록시**: `/api/{service}/*` → 해당 서비스로 라우팅
4. **Materialized View**: `user_points`는 `point_ledger`에서 집계 — 직접 INSERT 불가
5. **캐시 없음**: 게이트웨이에서 `no-cache` 헤더 강제
