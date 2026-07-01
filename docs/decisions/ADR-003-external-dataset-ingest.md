# ADR-003: 외부 데이터셋 배치 적재 — 내부 API 단일 경로 + 격리 봇 계정

**상태**: 채택  
**일자**: 2026-06-01  
**관련**: [batch-report-ingest 스킬](../../.claude/skills/batch-report-ingest/SKILL.md), [dataset-ingest 에이전트](../../.claude/agents/dataset-ingest.md)

## 맥락
외부 DI 데이터셋(`메타플로깅_DI_데이터`, 이미지 1044장 + `merged_records.json` 정답 라벨/좌표)을
플랫폼의 `trash_reports`/`posts` 로 대량 적재해야 했다. 단순 DB INSERT 스크립트로 밀어넣으면
멱등성·정답 저장·MinIO 업로드·zone 매칭 로직이 본 서비스와 갈라지고, 1044건이 메인 피드/랭킹/XP를
오염시키는 부작용이 발생한다.

## 결정

### 1. 적재는 내부 API + 공유 코어 단일 경로만 사용
- 모든 적재는 `SNS :8501 /admin/reports/ingest`(단건) · `/admin/reports/ingest-batch`(배치)를 거친다.
- 실제 로직은 `shared/core/ingest.py` 한 곳에 둔다(EXIF/멱등/MinIO/INSERT/zone매칭/gt_label).
- 스크립트(`scripts/batch_report_ingest.py`)·에이전트·외부 저장소가 **동일 코어**를 통과한다(SSOT).

### 2. SHA-256 이미지 해시 멱등
- 동일 이미지 재적재 시 해시 비교로 자동 skip → 배치 재실행이 안전하다(중복 9건 skip 검증됨).

### 3. `dataset_bot`(role=system) 격리 계정으로 적재
- 적재 데이터는 모두 `dataset_bot`(id=`f2d472f8-…`) 소유.
- 배치 코어는 `grant_xp`·트렌드 캐시를 **호출하지 않는다** → XP/랭킹/트렌드 미오염.
- 제보 통계(현황·핀맵)에만 반영된다.

### 4. 메인 피드 분리 + 전용 탭
- 메인 피드는 `source='external_di'` 제외 → 1044건 범람 방지.
- 전용 조회는 `/feed?source=external_di` (관리자 "데이터셋" 탭).

### 5. 다축 분류체계 (SSOT: `shared/core/trash_taxonomy.py`)
- 품목 9종 + 크기(size) + 수거구분(handling: normal/recycle/bulk) + 긴급성(severity 자동 산출).
- 한글 라벨 → key 매핑을 이 파일에 집약. 다중 라벨 이미지는 대표 1개를 컬럼에,
  전체를 `gt_label.items`(jsonb)에 보존 → 정답(ground truth) 손실 없음.
- 신규 라벨 추가 시 `trash_taxonomy.py` + `report_categories` 시드만 갱신.

### 6. 위치는 매니페스트 JSON 좌표 사용
- 카카오톡 경유 이미지의 EXIF GPS가 제거되어, `merged_records.json`의 lat/lon을 위치 소스로 채택.

## 결과
- 적재 1035건(신규 1025 + 기존 10), 위치 보유 99.8%, 실패 0.
- DB 변경: `trash_reports.gt_label jsonb`·`handling varchar`, `report_categories` 테이블, `dataset_bot` 계정.

## 롤백
```sql
DELETE FROM trash_reports WHERE source = 'external_di';  -- 연결 posts도 함께 정리
```
배치 재실행은 SHA-256 멱등이라 안전(중복 자동 skip).

## 대안 (기각)
- **직접 DB INSERT 스크립트**: 로직 분기·멱등성 누락 위험 → 기각.
- **일반 사용자 계정으로 적재**: 랭킹/XP/피드 오염 → 기각.
- **EXIF 좌표 강제 사용**: 카카오톡 EXIF 제거로 위치 대량 누락 → 기각, JSON 좌표 채택.
