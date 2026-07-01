---
name: batch-report-ingest
description: 외부 데이터셋(zip + merged_records.json 형태)의 사진을 정답 라벨·위치와 함께 제보로 배치 적재한다. zip 압축 해제 → 매니페스트 매핑 → dry-run → 본 적재 → 검증까지 재현 가능한 절차. "배치 적재", "데이터셋 업로드", "bulk 제보 등록" 요청 시 사용.
---

# 배치 제보 적재 (외부 데이터셋)

외부 이미지 데이터셋을 **정답 라벨(다축 분류) + 위치**와 함께 `trash_reports`/`posts` 로 배치 적재한다.
모든 적재는 내부 API(`SNS :8501 /admin/reports/ingest-batch`)와 공유 코어(`shared/core/ingest.py`)를 거치므로
스크립트·에이전트·외부 저장소가 동일 로직·멱등성·정답 저장 규칙을 통과한다.

## 분류체계 (SSOT: `platform/shared/core/trash_taxonomy.py`)
- **품목(trash_type)**: general/plastic/large/food/unknown(빌트인) + cigarette/paper/vinyl/metal/glass/styrofoam(신규)
- **크기(size)**: small/medium/large
- **수거구분(handling)**: normal(일반수거) / recycle(분리배출) / bulk(대형·수거불가)
- **긴급성(severity)**: size+handling 자동 산출 (bulk→high, large→high, medium→mid, small→low)
- 다중 라벨 이미지: 대표 1개를 `trash_type`/`severity`/`handling` 에, 전체는 `gt_label.items` 에 보존
- 한글 라벨 → key 매핑도 이 파일에 정의. 신규 라벨 추가 시 여기 + `report_categories` 시드만 갱신.

## 매니페스트 형식 (정답 소스)
`merged_records.json` = 레코드 리스트. 레코드당:
```json
{"imagePath":"a.jpg","latitude":35.94,"longitude":126.68,"timestamp":"2026-03-31T19:18:30",
 "items":[{"label":"담배꽁초","size":"small","quantity":1}],"source":"gdw_json"}
```
- `latitude/longitude` 있으면 위치 저장(없으면 위치 없는 제보).
- `items[].label` 은 한글. taxonomy 매핑에 없으면 `unknown` 으로 적재(로그에 표시).

## 절차 (재현성)
1. **업로드 완료·무결성 검증** — 대용량 zip은 업로드 중일 수 있다.
   ```bash
   cd <zip_dir>; f=$(ls *.zip|head -1)
   s1=$(stat -c%s "$f"); sleep 3; s2=$(stat -c%s "$f")   # 크기 고정 확인
   7z t "$f"                                              # 무결성 (Everything is Ok)
   ```
2. **압축 해제** — `7z x -y -o<dest> "$f"`
3. **(선택) DB 준비** — 최초 1회: `conda run -n meta-flow python platform/scripts/migrate_ingest_taxonomy.py`
   (gt_label/handling 컬럼, report_categories 시드, dataset_bot 생성. idempotent)
4. **dry-run** — 소량으로 매핑·좌표·정답 확인 (DB/MinIO 미반영):
   ```bash
   cd platform; export $(grep -v '^#' .env|grep -v '^$'|xargs)
   conda run -n meta-flow python scripts/batch_report_ingest.py \
     --manifest <merged_records.json> --image-dir <image_dir> \
     --batch-id <YYYY-MM-DD-id> --limit 10 --dry-run
   ```
   `logs/ingest-<batch-id>.jsonl` 에서 trash_type/severity/handling/gt_label 확인.
5. **본 적재** — `--dry-run` 제거. SHA-256 멱등이라 재실행해도 중복은 skip.
6. **검증** — DB 카운트(`source='external_di'`), 위치/zone, MinIO 객체 수, 피드/핀맵 렌더.

## 주의
- **재현성/멱등성**: `gt_label.file_sha256` 로 중복 차단 → 같은 파일 재적재 안 됨. 안전하게 재실행 가능.
- **소유 계정**: 모든 적재는 `dataset_bot`(role=system) 소유. CLI가 JWT를 자동 발급.
- **외부 저장소 참조**: API는 `local_path`(서버 파일, base_dir 제한) 또는 `url`(HTTP fetch) 모드 지원.
- **신규 카테고리**: taxonomy에 없는 라벨은 `unknown`. 정식 추가하려면 `trash_taxonomy.py` + 마이그레이션 + 프론트(`TRASH_LABELS`,`trashIcons`,`typeNames`,`.type-*` CSS) 갱신.
- index.html 은 게이트웨이가 no-cache 정적 서빙 → 프론트 변경은 `Ctrl+Shift+R` 로 반영.

## 핵심 파일
- `platform/shared/core/trash_taxonomy.py` — 분류체계 SSOT
- `platform/shared/core/ingest.py` — 적재 코어
- `platform/services/sns/main.py` — `/admin/reports/ingest`, `/admin/reports/ingest-batch`
- `platform/scripts/batch_report_ingest.py` — 배치 CLI
- `platform/scripts/migrate_ingest_taxonomy.py` — DB 마이그레이션
