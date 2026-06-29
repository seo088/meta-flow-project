# Meta SW Plogging — Claude 작업 규칙

*이 CLAUDE.md는 프로젝트 루트에 위치하며, 모든 Claude Code 세션에서 자동으로 로드됩니다.*

---

## 1. 세션 관리 규칙 (최우선)

### 1.1 신규 세션 시작 프로토콜 (코드 수정 전 필수)

1. `docs/sessions/` 디렉토리에서 **최신 세션 파일**을 읽어 이전 작업 현황 파악
2. `git log --oneline -10`으로 최근 커밋 확인
3. `git status`로 미커밋 변경사항 확인
4. 이전 세션의 미완료 작업(P0 → P1 → P2 순)부터 이어서 진행

### 1.2 세션 요약 자동화 (필수)

**컨텍스트 85% 사용 시 또는 세션 종료 시 반드시 수행:**
1. `docs/sessions/YYYY-MM-DD-session-N.md`에 현재 세션 작업 요약 작성
2. 수정한 파일 목록, 완료/미완료 항목, 핵심 결정사항을 기록
3. **작성 후 반드시 사용자에게 세션 문서 경로를 전달**한다:
   예) "세션 문서: `docs/sessions/2026-04-13-session-18-wt.md`"

### 1.3 docs/ 디렉토리 구조 (OpenHarness 패턴 적용)

```
docs/
├── sessions/                     # 세션별 작업 요약 (최신순)
│   ├── YYYY-MM-DD-session.md     # 해당 일자 첫 세션
│   ├── YYYY-MM-DD-session-2.md   # 같은 날 2번째 세션
│   └── YYYY-MM-DD-session-N-wt.md  # worktree 세션 (-wt 접미사)
├── skills/                       # 작업별 가이드 (온디맨드 참조)
│   ├── frontend-spa.md           # SPA 수정 규칙, 영역 맵, 컨벤션
│   ├── backend-fastapi.md        # FastAPI 서비스 패턴, 인증, DB
│   ├── database-schema.md        # DB 스키마 변경 절차, 테이블 맵
│   └── deployment.md             # 배포/재시작/환경변수 가이드
├── architecture/                 # 아키텍처 개요
│   └── overview.md               # 시스템 구성도, 설계 원칙
└── decisions/                    # ADR (Architecture Decision Records)
    ├── ADR-001-single-file-spa.md
    └── ADR-002-reaction-system.md
```

**Skills 사용법**: 특정 작업 시 해당 skill 파일을 참조하여 컨벤션 준수
**ADR 사용법**: 설계 결정의 "왜"를 기록 → 미래 세션에서 같은 결정을 반복하지 않음

### 1.4 세션 파일 작성 형식

```markdown
# 세션 YYYY-MM-DD-N

## 완료된 작업
- [x] 작업 항목 (관련 파일, 라인)

## 미완료 작업 (우선순위 순)
### P0 — 즉시 필요
### P1 — 기능 보완
### P2 — 품질 개선

## 발견된 버그/이슈
## DB 스키마 변경
## 수정된 파일 목록
## 주의사항
```

---

## 2. 프로젝트 개요
- **프로젝트**: Meta SW Plogging (메타 SW 플로깅) 플랫폼
- **기관**: 군산대학교
- **버전**: v0.0.7
- **GitLab**: https://203.234.62.175:5443/system/meta-flow

## 3. 아키텍처
- 6-Layer Microservice (SPA + Gateway + Backend + AI + MCP + Infra)
- **Gateway**: :8500 (정적 파일 + API 프록시)
- **SNS**: :8501 (피드, 게시글, 인증, 프로필)
- **Game**: :8502 (퀘스트, 랭킹, 배지, XP)
- **GIS**: :8503 (지도, 핀맵, 구역 통계)
- **AI**: :8504 / **Data**: :8505 / **MCP**: :8510
- **DB**: PostgreSQL+PostGIS :5433 (`plogging:plogging_dev_2026@203.234.62.176`)
- **Redis**: :6380 / **Kafka**: :9093 / **MinIO**: :9001

## 4. 주요 파일
- `platform/apps/web/index.html` — 단일 파일 SPA (전체 프론트엔드)
- `platform/services/sns/main.py` — SNS 서비스
- `platform/services/game/main.py` — Game 서비스
- `platform/services/gis/main.py` — GIS 서비스
- `platform/services/gateway/main.py` — API 게이트웨이
- `platform/shared/core/auth.py` — JWT 인증
- `platform/shared/core/types.py` — 공유 타입 (TrashType, Severity, DataSource 등)

## 5. 서비스 관리 명령
```bash
# 서비스 시작 (환경변수 필수)
cd /home/spark/research/meta-flow/platform
export $(grep -v '^#' .env | grep -v '^$' | xargs) 2>/dev/null
conda run -n meta-flow python services/gateway/main.py &
conda run -n meta-flow python services/sns/main.py &
conda run -n meta-flow python services/game/main.py &
conda run -n meta-flow python services/gis/main.py &

# 서비스 재시작 (예: SNS)
kill $(ps aux | grep "python services/sns/main.py" | grep -v grep | grep -v conda | awk '{print $2}') 2>/dev/null
sleep 2 && cd /home/spark/research/meta-flow/platform && export $(grep -v '^#' .env | grep -v '^$' | xargs) 2>/dev/null && conda run -n meta-flow python services/sns/main.py &

# JS 구문 검증
cd /home/spark/research/meta-flow/platform && node -e "const fs=require('fs');const h=fs.readFileSync('apps/web/index.html','utf8');const m=h.match(/<script>([\s\S]*)<\/script>/);try{new Function(m[1]);console.log('OK')}catch(e){console.log('ERROR:',e.message)}"
```

## 5.1 Worktree 동기화 규칙 (필수)

> **게이트웨이는 항상 main 디렉토리(`/home/spark/research/meta-flow/platform`)에서 실행됩니다.**
> Worktree에서 작업 시, 수정된 파일이 실제 서비스에 반영되지 않습니다.

### Worktree 작업 시 필수 절차
1. **프론트엔드/백엔드 수정 후** 반드시 main 디렉토리로 파일 복사:
   ```bash
   # 프론트엔드 (SPA)
   cp <worktree>/platform/apps/web/index.html /home/spark/research/meta-flow/platform/apps/web/index.html
   
   # 백엔드 서비스 (수정한 파일만)
   cp <worktree>/platform/services/<service>/main.py /home/spark/research/meta-flow/platform/services/<service>/main.py
   ```
2. **백엔드 서비스 수정 시** 해당 서비스 재시작 필수 (5절 참조)
3. **브라우저 강제 새로고침** (`Ctrl+Shift+R`)으로 캐시 방지
4. 변경 확인 후 worktree에서 커밋 (main 디렉토리에서는 커밋하지 않음)

### 주의사항
- 게이트웨이의 `Cache-Control: no-cache` 헤더가 있지만, 브라우저 메모리 캐시는 남을 수 있음
- **UI 수정 후 동작 확인이 필요할 때**: 파일 복사 → 강제 새로고침 → 테스트

---

## 6. 버전 관리 규칙

### 6.1 Semantic Versioning (v0.MAJOR.MINOR)
- **MAJOR** (v0.X.0): 아키텍처 변경, 대규모 기능 추가
- **MINOR** (v0.0.X): 기능 추가, UI 개편, 버그 수정 묶음

### 6.2 버전 업 기준 (다음 중 하나 이상 충족 시)
- 새로운 사용자 기능 추가 (API 엔드포인트, UI 컴포넌트)
- DB 스키마 변경
- 보안/법적 변경 (개인정보 처리방침, 회원 탈퇴 등)
- UI 전면 개편

### 6.3 버전 업 절차 (반드시 순서대로)
1. **현재 버전 확인**: `git tag -l --sort=-v:refname | head -1`
2. **패치 번호 증가**: v0.0.4 → v0.0.5
3. **README.md 변경 이력 업데이트**: `## 16. 변경 이력` 테이블에 행 추가
4. **커밋**: 변경사항 + README 함께 커밋
5. **태그 생성**: `git tag v0.0.X`
6. **Push**: `git push origin main --tags`

### 6.4 README 변경 이력 형식
```markdown
| v0.0.X | YYYY-MM-DD | 한 줄 요약 |
```

---

## 7. 코딩 규칙
1. **코드 수정 후 반드시 JS 구문 검증** 실행
2. **서비스 수정 시 해당 서비스 재시작** 필수 (`.env` 환경변수 로드 포함)
3. **DB 스키마 변경 시** asyncpg로 직접 ALTER TABLE 실행
4. **user_points는 Materialized View** — INSERT 불가, `point_ledger`에 기록 후 `REFRESH MATERIALIZED VIEW CONCURRENTLY user_points`
5. **trash_reports.reporter_id** (user_id 아님) 주의
6. **posts.post_type**: 'general' 또는 'report' — 기본값 'general'
7. **기본 권한**: feed, ranking, zone, quest (회원가입 시 자동 부여)
8. **테마**: localStorage('mp_theme_mode') — auto/light/dark, 프로필에서 설정
9. **GitLab push**: `git push origin main --tags`

## 8. DB 스키마 참고
- `quest_posts.visibility`: 'quest' (기본) 또는 'public' (SNS 동시 게시)
- `trash_reports`: reporter_id, location(PostGIS), zone_id, trash_type, severity, image_urls, source
- `posts`: user_id, content, image_urls, hashtags, entity_tags, visibility, post_type, report_id
- `notifications`: user_id, type, title, body, ref_post_id, ref_comment_id, actor_username, is_read

## 9. API 엔드포인트 참고
- `POST /posts/report` — 제보 전용 (Form: lat, lon, trash_type, severity, content, hashtags, files)
- `POST /posts/create` — 일반 게시 (JSON: content, hashtags, image_urls, visibility, post_type)
- `PUT /profile/me` — 프로필 편집 (JSON: display_name, bio)
- `DELETE /profile/me` — 회원 탈퇴 (모든 데이터 즉시 삭제)
- `GET /notifications` — 알림 조회 / `POST /notifications/read` — 읽음 처리
- `GET /users/search/invite?q=` — 초대 가능 사용자 검색 (opt-in만)
- `GET /trending/users` — 급상승 랭커 (7일 XP 기준)
- `GET /trending/feeds` — 급상승 피드 (7일 engagement 기준)

---

## 10. 최근 주요 변경 이력 (v0.0.5)
- 홈 레이아웃 개편: 메인+사이드바 비대칭 그리드 (X/LinkedIn 스타일)
- 내 활동 요약 위젯 + 참여 퀘스트 가로 스크롤
- 피드 카드 ⋯ 더보기 메뉴 (Instagram 스타일)
- 리액션 정리: 항상 ❤️, reaction-summary 제거
- 급상승 랭커: point_ledger 기준 7일 XP 정렬
- 댓글 알림 시스템 (로그인 시 팝업)
- 퀘스트 초대 전용 모달 (자동완성 + 카드 미리보기 + 다중 전송)
- 퀘스트 초대 DM 카드 UI (실시간 인원 조회)
- 초대 답장 후 퀘스트 이동 확인 + 카드 하이라이트
- 인기 퀘스트 썸네일 (인기 게시글 이미지 + SVG 폴백)
- 제보 세부 옵션 UI (severity/trash_type 셀렉터)
- 프로필 편집 UI (display_name, bio)
- 개인정보 처리방침 12조 + 이용약관 + 회원 탈퇴
- 도움말 비로그인 접근 허용
- 게이트웨이 캐시 방지 헤더
