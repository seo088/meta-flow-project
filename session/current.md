# 세션 현황 — 2026-04-13

## 완료된 작업

### v0.0.3 (커밋 완료, GitLab push 완료)
- 모바일 반응형 UI (하단 네비, FAB, viewport-fit)
- 다크/라이트 테마 토글 (CSS Custom Properties)
- 퀘스트 피드 XP 시스템 (+30XP 퀘스트, +20XP 일반)
- 마당 공개 시 SNS 동시 게시
- point_ledger → user_points MV 연동
- 네비게이션 스택 + 브라우저 뒤로가기 연동

### v0.0.3+ (미커밋, 작업 트리에 있음)
- 리액션 시스템 (6종 이모지: like/love/haha/wow/sad/angry)
- 게시글 삭제/수정 + 아카이브 백업 (quest_posts_archive, posts_archive)
- 관리자 게시글 숨김 (is_hidden, confirm 팝업)
- 위치 정보 시스템 (geolocation, quest_posts.lat/lon)
- 게시글 카테고리 (general/report) + 시각적 구분
- 퀘스트 멤버 카드 그리드 + 호스트 관리 (경고/정지/강퇴)
- 초대 검색 시스템 (quest_invite_opt_in)
- 테마 자동 모드 (프로필 설정, 시간 기반)
- 폰트: Gowun Batang
- 명칭 변경: 피드→플로그
- 이미지 우클릭 방지
- 더미 이미지(picsum) 완전 제거
- 랭킹 실제 DB 데이터 기반 전환
- 구역 페이지 서브탭 (지도/현황/제보목록)
- 기본 권한에 quest 추가
- 게이트웨이 캐시 방지 헤더 추가
- JS 템플릿 구문 오류 수정 (ternary 세미콜론)
- 로그인 후 원래 화면 복귀 (_pendingAfterLogin)
- 도움말 탭 추가 (내용 미작성)
- 댓글 중복 저장 수정
- 댓글 UI 개선 (이모지 아바타, 인라인 스타일)
- 아바타 업로드 (ico/png, 4MB, /avatars/ 서빙)
- 토큰 만료 자동 감지 + 재로그인 안내
- 퀘스트 참가 오류 수정 (is_member 체크)
- 구역 통계 7일 필터 제거 (전체 기간)
- timeAgo 음수 수정 (방금 전)

## 미완료 작업 (우선순위 순)

### P0 — 즉시 필요
1. **v0.0.4 커밋 및 GitLab push** — 모든 변경사항 커밋 필요
2. **구역 현황/제보 목록 데이터 표시 확인** — Promise.all 적용했으나 실제 브라우저에서 확인 필요
3. **도움말 탭 내용 작성** — p-help 패널이 비어있음

### P1 — 기능 보완
4. **제보 게시글 세부 옵션** — 심각도/유형 선택 UI 필요 (현재 카테고리만 선택)
5. **제보→trash_reports 연동** — report 유형 게시글 작성 시 trash_reports에도 저장
6. **SNS 피드 liked 필드** — feed API에서 현재 사용자의 좋아요 여부 반환 필요
7. **프로필 편집 UI** — display_name, bio 수정 기능
8. **경고/정지 멤버 게시 제한** — 백엔드에서 status 체크 로직 추가

### P2 — 품질 개선
9. **모바일 반응형 최종 점검** — 퀘스트 상세, 댓글, 구역 서브탭
10. **데이터 정합성 검증** — 랭킹/멤버 XP/구역 통계
11. **위치 정보 설정** — 프로필에서 위치 허용/거부 토글
12. **캐시 관리 에이전트** — 정기적 캐시 클리어 크론

## 발견된 버그/이슈
- 게이트웨이가 HTML을 캐시하여 JS 수정 후에도 이전 버전 서빙 → **캐시 방지 헤더 추가로 해결**
- 구역 현황/제보 목록이 빈 화면으로 표시될 수 있음 → Promise.all 적용, 확인 필요
- 도움말 탭 클릭 시 빈 화면

## 참조 파일 경로
- 프론트엔드: `/home/spark/research/meta-flow/platform/apps/web/index.html`
- SNS 서비스: `/home/spark/research/meta-flow/platform/services/sns/main.py`
- Game 서비스: `/home/spark/research/meta-flow/platform/services/game/main.py`
- GIS 서비스: `/home/spark/research/meta-flow/platform/services/gis/main.py`
- Gateway: `/home/spark/research/meta-flow/platform/services/gateway/main.py`
- 세션 규칙: `/home/spark/research/meta-flow/CLAUDE.md`

## 주의사항
- `index.html` 수정 후 반드시 `node -e` JS 구문 검증 실행
- `user_points`는 Materialized View — INSERT 불가
- `trash_reports.reporter_id` (user_id 아님)
- 서비스 수정 후 반드시 해당 서비스 재시작
- 게이트웨이 재시작 후 브라우저 강제 새로고침 필요
