# CHANGELOG

### [2026-06-26 16:18:51]
- 203.234.62.166 서버 배포 전 상태를 점검하고 SSH 접속 포트, 리소스, 포트 충돌, Docker 서비스, 홈 디렉터리 쓰기 권한을 확인
- Git 저장소를 초기화하고 대용량 원천 데이터가 커밋되지 않도록 `data/` 제외 규칙을 추가
- 서버 점검 결과와 후속 배포 주의사항을 세션 문서로 기록

### [2026-06-26 16:23:18]
- GitLab 원격 저장소 `ssh://git@203.234.62.175:8022/system/meta-flow.git`를 `origin`으로 등록
- 로컬 작업 브랜치 `young`을 생성하고 `origin/young` 추적 설정을 추가
- 현재 PC SSH 키 기준 GitLab 인증이 `Permission denied (publickey)`로 실패하는 것을 확인하고 후속 조치 필요사항으로 기록

### [2026-06-26 16:25:30]
- 서버 `/home/hong/meta-flow` 경로에 업로드할 Git bundle 기반 배포 절차를 준비
- 서버 작업 저장소도 `young` 브랜치와 GitLab `origin` 원격 저장소를 사용하도록 구성 예정
- GitLab push/pull 사용 전 `hong` 계정의 SSH 공개키를 GitLab에 등록해야 함을 기록

### [2026-06-26 16:26:00]
- 서버 `/home/hong/meta-flow`에 코드 업로드를 완료하고 `young` 브랜치와 GitLab `origin` 원격 저장소 설정을 확인
- 서버 작업 저장소에서 대용량 `data/`, `.env`, `node_modules`가 포함되지 않은 것을 확인
- `hong` 계정에 GitLab 등록용 SSH 공개키를 생성하고, 등록 전까지 원격 인증이 실패하는 상태를 기록

### [2026-06-26 16:29:14]
- 서버 `/home/hong/meta-flow`에 `young` 브랜치 bundle을 다시 전송하고 fast-forward 기준으로 재동기화
- 서버 저장소의 현재 브랜치, 최신 커밋, 원격 저장소 URL, 추적 파일 수를 재확인
- 서버 접속 시 반드시 `ssh -p 4441 hong@203.234.62.166`로 `hong` 계정 홈을 확인해야 함을 기록

### [2026-06-26 16:36:47]
- `Calculator test` 폴더에 덧셈과 곱셈을 지원하는 귀여운 계산기 정적 페이지 추가
- 나중에 뺄셈과 나눗셈 구현과 병합하기 쉽도록 연산 정의를 `operations.js`의 `OPERATIONS` 테이블로 분리
- `file://`로 HTML을 직접 열어도 동작하도록 일반 스크립트 구조를 적용하고 Node 테스트 및 Chrome 파일 열기 검증을 완료

### [2026-06-29 17:09:21]
- GitHub와 GitLab 동시 push 요청에 대해 현재 원격 저장소와 인증 상태를 점검
- GitLab `origin` push가 `Permission denied (publickey)`로 실패하는 것을 확인
- GitHub 원격 저장소 URL이 아직 설정되어 있지 않아 `github` remote 추가가 필요함을 기록

### [2026-06-29 17:23:31]
- 서버에서 확인된 `Calculator_test` 폴더명을 기준으로 로컬 계산기 테스트 폴더를 `Calculator test`에서 `Calculator_test`로 변경
- 서버 `Calculator_test` 파일과 로컬 파일의 해시가 동일함을 확인하고 rename만 커밋 대상으로 정리
- GitLab `young` 브랜치 push를 재시도하기 위해 변경사항을 준비

### [2026-07-01 09:56:11]
- `C:/Users/jwon/Downloads/meta-flow-v0.2.0/meta-flow-v0.2.0` 원본을 최신 기준으로 삼아 `young` 브랜치 작업 트리에 v0.2.0 코드와 문서를 포팅
- v0.2.0의 게이미피케이션 완성, 지도 개편, API 개발자 도구, Docker/운영 문서, ADR 및 세션 문서를 원본 우선으로 반영
- 대상 저장소 전용 작업 규칙과 로컬 이력인 `AGENTS.md`, `CHANGELOG.md`, `Calculator_test/`는 보존하고 HTML 스크립트 구문 검증을 완료
- GitHub Push Protection에서 감지한 `GITLAB_TUTORIAL.md`의 실제 GitLab 토큰 예시를 placeholder로 치환

### [2026-07-21 17:00:14]
- `C:/Users/jwon/Downloads/meta-flow-main/meta-flow-main` 원본과 현재 작업 트리의 코드 동기화 상태를 비교
- 공통 코드 파일은 모두 동일함을 확인하고, 실제 차이는 보안상 placeholder를 유지해야 하는 `GITLAB_TUTORIAL.md`와 런타임 업로드 경로 `platform/apps/web/avatars/`뿐임을 기록
- 원본의 실제 GitLab 토큰 문자열과 사용자 업로드 아바타 파일은 코드 동기화 대상에서 제외

### [2026-07-21 17:16:39]
- 교수님 요청사항에 맞춰 데이터 제공 API가 쓰레기 크기 분류, 사진 URL, 대표 사진 URL, 제보 위치, 사진 위치를 함께 반환하도록 확장
- `/api/v1/reports`에 `trash_type`, `trash_size`, `handling`, `has_image`, `has_location`, `source`, `include_items` 입력 조건을 추가
- `/api/v1/datasets/{batch_id}` 응답도 동일한 사진/위치/크기 분류 구조로 정규화하고 API 콘솔 테스트 입력을 보강
- Python 문법 검증, API 콘솔 스크립트 검증, `git diff --check`를 완료

### [2026-07-22 14:35:00]
- 166번 `hong` 정적 프리뷰(`:8600`)에서 프로필의 API 콘솔 버튼이 운영 포트 `:8500`으로 이동하던 링크 문제를 수정
- 프리뷰 환경에서는 `/api-console.html`로 이동하고, 운영 게이트웨이 환경에서는 기존 `/api-console`로 이동하도록 분기 처리
- 프로필의 데이터 제공 API 문서 표에 `trash_size`, `has_image`, `has_location`, `include_items` 등 신규 파라미터 요약을 반영

### [2026-07-22 14:48:01]
- 마이페이지 타임라인 하단에 노출되던 데이터 활용 API 신청/문서 블록을 제거
- 데이터 활용 API 신청/문서 블록은 메시지함 탭에서만 로드되고 표시되도록 프로필 탭 구조와 로딩 조건을 수정
