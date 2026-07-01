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
