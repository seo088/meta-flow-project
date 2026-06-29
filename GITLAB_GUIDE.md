# GitLab 업로드 가이드 — 메타 플로깅 SW 플랫폼

> **GitLab URL**: https://203.234.62.175:5443
> **프로젝트 경로**: `system/meta-flow`
> **전체 URL**: https://203.234.62.175:5443/system/meta-flow

---

## 1. 사전 준비

### 1.1 Git 설치 확인

```bash
git --version
# git version 2.x.x 이상 필요
```

### 1.2 Git 사용자 설정

```bash
git config --global user.name "사용자이름"
git config --global user.email "email@kunsan.ac.kr"
```

### 1.3 SSL 인증서 설정 (자체 서명 인증서 사용 시)

GitLab 서버가 자체 서명(self-signed) SSL 인증서를 사용하므로 아래 설정이 필요합니다:

```bash
# 방법 1: 전역 SSL 검증 비활성화 (개발 환경에서만 권장)
git config --global http.sslVerify false

# 방법 2: 특정 호스트에 대해서만 비활성화 (권장)
git config --global http.https://203.234.62.175:5443.sslVerify false
```

---

## 2. 최초 클론 (Clone)

### 2.1 HTTPS 방식 (기본)

```bash
git clone https://203.234.62.175:5443/system/meta-flow.git
cd meta-flow
```

인증 창이 뜨면 GitLab 계정 정보를 입력합니다:
- **Username**: `jwon` (또는 본인 GitLab 계정)
- **Password**: GitLab 비밀번호 또는 Personal Access Token

### 2.2 Personal Access Token 사용 (추천)

비밀번호 대신 토큰을 사용하면 더 안전합니다:

1. GitLab 접속 → **Settings** → **Access Tokens**
2. 토큰 생성 (scope: `read_repository`, `write_repository`)
3. 클론 시 토큰 사용:

```bash
git clone https://jwon:<YOUR_TOKEN>@203.234.62.175:5443/system/meta-flow.git
```

### 2.3 자격 증명 저장 (매번 입력 방지)

```bash
# 자격 증명 캐시 (15분간 기억)
git config --global credential.helper cache

# 또는 영구 저장 (파일에 저장, 보안 주의)
git config --global credential.helper store
```

---

## 3. 기본 작업 흐름

### 3.1 변경 사항 확인

```bash
# 현재 상태 확인
git status

# 변경 내용 확인
git diff

# 최근 커밋 이력 확인
git log --oneline -10
```

### 3.2 변경 사항 커밋

```bash
# 1) 변경 파일 스테이징
git add platform/services/sns/main.py
git add platform/apps/web/index.html

# 또는 모든 변경사항 한 번에 스테이징
git add -A

# 2) 커밋 (의미 있는 메시지 작성)
git commit -m "feat: 댓글 기능 추가 및 피드 UI 개선"
```

### 3.3 커밋 메시지 규칙

```
<type>: <한줄 요약>

# type 종류:
# feat:     새로운 기능
# fix:      버그 수정
# refactor: 리팩토링
# docs:     문서 수정
# style:    코드 포맷팅
# test:     테스트 추가
# chore:    빌드/설정 변경
```

### 3.4 원격 저장소로 Push

```bash
# main 브랜치로 Push
git push origin main

# 또는 현재 브랜치 Push
git push
```

---

## 4. 브랜치 전략

### 4.1 새 기능 개발

```bash
# 1) 새 브랜치 생성
git checkout -b feature/auth-system

# 2) 개발 작업 수행
# ... 코드 수정 ...

# 3) 커밋
git add -A
git commit -m "feat: JWT 인증 시스템 구현"

# 4) 원격에 Push
git push -u origin feature/auth-system

# 5) GitLab에서 Merge Request 생성
#    https://203.234.62.175:5443/system/meta-flow/-/merge_requests/new
```

### 4.2 main 브랜치 최신화

```bash
# 원격 변경사항 가져오기
git fetch origin

# main 브랜치로 이동 후 Pull
git checkout main
git pull origin main
```

---

## 5. 태그 및 릴리스

### 5.1 버전 태그 생성

```bash
# 어노테이션 태그 생성
git tag -a v0.0.2 -m "v0.0.2: 인증/프로필/소셜/지도/권한 시스템"

# 태그 Push
git push origin v0.0.2

# 또는 모든 태그 Push
git push origin --tags
```

### 5.2 태그 목록 확인

```bash
git tag -l

# 특정 태그 상세 정보
git show v0.0.2
```

---

## 6. 충돌 해결

### 6.1 Pull 시 충돌 발생

```bash
# Pull 시도
git pull origin main

# 충돌 발생 시 충돌 파일 확인
git status

# 충돌 파일 수정 (<<<<<<< / ======= / >>>>>>> 마커 해결)
vi platform/services/sns/main.py

# 해결 후 커밋
git add platform/services/sns/main.py
git commit -m "merge: 충돌 해결"
git push origin main
```

---

## 7. 유용한 명령어

```bash
# 특정 파일의 변경 이력
git log --follow -p platform/apps/web/index.html

# 마지막 커밋 수정 (아직 Push 하지 않은 경우만)
git commit --amend -m "수정된 메시지"

# 특정 커밋으로 되돌리기 (새 커밋 생성)
git revert <commit-hash>

# 원격 저장소 정보 확인
git remote -v

# .gitignore에 추가할 파일 패턴
echo "*.pyc" >> .gitignore
echo "__pycache__/" >> .gitignore
echo ".env" >> .gitignore
echo "*.log" >> .gitignore
```

---

## 8. 프로젝트 구조 참고

```
meta-flow/                              # Git 루트
├── README.md                           # 프로젝트 문서
├── GITLAB_GUIDE.md                     # 이 가이드
├── 00_overview.md ~ 07_deployment.md   # 설계 문서
├── data/                               # 수집 데이터
└── platform/                           # 메인 코드
    ├── .env                            # 환경 변수 (Git 추적 X 권장)
    ├── services/                       # 백엔드 서비스
    ├── agents/                         # AI 에이전트
    ├── apps/                           # 프론트엔드
    ├── shared/                         # 공유 모듈
    ├── infra/                          # 인프라 설정
    └── scripts/                        # 운영 스크립트
```

---

## 9. CI/CD (선택)

GitLab CI/CD를 활용하려면 `.gitlab-ci.yml`을 프로젝트 루트에 생성합니다:

```yaml
stages:
  - test
  - deploy

test:
  stage: test
  script:
    - source /home/spark/anaconda3/etc/profile.d/conda.sh
    - conda activate meta-flow
    - cd platform
    - python -m pytest tests/ -v
  only:
    - main
    - merge_requests

deploy:
  stage: deploy
  script:
    - cd platform
    - bash scripts/stop_all.sh
    - bash scripts/start_all.sh
  only:
    - main
  when: manual
```

---

## 10. 문제 해결

### SSL 관련 오류

```
fatal: unable to access 'https://...': SSL certificate problem
```

**해결**: §1.3의 SSL 설정을 적용합니다.

### 인증 실패

```
remote: HTTP Basic: Access denied
```

**해결**: Personal Access Token을 재생성하고 자격 증명을 초기화합니다:

```bash
git credential reject <<EOF
protocol=https
host=203.234.62.175:5443
EOF

# 이후 다시 push/pull 시 새 자격 증명 입력
```

### Push 거부 (권한 없음)

```
remote: You are not allowed to push code to this project.
```

**해결**: GitLab 프로젝트 설정에서 Developer 이상 권한을 부여받아야 합니다.
**프로젝트 관리자**에게 접근 권한을 요청하세요.

---

> 작성: 메타 플로깅 개발팀 | 최종 업데이트: 2026-03-17
