# GitLab 업로드 튜토리얼

> 메타 플로깅 SW 플랫폼을 GitLab 저장소에 업로드하는 방법을 단계별로 설명합니다.

---

## 1. 사전 요구사항

### 1.1 GitLab 접속 정보

| 항목 | 값 |
|------|-----|
| GitLab URL | `https://203.234.62.175:5443` |
| 프로젝트 경로 | `system/meta-flow` |
| 사용자명 | `jwon` |
| 인증 방식 | Personal Access Token (PAT) |

### 1.2 Git 설치 확인

```bash
git --version
# git version 2.34.1 이상 필요
```

### 1.3 Self-signed SSL 인증서 설정

GitLab이 자체 서명 인증서를 사용하므로, SSL 검증을 비활성화합니다:

```bash
# 방법 1: 전역 설정 (권장하지 않음, 개발 환경 전용)
git config --global http.sslVerify false

# 방법 2: 해당 호스트만 (권장)
git config --global http.https://203.234.62.175:5443.sslVerify false
```

---

## 2. GitLab 프로젝트 생성 (최초 1회)

### 2.1 웹 UI에서 생성

1. 브라우저에서 `https://203.234.62.175:5443` 접속
2. `jwon` 계정으로 로그인
3. **"New project"** → **"Create blank project"** 클릭
4. 프로젝트 설정:
   - **Project name**: `meta-flow`
   - **Project URL**: `system` 그룹 선택 → `meta-flow`
   - **Visibility Level**: Private (또는 Internal)
   - **"Initialize repository with a README"**: 체크 해제 (❌)
5. **"Create project"** 클릭

### 2.2 또는 API로 생성

```bash
# 그룹 ID 확인 (system 그룹)
curl -sk --header "PRIVATE-TOKEN: glpat-REPLACE_WITH_YOUR_TOKEN" \
  "https://203.234.62.175:5443/api/v4/groups?search=system" | python3 -m json.tool

# 프로젝트 생성 (namespace_id는 위에서 확인한 그룹 ID)
curl -sk --header "PRIVATE-TOKEN: glpat-REPLACE_WITH_YOUR_TOKEN" \
  --data "name=meta-flow&namespace_id=<GROUP_ID>&visibility=private" \
  "https://203.234.62.175:5443/api/v4/projects"
```

---

## 3. 로컬 Git 저장소 초기화

### 3.1 프로젝트 디렉토리로 이동

```bash
cd /home/spark/research/meta-flow
```

### 3.2 Git 초기화

```bash
git init
git branch -M main
```

### 3.3 사용자 정보 설정

```bash
git config user.name "jwon"
git config user.email "jwon@kunsan.ac.kr"  # 본인 이메일로 변경
```

---

## 4. .gitignore 설정

민감 정보와 불필요한 파일을 제외합니다:

```bash
cat > .gitignore << 'EOF'
# Python
__pycache__/
*.py[cod]
*.egg-info/
*.egg
dist/
build/

# 환경
.env
*.env.local
.env.*.local

# IDE
.vscode/
.idea/
*.swp
*.swo
*~

# OS
.DS_Store
Thumbs.db

# Logs
*.log
platform/logs/

# Data (대용량)
data/*.json
data/*.csv
data/*.geojson
*.tar.gz
*.zip

# Conda
.conda/

# Docker volumes
platform/infra/docker/volumes/

# MinIO data
platform/infra/minio/

# Node (향후 프론트엔드)
node_modules/

# AI 모델 가중치
*.pt
*.pth
*.onnx
*.mar
EOF
```

---

## 5. Remote 설정 및 Push

### 5.1 GitLab Remote 추가

```bash
# Personal Access Token을 URL에 포함하는 방법
git remote add origin https://jwon:glpat-REPLACE_WITH_YOUR_TOKEN@203.234.62.175:5443/system/meta-flow.git

# 또는 토큰 없이 추가 후, push 시 입력
git remote add origin https://203.234.62.175:5443/system/meta-flow.git
```

### 5.2 .env 파일 별도 보관

```bash
# .env는 push하지 않으므로, 예제 파일 생성
cp platform/.env platform/.env.example

# .env.example에서 민감 정보 제거
sed -i 's/POSTGRES_PASSWORD=.*/POSTGRES_PASSWORD=your_password_here/' platform/.env.example
sed -i 's/REDIS_PASSWORD=.*/REDIS_PASSWORD=your_redis_password/' platform/.env.example
sed -i 's/JWT_SECRET_KEY=.*/JWT_SECRET_KEY=your_jwt_secret/' platform/.env.example
sed -i 's/MINIO_SECRET_KEY=.*/MINIO_SECRET_KEY=your_minio_secret/' platform/.env.example
sed -i 's/GITLAB_TOKEN=.*/GITLAB_TOKEN=your_gitlab_token/' platform/.env.example
```

### 5.3 파일 스테이징 및 커밋

```bash
# 전체 파일 추가
git add .

# 상태 확인
git status

# 커밋
git commit -m "feat: 메타 플로깅 SW 플랫폼 v0.0.1 초기 릴리스

- 6-레이어 마이크로서비스 아키텍처 (8 서비스 + 5 에이전트)
- PostGIS 지오펜싱 4구역 (군산대, 은파, 새만금, 금강)
- Kafka 이벤트 기반 비동기 파이프라인 (7 토픽)
- FastMCP 에이전트 허브 (7 도구)
- 게미피케이션 (레벨/배지/퀘스트/랭킹)
- 드론 영상 프레임 추출 및 AI 탐지
- AIHub 새만금 쓰레기 데이터 연동
- 웹 포털 (9탭 SPA) + 관리자 대시보드
- Elasticsearch 실시간 동기화
- 공공데이터 GeoJSON/COCO 내보내기"
```

### 5.4 Push

```bash
# 최초 push (upstream 설정)
git push -u origin main

# 이후 push
git push
```

---

## 6. 태그 (버전) 생성

```bash
# v0.0.1 태그 생성
git tag -a v0.0.1 -m "v0.0.1: 메타 플로깅 SW 플랫폼 초기 릴리스"

# 태그 Push
git push origin v0.0.1

# 모든 태그 Push
git push origin --tags
```

---

## 7. GitLab CI/CD 설정 (선택)

향후 자동 배포를 위한 `.gitlab-ci.yml` 예시:

```yaml
stages:
  - test
  - deploy

variables:
  CONDA_ENV: meta-flow

test:
  stage: test
  script:
    - source /home/spark/anaconda3/etc/profile.d/conda.sh
    - conda activate $CONDA_ENV
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
    - git pull origin main
    - bash scripts/start_all.sh
  only:
    - main
  when: manual
```

---

## 8. 자주 사용하는 Git 명령어

```bash
# 상태 확인
git status
git log --oneline -10

# 브랜치 작업
git checkout -b feature/new-feature   # 새 브랜치
git checkout main                      # main으로 전환
git merge feature/new-feature          # 병합

# 변경사항 push
git add -A
git commit -m "feat: 새 기능 추가"
git push

# GitLab에서 변경사항 가져오기
git pull origin main

# 태그 관리
git tag -l              # 태그 목록
git tag -a v0.0.2 -m "v0.0.2: 업데이트 설명"
git push origin v0.0.2
```

---

## 9. 문제 해결

### SSL 인증서 오류

```bash
# "SSL certificate problem: self-signed certificate" 오류 시
git config --global http.https://203.234.62.175:5443.sslVerify false
```

### 인증 오류

```bash
# "Authentication failed" 오류 시
# 1) Personal Access Token 만료 확인
# 2) GitLab > Settings > Access Tokens 에서 새 토큰 생성
# 3) Remote URL 업데이트
git remote set-url origin https://jwon:<새_토큰>@203.234.62.175:5443/system/meta-flow.git
```

### 대용량 파일 오류

```bash
# 100MB 이상 파일 push 실패 시
# Git LFS 사용
git lfs install
git lfs track "*.pt" "*.pth" "*.onnx"
git add .gitattributes
git commit -m "chore: Git LFS 설정"
```

### Push 권한 오류

```bash
# "You are not allowed to push code to protected branches" 오류 시
# GitLab > Settings > Repository > Protected Branches 에서 권한 확인
# 또는 다른 브랜치로 push 후 Merge Request 생성
git push origin main:develop
```

---

## 10. 프로젝트 클론 (다른 환경에서)

```bash
# 1) 프로젝트 클론
git clone https://jwon@203.234.62.175:5443/system/meta-flow.git
cd meta-flow

# 2) .env 파일 복사 (별도 전달받은 파일)
cp /path/to/.env platform/.env

# 3) Conda 환경 생성
cd platform
conda env create -f environment.yml
conda activate meta-flow

# 4) Docker 인프라 시작
cd infra/docker
docker-compose up -d
cd ../..

# 5) DB 초기화
psql -h localhost -p 5433 -U plogging -d plogging_db -f infra/postgres/init.sql
psql -h localhost -p 5433 -U plogging -d plogging_db -f infra/postgres/seed.sql

# 6) 서비스 시작
bash scripts/start_all.sh
```
