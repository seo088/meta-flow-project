# Meta SW 플로깅 — 모든 Python 서비스 공통 베이스 이미지
# 멀티아키텍처(linux/amd64 + linux/arm64) 자동 지원 (python:3.11-slim은 multi-arch)
#
# 사용 방법:
#   docker build -t meta-plogging:latest -f Dockerfile .
#   docker run --rm -e SERVICE=sns -p 8501:8501 meta-plogging:latest
#
# docker-compose는 service별로 command를 override해 동일 이미지로 6개 컨테이너 실행

FROM python:3.11-slim AS base

# 시스템 의존성 (PostgreSQL/SSL/curl 헬스체크 + Pillow/HEIC 이미지 처리)
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    libpq5 \
    librdkafka1 \
    ca-certificates \
    libheif1 \
    libjpeg62-turbo \
    libpng16-16 \
    && rm -rf /var/lib/apt/lists/*

# 비-root 사용자 (보안)
RUN useradd --create-home --shell /bin/bash app
WORKDIR /app

# 의존성 먼저 복사 (Docker 레이어 캐시 활용)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -r requirements.txt

# 앱 소스 (services/shared/apps/scripts)
COPY --chown=app:app shared/ /app/shared/
COPY --chown=app:app services/ /app/services/
COPY --chown=app:app apps/ /app/apps/
COPY --chown=app:app scripts/ /app/scripts/

# Python 모듈 경로 — services/X/main.py가 `from core.xxx`로 import 가능하게
ENV PYTHONPATH=/app/shared \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    AI_MODE=dummy

USER app

# 기본은 SNS 서비스. docker-compose에서 service마다 command 오버라이드
EXPOSE 8500 8501 8502 8503 8504 8505 8510
CMD ["python", "services/sns/main.py"]
