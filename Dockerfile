FROM python:3.11-slim

WORKDIR /app

# 시스템 패키지 설치 (psycopg2 빌드에 필요)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# 의존성 먼저 설치 (코드 변경 시 캐시 재사용)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 앱 코드 복사
COPY . .

# entrypoint.sh 실행 권한 부여
RUN chmod +x entrypoint.sh

CMD ["bash", "entrypoint.sh"]
