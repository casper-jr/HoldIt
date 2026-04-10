import os
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

# DART API 설정
DART_API_KEY = os.getenv("DART_API_KEY")

# 데이터베이스 설정
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")