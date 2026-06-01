import os
from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker, declarative_base

# Cloud Run 환경(INSTANCE_CONNECTION_NAME이 있을 때)은 Cloud SQL Python Connector로 연결
# 로컬/Supabase 환경은 기존 psycopg2 방식 그대로 사용
INSTANCE_CONNECTION_NAME = os.getenv("INSTANCE_CONNECTION_NAME")

if INSTANCE_CONNECTION_NAME:
    # Cloud SQL Python Connector: IAM 기반 인증으로 안전하게 연결, 별도 프록시 불필요
    from google.cloud.sql.connector import Connector
    connector = Connector()

    def _getconn():
        return connector.connect(
            INSTANCE_CONNECTION_NAME,
            "pg8000",
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            db=os.getenv("DB_NAME"),
        )

    engine = create_engine("postgresql+pg8000://", creator=_getconn)
else:
    # 로컬 개발 / Supabase 환경 (기존 방식 유지)
    from config import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME
    engine = create_engine(URL.create(
        drivername="postgresql+psycopg2",
        username=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=int(DB_PORT) if DB_PORT else 5432,
        database=DB_NAME,
    ))

# 세션 팩토리 생성
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

# Base 클래스 생성 (모든 모델 클래스가 이를 상속받음)
Base = declarative_base()

def get_db():
    """
    DB 세션을 생성하고 반환하는 제너레이터 함수.
    with 구문이나 FastAPI의 Depends 등과 함께 사용하기 좋습니다.
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()