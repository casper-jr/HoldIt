from sqlalchemy import create_engine
from sqlalchemy.engine import URL
from sqlalchemy.orm import sessionmaker, declarative_base
from config import DB_USER, DB_PASSWORD, DB_HOST, DB_PORT, DB_NAME

# SQLAlchemy 엔진 생성
# URL.create()를 사용하면 비밀번호의 특수문자를 자동으로 처리함
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