from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from config import DATABASE_URL

# SQLAlchemy 엔진 생성
engine = create_engine(DATABASE_URL)

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