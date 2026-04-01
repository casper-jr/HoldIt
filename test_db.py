import sys
from sqlalchemy import text
from database import engine, Base
from models import *  # 모든 모델을 불러와야 테이블이 생성됨

def test_connection():
    try:
        # 1. DB 연결 테스트
        with engine.connect() as connection:
            result = connection.execute(text("SELECT version();"))
            version = result.scalar()
            print("데이터베이스 연결 성공!")
            print(f"DB 버전: {version}")

        # 2. 테이블 생성 테스트
        print("테이블 생성 중...")
        Base.metadata.create_all(bind=engine)
        print("모든 테이블이 성공적으로 생성되었습니다!")
        
        # 3. 생성된 테이블 확인
        with engine.connect() as connection:
            # PostgreSQL에서 현재 스키마의 테이블 목록 조회
            result = connection.execute(text("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
            """))
            tables = [row[0] for row in result]
            print(f"생성된 테이블 목록 ({len(tables)}개):")
            for table in tables:
                print(f"  - {table}")

    except Exception as e:
        print("데이터베이스 연결 또는 테이블 생성 실패!")
        print(f"에러 내용: {e}")
        sys.exit(1)

if __name__ == "__main__":
    test_connection()