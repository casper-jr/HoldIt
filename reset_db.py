from database import SessionLocal
from models import Company, RawFinancialData, ProcessedFinancialData, ScoringResult, QualitativeAssessment, FetchHistory

def reset_database():
    """
    데이터베이스의 모든 데이터를 삭제(초기화)합니다.
    외래키(Foreign Key) 제약조건을 피하기 위해 자식 테이블부터 순서대로 삭제합니다.
    """
    print("⚠️ 데이터베이스 초기화를 시작합니다...")
    
    # 사용자 확인 (실수 방지)
    confirm = input("정말로 모든 데이터를 삭제하시겠습니까? (y/n): ")
    if confirm.lower() != 'y':
        print("초기화를 취소합니다.")
        return

    db = SessionLocal()
    try:
        # 1. 자식 테이블들 먼저 삭제
        deleted_scores = db.query(ScoringResult).delete()
        deleted_qual = db.query(QualitativeAssessment).delete()
        deleted_proc = db.query(ProcessedFinancialData).delete()
        deleted_raw = db.query(RawFinancialData).delete()
        deleted_hist = db.query(FetchHistory).delete()
        
        # 2. 부모 테이블(Company) 마지막에 삭제
        deleted_comp = db.query(Company).delete()
        
        db.commit()
        
        print("\n✅ DB 초기화 완료!")
        print(f" - 삭제된 기업(Company) 수: {deleted_comp}")
        print(f" - 삭제된 원본 데이터(Raw) 수: {deleted_raw}")
        print(f" - 삭제된 가공 데이터(Processed) 수: {deleted_proc}")
        print(f" - 삭제된 평가 결과(Score) 수: {deleted_scores}")
        print(f" - 삭제된 수집 이력(History) 수: {deleted_hist}")
        
    except Exception as e:
        db.rollback()
        print(f"\n❌ 데이터 초기화 중 에러 발생: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    reset_database()