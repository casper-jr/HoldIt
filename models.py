from sqlalchemy import Column, Integer, String, Float, Date, Boolean, ForeignKey, DateTime
from sqlalchemy.sql import func
from database import Base

class Company(Base):
    __tablename__ = 'companies'

    ticker = Column(String, primary_key=True)
    name = Column(String, nullable=False)
    market = Column(String)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class RawFinancialData(Base):
    __tablename__ = 'raw_financial_data'

    id = Column(Integer, primary_key=True, autoincrement=True)
    ticker = Column(String, ForeignKey('companies.ticker'))
    record_date = Column(Date, nullable=False)

    # 주가 및 주식수
    current_price = Column(Float)
    total_shares = Column(Float)

    # 손익 (KR: DART 사업보고서 연간 / US: yfinance TTM)
    net_income = Column(Float)

    # 재무상태표 (최근 분기 기준)
    total_equity = Column(Float, default=0.0)       # 자본총계 - PBR·ROE 계산용
    total_liabilities = Column(Float, default=0.0)  # 총부채 - 부채비율 계산용

    # 현금흐름표 (KR: DART / US: yfinance cashflow)
    operating_cash_flow = Column(Float, default=0.0)  # 영업현금흐름 - FCF 계산용
    capital_expenditure = Column(Float, default=0.0)  # 설비투자(CapEx) - FCF 계산용

    # 배당
    dividend_per_share = Column(Float)              # 1주당 연간 배당금 (작년도 확정값)
    dividend_increase_years = Column(Integer, default=0)  # 배당 연속 인상 연수

    # EPS 성장률 (PEG 계산용)
    eps_growth_rate = Column(Float)  # EPS 연간 성장률(%), US: yfinance earningsGrowth 또는 EPS CAGR

    # 자사주
    share_cancel = Column(Boolean, default=False)   # 최근 1년 내 자사주 소각 여부 (매입 제외, 소각만)
    treasury_shares = Column(Float, default=0.0)    # 자사주 보유 수 (ROE 계산 보조용)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

class ProcessedFinancialData(Base):
    __tablename__ = 'processed_financial_data'

    id = Column(Integer, primary_key=True, autoincrement=True)
    ticker = Column(String, ForeignKey('companies.ticker'))
    record_date = Column(Date, nullable=False)

    # 카테고리 1: 수익 창출력 및 내재 가치
    eps = Column(Float)     # 주당순이익 = 당기순이익 / 유통주식수
    per = Column(Float)     # 주가수익비율 = 현재가 / EPS
    pbr = Column(Float)     # 주가순자산비율 = 현재가 / (자본총계 / 유통주식수)
    roe = Column(Float)     # 자기자본이익률(%) = (당기순이익 / 자기자본) × 100
    fcf = Column(Float)     # 잉여현금흐름 = 영업현금흐름 - CapEx
    fcf_yield = Column(Float)  # FCF 수익률(%) = (FCF / 시가총액) × 100

    # 카테고리 2: 성장성 및 재무 안전성
    peg_ratio = Column(Float)   # PEG = PER ÷ EPS 연간 성장률(%), 데이터 미확보 시 NULL
    debt_ratio = Column(Float)  # 부채비율(%) = (총부채 / 자기자본) × 100

    # 카테고리 3: 주주환원
    dividend_yield = Column(Float)  # 배당수익률(%) = (주당 연간 배당금 / 현재 주가) × 100

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

class QualitativeAssessment(Base):
    __tablename__ = 'qualitative_assessments'

    id = Column(Integer, primary_key=True, autoincrement=True)
    ticker = Column(String, ForeignKey('companies.ticker'))
    assessment_date = Column(Date, nullable=False)

    economic_moat = Column(String)      # 경제적 해자 및 성장성: STRONG, MODERATE, NONE
    management_quality = Column(String) # 경영진 역량: EXCELLENT, PROFESSIONAL, POOR (총점 합산 제외, 참고용)

    created_at = Column(DateTime(timezone=True), server_default=func.now())

class FetchHistory(Base):
    __tablename__ = 'fetch_history'

    id = Column(Integer, primary_key=True, autoincrement=True)
    ticker = Column(String, ForeignKey('companies.ticker'))
    fetch_date = Column(Date, nullable=False)   # 수집 시도 날짜
    status = Column(String, nullable=False)     # SUCCESS, SKIP_NO_DIVIDEND, FAIL_NO_DATA, ERROR 등
    message = Column(String)                    # 상세 메시지

    created_at = Column(DateTime(timezone=True), server_default=func.now())

class ScoringResult(Base):
    __tablename__ = 'scoring_results'

    id = Column(Integer, primary_key=True, autoincrement=True)
    ticker = Column(String, ForeignKey('companies.ticker'))
    score_date = Column(Date, nullable=False)

    # 카테고리 1: 수익 창출력 및 내재 가치 (40점)
    score_per = Column(Integer, default=0)   # 최대 10점
    score_roe = Column(Integer, default=0)   # 최대 15점
    score_fcf = Column(Integer, default=0)   # 최대 10점
    score_pbr = Column(Integer, default=0)   # 최대 5점

    # 카테고리 2: 성장성 및 재무 안전성 (30점)
    score_moat = Column(Integer, default=0)        # 경제적 해자 및 성장성, 최대 10점 (정성)
    score_peg = Column(Integer, default=0)         # 최대 10점
    score_debt_ratio = Column(Integer, default=0)  # 최대 10점

    # 카테고리 3: 주주환원 정책 (30점)
    score_div_yield = Column(Integer, default=0)   # 최대 10점
    score_div_growth = Column(Integer, default=0)  # 배당 성장성, 최대 10점
    score_cancel = Column(Integer, default=0)      # 자사주 소각 실적, 최대 10점

    total_score = Column(Integer, default=0)
    grade = Column(String)       # A, B, C, D
    scorer_version = Column(String, default='v2')  # 사용된 scorer 버전

    created_at = Column(DateTime(timezone=True), server_default=func.now())
