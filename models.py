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
    
    current_price = Column(Float)
    net_income = Column(Float)
    total_shares = Column(Float)
    total_equity = Column(Float, default=0.0) # 자본총계(순자산) 추가
    dividend_per_share = Column(Float)
    quarterly_dividend = Column(Boolean, default=False)
    dividend_increase_years = Column(Integer, default=0)
    share_buyback_cancel = Column(Boolean, default=False)
    cancel_shares = Column(Float, default=0.0)
    treasury_shares = Column(Float, default=0.0)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

class ProcessedFinancialData(Base):
    __tablename__ = 'processed_financial_data'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    ticker = Column(String, ForeignKey('companies.ticker'))
    record_date = Column(Date, nullable=False)
    
    eps = Column(Float)
    per = Column(Float)
    pbr = Column(Float)
    dividend_yield = Column(Float)
    cancel_ratio = Column(Float)
    treasury_share_ratio = Column(Float)

    created_at = Column(DateTime(timezone=True), server_default=func.now())
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now())

class QualitativeAssessment(Base):
    __tablename__ = 'qualitative_assessments'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    ticker = Column(String, ForeignKey('companies.ticker'))
    assessment_date = Column(Date, nullable=False)
    
    profit_sustainability = Column(String) # SUSTAINABLE, UNSTABLE
    multiple_listing = Column(String)      # SINGLE, MULTIPLE
    future_growth = Column(String)         # VERY_HIGH, HIGH, NORMAL, LOW
    management_quality = Column(String)    # EXCELLENT, PROFESSIONAL, POOR
    global_brand = Column(Boolean, default=False)
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())

class FetchHistory(Base):
    __tablename__ = 'fetch_history'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    ticker = Column(String, ForeignKey('companies.ticker'))
    fetch_date = Column(Date, nullable=False) # 수집 시도한 날짜
    status = Column(String, nullable=False)   # SUCCESS, SKIP_NO_DIVIDEND, FAIL_NO_DATA, ERROR 등
    message = Column(String)                  # 상세 메시지
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())
class ScoringResult(Base):
    __tablename__ = 'scoring_results'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    ticker = Column(String, ForeignKey('companies.ticker'))
    score_date = Column(Date, nullable=False)
    
    score_per = Column(Integer, default=0)
    score_pbr = Column(Integer, default=0)
    score_profit_sus = Column(Integer, default=0)
    score_listing = Column(Integer, default=0)
    
    score_div_yield = Column(Integer, default=0)
    score_div_quarter = Column(Integer, default=0)
    score_div_inc = Column(Integer, default=0)
    score_buyback = Column(Integer, default=0)
    score_cancel_ratio = Column(Integer, default=0)
    score_treasury_ratio = Column(Integer, default=0)
    
    score_growth = Column(Integer, default=0)
    score_management = Column(Integer, default=0)
    score_brand = Column(Integer, default=0)
    
    total_score = Column(Integer, default=0)
    grade = Column(String) # A, B, C, D
    
    created_at = Column(DateTime(timezone=True), server_default=func.now())