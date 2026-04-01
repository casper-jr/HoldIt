from database import SessionLocal
from models import RawFinancialData, ProcessedFinancialData, ScoringResult
from datetime import date

class StockScorer:
    def __init__(self):
        self.db = SessionLocal()

    def calculate_per_score(self, per):
        """PER 점수 산정 (최대 20점)"""
        if per <= 0: return 0  # 적자이거나 데이터가 없는 경우
        if per < 5: return 20
        if per < 8: return 15
        if per < 10: return 10
        return 5  # 10 이상

    def calculate_pbr_score(self, pbr):
        """PBR 점수 산정 (최대 5점)"""
        if pbr <= 0: return 0
        if pbr < 0.3: return 5
        if pbr < 0.6: return 4
        if pbr < 1.0: return 3
        return 0  # 1.0 이상

    def calculate_div_yield_score(self, div_yield):
        """배당수익률 점수 산정 (최대 10점)"""
        if div_yield <= 0: return 0  # 배당이 없는 경우 0점 처리
        if div_yield > 7: return 10
        if div_yield > 5: return 7
        if div_yield > 3: return 5
        return 2  # 0% 초과 3% 이하

    def calculate_div_quarter_score(self, is_quarterly):
        """분기 배당 점수 산정 (최대 5점)"""
        return 5 if is_quarterly else 0

    def calculate_div_inc_score(self, inc_years):
        """배당 연속 인상 연수 점수 산정 (최대 5점)"""
        if inc_years >= 10: return 5
        if inc_years >= 5: return 4
        if inc_years >= 3: return 3
        return 0

    def calculate_treasury_ratio_score(self, ratio):
        """자사주 보유 비율 점수 산정 (최대 5점)"""
        if ratio == 0: return 5
        if ratio < 2: return 4
        if ratio < 5: return 2
        return 0

    def calculate_buyback_score(self, is_buyback_cancel):
        """자사주 매입 및 소각 여부 점수 산정 (최대 7점)"""
        return 7 if is_buyback_cancel else 0

    def get_grade(self, total_score):
        """총점에 따른 투자 등급 산정"""
        if total_score > 80: return 'A'
        if total_score >= 70: return 'B'
        if total_score >= 50: return 'C'
        return 'D'

    def score_all(self):
        """
        가공된 데이터를 바탕으로 점수를 계산하고 저장합니다.
        현재는 정량적 데이터(PER, PBR, 배당수익률)만 점수에 반영합니다.
        """
        print("🧮 점수 계산을 시작합니다...")
        processed_list = self.db.query(ProcessedFinancialData).all()
        
        if not processed_list:
            print("⚠️ 평가할 가공 데이터(ProcessedFinancialData)가 없습니다.")
            return

        for processed in processed_list:
            self.score_single(processed)
            
        self.db.commit()
        self.db.close()
        print("✅ 모든 점수 계산 및 DB 저장 완료!")

    def score_single(self, processed):
        # 1. 정량적 지표 점수 계산
        score_per = self.calculate_per_score(processed.per)
        score_pbr = self.calculate_pbr_score(processed.pbr)
        score_div_yield = self.calculate_div_yield_score(processed.dividend_yield)
        
        # Raw 데이터에서 배당 정보 가져오기
        raw_data = self.db.query(RawFinancialData).filter(
            RawFinancialData.ticker == processed.ticker,
            RawFinancialData.record_date == processed.record_date
        ).first()
        
        score_div_quarter = 0
        score_div_inc = 0
        score_buyback = 0
        if raw_data:
            score_div_quarter = self.calculate_div_quarter_score(raw_data.quarterly_dividend)
            score_div_inc = self.calculate_div_inc_score(raw_data.dividend_increase_years)
            score_buyback = self.calculate_buyback_score(raw_data.share_buyback_cancel)
            
        score_treasury_ratio = self.calculate_treasury_ratio_score(processed.treasury_share_ratio)
        
        # 2. 정성적 지표 및 추가 데이터 필요 지표 (우선 0점 처리)
        score_profit_sus = 0
        score_listing = 0
        score_cancel_ratio = 0
        score_growth = 0
        score_management = 0
        score_brand = 0
        
        # 3. 총점 및 등급 계산
        total_score = sum([
            score_per, score_pbr, score_div_yield,
            score_profit_sus, score_listing, score_div_quarter, score_div_inc,
            score_buyback, score_cancel_ratio, score_treasury_ratio,
            score_growth, score_management, score_brand
        ])
        
        grade = self.get_grade(total_score)
        
        # 4. DB 저장 또는 업데이트
        today = date.today()
        
        scoring_result = self.db.query(ScoringResult).filter(
            ScoringResult.ticker == processed.ticker,
            ScoringResult.score_date == today
        ).first()
        
        if scoring_result:
            scoring_result.score_per = score_per
            scoring_result.score_pbr = score_pbr
            scoring_result.score_div_yield = score_div_yield
            scoring_result.score_div_quarter = score_div_quarter
            scoring_result.score_div_inc = score_div_inc
            scoring_result.score_treasury_ratio = score_treasury_ratio
            scoring_result.score_buyback = score_buyback
            scoring_result.total_score = total_score
            scoring_result.grade = grade
            print(f"🔄 점수 업데이트: {processed.ticker} (총점: {total_score}점, 등급: {grade})")
        else:
            scoring_result = ScoringResult(
                ticker=processed.ticker,
                score_date=today,
                score_per=score_per,
                score_pbr=score_pbr,
                score_div_yield=score_div_yield,
                score_div_quarter=score_div_quarter,
                score_div_inc=score_div_inc,
                score_treasury_ratio=score_treasury_ratio,
                score_buyback=score_buyback,
                total_score=total_score,
                grade=grade
            )
            self.db.add(scoring_result)
            print(f"🏆 새 점수 저장: {processed.ticker} (총점: {total_score}점, 등급: {grade})")
            
        print(f"   └─ PER: {score_per} | PBR: {score_pbr} | 배당수익률: {score_div_yield} | 분기배당: {score_div_quarter} | 배당인상: {score_div_inc} | 자사주비율: {score_treasury_ratio} | 매입/소각: {score_buyback}")

if __name__ == "__main__":
    scorer = StockScorer()
    scorer.score_all()