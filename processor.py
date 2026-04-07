from database import SessionLocal
from models import RawFinancialData, ProcessedFinancialData

class FinancialProcessor:
    def __init__(self):
        self.db = SessionLocal()

    def process_all(self):
        """
        DB에 저장된 모든 Raw 데이터를 읽어와서 가공 지표를 계산하고 저장합니다.
        """
        print("데이터 가공을 시작합니다...")
        raw_data_list = self.db.query(RawFinancialData).all()
        
        if not raw_data_list:
            print("가공할 원본 데이터(RawFinancialData)가 없습니다.")
            return

        for raw in raw_data_list:
            self.process_single(raw)
            
        self.db.commit()
        self.db.close()
        print("모든 데이터 가공 및 DB 저장 완료!")

    def process_single(self, raw_data):
        """
        단일 Raw 데이터를 바탕으로 EPS, PER, 배당수익률 등을 계산합니다.
        """
        eps = 0.0
        per = 0.0
        pbr = 0.0
        dividend_yield = 0.0
        treasury_share_ratio = 0.0
        
        # 1. EPS (주당순이익) = 당기순이익 / 유통주식수
        # DART API에서 항상 사업보고서(1년 치 최종 실적)를 가져오도록 수정되었으므로,
        # 별도의 연환산 없이 그대로 사용합니다.
        if raw_data.total_shares and raw_data.total_shares > 0:
            eps = float(raw_data.net_income) / raw_data.total_shares
            
        # 2. PER (주가수익비율) = 현재주가 / EPS
        if eps > 0 and raw_data.current_price:
            per = raw_data.current_price / eps
            
        # 3. PBR (주가순자산비율) 계산
        if raw_data.total_equity and raw_data.total_shares and raw_data.total_shares > 0:
            bps = raw_data.total_equity / raw_data.total_shares
            if bps > 0 and raw_data.current_price:
                pbr = raw_data.current_price / bps
            
        # 4. 배당수익률(%) = (1주당 연간 배당금 / 현재주가) * 100
        if raw_data.current_price and raw_data.current_price > 0:
            dividend_yield = (raw_data.dividend_per_share / raw_data.current_price) * 100
            
        # 5. 자사주 보유 비율(%) = (자기주식수 / (유통주식수 + 자기주식수)) * 100
        # DART에서 가져온 treasury_shares를 활용
        if hasattr(raw_data, 'treasury_shares') and raw_data.treasury_shares > 0:
            total_issued = raw_data.total_shares + raw_data.treasury_shares
            if total_issued > 0:
                treasury_share_ratio = (raw_data.treasury_shares / total_issued) * 100
            
        # DB에 Processed 데이터 저장 또는 업데이트
        processed = self.db.query(ProcessedFinancialData).filter(
            ProcessedFinancialData.ticker == raw_data.ticker,
            ProcessedFinancialData.record_date == raw_data.record_date
        ).first()
        
        if processed:
            processed.eps = eps
            processed.per = per
            processed.pbr = pbr
            processed.dividend_yield = dividend_yield
            processed.treasury_share_ratio = treasury_share_ratio
            print(f"가공 데이터 업데이트: {raw_data.ticker} ({raw_data.record_date})")
        else:
            processed = ProcessedFinancialData(
                ticker=raw_data.ticker,
                record_date=raw_data.record_date,
                eps=eps,
                per=per,
                pbr=pbr,
                dividend_yield=dividend_yield,
                cancel_ratio=0.0,
                treasury_share_ratio=treasury_share_ratio
            )
            self.db.add(processed)
            print(f"가공 데이터 신규 저장: {raw_data.ticker} ({raw_data.record_date})")
            
        print(f"   └─ EPS: {eps:,.0f}원 | PER: {per:.2f}배 | PBR: {pbr:.2f}배 | 배당수익률: {dividend_yield:.2f}% | 자사주비율: {treasury_share_ratio:.2f}%")

if __name__ == "__main__":
    processor = FinancialProcessor()
    processor.process_all()