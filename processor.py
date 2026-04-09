from database import SessionLocal
from models import Company, RawFinancialData, ProcessedFinancialData

KR_MARKETS = ('KOSPI', 'KOSDAQ', 'KOSPI/KOSDAQ')

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

    def _get_yfinance_per_pbr(self, ticker):
        """
        yfinance info에서 PER(trailingPE)과 PBR(priceToBook)을 직접 가져옵니다.
        ADR 등 주가 통화와 재무제표 통화가 다른 종목에 사용합니다.
        """
        try:
            import yfinance as yf
            info = yf.Ticker(ticker).info
            per = float(info.get('trailingPE', 0) or 0)
            pbr = float(info.get('priceToBook', 0) or 0)
            return per, pbr
        except Exception:
            return 0.0, 0.0

    def _is_currency_mismatch(self, ticker):
        """
        주가 통화(currency)와 재무제표 통화(financialCurrency)가 다른지 확인합니다.
        """
        try:
            import yfinance as yf
            info = yf.Ticker(ticker).info
            currency = info.get('currency', '')
            financial_currency = info.get('financialCurrency', '')
            if currency and financial_currency and currency != financial_currency:
                return True
        except Exception:
            pass
        return False

    def process_single(self, raw_data):
        """
        단일 Raw 데이터를 바탕으로 EPS, PER, 배당수익률 등을 계산합니다.
        미국 종목 중 통화 불일치(ADR 등)가 있으면 yfinance 제공 PER/PBR을 사용합니다.
        """
        eps = 0.0
        per = 0.0
        pbr = 0.0
        dividend_yield = 0.0
        treasury_share_ratio = 0.0

        # 시장 구분
        company = self.db.query(Company).filter(Company.ticker == raw_data.ticker).first()
        is_kr = company and company.market and company.market.upper() in KR_MARKETS

        # 1. EPS (주당순이익) = 당기순이익 / 유통주식수
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

        # US 종목: 통화 불일치 시 yfinance 제공 PER/PBR 사용
        if not is_kr and self._is_currency_mismatch(raw_data.ticker):
            yf_per, yf_pbr = self._get_yfinance_per_pbr(raw_data.ticker)
            if yf_per > 0 or yf_pbr > 0:
                print(f"   통화 불일치 감지 ({raw_data.ticker}) → yfinance PER/PBR 사용: PER={yf_per:.2f}, PBR={yf_pbr:.2f}")
                per = yf_per
                pbr = yf_pbr

        # 4. 배당수익률(%) = (1주당 연간 배당금 / 현재주가) * 100
        if raw_data.current_price and raw_data.current_price > 0:
            dividend_yield = (raw_data.dividend_per_share / raw_data.current_price) * 100

        # 5. 자사주 보유 비율(%) = (자기주식수 / (유통주식수 + 자기주식수)) * 100
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
            
        print(f"   └─ EPS: {eps:,.0f}(₩or$) | PER: {per:.2f}배 | PBR: {pbr:.2f}배 | 배당수익률: {dividend_yield:.2f}% | 자사주비율: {treasury_share_ratio:.2f}%")

if __name__ == "__main__":
    processor = FinancialProcessor()
    processor.process_all()