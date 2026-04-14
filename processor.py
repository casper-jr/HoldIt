from datetime import date, datetime, timezone
from sqlalchemy import func
from database import SessionLocal
from models import Company, RawFinancialData, ProcessedFinancialData

KR_MARKETS = ('KOSPI', 'KOSDAQ', 'KOSPI/KOSDAQ')

class FinancialProcessor:
    def __init__(self):
        self.db = SessionLocal()

    def process_all(self, today_only=True):
        """
        DB에 저장된 Raw 데이터를 읽어와서 가공 지표를 계산하고 저장합니다.
        today_only=True (기본값): 오늘 fetch된 종목만 처리 (updated_at 기준)
        today_only=False      : DB 전체 재처리
        """
        print("데이터 가공을 시작합니다...")

        if today_only:
            today_utc = datetime.now(timezone.utc).date()
            raw_data_list = self.db.query(RawFinancialData).filter(
                func.date(RawFinancialData.updated_at) == today_utc
            ).all()
            if not raw_data_list:
                print("오늘 업데이트된 원본 데이터가 없습니다.")
                print("전체 재처리가 필요하면: python3 main.py process --all")
                return
            print(f"오늘 업데이트된 {len(raw_data_list)}개 종목만 처리합니다.")
        else:
            raw_data_list = self.db.query(RawFinancialData).all()
            print(f"전체 {len(raw_data_list)}개 종목을 처리합니다.")

        if not raw_data_list:
            print("가공할 원본 데이터(RawFinancialData)가 없습니다.")
            return

        for raw in raw_data_list:
            self.process_single(raw)

        self.db.commit()
        self.db.close()
        print("모든 데이터 가공 및 DB 저장 완료!")

    def _get_yfinance_info_if_currency_mismatch(self, ticker):
        """
        주가 통화(currency)와 재무제표 통화(financialCurrency)가 다른 경우에만
        yfinance PER/PBR을 반환합니다. API 호출을 1번으로 통합합니다.
        통화가 일치하거나 오류 시 (False, 0.0, 0.0) 반환.
        """
        try:
            import yfinance as yf
            info = yf.Ticker(ticker).info
            currency = info.get('currency', '')
            financial_currency = info.get('financialCurrency', '')
            if currency and financial_currency and currency != financial_currency:
                per = float(info.get('trailingPE', 0) or 0)
                pbr = float(info.get('priceToBook', 0) or 0)
                return True, per, pbr
        except Exception:
            pass
        return False, 0.0, 0.0

    def process_single(self, raw_data):
        """
        단일 Raw 데이터를 바탕으로 각 평가 지표를 계산합니다.

        [카테고리 1] 수익 창출력 및 내재 가치
          - EPS, PER, PBR: 기존 방식 유지
          - ROE: 즉시 계산 가능 (net_income + total_equity 이미 수집 중)
          - FCF, FCF 수익률: operating_cash_flow·capital_expenditure 수집 후 계산 (현재 None)

        [카테고리 2] 성장성 및 재무 안전성
          - 부채비율: total_liabilities 수집 후 계산 (현재 None)
          - PEG: EPS 성장률 데이터 확보 후 계산 (현재 None, 장기 과제)

        [카테고리 3] 주주환원
          - 배당수익률: 기존 방식 유지

        미국 ADR 통화 불일치 종목은 yfinance trailingPE/priceToBook 직접 사용.
        """
        eps = 0.0
        per = 0.0
        pbr = 0.0
        roe = None
        fcf = None
        fcf_yield = None
        debt_ratio = None
        peg_ratio = None
        dividend_yield = 0.0

        # 시장 구분
        company = self.db.query(Company).filter(Company.ticker == raw_data.ticker).first()
        is_kr = company and company.market and company.market.upper() in KR_MARKETS

        # 1. EPS (주당순이익) = 당기순이익 / 유통주식수
        if raw_data.total_shares and raw_data.total_shares > 0:
            eps = float(raw_data.net_income) / raw_data.total_shares

        # 2. PER (주가수익비율) = 현재주가 / EPS
        if eps > 0 and raw_data.current_price:
            per = raw_data.current_price / eps

        # 3. PBR (주가순자산비율) = 현재주가 / BPS
        if raw_data.total_equity and raw_data.total_shares and raw_data.total_shares > 0:
            bps = raw_data.total_equity / raw_data.total_shares
            if bps > 0 and raw_data.current_price:
                pbr = raw_data.current_price / bps

        # US 종목: 통화 불일치(ADR 등) 시 yfinance 제공 PER/PBR 사용
        if not is_kr:
            mismatch, yf_per, yf_pbr = self._get_yfinance_info_if_currency_mismatch(raw_data.ticker)
            if mismatch and (yf_per > 0 or yf_pbr > 0):
                print(f"   통화 불일치 감지 ({raw_data.ticker}) → yfinance PER/PBR 사용: PER={yf_per:.2f}, PBR={yf_pbr:.2f}")
                per = yf_per
                pbr = yf_pbr

        # 4. ROE (자기자본이익률, %) = (당기순이익 / 자기자본) × 100
        #    버핏의 핵심 지표. net_income·total_equity 모두 기존 수집 데이터로 즉시 계산 가능.
        if raw_data.net_income and raw_data.total_equity and raw_data.total_equity > 0:
            roe = (float(raw_data.net_income) / raw_data.total_equity) * 100

        # 5. FCF (잉여현금흐름) = 영업현금흐름 - CapEx
        #    FCF 수익률(%) = (FCF / 시가총액) × 100
        #    fetcher에서 operating_cash_flow·capital_expenditure 수집 후 계산 가능.
        if (raw_data.operating_cash_flow is not None and raw_data.operating_cash_flow != 0.0
                or raw_data.capital_expenditure is not None and raw_data.capital_expenditure != 0.0):
            fcf = float(raw_data.operating_cash_flow or 0) - float(raw_data.capital_expenditure or 0)
            if raw_data.current_price and raw_data.total_shares and raw_data.total_shares > 0:
                market_cap = raw_data.current_price * raw_data.total_shares
                if market_cap > 0:
                    fcf_yield = (fcf / market_cap) * 100

        # 6. 부채비율(%) = (총부채 / 자기자본) × 100
        #    fetcher에서 total_liabilities 수집 후 계산 가능.
        if (raw_data.total_liabilities is not None and raw_data.total_liabilities > 0
                and raw_data.total_equity and raw_data.total_equity > 0):
            debt_ratio = (float(raw_data.total_liabilities) / float(raw_data.total_equity)) * 100

        # 7. PEG = PER ÷ EPS 연간 성장률(%)
        #    PER > 0 AND 성장률 > 0 일 때만 의미 있음. 그 외 케이스는 모두 None → scorer 0점 처리.
        #    (PER<0 적자, 성장률<0 역성장, 양쪽 모두 음수 → 가짜 양수 방지 포함)
        eps_growth_rate = getattr(raw_data, 'eps_growth_rate', None)
        if per > 0 and eps_growth_rate is not None and eps_growth_rate > 0:
            peg_ratio = per / eps_growth_rate

        # 8. 배당수익률(%) = (1주당 연간 배당금 / 현재주가) × 100
        if raw_data.current_price and raw_data.current_price > 0:
            dividend_yield = (raw_data.dividend_per_share / raw_data.current_price) * 100

        # ── DB 저장 또는 업데이트 ─────────────────────────────────────────────
        processed = self.db.query(ProcessedFinancialData).filter(
            ProcessedFinancialData.ticker == raw_data.ticker,
            ProcessedFinancialData.record_date == raw_data.record_date
        ).first()

        fields = dict(
            eps=eps,
            per=per,
            pbr=pbr,
            roe=roe,
            fcf=fcf,
            fcf_yield=fcf_yield,
            debt_ratio=debt_ratio,
            peg_ratio=peg_ratio,
            dividend_yield=dividend_yield,
        )

        if processed:
            for k, v in fields.items():
                setattr(processed, k, v)
            print(f"가공 데이터 업데이트: {raw_data.ticker} ({raw_data.record_date})")
        else:
            processed = ProcessedFinancialData(
                ticker=raw_data.ticker,
                record_date=raw_data.record_date,
                **fields
            )
            self.db.add(processed)
            print(f"가공 데이터 신규 저장: {raw_data.ticker} ({raw_data.record_date})")

        roe_str       = f"{roe:.1f}%" if roe is not None else "N/A"
        fcf_yield_str = f"{fcf_yield:.1f}%" if fcf_yield is not None else "N/A"
        debt_str      = f"{debt_ratio:.1f}%" if debt_ratio is not None else "N/A"
        peg_str       = f"{peg_ratio:.2f}" if peg_ratio is not None else "N/A"
        print(
            f"   └─ EPS:{eps:,.0f} | PER:{per:.2f} | PBR:{pbr:.2f} | "
            f"ROE:{roe_str} | FCF수익률:{fcf_yield_str} | 부채비율:{debt_str} | PEG:{peg_str} | 배당수익률:{dividend_yield:.2f}%"
        )


if __name__ == "__main__":
    processor = FinancialProcessor()
    processor.process_all()
