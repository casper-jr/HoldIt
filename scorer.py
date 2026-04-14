from datetime import date, datetime, timezone
from sqlalchemy import func
from database import SessionLocal
from models import RawFinancialData, ProcessedFinancialData, ScoringResult


class StockScorer:
    def __init__(self):
        self.db = SessionLocal()

    # ── 카테고리 1: 수익 창출력 및 내재 가치 (40점) ──────────────────────────

    def calculate_per_score(self, per):
        """PER 점수 산정 (최대 10점)
        단순 저PER만으로 고배점을 주지 않도록 비중 축소. ROE·FCF와 연계하여 평가.
        """
        if per <= 0: return 0  # 적자이거나 데이터 없는 경우
        if per < 5:  return 10
        if per < 8:  return 7
        if per < 10: return 5
        if per < 15: return 2
        return 0  # 15 이상

    def calculate_roe_score(self, roe):
        """ROE(자기자본이익률) 점수 산정 (최대 15점)
        버핏: '지속적인 고ROE는 경제적 해자의 증거'. 가장 중요한 수익성 지표.
        """
        if roe is None or roe <= 0: return 0
        if roe > 20: return 15
        if roe > 15: return 10
        if roe > 10: return 5
        return 0  # 10% 이하

    def calculate_fcf_score(self, fcf_yield):
        """FCF 수익률 점수 산정 (최대 10점)
        장부 이익이 아닌 실제 배당·재투자에 사용 가능한 현금 창출력 평가.
        FCF 수익률 = (영업현금흐름 - CapEx) / 시가총액 × 100
        """
        if fcf_yield is None or fcf_yield <= 0: return 0  # FCF 음수: 현금 순유출
        if fcf_yield > 8: return 10
        if fcf_yield > 5: return 7
        if fcf_yield > 3: return 4
        return 2  # 0% 초과 3% 이하

    def calculate_pbr_score(self, pbr, roe=None):
        """PBR 점수 산정 (최대 5점)
        버핏: 고ROE 기업은 장부가치 이상의 프리미엄이 정당화된다.
        ROE > 20% → PBR 3.0 미만이면 만점, 이상이면 base+3 (최대 5)
        ROE > 15% → PBR 2.0 미만이면 만점, 이상이면 base+2 (최대 5)
        ROE ≤ 15% → 기존 기준 그대로
        """
        if pbr <= 0: return 0

        # 기본 PBR 기준 점수
        if pbr < 0.3:   base = 5
        elif pbr < 0.6: base = 4
        elif pbr < 1.0: base = 3
        else:           base = 0

        if roe is not None:
            if roe > 20:
                return 5 if pbr < 3.0 else min(base + 3, 5)
            if roe > 15:
                return 5 if pbr < 2.0 else min(base + 2, 5)

        return base

    # ── 카테고리 2: 성장성 및 재무 안전성 (30점) ─────────────────────────────

    def calculate_moat_score(self, economic_moat):
        """경제적 해자 및 성장성 점수 산정 (최대 10점, 정성 평가)
        가격 결정력·시장 점유율 유지 능력·브랜드 등을 통합 평가.
        기존 '미래 성장 잠재력'과 '세계적 브랜드' 항목을 통합.
        """
        if economic_moat == 'STRONG':   return 10
        if economic_moat == 'MODERATE': return 5
        return 0  # NONE 또는 미입력

    def calculate_peg_score(self, peg_ratio):
        """PEG(주가수익성장비율) 점수 산정 (최대 10점)
        린치: 'PEG 1.0 이하는 합리적 가치, 0.5 이하는 최고의 기회'.
        저PER만 찾는 Value Trap 오류를 방지하기 위한 핵심 지표.
        EPS 성장률 데이터 없거나 성장률 ≤ 0이면 0점 처리.
        """
        if peg_ratio is None or peg_ratio <= 0: return 0
        if peg_ratio < 0.5: return 10
        if peg_ratio < 1.0: return 7
        if peg_ratio < 1.5: return 4
        return 0  # 1.5 이상 — Lynch 기준 과대평가 영역

    def calculate_debt_ratio_score(self, debt_ratio):
        """부채비율 점수 산정 (최대 10점)
        린치: '부채 없는 회사는 망하지 않는다'. 장기 투자를 위한 재무 안전성 확인.
        부채비율 = 총부채 / 자기자본 × 100
        """
        if debt_ratio is None or debt_ratio < 0: return 0
        if debt_ratio < 30:  return 10
        if debt_ratio < 60:  return 7
        if debt_ratio < 100: return 4
        if debt_ratio < 200: return 2
        return 0  # 200% 이상

    # ── 카테고리 3: 주주환원 정책 (30점) ────────────────────────────────────

    def calculate_div_yield_score(self, div_yield):
        """배당수익률 점수 산정 (최대 10점)"""
        if div_yield is None or div_yield <= 0: return 0
        if div_yield > 7: return 10
        if div_yield > 5: return 7
        if div_yield > 3: return 5
        return 2  # 0% 초과 3% 이하

    def calculate_div_growth_score(self, inc_years):
        """배당 성장성(연속 인상 연수) 점수 산정 (최대 10점)
        버핏: 현재 배당률보다 '배당이 지속적으로 우상향하는지'가 기업 자신감의 증거.
        기존 5점 → 10점으로 비중 상향, 7년 이상 구간 추가.
        """
        if inc_years is None: return 0
        if inc_years >= 10: return 10
        if inc_years >= 7:  return 8
        if inc_years >= 5:  return 6
        if inc_years >= 3:  return 3
        return 0

    def calculate_cancel_score(self, share_cancel):
        """자사주 소각 실적 점수 산정 (최대 10점)
        단순 매입 보유가 아닌, 실제 주식 수를 줄여 주당 가치를 높이는 소각 실적만 평가.
        기존 매입+소각 7점 → 소각 여부만 10점으로 단순화.
        """
        return 10 if share_cancel else 0

    # ── 등급 산정 ─────────────────────────────────────────────────────────────

    def get_grade(self, total_score):
        """총점에 따른 투자 등급 산정"""
        if total_score > 80:  return 'A'
        if total_score >= 70: return 'B'
        if total_score >= 50: return 'C'
        return 'D'

    # ── 채점 실행 ─────────────────────────────────────────────────────────────

    def score_all(self, today_only=True):
        """
        가공된 데이터를 바탕으로 점수를 계산하고 저장합니다.
        today_only=True (기본값): 오늘 처리된 종목만 채점 (updated_at 기준)
        today_only=False      : DB 전체 재채점
        """
        print("🧮 점수 계산을 시작합니다...")

        if today_only:
            today_utc = datetime.now(timezone.utc).date()
            processed_list = self.db.query(ProcessedFinancialData).filter(
                func.date(ProcessedFinancialData.updated_at) == today_utc
            ).all()
            if not processed_list:
                print("오늘 업데이트된 가공 데이터가 없습니다.")
                print("전체 재채점이 필요하면: python3 main.py score --all")
                return
            print(f"오늘 업데이트된 {len(processed_list)}개 종목만 채점합니다.")
        else:
            processed_list = self.db.query(ProcessedFinancialData).all()
            print(f"전체 {len(processed_list)}개 종목을 채점합니다.")

        if not processed_list:
            print("⚠️ 평가할 가공 데이터(ProcessedFinancialData)가 없습니다.")
            return

        for processed in processed_list:
            self.score_single(processed)

        self.db.commit()
        self.db.close()
        print("✅ 모든 점수 계산 및 DB 저장 완료!")

    def score_single(self, processed):
        # ── 카테고리 1: 수익 창출력 및 내재 가치 ────────────────────────────
        score_per = self.calculate_per_score(processed.per)
        score_roe = self.calculate_roe_score(processed.roe)
        score_fcf = self.calculate_fcf_score(processed.fcf_yield)
        score_pbr = self.calculate_pbr_score(processed.pbr, roe=processed.roe)

        # ── 카테고리 2: 성장성 및 재무 안전성 ───────────────────────────────
        score_moat = 0  # 정성 평가 - 현재 0점 처리 (qualitative_assessments 테이블에서 별도 관리)
        score_peg  = self.calculate_peg_score(processed.peg_ratio)
        score_debt_ratio = self.calculate_debt_ratio_score(processed.debt_ratio)

        # ── 카테고리 3: 주주환원 정책 ────────────────────────────────────────
        score_div_yield  = self.calculate_div_yield_score(processed.dividend_yield)

        # 배당 성장성·소각 실적은 raw 데이터에서 가져옴
        raw_data = self.db.query(RawFinancialData).filter(
            RawFinancialData.ticker == processed.ticker,
            RawFinancialData.record_date == processed.record_date
        ).first()

        score_div_growth = 0
        score_cancel     = 0
        if raw_data:
            score_div_growth = self.calculate_div_growth_score(raw_data.dividend_increase_years)
            score_cancel     = self.calculate_cancel_score(raw_data.share_cancel)

        # ── 총점 및 등급 ──────────────────────────────────────────────────────
        total_score = sum([
            score_per, score_roe, score_fcf, score_pbr,       # 카테고리 1 (40점)
            score_moat, score_peg, score_debt_ratio,           # 카테고리 2 (30점)
            score_div_yield, score_div_growth, score_cancel,   # 카테고리 3 (30점)
        ])
        grade = self.get_grade(total_score)

        # ── DB 저장 또는 업데이트 ─────────────────────────────────────────────
        today = date.today()
        scoring_result = self.db.query(ScoringResult).filter(
            ScoringResult.ticker == processed.ticker,
            ScoringResult.score_date == today
        ).first()

        fields = dict(
            score_per=score_per,
            score_roe=score_roe,
            score_fcf=score_fcf,
            score_pbr=score_pbr,
            score_moat=score_moat,
            score_peg=score_peg,
            score_debt_ratio=score_debt_ratio,
            score_div_yield=score_div_yield,
            score_div_growth=score_div_growth,
            score_cancel=score_cancel,
            total_score=total_score,
            grade=grade,
        )

        if scoring_result:
            for k, v in fields.items():
                setattr(scoring_result, k, v)
            print(f"🔄 점수 업데이트: {processed.ticker} (총점: {total_score}점, 등급: {grade})")
        else:
            scoring_result = ScoringResult(ticker=processed.ticker, score_date=today, **fields)
            self.db.add(scoring_result)
            print(f"🏆 새 점수 저장: {processed.ticker} (총점: {total_score}점, 등급: {grade})")

        print(
            f"   └─ PER:{score_per} ROE:{score_roe} FCF:{score_fcf} PBR:{score_pbr} | "
            f"해자:{score_moat} PEG:{score_peg} 부채:{score_debt_ratio} | "
            f"배당율:{score_div_yield} 배당성장:{score_div_growth} 소각:{score_cancel}"
        )


if __name__ == "__main__":
    scorer = StockScorer()
    scorer.score_all()
