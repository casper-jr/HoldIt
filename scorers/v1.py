"""
scorers/v1.py — main 브랜치 기준 채점 로직 (레거시)

v1 점수 체계 (최대 47점 정량):
  - PER:       최대 20점  (< 5→20, < 8→15, < 10→10, else→5)
  - PBR:       최대  5점  (ROE 연계 없이 순수 PBR만)
  - 배당수익:  최대 10점
  - 배당성장:  최대  5점  (연속 인상 연수, ≥10→5, ≥5→4, ≥3→3)
  - 자사주소각: 최대  7점  (share_cancel 여부)
  - 그 외(ROE/FCF/PEG/부채비율/해자): 0점 (데이터 없음 또는 미구현)

삭제된 컬럼 처리:
  - quarterly_dividend  → 분기배당 점수 0점 고정
  - treasury_share_ratio → 자사주비율 점수 0점 고정
"""

from models import RawFinancialData
from .base import ScorerBase


class ScorerV1(ScorerBase):
    VERSION = 'v1'

    # ── 카테고리 1: 수익 창출력 및 내재 가치 ──────────────────────────────────

    def calculate_per_score(self, per):
        """PER 점수 산정 (최대 20점) — v1 기준"""
        if per <= 0: return 0
        if per < 5:  return 20
        if per < 8:  return 15
        if per < 10: return 10
        return 5  # 10 이상

    def calculate_pbr_score(self, pbr):
        """PBR 점수 산정 (최대 5점) — ROE 연계 없이 단순 PBR 기준"""
        if pbr <= 0: return 0
        if pbr < 0.3: return 5
        if pbr < 0.6: return 4
        if pbr < 1.0: return 3
        return 0  # 1.0 이상

    # ── 카테고리 3: 주주환원 정책 ──────────────────────────────────────────────

    def calculate_div_yield_score(self, div_yield):
        """배당수익률 점수 산정 (최대 10점)"""
        if div_yield is None or div_yield <= 0: return 0
        if div_yield > 7: return 10
        if div_yield > 5: return 7
        if div_yield > 3: return 5
        return 2  # 0% 초과 3% 이하

    def calculate_div_growth_score(self, inc_years):
        """배당 연속 인상 연수 점수 산정 (최대 5점) — v1 기준"""
        if inc_years is None: return 0
        if inc_years >= 10: return 5
        if inc_years >= 5:  return 4
        if inc_years >= 3:  return 3
        return 0

    def calculate_cancel_score(self, share_cancel):
        """자사주 소각/매입 점수 산정 (최대 7점) — v1 기준"""
        return 7 if share_cancel else 0

    # ── 채점 실행 ─────────────────────────────────────────────────────────────

    def score_single(self, processed):
        # 카테고리 1: PER + PBR (ROE/FCF는 v1에서 0점)
        score_per = self.calculate_per_score(processed.per)
        score_roe = 0   # v1에서 미구현
        score_fcf = 0   # v1에서 미구현
        score_pbr = self.calculate_pbr_score(processed.pbr)

        # 카테고리 2: 모두 0점 (v1에서 미구현)
        score_moat      = 0
        score_peg       = 0
        score_debt_ratio = 0

        # 카테고리 3: 배당 관련 + 소각
        score_div_yield  = self.calculate_div_yield_score(processed.dividend_yield)

        raw_data = self.db.query(RawFinancialData).filter(
            RawFinancialData.ticker == processed.ticker,
            RawFinancialData.record_date == processed.record_date
        ).first()

        score_div_growth = 0
        score_cancel     = 0
        if raw_data:
            score_div_growth = self.calculate_div_growth_score(raw_data.dividend_increase_years)
            score_cancel     = self.calculate_cancel_score(raw_data.share_cancel)

        total_score = sum([
            score_per, score_roe, score_fcf, score_pbr,
            score_moat, score_peg, score_debt_ratio,
            score_div_yield, score_div_growth, score_cancel,
        ])
        grade = self.get_grade(total_score)

        from datetime import datetime, timezone
        score_date = datetime.now(timezone.utc).date()

        self._save(processed.ticker, score_date, dict(
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
        ))
