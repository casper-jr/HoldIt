"""
scorer_v2.py — Buffett & Lynch 철학 기반 채점 로직 (현재 기본 버전)

v2 점수 체계 (최대 100점):
  카테고리 1 — 수익 창출력 및 내재 가치 (40점)
    PER:       최대 10점
    ROE:       최대 15점  (버핏 핵심 지표)
    FCF:       최대 10점  (FCF 수익률 기준)
    PBR:       최대  5점  (ROE 연계 보너스 포함)

  카테고리 2 — 성장성 및 재무 안전성 (30점)
    경제적 해자: 최대 10점  (정성 평가, 현재 0점 고정)
    PEG:         최대 10점  (린치 핵심 지표)
    부채비율:    최대 10점

  카테고리 3 — 주주환원 정책 (30점)
    배당수익률:  최대 10점
    배당성장:    최대 10점  (연속 인상 연수, v1 대비 비중 상향)
    자사주소각:  최대 10점  (소각 여부만, v1 대비 단순화)
"""

from models import RawFinancialData
from scorer_base import ScorerBase


class ScorerV2(ScorerBase):
    VERSION = 'v2'

    # ── 카테고리 1: 수익 창출력 및 내재 가치 (40점) ──────────────────────────

    def calculate_per_score(self, per):
        """PER 점수 산정 (최대 10점)
        단순 저PER만으로 고배점을 주지 않도록 비중 축소. ROE·FCF와 연계하여 평가.
        """
        if per <= 0: return 0
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
        if fcf_yield is None or fcf_yield <= 0: return 0
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
        """경제적 해자 및 성장성 점수 산정 (최대 10점, 정성 평가)"""
        if economic_moat == 'STRONG':   return 10
        if economic_moat == 'MODERATE': return 5
        return 0  # NONE 또는 미입력

    def calculate_peg_score(self, peg_ratio):
        """PEG(주가수익성장비율) 점수 산정 (최대 10점)
        린치: 'PEG 1.0 이하는 합리적 가치, 0.5 이하는 최고의 기회'.
        """
        if peg_ratio is None or peg_ratio <= 0: return 0
        if peg_ratio < 0.5: return 10
        if peg_ratio < 1.0: return 7
        if peg_ratio < 1.5: return 4
        return 0  # 1.5 이상

    def calculate_debt_ratio_score(self, debt_ratio):
        """부채비율 점수 산정 (최대 10점)
        린치: '부채 없는 회사는 망하지 않는다'.
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
        버핏: 배당이 지속적으로 우상향하는지가 기업 자신감의 증거.
        v1 대비 비중 상향 (5점 → 10점), 7년 이상 구간 추가.
        """
        if inc_years is None: return 0
        if inc_years >= 10: return 10
        if inc_years >= 7:  return 8
        if inc_years >= 5:  return 6
        if inc_years >= 3:  return 3
        return 0

    def calculate_cancel_score(self, share_cancel):
        """자사주 소각 실적 점수 산정 (최대 10점)
        소각 여부만 평가 (v1의 매입+소각 7점 → 소각만 10점으로 단순화).
        """
        return 10 if share_cancel else 0

    # ── 채점 실행 ─────────────────────────────────────────────────────────────

    def score_single(self, processed):
        # 카테고리 1: 수익 창출력 및 내재 가치
        score_per = self.calculate_per_score(processed.per)
        score_roe = self.calculate_roe_score(processed.roe)
        score_fcf = self.calculate_fcf_score(processed.fcf_yield)
        score_pbr = self.calculate_pbr_score(processed.pbr, roe=processed.roe)

        # 카테고리 2: 성장성 및 재무 안전성
        score_moat       = 0  # 정성 평가 — qualitative_assessments 테이블에서 별도 관리
        score_peg        = self.calculate_peg_score(processed.peg_ratio)
        score_debt_ratio = self.calculate_debt_ratio_score(processed.debt_ratio)

        # 카테고리 3: 주주환원 정책
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

        print(
            f"   └─ PER:{score_per} ROE:{score_roe} FCF:{score_fcf} PBR:{score_pbr} | "
            f"해자:{score_moat} PEG:{score_peg} 부채:{score_debt_ratio} | "
            f"배당율:{score_div_yield} 배당성장:{score_div_growth} 소각:{score_cancel}"
        )
