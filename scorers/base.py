"""
scorers/base.py — 모든 scorer 버전의 공통 인프라

공통 로직:
  - score_all()  : DB 전체/오늘치 채점 실행
  - get_grade()  : 등급 산정 (A/B/C/D)
  - _save()      : ScoringResult DB 저장/업데이트

각 버전은 ScorerBase를 상속하고 VERSION과 score_single()만 구현하면 됩니다.
"""

from datetime import date, datetime, timezone
from sqlalchemy import func
from database import SessionLocal
from models import RawFinancialData, ProcessedFinancialData, ScoringResult


class ScorerBase:
    VERSION = 'base'  # 서브클래스에서 반드시 오버라이드

    def __init__(self):
        self.db = SessionLocal()

    # ── 등급 산정 (모든 버전 공통) ───────────────────────────────────────────

    def get_grade(self, total_score: int) -> str:
        if total_score > 80:  return 'A'
        if total_score >= 70: return 'B'
        if total_score >= 50: return 'C'
        return 'D'

    # ── 채점 실행 ─────────────────────────────────────────────────────────────

    def score_all(self, today_only: bool = True):
        print(f"🧮 점수 계산을 시작합니다... [scorer {self.VERSION}]")

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
        print(f"✅ 모든 점수 계산 및 DB 저장 완료! [scorer {self.VERSION}]")

    # ── DB 저장/업데이트 (공통) ───────────────────────────────────────────────

    def _save(self, ticker: str, score_date: date, fields: dict):
        """
        ScoringResult를 저장하거나 (같은 날짜 + 같은 버전 레코드가 있으면) 업데이트합니다.
        fields에는 score_* 컬럼값과 total_score, grade를 담습니다.
        scorer_version은 자동으로 주입됩니다.
        """
        fields['scorer_version'] = self.VERSION

        existing = self.db.query(ScoringResult).filter(
            ScoringResult.ticker == ticker,
            ScoringResult.score_date == score_date,
            ScoringResult.scorer_version == self.VERSION,
        ).first()

        total = fields.get('total_score', 0)
        grade = fields.get('grade', 'D')

        if existing:
            for k, v in fields.items():
                setattr(existing, k, v)
            print(f"🔄 점수 업데이트: {ticker} [{self.VERSION}] (총점: {total}점, 등급: {grade})")
        else:
            result = ScoringResult(ticker=ticker, score_date=score_date, **fields)
            self.db.add(result)
            print(f"🏆 새 점수 저장: {ticker} [{self.VERSION}] (총점: {total}점, 등급: {grade})")

    # ── 서브클래스에서 구현 ───────────────────────────────────────────────────

    def score_single(self, processed: ProcessedFinancialData):
        raise NotImplementedError(f"{self.__class__.__name__}.score_single()을 구현해야 합니다.")
