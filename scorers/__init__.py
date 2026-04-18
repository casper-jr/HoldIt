"""
scorers/ — scorer 패키지

사용법:
    from scorers import get_scorer
    scorer = get_scorer('v2')   # 기본값
    scorer.score_all()

버전 목록:
    v1 — main 브랜치 레거시 (PER/PBR/배당 중심, 최대 47점)
    v2 — Buffett & Lynch 기반 현행 기준 (ROE/FCF/PEG/부채비율 추가, 최대 100점)
"""

from .v1 import ScorerV1
from .v2 import ScorerV2

_SCORERS = {
    'v1': ScorerV1,
    'v2': ScorerV2,
}

DEFAULT_VERSION = 'v2'


def get_scorer(version: str = DEFAULT_VERSION):
    """
    버전 문자열에 맞는 Scorer 인스턴스를 반환합니다.
    지원하지 않는 버전이면 ValueError를 발생시킵니다.
    """
    key = version.lower()
    cls = _SCORERS.get(key)
    if cls is None:
        supported = ', '.join(_SCORERS.keys())
        raise ValueError(f"지원하지 않는 scorer 버전: '{version}'. 지원 버전: {supported}")
    return cls()


# 하위 호환성: 기존 코드에서 StockScorer를 직접 import하는 경우를 위해 v2를 alias로 제공
StockScorer = ScorerV2
