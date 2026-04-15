"""
backtest.py — 연도별 점수 백테스팅 스크립트

DB를 건드리지 않고 과거 시점 기준으로 점수를 재현합니다.
yfinance(US) 또는 DART API(KR)에서 연도별 재무 데이터를 수집하여
현재 scorer 로직을 그대로 적용한 뒤 연도별 비교 테이블을 출력합니다.

사용법:
  python3 backtest.py AAPL 2022 2023 2024
  python3 backtest.py 005930 2022 2023 2024
"""

import sys
import requests
import zipfile
import io
import xml.etree.ElementTree as ET
import unicodedata
from datetime import date, timedelta

import yfinance as yf
import pandas as pd

from config import DART_API_KEY
from scorer import get_scorer, StockScorer

# ── 유틸 ─────────────────────────────────────────────────────────────────────

def get_display_width(s):
    width = 0
    for char in str(s):
        status = unicodedata.east_asian_width(char)
        width += 2 if status in ('W', 'F') else 1
    return width

def pad_string(s, total_width):
    s = str(s)
    padding = total_width - get_display_width(s)
    return s + " " * max(padding, 0)

def fmt(val, suffix='', decimals=1, none_str='-'):
    """None이면 none_str, 아니면 소수점 포맷"""
    if val is None:
        return none_str
    return f"{val:.{decimals}f}{suffix}"


# ── US 백테스터 ───────────────────────────────────────────────────────────────

class USBacktester:
    """
    yfinance의 연간 재무제표(income_stmt, balance_sheet, cashflow)를 사용.
    연도별 컬럼이 포함되어 있어 최대 4개년 히스토리 백테스팅 가능.
    """

    def __init__(self, ticker: str):
        self.ticker = ticker.upper()
        self._stock = yf.Ticker(self.ticker)
        self._income    = None
        self._balance   = None
        self._cashflow  = None
        self._price_hist = None
        self._name = None
        self._load()

    def _load(self):
        print(f"  yfinance 데이터 로드 중: {self.ticker}...")
        try:
            self._income   = self._stock.income_stmt        # 연간
            self._balance  = self._stock.balance_sheet      # 연간
            self._cashflow = self._stock.cashflow           # 연간
            self._price_hist = self._stock.history(period='max')
            self._name = self._stock.info.get('shortName', self.ticker)
        except Exception as e:
            print(f"  ⚠️ 데이터 로드 실패: {e}")

    def _col_for_year(self, df, year: int):
        """DataFrame에서 연도(year)에 해당하는 컬럼을 찾아 반환. 없으면 None."""
        if df is None or df.empty:
            return None
        for col in df.columns:
            try:
                if pd.Timestamp(col).year == year:
                    return col
            except Exception:
                pass
        return None

    def _get_val(self, df, row_key, year: int, fallback_keys=None):
        col = self._col_for_year(df, year)
        if col is None:
            return None
        keys = [row_key] + (fallback_keys or [])
        for k in keys:
            if k in df.index:
                v = df.loc[k, col]
                if pd.notna(v):
                    return float(v)
        return None

    def _price_at_year_end(self, year: int):
        """해당 연도 마지막 거래일의 종가 반환."""
        if self._price_hist is None or self._price_hist.empty:
            return None
        # timezone-aware 인덱스를 tz-naive로 정규화 후 비교
        target = pd.Timestamp(f"{year}-12-31")
        idx = self._price_hist.index
        if idx.tz is not None:
            idx = idx.tz_localize(None)
        hist = self._price_hist.copy()
        hist.index = idx
        hist = hist[hist.index <= target]
        if hist.empty:
            return None
        return float(hist['Close'].iloc[-1])

    def _eps_growth_for_year(self, year: int):
        """
        해당 연도 기준 EPS CAGR 계산.
        income_stmt의 Diluted EPS / Basic EPS 컬럼에서 (year / year-n) CAGR.
        """
        if self._income is None or self._income.empty:
            return None
        for eps_key in ('Diluted EPS', 'Basic EPS'):
            if eps_key not in self._income.index:
                continue
            eps_series = self._income.loc[eps_key].dropna()
            # 컬럼 = Timestamp, 최근→과거 순
            year_cols = {pd.Timestamp(c).year: float(v)
                         for c, v in eps_series.items()
                         if pd.notna(v)}
            if year not in year_cols:
                continue
            eps_end = year_cols[year]
            if eps_end <= 0:
                return None
            # 최대 3년 CAGR
            for n in (3, 2, 1):
                prior = year - n
                if prior in year_cols and year_cols[prior] > 0:
                    return ((eps_end / year_cols[prior]) ** (1 / n) - 1) * 100
        return None

    def _share_cancel_for_year(self, year: int):
        """해당 연도 자사주 매입(≈소각) 여부. cashflow의 Repurchase 항목."""
        val = self._get_val(self._cashflow, 'Repurchase Of Capital Stock', year,
                            fallback_keys=['Common Stock Repurchased'])
        return val is not None and val < 0

    def _div_increase_years_as_of(self, year: int):
        """해당 연도 말 기준 배당 연속 인상 연수."""
        try:
            dividends = self._stock.dividends
            if dividends.empty:
                return 0
            yearly = dividends.groupby(dividends.index.year).sum()
            y, streak = year, 0
            while True:
                prev = y - 1
                if prev not in yearly.index or y not in yearly.index:
                    break
                if float(yearly[y]) > float(yearly[prev]):
                    streak += 1
                    y = prev
                else:
                    break
            return streak
        except Exception:
            return 0

    def fetch_year(self, year: int) -> dict | None:
        """
        특정 연도 말 기준의 재무 데이터를 딕셔너리로 반환.
        scorer.StockScorer의 각 calculate_* 메서드에 바로 넣을 수 있는 형태.
        """
        price = self._price_at_year_end(year)
        if price is None:
            print(f"  ⚠️ {self.ticker} {year}년 말 주가 데이터 없음 — 스킵")
            return None

        net_income   = self._get_val(self._income, 'Net Income', year)
        total_equity = self._get_val(self._balance, 'Stockholders Equity', year)
        total_liab   = self._get_val(self._balance, 'Total Liabilities Net Minority Interest', year)
        ocf          = self._get_val(self._cashflow, 'Operating Cash Flow', year,
                                     ['Cash Flow From Continuing Operating Activities'])
        capex        = self._get_val(self._cashflow, 'Capital Expenditure', year,
                                     ['Purchase Of Property Plant And Equipment'])
        if capex is not None:
            capex = abs(capex)

        # 주식수: Diluted Average Shares 우선
        total_shares = self._get_val(self._income, 'Diluted Average Shares', year,
                                     ['Basic Average Shares'])
        if not total_shares:
            total_shares = float(self._stock.info.get('sharesOutstanding', 0))

        # 배당 (해당 연도 합산)
        try:
            dividends = self._stock.dividends
            yearly = dividends.groupby(dividends.index.year).sum() if not dividends.empty else pd.Series()
            div_per_share = float(yearly[year]) if year in yearly.index else 0.0
        except Exception:
            div_per_share = 0.0

        # 파생 지표 계산
        eps = (net_income / total_shares) if (net_income and total_shares) else None
        per = (price / eps) if (eps and eps > 0) else None

        bps = (total_equity / total_shares) if (total_equity and total_shares) else None
        pbr = (price / bps) if (bps and bps > 0) else None

        roe = ((net_income / total_equity) * 100) if (net_income and total_equity and total_equity > 0) else None

        market_cap = price * total_shares if total_shares else None
        fcf = (ocf - capex) if (ocf is not None and capex is not None) else None
        fcf_yield = ((fcf / market_cap) * 100) if (fcf and market_cap) else None

        debt_ratio = ((total_liab / total_equity) * 100) if (total_liab and total_equity and total_equity > 0) else None

        div_yield = ((div_per_share / price) * 100) if (price and div_per_share) else 0.0

        eps_growth = self._eps_growth_for_year(year)
        peg = (per / eps_growth) if (per and per > 0 and eps_growth and eps_growth > 0) else None

        div_inc_years = self._div_increase_years_as_of(year)
        share_cancel  = self._share_cancel_for_year(year)

        return {
            'year': year,
            'price': price,
            'net_income': net_income,
            'total_equity': total_equity,
            'total_liabilities': total_liab,
            'ocf': ocf,
            'capex': capex,
            'total_shares': total_shares,
            'div_per_share': div_per_share,
            # 파생
            'eps': eps,
            'per': per,
            'pbr': pbr,
            'roe': roe,
            'fcf': fcf,
            'fcf_yield': fcf_yield,
            'debt_ratio': debt_ratio,
            'div_yield': div_yield,
            'peg': peg,
            'eps_growth_rate': eps_growth,
            'div_increase_years': div_inc_years,
            'share_cancel': share_cancel,
        }

    @property
    def name(self):
        return self._name or self.ticker


# ── KR 백테스터 ───────────────────────────────────────────────────────────────

class KRBacktester:
    """
    DART API(bsns_year 파라미터)와 yfinance 과거 주가로 연도별 재무 데이터 재현.
    """

    BASE_URL = "https://opendart.fss.or.kr/api"

    def __init__(self, ticker: str):
        self.ticker = ticker
        self.api_key = DART_API_KEY
        self.corp_code = None
        self.corp_name = None
        self._price_hist = None
        self._load_corp_code()
        self._load_price_hist()

    def _load_corp_code(self):
        print("  DART 기업 고유번호 로드 중...")
        try:
            url = f"{self.BASE_URL}/corpCode.xml"
            resp = requests.get(url, params={'crtfc_key': self.api_key}, timeout=30)
            with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
                root = ET.fromstring(z.read('CORPCODE.xml'))
            for item in root.findall('list'):
                code = item.find('stock_code').text
                if code and code.strip() == self.ticker:
                    self.corp_code = item.find('corp_code').text
                    self.corp_name = item.find('corp_name').text
                    print(f"  기업: {self.corp_name} ({self.ticker})")
                    return
            print(f"  ⚠️ DART에서 {self.ticker}를 찾을 수 없음")
        except Exception as e:
            print(f"  ⚠️ corp_code 로드 실패: {e}")

    def _load_price_hist(self):
        for suffix in ('.KS', '.KQ'):
            try:
                stock = yf.Ticker(f"{self.ticker}{suffix}")
                hist = stock.history(period='max')
                if not hist.empty:
                    self._price_hist = hist
                    if self.corp_name is None:
                        self.corp_name = stock.info.get('shortName', self.ticker)
                    break
            except Exception:
                continue

    def _price_at_year_end(self, year: int):
        if self._price_hist is None or self._price_hist.empty:
            return None
        target = pd.Timestamp(f"{year}-12-31")
        idx = self._price_hist.index
        if idx.tz is not None:
            idx = idx.tz_localize(None)
        hist = self._price_hist.copy()
        hist.index = idx
        hist = hist[hist.index <= target]
        if hist.empty:
            return None
        return float(hist['Close'].iloc[-1])

    def _dart_financial(self, year: int):
        """DART fnlttSinglAcnt.json으로 연간 재무 데이터 조회."""
        corp = self.corp_code
        if not corp:
            return {}

        result = {
            'net_income': None, 'net_income_prev': None, 'net_income_prev2': None,
            'total_equity': None, 'total_liabilities': None,
            'operating_cash_flow': None,
        }

        # 사업보고서(11011): 손익계산서 + 현금흐름표
        url = f"{self.BASE_URL}/fnlttSinglAcnt.json"
        params = {'crtfc_key': self.api_key, 'corp_code': corp,
                  'bsns_year': str(year), 'reprt_code': '11011'}
        try:
            resp = requests.get(url, params=params, timeout=30)
            data = resp.json()
            if data.get('status') == '000':
                for item in data.get('list', []):
                    nm = item.get('account_nm', '').replace(' ', '')
                    fs = item.get('fs_div', '')

                    def _amt(key):
                        raw = (item.get(key) or '').replace(',', '')
                        try:
                            return float(raw)
                        except ValueError:
                            return None

                    thstrm = _amt('thstrm_amount')
                    frmtrm = _amt('frmtrm_amount')
                    lwfr   = _amt('lwfr_amount')

                    if ('당기순이익' in nm or '당기순손실' in nm):
                        if fs == 'CFS' or (fs == 'OFS' and result['net_income'] is None):
                            result['net_income'] = thstrm
                            if result['net_income_prev'] is None:
                                result['net_income_prev'] = frmtrm
                            if result['net_income_prev2'] is None:
                                result['net_income_prev2'] = lwfr

                    if '자본총계' in nm:
                        if fs == 'CFS' or (fs == 'OFS' and result['total_equity'] is None):
                            result['total_equity'] = thstrm

                    if '부채총계' in nm:
                        if fs == 'CFS' or (fs == 'OFS' and result['total_liabilities'] is None):
                            result['total_liabilities'] = thstrm

                    if '영업활동' in nm and '현금흐름' in nm:
                        if fs == 'CFS' or (fs == 'OFS' and result['operating_cash_flow'] is None):
                            result['operating_cash_flow'] = thstrm
        except Exception as e:
            print(f"  ⚠️ DART 재무제표 조회 실패 ({year}): {e}")

        return result

    def _dart_capex(self, year: int):
        """DART fnlttSinglAcntAll.json에서 CapEx 추출."""
        if not self.corp_code:
            return None
        url = f"{self.BASE_URL}/fnlttSinglAcntAll.json"
        for fs_div in ('CFS', 'OFS'):
            params = {'crtfc_key': self.api_key, 'corp_code': self.corp_code,
                      'bsns_year': str(year), 'reprt_code': '11011', 'fs_div': fs_div}
            try:
                resp = requests.get(url, params=params, timeout=30)
                data = resp.json()
                if data.get('status') != '000':
                    continue
                for item in data.get('list', []):
                    nm = item.get('account_nm', '').replace(' ', '')
                    if '유형자산' in nm and ('취득' in nm or '구입' in nm):
                        raw = (item.get('thstrm_amount') or '').replace(',', '')
                        try:
                            return abs(float(raw))
                        except ValueError:
                            continue
            except Exception:
                continue
        return None

    def _dart_shares(self, year: int):
        """DART 주식총수현황에서 발행주식수·자사주 수 조회."""
        if not self.corp_code:
            return None, None
        for reprt_code in ('11011', '11012', '11014', '11013'):
            url = f"{self.BASE_URL}/stockTotqySttus.json"
            params = {'crtfc_key': self.api_key, 'corp_code': self.corp_code,
                      'bsns_year': str(year), 'reprt_code': reprt_code}
            try:
                resp = requests.get(url, params=params, timeout=30)
                data = resp.json()
                if data.get('status') != '000':
                    continue
                from fetcher import _parse_stock_totqy_list
                parsed = _parse_stock_totqy_list(data.get('list', []))
                if parsed:
                    return parsed[0], parsed[1]
            except Exception:
                continue
        return None, None

    def _dart_dividend(self, year: int):
        """DART alotMatter.json으로 해당 연도 주당 배당금 + 연속 인상 연수."""
        if not self.corp_code:
            return 0.0, 0
        yearly_divs = {}
        # 해당 연도 포함 3개 연도 구간 4번 조회
        base_year = year
        for by in [base_year, base_year - 3, base_year - 6, base_year - 9]:
            url = f"{self.BASE_URL}/alotMatter.json"
            params = {'crtfc_key': self.api_key, 'corp_code': self.corp_code,
                      'bsns_year': str(by), 'reprt_code': '11011'}
            try:
                resp = requests.get(url, params=params, timeout=30)
                data = resp.json()
                if data.get('status') != '000':
                    continue
                for item in data.get('list', []):
                    se = (item.get('se') or '').strip()
                    if '주당' not in se or '현금배당금' not in se or '우선주' in se:
                        continue
                    for period_key, offset in [('thstrm', 0), ('frmtrm', -1), ('lwfr', -2)]:
                        raw = (item.get(period_key) or '').strip().replace(',', '')
                        if raw in ('', '-', '—'):
                            continue
                        try:
                            y2 = by + offset
                            if y2 not in yearly_divs:
                                yearly_divs[y2] = float(raw)
                        except ValueError:
                            pass
            except Exception:
                continue

        div_per_share = yearly_divs.get(year, 0.0)

        # 연속 인상 연수 (해당 연도 말 기준)
        streak = 0
        y = year
        while True:
            prev = y - 1
            if prev not in yearly_divs or y not in yearly_divs:
                break
            if yearly_divs[y] > yearly_divs[prev]:
                streak += 1
                y = prev
            else:
                break

        return div_per_share, streak

    def _check_cancel_year(self, year: int):
        """
        DART 공시 목록에서 해당 연도 내 자사주 소각 공시가 있는지 확인.
        (year-01-01 ~ year-12-31)
        """
        if not self.corp_code:
            return False
        bgn = f"{year}0101"
        end = f"{year}1231"
        url = f"{self.BASE_URL}/list.json"
        params = {'crtfc_key': self.api_key, 'corp_code': self.corp_code,
                  'bgn_de': bgn, 'end_de': end, 'page_count': 100}
        try:
            resp = requests.get(url, params=params, timeout=30)
            data = resp.json()
            if data.get('status') == '000':
                for r in data.get('list', []):
                    if '소각' in r.get('report_nm', ''):
                        return True
        except Exception:
            pass
        return False

    def fetch_year(self, year: int) -> dict | None:
        price = self._price_at_year_end(year)
        if price is None:
            print(f"  ⚠️ {self.ticker} {year}년 말 주가 데이터 없음 — 스킵")
            return None

        fin = self._dart_financial(year)
        if not fin.get('net_income') and not fin.get('total_equity'):
            print(f"  ⚠️ {self.ticker} {year}년 DART 재무 데이터 없음 — 스킵")
            return None

        net_income       = fin.get('net_income')
        total_equity     = fin.get('total_equity')
        total_liab       = fin.get('total_liabilities')
        ocf              = fin.get('operating_cash_flow')
        capex            = self._dart_capex(year)
        total_issued, treasury_shares = self._dart_shares(year)

        # 유통주식수 = 발행총수 - 자사주
        if total_issued and total_issued > 0:
            total_shares = total_issued - (treasury_shares or 0)
        else:
            total_shares = None

        div_per_share, div_inc_years = self._dart_dividend(year)
        share_cancel = self._check_cancel_year(year)

        # 파생 지표 계산
        eps = (net_income / total_shares) if (net_income and total_shares) else None
        per = (price / eps) if (eps and eps > 0) else None

        bps = (total_equity / total_shares) if (total_equity and total_shares) else None
        pbr = (price / bps) if (bps and bps > 0) else None

        roe = ((net_income / total_equity) * 100) if (net_income and total_equity and total_equity > 0) else None

        market_cap = price * total_shares if total_shares else None
        fcf = (ocf - capex) if (ocf is not None and capex is not None) else None
        fcf_yield = ((fcf / market_cap) * 100) if (fcf and market_cap) else None

        debt_ratio = ((total_liab / total_equity) * 100) if (total_liab and total_equity and total_equity > 0) else None

        div_yield = ((div_per_share / price) * 100) if (price and div_per_share) else 0.0

        # EPS 성장률
        ni = net_income
        ni_prev  = fin.get('net_income_prev')
        ni_prev2 = fin.get('net_income_prev2')
        eps_growth = None
        if ni and ni > 0:
            if ni_prev2 and ni_prev2 > 0:
                eps_growth = ((ni / ni_prev2) ** 0.5 - 1) * 100
            elif ni_prev and ni_prev > 0:
                eps_growth = (ni / ni_prev - 1) * 100

        peg = (per / eps_growth) if (per and per > 0 and eps_growth and eps_growth > 0) else None

        return {
            'year': year,
            'price': price,
            'net_income': net_income,
            'total_equity': total_equity,
            'total_liabilities': total_liab,
            'ocf': ocf,
            'capex': capex,
            'total_shares': total_shares,
            'div_per_share': div_per_share,
            'eps': eps,
            'per': per,
            'pbr': pbr,
            'roe': roe,
            'fcf': fcf,
            'fcf_yield': fcf_yield,
            'debt_ratio': debt_ratio,
            'div_yield': div_yield,
            'peg': peg,
            'eps_growth_rate': eps_growth,
            'div_increase_years': div_inc_years,
            'share_cancel': share_cancel,
        }

    @property
    def name(self):
        return self.corp_name or self.ticker


# ── 점수 계산 ─────────────────────────────────────────────────────────────────

def calc_scores(d: dict, scorer_version: str = 'v2') -> dict:
    """수집된 연도별 데이터에 scorer 로직을 적용해 점수 딕셔너리를 반환.

    scorer_version: 'v1' 또는 'v2' (기본값 'v2')
    DB 연결 없이 calculate_* 메서드만 직접 호출.
    """
    cls = get_scorer(scorer_version).__class__
    scorer = cls.__new__(cls)  # DB 연결 없이 메서드만 사용

    s_per   = scorer.calculate_per_score(d['per'] or 0)
    s_roe   = scorer.calculate_roe_score(d['roe']) if hasattr(scorer, 'calculate_roe_score') else 0
    s_fcf   = scorer.calculate_fcf_score(d['fcf_yield']) if hasattr(scorer, 'calculate_fcf_score') else 0
    # PBR: v1은 roe 파라미터 없음, v2는 있음
    if hasattr(scorer, 'calculate_pbr_score'):
        import inspect
        pbr_sig = inspect.signature(scorer.calculate_pbr_score)
        if 'roe' in pbr_sig.parameters:
            s_pbr = scorer.calculate_pbr_score(d['pbr'] or 0, roe=d['roe'])
        else:
            s_pbr = scorer.calculate_pbr_score(d['pbr'] or 0)
    else:
        s_pbr = 0
    s_moat  = 0  # 정성
    s_peg   = scorer.calculate_peg_score(d['peg']) if hasattr(scorer, 'calculate_peg_score') else 0
    s_debt  = scorer.calculate_debt_ratio_score(d['debt_ratio']) if hasattr(scorer, 'calculate_debt_ratio_score') else 0
    s_divy  = scorer.calculate_div_yield_score(d['div_yield'])
    s_divg  = scorer.calculate_div_growth_score(d['div_increase_years'])
    s_cncl  = scorer.calculate_cancel_score(d['share_cancel'])
    total   = s_per + s_roe + s_fcf + s_pbr + s_moat + s_peg + s_debt + s_divy + s_divg + s_cncl
    grade   = scorer.get_grade(total)

    return {
        's_per': s_per, 's_roe': s_roe, 's_fcf': s_fcf, 's_pbr': s_pbr,
        's_moat': s_moat, 's_peg': s_peg, 's_debt': s_debt,
        's_divy': s_divy, 's_divg': s_divg, 's_cncl': s_cncl,
        'total': total, 'grade': grade,
    }


# ── 테이블 출력 ───────────────────────────────────────────────────────────────

def print_table(name: str, ticker: str, rows: list[tuple[dict, dict]]):
    """
    rows: [(data_dict, score_dict), ...]  — 연도 순서대로
    """
    years = [d['year'] for d, _ in rows]
    is_kr = ticker.isdigit()
    price_fmt = lambda v: f"{v:,.0f}원" if is_kr else f"${v:,.2f}"

    # 컬럼 너비 설정
    YW = 10  # 연도별 값 컬럼 너비
    LW = 20  # 레이블 컬럼 너비

    year_header = "  ".join(pad_string(y, YW) for y in years)
    sep_line = "─" * (LW + 4 + YW * len(years) + 2 * (len(years) - 1))

    print(f"\n{'=' * len(sep_line)}")
    print(f"  [{name} ({ticker})] 연도별 점수 백테스팅")
    print(f"{'=' * len(sep_line)}")
    print(f"  {pad_string('', LW)}    {year_header}")
    print(sep_line)

    def row(label, values):
        vals = "  ".join(pad_string(v, YW) for v in values)
        print(f"  {pad_string(label, LW)}  | {vals}")

    def section(title):
        print(f"\n  {title}")
        print("  " + "─" * (len(sep_line) - 2))

    section("📊 시장 데이터")
    row("주가",             [price_fmt(d['price']) if d['price'] else '-' for d, _ in rows])
    row("EPS 성장률(%)",    [fmt(d['eps_growth_rate'], '%') for d, _ in rows])

    section("카테고리 1 — 수익 창출력 및 내재 가치 (40점)")
    row("PER",              [fmt(d['per']) for d, _ in rows])
    row("  → 점수(/10)",   [str(s['s_per']) for _, s in rows])
    row("ROE(%)",           [fmt(d['roe'], '%') for d, _ in rows])
    row("  → 점수(/15)",   [str(s['s_roe']) for _, s in rows])
    row("FCF 수익률(%)",    [fmt(d['fcf_yield'], '%') for d, _ in rows])
    row("  → 점수(/10)",   [str(s['s_fcf']) for _, s in rows])
    row("PBR",              [fmt(d['pbr']) for d, _ in rows])
    row("  → 점수(/5)",    [str(s['s_pbr']) for _, s in rows])

    section("카테고리 2 — 성장성 및 재무 안전성 (30점)")
    row("경제적 해자",      ["-(정성)" for _ in rows])
    row("  → 점수(/10)",   [str(s['s_moat']) for _, s in rows])
    row("PEG",              [fmt(d['peg']) for d, _ in rows])
    row("  → 점수(/10)",   [str(s['s_peg']) for _, s in rows])
    row("부채비율(%)",      [fmt(d['debt_ratio'], '%', 0) for d, _ in rows])
    row("  → 점수(/10)",   [str(s['s_debt']) for _, s in rows])

    section("카테고리 3 — 주주환원 정책 (30점)")
    row("배당수익률(%)",    [fmt(d['div_yield'], '%') for d, _ in rows])
    row("  → 점수(/10)",   [str(s['s_divy']) for _, s in rows])
    row("배당 연속 인상(년)",[str(d['div_increase_years']) for d, _ in rows])
    row("  → 점수(/10)",   [str(s['s_divg']) for _, s in rows])
    row("자사주 소각",      ["O" if d['share_cancel'] else "X" for d, _ in rows])
    row("  → 점수(/10)",   [str(s['s_cncl']) for _, s in rows])

    print(f"\n  {sep_line[2:]}")
    row("총점 (정량, /90)", [str(s['total']) for _, s in rows])
    row("등급",             [s['grade'] for _, s in rows])
    row("  + 해자 만점 시", [str(s['total'] + 10) for _, s in rows])
    print(f"  {'=' * (len(sep_line) - 2)}\n")


# ── DB에서 현재 시점 데이터 조회 ─────────────────────────────────────────────

def fetch_current_from_db(ticker: str, scorer_version: str = 'v2') -> tuple[dict, dict] | None:
    """
    DB에 저장된 가장 최근 Raw/Processed 데이터를 읽고,
    지정된 scorer_version으로 점수를 재계산하여 (data, scores) 튜플로 반환.
    DB 점수를 그대로 쓰지 않고 재계산함으로써 --scorer 플래그와 일관성을 보장.
    데이터가 없으면 None 반환.
    """
    from database import SessionLocal
    from models import RawFinancialData, ProcessedFinancialData, ScoringResult

    db = SessionLocal()
    try:
        raw = db.query(RawFinancialData).filter(
            RawFinancialData.ticker == ticker
        ).order_by(RawFinancialData.record_date.desc()).first()

        processed = db.query(ProcessedFinancialData).filter(
            ProcessedFinancialData.ticker == ticker
        ).order_by(ProcessedFinancialData.record_date.desc()).first()

        # 헤더 연도 표시용으로만 사용 (점수는 재계산)
        score_ref = db.query(ScoringResult).filter(
            ScoringResult.ticker == ticker
        ).order_by(ScoringResult.score_date.desc()).first()

        if not raw or not processed:
            return None

        ref_year = score_ref.score_date.year if score_ref else date.today().year
        ref_date = score_ref.score_date if score_ref else date.today()

        data = {
            'year': f"현재({ref_year})",  # 테이블 헤더용 레이블
            'price': raw.current_price,
            'net_income': raw.net_income,
            'total_equity': raw.total_equity,
            'total_liabilities': raw.total_liabilities,
            'ocf': raw.operating_cash_flow,
            'capex': raw.capital_expenditure,
            'total_shares': raw.total_shares,
            'div_per_share': raw.dividend_per_share,
            'eps': processed.eps,
            'per': processed.per,
            'pbr': processed.pbr,
            'roe': processed.roe,
            'fcf': processed.fcf,
            'fcf_yield': processed.fcf_yield,
            'debt_ratio': processed.debt_ratio,
            'div_yield': processed.dividend_yield,
            'peg': processed.peg_ratio,
            'eps_growth_rate': getattr(raw, 'eps_growth_rate', None),
            'div_increase_years': raw.dividend_increase_years,
            'share_cancel': raw.share_cancel,
        }

        # --scorer 플래그와 동일한 버전으로 점수 재계산 (DB 저장 점수 사용 안 함)
        scores = calc_scores(data, scorer_version=scorer_version)

        print(f"  DB 현재 데이터 로드 완료 (기준일: {raw.record_date}, 평가일: {ref_date}, scorer: {scorer_version})")
        return data, scores

    finally:
        db.close()


# ── 진입점 ────────────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) < 3:
        print("사용법:")
        print("  python3 backtest.py <종목코드> <연도수> [--scorer v1|v2]")
        print("  python3 backtest.py <종목코드> <연도1> [연도2] [--scorer v1|v2]")
        print("  예시: python3 backtest.py AAPL 5")
        print("  예시: python3 backtest.py AAPL 5 --scorer v1")
        print("  예시: python3 backtest.py AAPL 2022 2023 2024")
        print("  예시: python3 backtest.py 005930 5")
        sys.exit(1)

    # --scorer 플래그 파싱 (연도 인자와 분리)
    scorer_version = 'v2'
    raw_args = sys.argv[2:]
    if '--scorer' in raw_args:
        idx = raw_args.index('--scorer')
        if idx + 1 < len(raw_args):
            scorer_version = raw_args[idx + 1]
            raw_args = [a for i, a in enumerate(raw_args) if i != idx and i != idx + 1]
        else:
            raw_args = [a for i, a in enumerate(raw_args) if i != idx]

    ticker = sys.argv[1]
    args   = raw_args

    # 인자가 하나이고 4자리 미만(연도 수)이면 "최근 N년" 모드
    current_year = date.today().year
    if len(args) == 1 and int(args[0]) < 2000:
        n = int(args[0])
        years = list(range(current_year - n, current_year))  # 현재 연도 제외 (회계연도 미완성)
    else:
        years = [int(y) for y in args]

    is_kr  = ticker.isdigit()

    print(f"\n백테스팅 시작: {ticker} / 대상 연도: {years} / scorer: {scorer_version}")
    print(f"{'─' * 50}")

    backtester = KRBacktester(ticker) if is_kr else USBacktester(ticker)

    rows = []
    for year in sorted(years):
        print(f"\n  [{year}년 데이터 수집 중...]")
        data = backtester.fetch_year(year)
        if data is None:
            continue
        scores = calc_scores(data, scorer_version=scorer_version)
        rows.append((data, scores))
        print(f"  → 총점: {scores['total']}점 ({scores['grade']}등급)")

    # DB에서 현재 시점 데이터 추가
    print(f"\n  [현재 데이터 (DB) 로드 중...]")
    current = fetch_current_from_db(ticker, scorer_version=scorer_version)
    if current:
        rows.append(current)
        print(f"  → 총점: {current[1]['total']}점 ({current[1]['grade']}등급)")
    else:
        print(f"  ⚠️ DB에 {ticker} 데이터 없음 — fetch 후 재시도")

    if not rows:
        print("수집된 데이터가 없습니다.")
        sys.exit(1)

    print_table(backtester.name, ticker, rows)


if __name__ == "__main__":
    main()
