import requests
import zipfile
import io
import xml.etree.ElementTree as ET
import datetime
from datetime import date
import yfinance as yf
import pandas as pd
from config import DART_API_KEY
from database import SessionLocal
from models import Company, RawFinancialData, QualitativeAssessment, FetchHistory


def _krx_dividend_label_year(ts) -> int:
    """
    yfinance 배당 지급일을 '연간 총액·연속 인상'용 연도로 바꿉니다.
    KRX에서는 결산 배당이 이듬해 1~2월에 찍히는 경우가 많아, 해당 분기만 전년도로 묶습니다.
    """
    t = pd.Timestamp(ts)
    if t.month in (1, 2):
        return t.year - 1
    return t.year


def _krx_yearly_dividend_totals(dividends: pd.Series) -> pd.Series:
    """KRX 관행 반영: 연도별 합계 Series (인덱스 = 집계 연도)."""
    if dividends.empty:
        return pd.Series(dtype=float)
    labels = dividends.index.map(_krx_dividend_label_year)
    return dividends.groupby(labels).sum()


def _dart_share_qty(raw) -> float:
    """주식총수현황 API 수량 필드 파싱 ('-', 빈칸, 콤마 처리)."""
    if raw is None:
        return 0.0
    s = str(raw).strip().replace(",", "")
    if s in ("", "-", "—"):
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def _parse_stock_totqy_list(items: list) -> tuple[float, float] | None:
    """
    stockTotqySttus 응답 list에서 보통주 발행총수·자기주식수를 추출합니다.
    분기 공시에 보통주 행이 없고 합계만 있는 경우 합계 행을 사용합니다.
    """
    if not items:
        return None
    ordinary_rows: list[tuple[float, float]] = []
    total_row = None
    for item in items:
        se = (item.get("se") or "").strip().replace(" ", "")
        if not se or "비고" in se:
            continue
        if se == "합계":
            total_row = item
            continue
        if "우선" in se:
            continue
        if "보통주" not in se:
            continue
        istc = _dart_share_qty(item.get("istc_totqy"))
        if istc <= 0:
            continue
        tesstk = _dart_share_qty(item.get("tesstk_co"))
        ordinary_rows.append((istc, tesstk))
    if ordinary_rows:
        return (sum(r[0] for r in ordinary_rows), sum(r[1] for r in ordinary_rows))
    if total_row:
        istc = _dart_share_qty(total_row.get("istc_totqy"))
        if istc > 0:
            return (istc, _dart_share_qty(total_row.get("tesstk_co")))
    return None


def record_fetch_history(db, ticker, fetch_date, status, message):
    """수집 이력을 FetchHistory 테이블에 기록합니다. (KR/US 공통)"""
    try:
        # Company가 없으면 FK 제약 위반이 발생하므로, 존재 여부 확인 후 기록
        company = db.query(Company).filter(Company.ticker == ticker).first()
        if not company:
            return

        history = db.query(FetchHistory).filter(
            FetchHistory.ticker == ticker,
            FetchHistory.fetch_date == fetch_date
        ).first()

        if history:
            history.status = status
            history.message = message
        else:
            new_history = FetchHistory(
                ticker=ticker,
                fetch_date=fetch_date,
                status=status,
                message=message
            )
            db.add(new_history)
        db.commit()
    except Exception:
        db.rollback()


class DartFetcher:
    def __init__(self):
        self.api_key = DART_API_KEY
        self.base_url = "https://opendart.fss.or.kr/api"
        self.corp_codes = {} # 종목코드(ticker) -> 고유번호(corp_code) 매핑 딕셔너리

    def load_corp_codes(self):
        """
        DART API에서 제공하는 기업 고유번호(XML ZIP 파일)를 다운로드하여 파싱합니다.
        상장된 기업(종목코드가 있는 기업)만 딕셔너리에 저장합니다.
        """
        print("📥 DART 기업 고유번호 목록을 다운로드 중입니다...")
        url = f"{self.base_url}/corpCode.xml"
        params = {'crtfc_key': self.api_key}
        
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            # ZIP 파일 메모리 내 압축 해제 및 XML 파싱
            with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                xml_data = z.read('CORPCODE.xml')
                root = ET.fromstring(xml_data)
                
                for list_tag in root.findall('list'):
                    corp_code = list_tag.find('corp_code').text
                    stock_code = list_tag.find('stock_code').text
                    corp_name = list_tag.find('corp_name').text
                    
                    # 주식코드가 있는 상장사만 필터링
                    if stock_code and stock_code.strip():
                        self.corp_codes[stock_code.strip()] = {
                            'corp_code': corp_code,
                            'corp_name': corp_name
                        }
            print(f"✅ 총 {len(self.corp_codes)}개의 상장사 고유번호를 로드했습니다.")
        else:
            print("❌ 고유번호 다운로드에 실패했습니다.")

    def get_latest_financial_summary(self, stock_code):
        """
        특정 기업의 가장 최근 재무제표 데이터를 가져옵니다. (PBR 자본총계용)
        3분기 -> 반기 -> 1분기 -> 전년도 사업보고서 순으로 조회합니다.
        """
        if not self.corp_codes:
            self.load_corp_codes()
            
        corp_info = self.corp_codes.get(stock_code)
        if not corp_info:
            print(f"❌ 종목코드 {stock_code}를 찾을 수 없습니다.")
            return None, None, None
            
        current_year = datetime.datetime.now().year
        
        # 조회할 보고서 코드 순서 (3분기, 반기, 1분기, 사업보고서)
        report_codes = ['11014', '11012', '11013', '11011']
        
        # 올해와 작년 2년치에 대해 순차적으로 시도
        for year in [current_year, current_year - 1]:
            for reprt_code in report_codes:
                url = f"{self.base_url}/fnlttSinglAcnt.json"
                params = {
                    'crtfc_key': self.api_key,
                    'corp_code': corp_info['corp_code'],
                    'bsns_year': str(year),
                    'reprt_code': reprt_code
                }
                
                response = requests.get(url, params=params)
                if response.status_code == 200:
                    data = response.json()
                    if data.get('status') == '000':
                        report_name = "사업보고서"
                        if reprt_code == '11014': report_name = "3분기보고서"
                        elif reprt_code == '11012': report_name = "반기보고서"
                        elif reprt_code == '11013': report_name = "1분기보고서"
                        
                        print(f"✅ {year}년 {report_name} 데이터 수집 성공! (자본총계용)")
                        return data.get('list'), year, reprt_code
                        
        print(f"❌ 최근 2년 내의 재무 데이터를 찾을 수 없습니다.")
        return None, None, None

    def get_annual_financial_summary(self, stock_code):
        """
        특정 기업의 가장 최근 '사업보고서(1년 치 최종 실적)' 데이터를 가져옵니다. (PER 당기순이익용)
        장기투자 관점에서 분기/반기 실적의 연환산 왜곡을 방지하기 위해 
        항상 확정된 연간 실적(사업보고서, reprt_code: 11011)만 조회합니다.
        """
        corp_info = self.corp_codes.get(stock_code)
        if not corp_info:
            return None, None, None
            
        current_year = datetime.datetime.now().year
        
        # 조회할 보고서 코드: 사업보고서(11011) 고정
        reprt_code = '11011'
        
        # 올해, 작년, 재작년 3년치에 대해 순차적으로 시도 (사업보고서는 보통 다음해 3월에 나오므로)
        for year in [current_year, current_year - 1, current_year - 2]:
            url = f"{self.base_url}/fnlttSinglAcnt.json"
            params = {
                'crtfc_key': self.api_key,
                'corp_code': corp_info['corp_code'],
                'bsns_year': str(year),
                'reprt_code': reprt_code
            }
            
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == '000':
                    print(f"✅ {year}년 사업보고서 데이터 수집 성공! (당기순이익용)")
                    return data.get('list'), year, reprt_code
                        
        return None, None, None

    def _stock_totals_one_report(self, corp_code, year, reprt_code):
        """단일 (사업연도, 보고서코드)에 대한 주식총수현황 조회. 파싱 불가 시 None."""
        url = f"{self.base_url}/stockTotqySttus.json"
        params = {
            "crtfc_key": self.api_key,
            "corp_code": corp_code,
            "bsns_year": str(year),
            "reprt_code": reprt_code,
        }
        response = requests.get(url, params=params, timeout=30)
        if response.status_code != 200:
            return None
        data = response.json()
        if data.get("status") != "000":
            return None
        return _parse_stock_totqy_list(data.get("list", []))

    def get_stock_totals(self, corp_code, year, reprt_code):
        """
        DART API '주식총수현황(stockTotqySttus)'으로 발행주식총수(istc_totqy)·자기주식(tesstk_co)을 가져옵니다.
        최신 분기 보고서에는 보통주 행이 비어 있는 경우가 많아, 동일·전년 사업보고서·반기 등으로 순차 폴백합니다.
        (주식 소유 현황 hyslrSttus는 최대주주 위주라 자기주식 수가 별도 행으로 없는 경우가 많음)
        """
        seen = set()
        candidates: list[tuple[int, str]] = []
        for y, rc in (
            (year, reprt_code),
            (year, "11011"),
            (year - 1, "11011"),
            (year, "11012"),
            (year - 1, "11012"),
            (year, "11014"),
            (year, "11013"),
            (year - 1, "11014"),
            (year - 1, "11013"),
        ):
            key = (y, rc)
            if key in seen:
                continue
            seen.add(key)
            candidates.append(key)
        for y, rc in candidates:
            parsed = self._stock_totals_one_report(corp_code, y, rc)
            if parsed is not None:
                return parsed[0], parsed[1]
        return 0.0, 0.0

    def check_cancel(self, corp_code):
        """
        현재 시점 기준으로 최근 1년 내에 자사주 소각 공시가 1건 이상 존재하는지 확인합니다.
        단순 매입 보유가 아닌 실제 소각 실적만 평가하도록 매입(취득) 조건을 제거했습니다.
        """
        one_year_ago = (datetime.datetime.now() - datetime.timedelta(days=365)).strftime('%Y%m%d')

        url = f"{self.base_url}/list.json"
        params = {
            'crtfc_key': self.api_key,
            'corp_code': corp_code,
            'bgn_de': one_year_ago,
            'page_count': 100
        }

        try:
            response = requests.get(url, params=params)
            if response.status_code == 200:
                data = response.json()
                if data.get('status') == '000':
                    for r in data.get('list', []):
                        if '소각' in r.get('report_nm', ''):
                            return True
        except Exception:
            pass

        return False

    def get_capex_dart(self, corp_code, year, reprt_code):
        """
        DART 전체 재무제표(fnlttSinglAcntAll)에서 CapEx(유형자산의 취득)를 추출합니다.
        투자활동현금흐름 내 유형자산 취득 항목을 찾아 양수로 반환합니다.
        데이터 없거나 오류 시 0.0 반환.
        """
        url = f"{self.base_url}/fnlttSinglAcntAll.json"
        for fs_div in ('CFS', 'OFS'):
            params = {
                'crtfc_key': self.api_key,
                'corp_code': corp_code,
                'bsns_year': str(year),
                'reprt_code': reprt_code,
                'fs_div': fs_div,
            }
            try:
                response = requests.get(url, params=params, timeout=30)
                if response.status_code != 200:
                    continue
                data = response.json()
                if data.get('status') != '000':
                    continue

                for item in data.get('list', []):
                    account_nm = item.get('account_nm', '').replace(' ', '')
                    # 유형자산의취득, 유형자산취득 등 매칭
                    if '유형자산' in account_nm and ('취득' in account_nm or '구입' in account_nm):
                        val_str = item.get('thstrm_amount', '').replace(',', '')
                        try:
                            val = float(val_str)
                            return abs(val)  # 현금 유출(음수)이므로 절댓값 반환
                        except ValueError:
                            continue
            except Exception:
                continue
        return 0.0

    def get_market_data(self, stock_code):
        """
        yfinance를 사용하여 현재 주가, 발행주식수, 배당금 정보를 가져옵니다.
        한국 주식은 종목코드 뒤에 .KS(코스피) 또는 .KQ(코스닥)를 붙여야 합니다.
        """
        market_data = {
            'current_price': 0.0,
            'total_shares': 0.0,
            'dividend_per_share': 0.0,
            'dividend_increase_years': 0,
        }

        # 코스피(.KS) 먼저 시도, 안되면 코스닥(.KQ) 시도
        for suffix in ['.KS', '.KQ']:
            yf_ticker = f"{stock_code}{suffix}"
            ticker = yf.Ticker(yf_ticker)

            try:
                info = ticker.info
                if info and ('regularMarketPrice' in info or 'currentPrice' in info or 'previousClose' in info):
                    current_price = info.get('currentPrice', info.get('previousClose', 0))
                    if current_price == 0:
                        continue

                    market_data['current_price'] = float(current_price)
                    market_data['total_shares'] = float(info.get('sharesOutstanding', 0))

                    # 1주당 연간 배당금: KRX는 1~2월 지급분을 전년도로 묶어 집계
                    dividends = ticker.dividends
                    if not dividends.empty:
                        yearly_divs = _krx_yearly_dividend_totals(dividends)
                        last_year = pd.Timestamp.now().year - 1

                        market_data['dividend_per_share'] = float(yearly_divs[last_year]) \
                            if last_year in yearly_divs else 0.0

                        # 배당 연속 인상 연수 (직전 완료 연도 기준, 중간 공백 시 연속 끊김으로 처리)
                        if last_year in yearly_divs.index:
                            y, streak = int(last_year), 0
                            while True:
                                prev_y = y - 1
                                if prev_y not in yearly_divs.index:
                                    break
                                if float(yearly_divs[y]) > float(yearly_divs[prev_y]):
                                    streak += 1
                                    y = prev_y
                                else:
                                    break
                            market_data['dividend_increase_years'] = streak

                    print(f"📈 yfinance 데이터 수집 성공 ({yf_ticker}): 현재가 {market_data['current_price']:,.0f}원")
                    print(f"   └─ 연속인상: {market_data['dividend_increase_years']}년")
                    return market_data
            except Exception:
                continue

        print(f"⚠️ yfinance에서 {stock_code}의 시장 데이터를 찾을 수 없습니다.")
        return market_data

    def get_dividend_history_dart(self, stock_code):
        """
        DART alotMatter.json API를 사용하여 최대 12년치 배당금 이력을 가져옵니다.
        각 호출은 당기(thstrm)/전기(frmtrm)/전전기(lwfr) 3개 연도를 반환하므로,
        4번 호출(last_year, last_year-3, last_year-6, last_year-9)로 약 12년치를 커버합니다.

        yfinance는 한국 주식 배당 이력을 3~5년치만 제공하는 경우가 많아 이를 대체합니다.
        배당 연속 인상 점수의 만점 기준(10년 이상)을 충족하기 위해 사용됩니다.

        Returns:
            dict: {year(int): dividend_per_share(float)} — 연도별 주당 현금배당금 (보통주)
                  데이터를 가져오지 못했거나 비배당 종목이면 빈 dict 반환.
        """
        corp_info = self.corp_codes.get(stock_code)
        if not corp_info:
            return {}

        corp_code = corp_info['corp_code']
        current_year = datetime.datetime.now().year
        last_year = current_year - 1

        yearly_divs = {}

        # 4번 호출: last_year부터 3년 간격으로 총 12년치 커버
        for base_year in [last_year, last_year - 3, last_year - 6, last_year - 9]:
            url = f"{self.base_url}/alotMatter.json"
            params = {
                'crtfc_key': self.api_key,
                'corp_code': corp_code,
                'bsns_year': str(base_year),
                'reprt_code': '11011'  # 사업보고서
            }

            try:
                response = requests.get(url, params=params, timeout=30)
                if response.status_code != 200:
                    continue
                data = response.json()
                if data.get('status') != '000':
                    continue

                items = data.get('list', [])
                for item in items:
                    se = (item.get('se') or '').strip()
                    # 주당 현금배당금 행만 추출
                    # "현금배당금총액(백만원)" 등 총액 행은 제외하고 "주당"/"1주당" 행만 허용
                    # 우선주 제외
                    if '주당' not in se:
                        continue
                    if '현금배당금' not in se:
                        continue
                    if '우선주' in se:
                        continue

                    # thstrm=base_year, frmtrm=base_year-1, lwfr=base_year-2
                    for period_key, year_offset in [('thstrm', 0), ('frmtrm', -1), ('lwfr', -2)]:
                        val_str = (item.get(period_key) or '').strip().replace(',', '')
                        if val_str in ('', '-', '—'):
                            continue
                        try:
                            val = float(val_str)
                            year = base_year + year_offset
                            # 이미 더 최근 호출에서 채워진 연도는 덮어쓰지 않음
                            if year not in yearly_divs:
                                yearly_divs[year] = val
                        except ValueError:
                            pass
            except Exception:
                continue

        return yearly_divs

    def save_to_db(self, stock_code):
        """
        수집한 최근 데이터를 DB에 저장합니다.
        성공/실패 여부를 FetchHistory에 기록합니다.
        """
        db = SessionLocal()
        today = date.today()
        
        corp_info = self.corp_codes.get(stock_code)
        if not corp_info:
            return
            
        corp_name = corp_info['corp_name']
        
        try:
            # 1. Company 테이블에 기업 정보가 없으면 우선 추가 (이력이 남으려면 Company가 있어야 함)
            company = db.query(Company).filter(Company.ticker == stock_code).first()
            if not company:
                company = Company(ticker=stock_code, name=corp_name, market="KOSPI/KOSDAQ")
                db.add(company)
                db.commit()
                print(f"🏢 DB에 기업 정보 추가됨: {corp_name} ({stock_code})")

            # 최근 재무 데이터 (자본총계용 - PBR)
            latest_data_list, latest_year, latest_reprt_code = self.get_latest_financial_summary(stock_code)
            if not latest_data_list:
                record_fetch_history(db, stock_code, today, "FAIL_NO_DATA", "최근 2년 내 재무 데이터 없음")
                return
                
            # 연간 재무 데이터 (당기순이익용 - PER)
            annual_data_list, annual_year, annual_reprt_code = self.get_annual_financial_summary(stock_code)
                
            # 2. 재무 데이터 파싱
            net_income = 0.0
            total_equity = 0.0
            total_liabilities = 0.0
            operating_cash_flow = 0.0
            net_income_prev = None   # 전년도 당기순이익 (frmtrm) — EPS 성장률용
            net_income_prev2 = None  # 전전년도 당기순이익 (lwfr)   — 2년 CAGR 시도용

            # 재무상태표 항목 파싱 (최근 분기 기준): 자본총계, 부채총계
            for item in latest_data_list:
                account_nm = item.get('account_nm', '').replace(' ', '')
                try:
                    amount = float(item.get('thstrm_amount', '0').replace(',', ''))
                except ValueError:
                    continue

                if '자본총계' in account_nm:
                    if item.get('fs_div') == 'CFS':
                        total_equity = amount
                    elif item.get('fs_div') == 'OFS' and total_equity == 0.0:
                        total_equity = amount

                if '부채총계' in account_nm:
                    if item.get('fs_div') == 'CFS':
                        total_liabilities = amount
                    elif item.get('fs_div') == 'OFS' and total_liabilities == 0.0:
                        total_liabilities = amount

            # 손익계산서·현금흐름표 항목 파싱 (사업보고서 연간 기준): 당기순이익, 영업활동현금흐름
            if annual_data_list:
                for item in annual_data_list:
                    account_nm = item.get('account_nm', '').replace(' ', '')
                    fs_div = item.get('fs_div')

                    def _parse_amount(key):
                        raw = item.get(key, '') or ''
                        try:
                            return float(raw.replace(',', ''))
                        except ValueError:
                            return None

                    thstrm = _parse_amount('thstrm_amount')
                    frmtrm = _parse_amount('frmtrm_amount')
                    lwfr   = _parse_amount('lwfr_amount')

                    if thstrm is None:
                        continue

                    if '당기순이익' in account_nm or '당기순손실' in account_nm:
                        if fs_div == 'CFS':
                            net_income = thstrm
                            if frmtrm is not None:
                                net_income_prev = frmtrm
                            if lwfr is not None:
                                net_income_prev2 = lwfr
                        elif fs_div == 'OFS' and net_income == 0.0:
                            net_income = thstrm
                            if net_income_prev is None and frmtrm is not None:
                                net_income_prev = frmtrm
                            if net_income_prev2 is None and lwfr is not None:
                                net_income_prev2 = lwfr

                    if '영업활동' in account_nm and '현금흐름' in account_nm:
                        if fs_div == 'CFS':
                            operating_cash_flow = thstrm
                        elif fs_div == 'OFS' and operating_cash_flow == 0.0:
                            operating_cash_flow = thstrm

            # EPS 성장률(YoY 또는 2년 CAGR) 계산
            # 유효 조건: 당해·비교 연도 모두 양수 (적자 포함 시 의미 없는 성장률 방지)
            eps_growth_rate = None
            if net_income > 0:
                if net_income_prev2 is not None and net_income_prev2 > 0:
                    # 2년 CAGR: (현재 / 2년전) ** (1/2) - 1
                    eps_growth_rate = ((net_income / net_income_prev2) ** 0.5 - 1) * 100
                    print(f"   └─ EPS 성장률(2년 CAGR): {eps_growth_rate:.1f}%")
                elif net_income_prev is not None and net_income_prev > 0:
                    # 1년 YoY: (현재 / 전년) - 1
                    eps_growth_rate = (net_income / net_income_prev - 1) * 100
                    print(f"   └─ EPS 성장률(1년 YoY): {eps_growth_rate:.1f}%")

            # CapEx(유형자산의 취득) - 사업보고서 전체 재무제표에서 별도 조회
            capital_expenditure = self.get_capex_dart(
                corp_info['corp_code'], annual_year or latest_year, '11011'
            ) if (annual_year or latest_year) else 0.0

            # 3. yfinance에서 현재 시장 데이터(주가, 주식수, 배당금 등) 가져오기
            market_data = self.get_market_data(stock_code)

            # 배당금이 0원인 종목은 평가 대상에서 제외
            if market_data.get('dividend_per_share', 0.0) <= 0:
                print(f"⏭️ 최근 1년간 배당금 지급 내역이 없어 제외합니다: {corp_name} ({stock_code})")
                record_fetch_history(db, stock_code, today, "SKIP_NO_DIVIDEND", "배당금 0원")
                return
            
            # 3.5. DART alotMatter.json으로 배당 연속 인상 연수 재계산
            # yfinance는 한국 주식 배당 이력을 3~5년치만 제공하는 경우가 많아,
            # 더 긴 이력(최대 12년)을 제공하는 DART API로 대체합니다.
            dart_div_history = self.get_dividend_history_dart(stock_code)
            if dart_div_history:
                current_year_dart = datetime.datetime.now().year
                last_year_dart = current_year_dart - 1
                # 연도별 배당금 이력 출력 (디버깅용)
                sorted_years = sorted(dart_div_history.keys())
                history_str = ", ".join(f"{y}:{dart_div_history[y]:,.0f}" for y in sorted_years)
                print(f"   📋 DART 배당 이력: {history_str}")
                if last_year_dart in dart_div_history:
                    y = last_year_dart
                    streak = 0
                    while True:
                        prev_y = y - 1
                        if prev_y not in dart_div_history:
                            break
                        if dart_div_history[y] > dart_div_history[prev_y]:
                            streak += 1
                            y = prev_y
                        else:
                            break
                    market_data['dividend_increase_years'] = streak
                    print(f"   📊 DART 배당 이력 기반 연속 인상: {streak}년 ({len(dart_div_history)}년치 데이터 확보)")

            # 3.7. DART 주식총수현황에서 자사주 수 가져오기 (최근 분기 기준)
            total_issued, treasury_shares = self.get_stock_totals(corp_info['corp_code'], latest_year, latest_reprt_code)

            # 3.8. 최근 1년 내 자사주 소각 여부 확인 (매입 제외, 소각만)
            is_cancel = self.check_cancel(corp_info['corp_code'])
            if is_cancel:
                print(f"🏢 자사주 소각 공시 확인됨")

            # 4. RawFinancialData 테이블에 저장
            month, day = 12, 31  # 기본: 사업보고서
            if latest_reprt_code == '11014': month, day = 9, 30   # 3분기
            elif latest_reprt_code == '11012': month, day = 6, 30  # 반기
            elif latest_reprt_code == '11013': month, day = 3, 31  # 1분기

            record_date = date(latest_year, month, day)

            existing_data = db.query(RawFinancialData).filter(
                RawFinancialData.ticker == stock_code,
                RawFinancialData.record_date == record_date
            ).first()

            raw_fields = dict(
                net_income=net_income,
                total_equity=total_equity,
                total_liabilities=total_liabilities,
                operating_cash_flow=operating_cash_flow,
                capital_expenditure=capital_expenditure,
                eps_growth_rate=eps_growth_rate,
                current_price=market_data['current_price'],
                total_shares=market_data['total_shares'],
                dividend_per_share=market_data['dividend_per_share'],
                dividend_increase_years=market_data['dividend_increase_years'],
                treasury_shares=treasury_shares,
                share_cancel=is_cancel,
            )

            if existing_data:
                for k, v in raw_fields.items():
                    setattr(existing_data, k, v)
                print(f"🔄 기존 데이터 업데이트 완료: {corp_name} (기준일: {record_date})")
            else:
                new_data = RawFinancialData(ticker=stock_code, record_date=record_date, **raw_fields)
                db.add(new_data)
                print(f"💾 새 데이터 DB 저장 완료: {corp_name} (기준일: {record_date}, 당기순이익: {net_income:,.0f}원, 자본총계: {total_equity:,.0f}원)")
                
            # 성공 이력 기록
            record_fetch_history(db, stock_code, today, "SUCCESS", "정상 수집 완료")
            db.commit()
            
        except Exception as e:
            db.rollback()
            print(f"❌ DB 저장 중 에러 발생: {e}")
            record_fetch_history(db, stock_code, today, "ERROR", str(e))
        finally:
            db.close()

class USFetcher:
    """미국 주식 데이터 수집 - yfinance만으로 모든 데이터 확보"""

    def get_financial_data(self, ticker):
        """
        yfinance에서 재무제표 데이터를 가져옵니다.
        - 당기순이익(EPS용): 최근 4분기 합산 TTM
        - 자본총계·자사주·총부채(PBR·부채비율용): 최근 분기 balance sheet
        - 영업현금흐름·CapEx(FCF용): 연간 cashflow
        - 총 주식수: Diluted Average Shares 우선, 없으면 sharesOutstanding 폴백
        - record_date: 분기 balance sheet의 기준일
        """
        result = {
            'net_income': 0.0,
            'total_equity': 0.0,
            'total_liabilities': 0.0,
            'treasury_shares': 0.0,
            'total_shares': 0.0,
            'operating_cash_flow': 0.0,
            'capital_expenditure': 0.0,
            'eps_growth_rate': None,
            'record_date': None,
        }

        try:
            stock = yf.Ticker(ticker)

            # 당기순이익 + 희석주식수: 최근 4분기 합산 (TTM)
            q_income = stock.quarterly_income_stmt
            if q_income is not None and not q_income.empty:
                if 'Net Income' in q_income.index:
                    recent_4q = q_income.loc['Net Income'].iloc[:4].dropna()
                    if not recent_4q.empty:
                        result['net_income'] = float(recent_4q.sum())

                # 이중 주식 구조 대응: Diluted Average Shares 우선
                for shares_key in ('Diluted Average Shares', 'Basic Average Shares'):
                    if shares_key in q_income.index:
                        val = q_income.loc[shares_key].iloc[0]
                        if pd.notna(val) and float(val) > 0:
                            result['total_shares'] = float(val)
                            break

            # 자본총계·총부채·자사주: 최근 분기 balance sheet
            q_balance = stock.quarterly_balance_sheet
            if q_balance is not None and not q_balance.empty:
                def _get(key):
                    if key in q_balance.index:
                        val = q_balance.loc[key].iloc[0]
                        if pd.notna(val):
                            return float(val)
                    return 0.0

                result['total_equity']     = _get('Stockholders Equity')
                result['total_liabilities'] = _get('Total Liabilities Net Minority Interest')
                treasury = _get('Treasury Shares Number')
                result['treasury_shares']  = abs(treasury) if treasury else 0.0
                result['record_date']      = q_balance.columns[0].date()

            # quarterly_balance_sheet 없으면 income_stmt 날짜로 폴백
            if result['record_date'] is None and q_income is not None and not q_income.empty:
                result['record_date'] = q_income.columns[0].date()

            # 영업현금흐름·CapEx: 연간 cashflow (가장 최근 연도 기준)
            cashflow = stock.cashflow
            if cashflow is not None and not cashflow.empty:
                for ocf_key in ('Operating Cash Flow', 'Cash Flow From Continuing Operating Activities'):
                    if ocf_key in cashflow.index:
                        val = cashflow.loc[ocf_key].iloc[0]
                        if pd.notna(val):
                            result['operating_cash_flow'] = float(val)
                            break

                for capex_key in ('Capital Expenditure', 'Purchase Of Property Plant And Equipment'):
                    if capex_key in cashflow.index:
                        val = cashflow.loc[capex_key].iloc[0]
                        if pd.notna(val):
                            result['capital_expenditure'] = abs(float(val))  # 유출이라 음수 → 양수
                            break

            # EPS 성장률: 연간 EPS 히스토리로 3년 CAGR 계산, 없으면 earningsGrowth 사용
            try:
                annual_income = stock.income_stmt
                if annual_income is not None and not annual_income.empty:
                    eps_growth = None
                    for eps_key in ('Diluted EPS', 'Basic EPS'):
                        if eps_key in annual_income.index:
                            eps_series = annual_income.loc[eps_key].dropna()
                            # 최근 연도부터 정렬 (columns는 최근순)
                            eps_values = eps_series.values  # 최근→과거 순
                            if len(eps_values) >= 2:
                                # 이용 가능한 기간으로 CAGR 계산 (최대 3년)
                                n = min(len(eps_values) - 1, 3)
                                eps_end = float(eps_values[0])    # 가장 최근
                                eps_start = float(eps_values[n])  # n년 전
                                # 양쪽 모두 양수일 때만 의미 있는 CAGR
                                if eps_end > 0 and eps_start > 0:
                                    eps_growth = ((eps_end / eps_start) ** (1 / n) - 1) * 100
                                elif eps_end > 0 and eps_start < 0:
                                    # 적자→흑자 전환: 성장률 계산 불가
                                    eps_growth = None
                                else:
                                    eps_growth = None
                            break

                    # EPS 히스토리로 계산 실패 시 earningsGrowth(TTM YoY) fallback
                    if eps_growth is None:
                        eg = stock.info.get('earningsGrowth')
                        if eg is not None and eg > 0:
                            eps_growth = float(eg) * 100  # 소수 → 퍼센트

                    result['eps_growth_rate'] = eps_growth
                    if eps_growth is not None:
                        print(f"   └─ EPS 성장률: {eps_growth:.1f}% ({n if 'n' in dir() else 'TTM'}년 기준)")
            except Exception as e:
                print(f"   ⚠️ {ticker} EPS 성장률 계산 실패: {e}")

            if result['record_date']:
                print(f"✅ {ticker} 재무제표 수집 성공 (기준일: {result['record_date']})")
            else:
                print(f"⚠️ {ticker} 재무제표 데이터를 찾을 수 없습니다.")

        except Exception as e:
            print(f"❌ {ticker} 재무제표 수집 실패: {e}")

        return result

    def get_market_data(self, ticker):
        """
        yfinance에서 주가, 주식수, 배당 정보를 가져옵니다.
        미국 주식은 캘린더 연도 기준으로 배당금을 집계합니다 (KRX 보정 없음).
        """
        market_data = {
            'current_price': 0.0,
            'total_shares': 0.0,
            'dividend_per_share': 0.0,
            'dividend_increase_years': 0,
        }

        try:
            stock = yf.Ticker(ticker)
            info = stock.info

            if not info:
                return market_data

            market_data['current_price'] = float(
                info.get('currentPrice', info.get('previousClose', 0))
            )
            market_data['total_shares'] = float(info.get('sharesOutstanding', 0))

            dividends = stock.dividends
            if not dividends.empty:
                last_year = pd.Timestamp.now().year - 1
                yearly_divs = dividends.groupby(dividends.index.year).sum()

                if last_year in yearly_divs.index:
                    market_data['dividend_per_share'] = float(yearly_divs[last_year])

                # 배당 연속 인상 연수 (캘린더 연도 기준)
                if last_year in yearly_divs.index:
                    y, streak = int(last_year), 0
                    while True:
                        prev_y = y - 1
                        if prev_y not in yearly_divs.index:
                            break
                        if float(yearly_divs[y]) > float(yearly_divs[prev_y]):
                            streak += 1
                            y = prev_y
                        else:
                            break
                    market_data['dividend_increase_years'] = streak

            print(f"📈 yfinance 데이터 수집 성공 ({ticker}): 현재가 ${market_data['current_price']:,.2f}")
            print(f"   └─ 연속인상: {market_data['dividend_increase_years']}년")

        except Exception as e:
            print(f"⚠️ {ticker} 시장 데이터 수집 실패: {e}")

        return market_data

    def check_cancel(self, ticker):
        """
        yfinance 현금흐름표에서 자사주 매입(Repurchase) 여부를 확인합니다.
        미국에서는 자사주 매입 시 대부분 소각(retire)하므로 매입 = 소각으로 간주합니다.
        """
        try:
            stock = yf.Ticker(ticker)
            cashflow = stock.cashflow
            if cashflow is None or cashflow.empty:
                return False

            for key in ('Repurchase Of Capital Stock', 'Common Stock Repurchased'):
                if key in cashflow.index:
                    val = cashflow.loc[key].iloc[0]
                    if pd.notna(val) and float(val) < 0:
                        return True
        except Exception:
            pass
        return False

    def save_to_db(self, ticker):
        """
        미국 주식 데이터를 수집하여 DB에 저장합니다.
        yfinance만으로 재무제표 + 시장 데이터 + 자사주 정보를 모두 확보합니다.
        """
        db = SessionLocal()
        today = date.today()

        try:
            # 1. Company 등록 (fetch_history FK 제약을 위해 가장 먼저 수행)
            stock_obj = yf.Ticker(ticker)
            company_name = stock_obj.info.get('shortName', ticker)
            exchange = stock_obj.info.get('exchange', 'US')

            company = db.query(Company).filter(Company.ticker == ticker).first()
            if not company:
                company = Company(ticker=ticker, name=company_name, market=exchange)
                db.add(company)
                db.commit()
                print(f"🏢 DB에 기업 정보 추가됨: {company_name} ({ticker})")

            # 2. 재무제표 데이터 수집 (당기순이익, 자본총계, 자사주)
            financial = self.get_financial_data(ticker)
            if not financial['record_date']:
                record_fetch_history(db, ticker, today, "FAIL_NO_DATA", "재무제표 데이터 없음")
                return

            # 3. 시장 데이터 수집 (주가, 주식수, 배당)
            market_data = self.get_market_data(ticker)

            if market_data['current_price'] <= 0:
                record_fetch_history(db, ticker, today, "FAIL_NO_DATA", "주가 데이터 없음")
                return

            # 배당금 없는 종목 제외
            if market_data['dividend_per_share'] <= 0:
                print(f"⏭️ 배당금 없어 제외: {company_name} ({ticker})")
                record_fetch_history(db, ticker, today, "SKIP_NO_DIVIDEND", "배당금 0")
                return

            # 4. 자사주 소각 여부 (US는 매입≈소각으로 간주)
            is_cancel = self.check_cancel(ticker)
            if is_cancel:
                print(f"🏢 자사주 소각(Buyback) 확인됨: {ticker}")

            # 5. RawFinancialData 저장
            record_date = financial['record_date']

            existing = db.query(RawFinancialData).filter(
                RawFinancialData.ticker == ticker,
                RawFinancialData.record_date == record_date
            ).first()

            # total_shares: 재무제표 희석주식수 우선, 없으면 info['sharesOutstanding'] 폴백
            # 이중 주식 구조(MKC-V 등)에서 sharesOutstanding은 해당 클래스 주식수만 반환하므로
            # Diluted Average Shares를 우선 사용하여 전사 기준 EPS/BPS를 올바르게 계산
            total_shares = financial['total_shares'] if financial['total_shares'] > 0 else market_data['total_shares']
            if financial['total_shares'] > 0 and abs(financial['total_shares'] - market_data['total_shares']) / max(market_data['total_shares'], 1) > 0.1:
                print(f"   ℹ️ 주식수 조정: sharesOutstanding={market_data['total_shares']:,.0f} → Diluted={financial['total_shares']:,.0f}")

            raw_fields = dict(
                net_income=financial['net_income'],
                total_equity=financial['total_equity'],
                total_liabilities=financial['total_liabilities'],
                operating_cash_flow=financial['operating_cash_flow'],
                capital_expenditure=financial['capital_expenditure'],
                eps_growth_rate=financial['eps_growth_rate'],
                current_price=market_data['current_price'],
                total_shares=total_shares,
                dividend_per_share=market_data['dividend_per_share'],
                dividend_increase_years=market_data['dividend_increase_years'],
                treasury_shares=financial['treasury_shares'],
                share_cancel=is_cancel,
            )

            if existing:
                for k, v in raw_fields.items():
                    setattr(existing, k, v)
                print(f"🔄 기존 데이터 업데이트: {company_name} ({ticker}, 기준일: {record_date})")
            else:
                new_data = RawFinancialData(ticker=ticker, record_date=record_date, **raw_fields)
                db.add(new_data)
                print(f"💾 새 데이터 저장: {company_name} ({ticker}, 기준일: {record_date})")

            record_fetch_history(db, ticker, today, "SUCCESS", "정상 수집 완료")
            db.commit()

        except Exception as e:
            db.rollback()
            print(f"❌ {ticker} 처리 중 에러: {e}")
            record_fetch_history(db, ticker, today, "ERROR", str(e))
        finally:
            db.close()


if __name__ == "__main__":
    # 테스트: 한국 주식
    # fetcher = DartFetcher()
    # fetcher.save_to_db('005930')

    # 테스트: 미국 주식
    us_fetcher = USFetcher()
    us_fetcher.save_to_db('AAPL')