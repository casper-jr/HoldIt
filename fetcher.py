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

    def check_buyback_and_cancel(self, corp_code):
        """
        현재 시점 기준으로 최근 1년 내에 자사주 매입(취득)과 소각 공시가 
        각각 1건 이상 존재하는지 확인합니다.
        """
        one_year_ago = (datetime.datetime.now() - datetime.timedelta(days=365)).strftime('%Y%m%d')
        
        has_buyback = False
        has_cancel = False
        
        # 최근 1년간의 전체 공시 목록을 가져와서 제목으로 검색 (최대 100건)
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
                        report_nm = r.get('report_nm', '')
                        # 자사주 취득(매입) 관련 공시
                        if '자기주식취득' in report_nm:
                            has_buyback = True
                        # 자사주 소각 관련 공시
                        if '소각' in report_nm:
                            has_cancel = True
                            
                        # 둘 다 찾았으면 더 이상 검색할 필요 없음
                        if has_buyback and has_cancel:
                            break
        except Exception as e:
            pass
            
        return has_buyback and has_cancel

    def get_market_data(self, stock_code):
        """
        yfinance를 사용하여 현재 주가, 발행주식수, 배당금 정보를 가져옵니다.
        한국 주식은 종목코드 뒤에 .KS(코스피) 또는 .KQ(코스닥)를 붙여야 합니다.
        """
        market_data = {
            'current_price': 0.0,
            'total_shares': 0.0,
            'dividend_per_share': 0.0,
            'quarterly_dividend': False,
            'dividend_increase_years': 0
        }
        
        # 코스피(.KS) 먼저 시도, 안되면 코스닥(.KQ) 시도
        for suffix in ['.KS', '.KQ']:
            yf_ticker = f"{stock_code}{suffix}"
            ticker = yf.Ticker(yf_ticker)
            
            try:
                info = ticker.info
                # info 딕셔너리에 주가 정보가 있는지 확인
                if info and ('regularMarketPrice' in info or 'currentPrice' in info or 'previousClose' in info):
                    current_price = info.get('currentPrice', info.get('previousClose', 0))
                    
                    if current_price == 0:
                        continue # 주가 정보가 없으면 다른 시장(suffix)으로 재시도
                        
                    market_data['current_price'] = float(current_price)
                    market_data['total_shares'] = float(info.get('sharesOutstanding', 0))
                    # 1주당 연간 배당금·연속 인상: TTM이 아님. KRX는 1~2월 지급분을 전년도로 묶은 뒤
                    # '직전 완료 연도(last_year)' 합계를 쓴다 (결산 배당이 캘린더만 쓸 때 연도가 갈라지는 문제 방지).
                    dividends = ticker.dividends
                    if not dividends.empty:
                        yearly_divs = _krx_yearly_dividend_totals(dividends)
                        
                        current_year = pd.Timestamp.now().year
                        last_year = current_year - 1
                        
                        # 작년 배당금이 있으면 작년 값을, 없으면 0으로 처리
                        if last_year in yearly_divs:
                            market_data['dividend_per_share'] = float(yearly_divs[last_year])
                        else:
                            market_data['dividend_per_share'] = 0.0
                        
                        # 1. 분기 배당 여부 확인 (최근 1년 내 배당금 지급 횟수가 3회 이상이면 분기배당으로 간주)
                        one_year_ago = pd.Timestamp.now(tz=dividends.index.tz) - pd.DateOffset(days=365)
                        recent_divs = dividends[dividends.index >= one_year_ago]
                        if len(recent_divs) >= 3:
                            market_data['quarterly_dividend'] = True
                            
                        # 2. 배당 연속 인상 연수: 연간 합계는 캘린더 연도별로 두고,
                        #    dividend_per_share와 동일하게 '직전 완료 연도(last_year)'를 끝점으로 삼는다.
                        #    last_year > last_year-1 > ... 를 (y, y-1)가 모두 데이터에 있을 때만 strict 비교한다.
                        #    중간에 배당 실적이 없는 해(인덱스 없음)가 끼면 연속이 끊긴 것으로 본다.
                        if last_year in yearly_divs.index:
                            y = int(last_year)
                            streak = 0
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
                    print(f"   └─ 분기배당: {market_data['quarterly_dividend']} | 연속인상: {market_data['dividend_increase_years']}년")
                    return market_data
            except Exception as e:
                continue
                
        print(f"⚠️ yfinance에서 {stock_code}의 시장 데이터를 찾을 수 없습니다.")
        return market_data

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
            
            # 자본총계 파싱 (최근 분기 데이터 기준)
            for item in latest_data_list:
                account_nm = item.get('account_nm', '').replace(' ', '')
                if '자본총계' in account_nm:
                    try:
                        amount_str = item.get('thstrm_amount', '0').replace(',', '')
                        amount = float(amount_str)
                        
                        if item.get('fs_div') == 'CFS':
                            total_equity = amount
                        elif item.get('fs_div') == 'OFS' and total_equity == 0.0:
                            total_equity = amount
                    except ValueError:
                        pass
                        
            # 당기순이익 파싱 (최근 사업보고서 데이터 기준)
            if annual_data_list:
                for item in annual_data_list:
                    account_nm = item.get('account_nm', '').replace(' ', '')
                    if '당기순이익' in account_nm or '당기순손실' in account_nm:
                        try:
                            amount_str = item.get('thstrm_amount', '0')
                            amount = float(amount_str.replace(',', ''))
                            
                            if item.get('fs_div') == 'CFS':
                                net_income = amount
                            elif item.get('fs_div') == 'OFS' and net_income == 0.0:
                                net_income = amount
                        except ValueError:
                            pass

            # 3. yfinance에서 현재 시장 데이터(주가, 주식수, 배당금 등) 가져오기
            market_data = self.get_market_data(stock_code)
            
            # 배당금이 0원인 종목은 평가 대상에서 제외
            if market_data.get('dividend_per_share', 0.0) <= 0:
                print(f"⏭️ 최근 1년간 배당금 지급 내역이 없어 제외합니다: {corp_name} ({stock_code})")
                record_fetch_history(db, stock_code, today, "SKIP_NO_DIVIDEND", "배당금 0원")
                return
            
            # 3.5. DART 주식총수현황에서 자사주 정보 가져오기 (최근 분기 기준)
            total_issued, treasury_shares = self.get_stock_totals(corp_info['corp_code'], latest_year, latest_reprt_code)
            
            # 3.6. 최근 1년 내 자사주 매입 및 소각 여부 확인
            is_buyback_cancel = self.check_buyback_and_cancel(corp_info['corp_code'])
            if is_buyback_cancel:
                print(f"🏢 자사주 매입 및 소각 공시 확인됨")

            # 4. RawFinancialData 테이블에 저장
            # 기준일자는 가장 최근 보고서(latest) 기준으로 설정
            month, day = 12, 31 # 기본 사업보고서
            if latest_reprt_code == '11014': month, day = 9, 30  # 3분기
            elif latest_reprt_code == '11012': month, day = 6, 30 # 반기
            elif latest_reprt_code == '11013': month, day = 3, 31 # 1분기
            
            record_date = date(latest_year, month, day)
            
            # 이미 같은 날짜의 데이터가 있는지 확인
            existing_data = db.query(RawFinancialData).filter(
                RawFinancialData.ticker == stock_code,
                RawFinancialData.record_date == record_date
            ).first()
            
            if existing_data:
                existing_data.net_income = net_income
                existing_data.total_equity = total_equity
                existing_data.current_price = market_data['current_price']
                existing_data.total_shares = market_data['total_shares']
                existing_data.dividend_per_share = market_data['dividend_per_share']
                existing_data.quarterly_dividend = market_data['quarterly_dividend']
                existing_data.dividend_increase_years = market_data['dividend_increase_years']
                existing_data.treasury_shares = treasury_shares
                existing_data.share_buyback_cancel = is_buyback_cancel
                print(f"🔄 기존 데이터 업데이트 완료: {corp_name} (기준일: {record_date})")
            else:
                new_data = RawFinancialData(
                    ticker=stock_code,
                    record_date=record_date,
                    net_income=net_income,
                    total_equity=total_equity,
                    current_price=market_data['current_price'],
                    total_shares=market_data['total_shares'],
                    dividend_per_share=market_data['dividend_per_share'],
                    quarterly_dividend=market_data['quarterly_dividend'],
                    dividend_increase_years=market_data['dividend_increase_years'],
                    treasury_shares=treasury_shares,
                    share_buyback_cancel=is_buyback_cancel
                )
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
        yfinance에서 재무제표 데이터(당기순이익, 자본총계, 자사주)를 가져옵니다.
        가장 최근 연간 재무제표(Annual) 기준입니다.
        """
        result = {
            'net_income': 0.0,
            'total_equity': 0.0,
            'treasury_shares': 0.0,
            'record_date': None
        }

        try:
            stock = yf.Ticker(ticker)

            # Income Statement - 당기순이익
            income_stmt = stock.income_stmt
            if income_stmt is not None and not income_stmt.empty:
                if 'Net Income' in income_stmt.index:
                    val = income_stmt.loc['Net Income'].iloc[0]
                    if pd.notna(val):
                        result['net_income'] = float(val)
                result['record_date'] = income_stmt.columns[0].date()

            # Balance Sheet - 자본총계, 자사주
            balance = stock.balance_sheet
            if balance is not None and not balance.empty:
                if 'Stockholders Equity' in balance.index:
                    val = balance.loc['Stockholders Equity'].iloc[0]
                    if pd.notna(val):
                        result['total_equity'] = float(val)

                if 'Treasury Shares Number' in balance.index:
                    val = balance.loc['Treasury Shares Number'].iloc[0]
                    if pd.notna(val):
                        result['treasury_shares'] = abs(float(val))

                if result['record_date'] is None:
                    result['record_date'] = balance.columns[0].date()

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
            'quarterly_dividend': False,
            'dividend_increase_years': 0
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
                current_year = pd.Timestamp.now().year
                last_year = current_year - 1

                # 캘린더 연도별 배당금 합계 (KRX 보정 없이 그대로)
                yearly_divs = dividends.groupby(dividends.index.year).sum()

                # 작년도 연간 총 배당금
                if last_year in yearly_divs.index:
                    market_data['dividend_per_share'] = float(yearly_divs[last_year])

                # 분기 배당 여부 (최근 1년 내 3회 이상 지급)
                one_year_ago = pd.Timestamp.now(tz=dividends.index.tz) - pd.DateOffset(days=365)
                recent_divs = dividends[dividends.index >= one_year_ago]
                if len(recent_divs) >= 3:
                    market_data['quarterly_dividend'] = True

                # 배당 연속 인상 연수 (캘린더 연도 기준)
                if last_year in yearly_divs.index:
                    y = int(last_year)
                    streak = 0
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
            print(f"   └─ 분기배당: {market_data['quarterly_dividend']} | 연속인상: {market_data['dividend_increase_years']}년")

        except Exception as e:
            print(f"⚠️ {ticker} 시장 데이터 수집 실패: {e}")

        return market_data

    def check_buyback(self, ticker):
        """
        yfinance 현금흐름표에서 자사주 매입(Repurchase) 여부를 확인합니다.
        미국에서는 자사주 매입 시 대부분 소각(retire)하므로 매입 = 소각으로 간주합니다.
        """
        try:
            stock = yf.Ticker(ticker)
            cashflow = stock.cashflow
            if cashflow is None or cashflow.empty:
                return False

            buyback_keys = ['Repurchase Of Capital Stock', 'Common Stock Repurchased']
            for key in buyback_keys:
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

            # 4. 자사주 매입/소각 여부
            is_buyback = self.check_buyback(ticker)
            if is_buyback:
                print(f"🏢 자사주 매입(Buyback) 확인됨: {ticker}")

            # 5. RawFinancialData 저장
            record_date = financial['record_date']

            existing = db.query(RawFinancialData).filter(
                RawFinancialData.ticker == ticker,
                RawFinancialData.record_date == record_date
            ).first()

            if existing:
                existing.net_income = financial['net_income']
                existing.total_equity = financial['total_equity']
                existing.current_price = market_data['current_price']
                existing.total_shares = market_data['total_shares']
                existing.dividend_per_share = market_data['dividend_per_share']
                existing.quarterly_dividend = market_data['quarterly_dividend']
                existing.dividend_increase_years = market_data['dividend_increase_years']
                existing.treasury_shares = financial['treasury_shares']
                existing.share_buyback_cancel = is_buyback
                print(f"🔄 기존 데이터 업데이트: {company_name} ({ticker}, 기준일: {record_date})")
            else:
                new_data = RawFinancialData(
                    ticker=ticker,
                    record_date=record_date,
                    net_income=financial['net_income'],
                    total_equity=financial['total_equity'],
                    current_price=market_data['current_price'],
                    total_shares=market_data['total_shares'],
                    dividend_per_share=market_data['dividend_per_share'],
                    quarterly_dividend=market_data['quarterly_dividend'],
                    dividend_increase_years=market_data['dividend_increase_years'],
                    treasury_shares=financial['treasury_shares'],
                    share_buyback_cancel=is_buyback
                )
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