import os
import sys
import signal
import unicodedata
import csv
import time
import ssl
import urllib.request
from datetime import date, datetime
import FinanceDataReader as fdr

from fetcher import DartFetcher, USFetcher

# Ctrl+C graceful shutdown을 위한 플래그
_interrupted = False

def _handle_sigint(sig, frame):
    """첫 번째 Ctrl+C: 현재 종목 처리 후 중단, 두 번째: 즉시 중단"""
    global _interrupted
    if _interrupted:
        raise KeyboardInterrupt
    _interrupted = True
    print("\n\n중단 요청됨. 현재 종목 처리 후 중단합니다... (한번 더 누르면 즉시 중단)")
from sqlalchemy import func as sa_func
from processor import FinancialProcessor
from scorer import StockScorer
from database import SessionLocal
from models import Company, RawFinancialData, ProcessedFinancialData, ScoringResult, FetchHistory

def fetch_data(limit=None):
    """
    1단계: DART API와 yfinance에서 원본 데이터를 수집하여 DB(RawFinancialData)에 저장합니다.
    limit이 None이면 DB에 없는 모든 상장 종목을 가져옵니다.
    """
    print(f"\n========================================")
    limit_text = "전체" if limit is None else f"{limit}개"
    print(f"[1단계: 수집] {limit_text} 종목 원본 데이터 수집 시작")
    print(f"========================================\n")
    
    # macOS 등에서 SSL 인증서 에러 방지용
    ssl._create_default_https_context = ssl._create_unverified_context
    
    # 한국 거래소(KRX) 상장 종목 가져오기 (상장폐지된 종목 제외)
    print("한국거래소(KRX) 활성 상장 종목 목록을 불러옵니다...")
    krx_df = fdr.StockListing('KRX')
    
    # 시가총액(Marcap) 기준으로 내림차순 정렬하여 큰 기업부터 가져오도록 설정
    if 'Marcap' in krx_df.columns:
        krx_df = krx_df.sort_values('Marcap', ascending=False)
        
    active_tickers = krx_df['Code'].tolist()
    
    fetcher = DartFetcher()
    fetcher.load_corp_codes()
    
    # DART에 등록된 종목 중 현재 상장되어 있는 종목만 필터링 (시가총액 순서 유지)
    all_tickers = [t for t in active_tickers if t in fetcher.corp_codes]
    
    db = SessionLocal()
    today = date.today()
    
    # 오늘 이미 수집을 시도했던 종목(성공, 실패, 스킵 등)은 제외
    fetched_today = [
        h.ticker for h in db.query(FetchHistory.ticker)
        .filter(FetchHistory.fetch_date == today).all()
    ]
    db.close()
    
    # 오늘 아직 수집 시도하지 않은 종목만 필터링
    target_tickers = [t for t in all_tickers if t not in fetched_today]
    
    if limit is not None:
        target_tickers = target_tickers[:limit]
    
    total_count = len(target_tickers)
    if total_count == 0:
        print("오늘은 더 이상 수집할 종목이 없습니다. (모두 이미 수집 시도함)")
        return
        
    global _interrupted
    _interrupted = False
    original_handler = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, _handle_sigint)

    success_count = 0
    try:
        for i, ticker in enumerate(target_tickers):
            if _interrupted:
                break
            print(f"\n--- [{i+1}/{total_count}] 종목코드: {ticker} ---")
            try:
                fetcher.save_to_db(ticker)
                success_count += 1
                time.sleep(0.3)
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"{ticker} 처리 중 에러 발생: {e}")
    finally:
        signal.signal(signal.SIGINT, original_handler)

    if _interrupted:
        print(f"\n사용자에 의해 수집이 중단되었습니다. (수집 완료: {success_count}개)")
    else:
        print(f"\n수집 완료! (성공: {success_count}/{total_count})")

def fetch_us_data(limit=None):
    """
    1단계 (US): yfinance에서 미국 주식 원본 데이터를 수집하여 DB(RawFinancialData)에 저장합니다.
    yfinance screener로 미국 증시(NYSE, NASDAQ) 전체를 시가총액 순으로 가져옵니다.
    """
    print(f"\n========================================")
    limit_text = "전체" if limit is None else f"{limit}개"
    print(f"[1단계: 수집] 미국 증시 시가총액 상위 {limit_text} 종목 원본 데이터 수집 시작")
    print(f"========================================\n")

    ssl._create_default_https_context = ssl._create_unverified_context

    # yfinance screener로 미국 증시 종목을 시가총액 순으로 가져오기
    print("미국 증시 종목 목록을 불러옵니다 (시가총액 순)...")
    try:
        import yfinance as yf

        query = yf.EquityQuery('AND', [
            yf.EquityQuery('OR', [
                yf.EquityQuery('eq', ['exchange', 'NMS']),
                yf.EquityQuery('eq', ['exchange', 'NYQ'])
            ]),
            yf.EquityQuery('gt', ['intradaymarketcap', 0])
        ])

        # 필요한 만큼만 페이지를 가져옴 (페이지당 250개)
        fetch_count = limit if limit is not None else 500
        all_tickers = []
        for offset in range(0, fetch_count, 250):
            size = min(250, fetch_count - offset)
            result = yf.screen(query, sortField='intradaymarketcap', sortAsc=False, size=size, offset=offset)
            quotes = result.get('quotes', [])
            for q in quotes:
                all_tickers.append(q.get('symbol', ''))
            if len(quotes) < size:
                break  # 더 이상 결과가 없으면 중단

        total_available = result.get('total', '?')
        print(f"미국 증시 종목 {len(all_tickers)}개 로드 완료 (시가총액 순, 전체 약 {total_available}개)")
        if len(all_tickers) >= 3:
            print(f"   상위: {all_tickers[0]}, {all_tickers[1]}, {all_tickers[2]}...")

    except Exception as e:
        print(f"미국 증시 종목 목록 로드 실패: {e}")
        return

    fetcher = USFetcher()

    db = SessionLocal()
    today = date.today()

    # 오늘 이미 수집 시도한 종목 제외
    fetched_today = [
        h.ticker for h in db.query(FetchHistory.ticker)
        .filter(FetchHistory.fetch_date == today).all()
    ]
    db.close()

    # 우선주 제외: 티커에 '-P' + 알파벳 패턴 (JPM-PC, WFC-PY, BAC-PE 등)
    # S&P 500 등 주요 지수와 동일하게 보통주만 평가 대상으로 함
    preferred_excluded = [t for t in all_tickers if '-P' in t]
    if preferred_excluded:
        print(f"우선주 {len(preferred_excluded)}개 제외: {', '.join(preferred_excluded[:5])}{'...' if len(preferred_excluded) > 5 else ''}")
    target_tickers = [t for t in all_tickers if t not in fetched_today and '-P' not in t]

    if limit is not None:
        target_tickers = target_tickers[:limit]

    total_count = len(target_tickers)
    if total_count == 0:
        print("오늘은 더 이상 수집할 종목이 없습니다. (모두 이미 수집 시도함)")
        return

    global _interrupted
    _interrupted = False
    original_handler = signal.getsignal(signal.SIGINT)
    signal.signal(signal.SIGINT, _handle_sigint)

    success_count = 0
    try:
        for i, ticker in enumerate(target_tickers):
            if _interrupted:
                break
            print(f"\n--- [{i+1}/{total_count}] {ticker} ---")
            try:
                fetcher.save_to_db(ticker)
                success_count += 1
                time.sleep(0.1)
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"{ticker} 처리 중 에러 발생: {e}")
    finally:
        signal.signal(signal.SIGINT, original_handler)

    if _interrupted:
        print(f"\n사용자에 의해 수집이 중단되었습니다. (수집 완료: {success_count}개)")
    else:
        print(f"\n수집 완료! (성공: {success_count}/{total_count})")


def process_data(all_data=False):
    """
    2단계: DB에 저장된 원본 데이터를 바탕으로 가공 지표(EPS, PER 등)를 계산하여 DB(ProcessedFinancialData)에 저장합니다.
    all_data=False (기본값): 오늘 fetch된 종목만 처리
    all_data=True         : DB 전체 재처리
    """
    print(f"\n========================================")
    print(f"[2단계: 가공] 원본 데이터를 평가 지표로 가공 시작" + (" (전체)" if all_data else " (오늘 업데이트분)"))
    print(f"========================================\n")

    processor = FinancialProcessor()
    processor.process_all(today_only=not all_data)

def score_data(all_data=False):
    """
    3단계: 가공된 지표를 바탕으로 점수와 등급을 계산하여 DB(ScoringResult)에 저장합니다.
    all_data=False (기본값): 오늘 처리된 종목만 채점
    all_data=True         : DB 전체 재채점
    """
    print(f"\n========================================")
    print(f"[3단계: 평가] 가공 지표를 바탕으로 점수 산정 시작" + (" (전체)" if all_data else " (오늘 업데이트분)"))
    print(f"========================================\n")

    scorer = StockScorer()
    scorer.score_all(today_only=not all_data)

def get_display_width(s):
    """
    문자열의 실제 출력 너비를 계산합니다.
    한글(W, F)은 2칸, 영문/숫자 등은 1칸으로 계산합니다.
    """
    width = 0
    for char in str(s):
        status = unicodedata.east_asian_width(char)
        if status in ('W', 'F'):
            width += 2
        else:
            width += 1
    return width

def pad_string(s, total_width):
    """
    주어진 너비에 맞게 문자열 뒤에 공백을 채웁니다.
    """
    s = str(s)
    current_width = get_display_width(s)
    padding = total_width - current_width
    if padding > 0:
        return s + " " * padding
    return s

def show_leaderboard(limit=None, market=None):
    """
    4단계: DB에 저장된 평가 결과를 총점 기준으로 내림차순 정렬하여 출력합니다.
    (정성평가 만점 43점을 받아도 70점(B등급) 미만인 종목은 제외합니다. 즉, 현재 점수 27점 이상만 표시)
    limit: 출력할 최대 종목 수. None이면 임계값(27점) 이상 전체 출력.
    market: 'kr'이면 한국 종목만, 'us'이면 미국 종목만, None이면 전체
    """
    KR_MARKETS = ('KOSPI', 'KOSDAQ', 'KOSPI/KOSDAQ')

    db = SessionLocal()
    # 정성 평가 총점인 43점을 모두 획득했을 때의 점수 기준 필터링(이후 필요시 ScoringResult.total_score >= 의 값 수정하여 사용)
    try:
        # 종목당 가장 최근 평가 결과만 가져오기 위한 서브쿼리
        latest_score = db.query(
            ScoringResult.ticker,
            sa_func.max(ScoringResult.score_date).label('max_date')
        ).group_by(ScoringResult.ticker).subquery()

        query = db.query(ScoringResult, Company.name)\
            .join(Company, ScoringResult.ticker == Company.ticker)\
            .join(latest_score, (ScoringResult.ticker == latest_score.c.ticker) & (ScoringResult.score_date == latest_score.c.max_date))\
            .filter(ScoringResult.total_score >= 27)

        # 시장 필터링
        if market == 'kr':
            query = query.filter(Company.market.in_(KR_MARKETS))
        elif market == 'us':
            query = query.filter(~Company.market.in_(KR_MARKETS))

        query = query.order_by(ScoringResult.total_score.desc(), ScoringResult.grade.asc())
        if limit is not None:
            query = query.limit(limit)
        results = query.all()

        if not results:
            print("표시할 평가 결과가 없습니다. (27점 이상인 종목이 없거나 평가를 진행하지 않았습니다.)")
            return

        market_label = "한국(KR)" if market == 'kr' else "미국(US)" if market == 'us' else "전체(KR+US)"
        limit_label = f"Top {limit}" if limit is not None else f"전체 {len(results)}개"
        print(f"\n{'=' * 160}")
        print(f"우량주 평가 리더보드 [{market_label}] ({limit_label}) - 정성평가 만점 시 70점 이상 가능한 종목만 표시 (현재 27점 이상)")
        print(f"{'=' * 160}")
        
        # 헤더 출력
        header = f"{pad_string('순위', 5)} | {pad_string('종목명', 16)} | {pad_string('종목코드', 8)} | {pad_string('정량합계(/57)', 14)} | {pad_string('PER(/20)', 12)} | {pad_string('PBR(/5)', 12)} | {pad_string('배당수익(/10)', 14)} | {pad_string('분기배당(/5)', 12)} | {pad_string('배당인상(/5)', 12)} | {pad_string('자사주비율(/5)', 15)} | {pad_string('매입소각(/7)', 13)}"
        print(header)
        print("-" * 160)

        for i, (score, company_name) in enumerate(results, 1):
            # 실제 값을 가져오기 위해 Raw, Processed 데이터 조회
            raw = db.query(RawFinancialData).filter(RawFinancialData.ticker == score.ticker).order_by(RawFinancialData.record_date.desc()).first()
            processed = db.query(ProcessedFinancialData).filter(ProcessedFinancialData.ticker == score.ticker).order_by(ProcessedFinancialData.record_date.desc()).first()

            # 값 포맷팅 (예: 12.3(5점))
            per_str = f"{processed.per:.1f} ({score.score_per}점)" if processed and processed.per > 0 else f"-({score.score_per}점)"
            pbr_str = f"{processed.pbr:.2f} ({score.score_pbr}점)" if processed and processed.pbr > 0 else f"-({score.score_pbr}점)"
            div_yield_str = f"{processed.dividend_yield:.1f}%({score.score_div_yield}점)" if processed else f"-({score.score_div_yield}점)"

            q_div_str = f"{'O' if raw and raw.quarterly_dividend else 'X'}({score.score_div_quarter}점)"
            div_inc_str = f"{raw.dividend_increase_years}년({score.score_div_inc}점)" if raw else f"-({score.score_div_inc}점)"

            treasury_str = f"{processed.treasury_share_ratio:.2f}%({score.score_treasury_ratio}점)" if processed else f"-({score.score_treasury_ratio}점)"
            buyback_str = f"{'O' if raw and raw.share_buyback_cancel else 'X'}({score.score_buyback}점)"

            # 종목명이 너무 길면 자르기 (한글 너비 고려)
            name_display = company_name
            if get_display_width(name_display) > 14:
                temp_width = 0
                temp_name = ""
                for char in name_display:
                    w = 2 if unicodedata.east_asian_width(char) in ('W', 'F') else 1
                    if temp_width + w > 11:
                        break
                    temp_name += char
                    temp_width += w
                name_display = temp_name + "..."

            row = f"{pad_string(i, 5)} | {pad_string(name_display, 16)} | {pad_string(score.ticker, 8)} | {pad_string(score.total_score, 14)} | {pad_string(per_str, 12)} | {pad_string(pbr_str, 12)} | {pad_string(div_yield_str, 14)} | {pad_string(q_div_str, 12)} | {pad_string(div_inc_str, 12)} | {pad_string(treasury_str, 15)} | {pad_string(buyback_str, 13)}"
            print(row)

        print("=" * 160 + "\n")
    finally:
        db.close()

def show_results(ticker):
    """
    특정 종목의 상세 평가 결과를 출력합니다.
    """
    db = SessionLocal()
    try:
        company = db.query(Company).filter(Company.ticker == ticker).first()
        if not company:
            print(f"DB에서 종목코드 {ticker}를 찾을 수 없습니다.")
            return

        raw = db.query(RawFinancialData).filter(RawFinancialData.ticker == ticker).order_by(RawFinancialData.record_date.desc()).first()
        processed = db.query(ProcessedFinancialData).filter(ProcessedFinancialData.ticker == ticker).order_by(ProcessedFinancialData.record_date.desc()).first()
        score = db.query(ScoringResult).filter(ScoringResult.ticker == ticker).order_by(ScoringResult.score_date.desc()).first()

        if not (raw and processed and score):
            print("평가 결과 데이터가 충분하지 않습니다. 수집/가공/평가 단계를 모두 거쳤는지 확인해 주세요.")
            return

        # 시장에 따른 통화 단위 결정
        is_us = company.market and company.market.upper() not in ('KOSPI', 'KOSDAQ', 'KOSPI/KOSDAQ')
        currency = '$' if is_us else '원'
        price_fmt = lambda v: f"${v:,.2f}" if is_us else f"{v:,.0f} 원"

        print(f"========================================")
        print(f"📊 [{company.name} ({ticker})] 평가 결과 보고서")
        print(f"   시장: {company.market} / 기준일자: {raw.record_date} / 평가일자: {score.score_date}")
        print(f"========================================\n")

        print("1️⃣ [수집된 원본 데이터 (Raw Data)]")
        print(f"  - 현재 주가: {price_fmt(raw.current_price)}")
        print(f"  - 당기순이익: {price_fmt(raw.net_income)}")
        print(f"  - 자본총계(순자산): {price_fmt(raw.total_equity)}")
        print(f"  - 총 유통주식수: {raw.total_shares:,.0f} 주 (자사주 제외)")
        print(f"  - 자기주식수(자사주): {raw.treasury_shares:,.0f} 주")
        print(f"  - 1주당 연간 배당금: {price_fmt(raw.dividend_per_share)}")
        print(f"  - 분기 배당 실시: {'예' if raw.quarterly_dividend else '아니오'}")
        print(f"  - 배당 연속 인상: {raw.dividend_increase_years} 년")
        print(f"  - 정기적 자사주 매입 및 소각: {'예' if raw.share_buyback_cancel else '아니오'}\n")

        print("2️⃣ [가공된 평가 지표 (Processed Data)]")
        print(f"  - EPS (주당순이익): {price_fmt(processed.eps)} (당기순이익 / 유통주식수)")
        print(f"  - PER (주가수익비율): {processed.per:.2f} 배 (현재주가 / EPS)")
        print(f"  - PBR (주가순자산비율): {processed.pbr:.2f} 배 (현재주가 / BPS)")
        print(f"  - 배당수익률: {processed.dividend_yield:.2f} % (주당배당금 / 현재주가 * 100)")
        print(f"  - 자사주 보유 비율: {processed.treasury_share_ratio:.2f} % (자사주 / 총발행주식수 * 100)\n")

        print("3️⃣ [최종 평가 점수 (Scoring Results)]")
        print(f"  - PER 점수: {score.score_per} / 20 점")
        print(f"  - PBR 점수: {score.score_pbr} / 5 점")
        print(f"  - 배당수익률 점수: {score.score_div_yield} / 10 점")
        print(f"  - 분기 배당 점수: {score.score_div_quarter} / 5 점")
        print(f"  - 배당 인상 점수: {score.score_div_inc} / 5 점")
        print(f"  - 자사주 비율 점수: {score.score_treasury_ratio} / 5 점")
        print(f"  - 자사주 매입/소각 점수: {score.score_buyback} / 7 점")
        print("  --------------------------------------")
        print(f"  - 정량적 합계: {score.total_score} 점 (정성적 평가 0점 처리 중)")
        # print(f"  - 최종 투자 등급: [{score.grade}] 등급\n")

    finally:
        db.close()

def export_data(market=None):
    """
    5단계: 정성적 평가를 수행할 수 있도록 현재 27점 이상인 종목들을 CSV(엑셀 호환) 파일로 내보냅니다.
    market: 'kr'이면 한국 종목만, 'us'이면 미국 종목만, None이면 전체
    """
    KR_MARKETS = ('KOSPI', 'KOSDAQ', 'KOSPI/KOSDAQ')

    db = SessionLocal()
    try:
        # 종목당 가장 최근 평가 결과만 가져오기
        latest_score = db.query(
            ScoringResult.ticker,
            sa_func.max(ScoringResult.score_date).label('max_date')
        ).group_by(ScoringResult.ticker).subquery()

        query = db.query(ScoringResult, Company.name)\
            .join(Company, ScoringResult.ticker == Company.ticker)\
            .join(latest_score, (ScoringResult.ticker == latest_score.c.ticker) & (ScoringResult.score_date == latest_score.c.max_date))\
            .filter(ScoringResult.total_score >= 27)

        # 시장 필터링
        if market == 'kr':
            query = query.filter(Company.market.in_(KR_MARKETS))
        elif market == 'us':
            query = query.filter(~Company.market.in_(KR_MARKETS))

        results = query.order_by(ScoringResult.total_score.desc()).all()

        if not results:
            print("내보낼 데이터가 없습니다. (27점 이상인 종목이 없습니다.)")
            return

        # export 폴더 생성 (없으면)
        export_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'export')
        os.makedirs(export_dir, exist_ok=True)

        # 동일 날짜에는 덮어쓰기 (날짜 기준 파일명, 시분초 제외)
        market_suffix = f"_{market}" if market else ""
        filename = os.path.join(export_dir, f"holdit_{market_suffix}_{date.today().strftime('%Y%m%d')}.csv")

        # utf-8-sig를 사용하여 엑셀에서 한글이 깨지지 않도록 함
        with open(filename, 'w', newline='\n', encoding='utf-8-sig') as f:
            writer = csv.writer(f)

            # 헤더 작성
            # A~D: 종목 정보 | E~R: 정량 항목(값+점수) | S~X: 정성 항목(빈칸) | Y: 정량합계 | Z: 정성합계 | AA: 최종점수 | AB: 메모
            headers = [
                '순위', '종목명', '종목코드', '시장',                              # A~D
                'PER', 'PER점수(/20)', 'PBR', 'PBR점수(/5)',                       # E~H
                '배당수익률(%)', '배당수익률점수(/10)',                              # I~J
                '분기배당', '분기배당점수(/5)',                                      # K~L
                '배당연속인상(년)', '배당인상점수(/5)',                              # M~N
                '자사주비율(%)', '자사주비율점수(/5)',                               # O~P
                '자사주매입소각', '매입소각점수(/7)',                                # Q~R
                '이익_지속가능성(5)', '중복_상장여부(5)', '연간_소각비율(8)',       # S~U
                '미래_성장잠재력(10)', '기업_경영(10)', '세계적_브랜드(5)',         # V~X
                '정량합계', '정성합계', '최종점수',                                  # Y~AA
                '메모'                                                               # AB
            ]
            writer.writerow(headers)

            # 데이터 작성
            for i, (score, name) in enumerate(results, 1):
                raw = db.query(RawFinancialData).filter(RawFinancialData.ticker == score.ticker).order_by(RawFinancialData.record_date.desc()).first()
                processed = db.query(ProcessedFinancialData).filter(ProcessedFinancialData.ticker == score.ticker).order_by(ProcessedFinancialData.record_date.desc()).first()

                company = db.query(Company).filter(Company.ticker == score.ticker).first()
                market_name = company.market if company else ''

                per_val = f"{processed.per:.1f}" if processed and processed.per > 0 else '-'
                pbr_val = f"{processed.pbr:.2f}" if processed and processed.pbr > 0 else '-'
                div_yield_val = f"{processed.dividend_yield:.1f}" if processed else '-'
                q_div_val = 'O' if raw and raw.quarterly_dividend else 'X'
                div_inc_val = raw.dividend_increase_years if raw else '-'
                treasury_val = f"{processed.treasury_share_ratio:.1f}" if processed else '-'
                buyback_val = 'O' if raw and raw.share_buyback_cancel else 'X'

                # 엑셀 수식
                # 정성합계(Z열): 정성 항목 6개(S~X) 합산
                # 최종점수(AA열): 정량합계(Y) + 정성합계(Z)
                row_num = i + 1
                qualitative_sum = f"=SUM(S{row_num}:X{row_num})"
                total_formula = f"=Y{row_num}+Z{row_num}"

                row = [
                    i, name, score.ticker, market_name,                 # A~D
                    per_val, score.score_per, pbr_val, score.score_pbr, # E~H
                    div_yield_val, score.score_div_yield,                # I~J
                    q_div_val, score.score_div_quarter,                  # K~L
                    div_inc_val, score.score_div_inc,                    # M~N
                    treasury_val, score.score_treasury_ratio,            # O~P
                    buyback_val, score.score_buyback,                    # Q~R
                    '', '', '', '', '', '',                               # S~X (정성, 빈칸)
                    score.total_score, qualitative_sum, total_formula,   # Y~AA
                    ''                                                    # AB (메모)
                ]
                writer.writerow(row)
                
        market_label = "한국(KR)" if market == 'kr' else "미국(US)" if market == 'us' else "전체(KR+US)"
        print(f"\n========================================")
        print(f"데이터 내보내기 완료: {filename}")
        print(f"   - 대상: {market_label} / 총 {len(results)}개 종목 (정량점수 27점 이상)")
        print(f"   - 엑셀에서 파일을 열어 E열~J열에 정성적 평가 점수를 기입하시면,")
        print(f"   - K열(최종_예상점수)에 총점이 자동으로 계산됩니다!")
        print(f"========================================\n")
        
    finally:
        db.close()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "fetch":
            if len(sys.argv) < 3:
                print("사용법: python3 main.py fetch <kr|us> <개수|all>")
                sys.exit(1)
            market = sys.argv[2].lower()
            arg = sys.argv[3] if len(sys.argv) > 3 else "10"
            limit = None if arg.lower() == "all" else int(arg)
            if market == "kr":
                fetch_data(limit)
            elif market == "us":
                fetch_us_data(limit)
            else:
                print("시장 구분은 'kr' 또는 'us'를 입력해주세요.")
            
        elif command == "refetch":
            # refetch <ticker> : FetchHistory 무관하게 특정 종목 강제 재수집
            if len(sys.argv) < 3:
                print("사용법: python3 main.py refetch <종목코드>  (예: refetch 316140, refetch AAPL)")
                sys.exit(1)
            target = sys.argv[2].upper()
            # KR vs US 구분: 숫자 6자리면 KR
            if target.isdigit():
                fetcher_kr = DartFetcher()
                fetcher_kr.load_corp_codes()
                print(f"\n--- [강제 재수집] 종목코드: {target} ---")
                fetcher_kr.save_to_db(target)
            else:
                fetcher_us = USFetcher()
                print(f"\n--- [강제 재수집] 티커: {target} ---")
                fetcher_us.save_to_db(target)

        elif command == "process":
            all_flag = '--all' in sys.argv
            process_data(all_flag)

        elif command == "score":
            all_flag = '--all' in sys.argv
            score_data(all_flag)
            
        elif command == "view":
            # view / view 50 / view kr / view us / view kr 50 / view us 50
            # 개수를 지정하지 않으면 임계값(27점) 이상 전체 출력
            market = None
            limit = None
            if len(sys.argv) > 2:
                arg2 = sys.argv[2].lower()
                if arg2 in ('kr', 'us'):
                    market = arg2
                    limit = int(sys.argv[3]) if len(sys.argv) > 3 else None
                else:
                    limit = int(arg2)
            show_leaderboard(limit, market)
            
        elif command == "detail":
            target_ticker = sys.argv[2] if len(sys.argv) > 2 else '005930'
            show_results(target_ticker)
            
        elif command == "export":
            # export / export kr / export us
            market = None
            if len(sys.argv) > 2 and sys.argv[2].lower() in ('kr', 'us'):
                market = sys.argv[2].lower()
            export_data(market)
            
        else:
            print("알 수 없는 명령어입니다.")
    else:
        print("사용법:")
        print("  python3 main.py fetch kr <개수|all>  : [1단계] 한국 주식 원본 데이터 수집 (예: fetch kr 100, fetch kr all)")
        print("  python3 main.py fetch us <개수|all>  : [1단계] 미국 주식 원본 데이터 수집 - 시가총액 순 (예: fetch us 50)")
        print("  python3 main.py refetch <종목코드>   : [1단계] 특정 종목 강제 재수집 (오늘 이미 수집한 종목도 재수집, 예: refetch 316140, refetch AAPL)")
        print("  python3 main.py process [--all]      : [2단계] 가공 지표 계산 (기본: 오늘 fetch분만 / --all: 전체 재처리)")
        print("  python3 main.py score [--all]        : [3단계] 점수 산정 (기본: 오늘 처리분만 / --all: 전체 재채점)")
        print("  python3 main.py view [kr|us] [개수]  : [4단계] 리더보드 조회 - 개수 미지정 시 임계값 이상 전체 출력 (예: view, view kr, view us 50)")
        print("  python3 main.py detail <종목>       : 특정 종목의 상세 평가 결과 조회 (예: 005930, KO)")
        print("  python3 main.py export [kr|us]       : [5단계] 정성 평가용 엑셀(CSV) 파일 내보내기 (예: export, export kr, export us)")