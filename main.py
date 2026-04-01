import sys
import unicodedata
from fetcher import DartFetcher
from processor import FinancialProcessor
from scorer import StockScorer
from database import SessionLocal
from datetime import date
from models import Company, RawFinancialData, ProcessedFinancialData, ScoringResult, FetchHistory

def fetch_data(limit=None):
    """
    1단계: DART API와 yfinance에서 원본 데이터를 수집하여 DB(RawFinancialData)에 저장합니다.
    limit이 None이면 DB에 없는 모든 상장 종목을 가져옵니다.
    """
    print(f"\n========================================")
    limit_text = "전체" if limit is None else f"{limit}개"
    print(f"📥 [1단계: 수집] {limit_text} 종목 원본 데이터 수집 시작")
    print(f"========================================\n")
    
    import FinanceDataReader as fdr
    import ssl
    import urllib.request
    import time
    
    # macOS 등에서 SSL 인증서 에러 방지용
    ssl._create_default_https_context = ssl._create_unverified_context
    
    # 한국 거래소(KRX) 상장 종목 가져오기 (상장폐지된 종목 제외)
    print("📈 한국거래소(KRX) 활성 상장 종목 목록을 불러옵니다...")
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
        
    success_count = 0
    for i, ticker in enumerate(target_tickers):
        print(f"\n--- [{i+1}/{total_count}] 종목코드: {ticker} ---")
        try:
            fetcher.save_to_db(ticker)
            success_count += 1
            # API 호출 제한(Rate Limit) 방지를 위해 약간의 대기 시간 추가
            time.sleep(0.5)
        except Exception as e:
            print(f"❌ {ticker} 처리 중 에러 발생: {e}")
            
    print(f"\n✅ 수집 완료! (성공: {success_count}/{total_count})")

def process_data():
    """
    2단계: DB에 저장된 원본 데이터를 바탕으로 가공 지표(EPS, PER 등)를 계산하여 DB(ProcessedFinancialData)에 저장합니다.
    """
    print(f"\n========================================")
    print(f"⚙️ [2단계: 가공] 원본 데이터를 평가 지표로 가공 시작")
    print(f"========================================\n")
    
    processor = FinancialProcessor()
    processor.process_all()

def score_data():
    """
    3단계: 가공된 지표를 바탕으로 점수와 등급을 계산하여 DB(ScoringResult)에 저장합니다.
    """
    print(f"\n========================================")
    print(f"🧮 [3단계: 평가] 가공 지표를 바탕으로 점수 산정 시작")
    print(f"========================================\n")
    
    scorer = StockScorer()
    scorer.score_all()

import unicodedata

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

def show_leaderboard(limit=50):
    """
    4단계: DB에 저장된 평가 결과를 총점 기준으로 내림차순 정렬하여 출력합니다.
    (정성평가 만점 43점을 받아도 70점(B등급) 미만인 종목은 제외합니다. 즉, 현재 점수 27점 이상만 표시)
    """
    db = SessionLocal()
    # 정성 평가 총점인 43점을 모두 획득했을 때의 점수 기준 필터링(이후 필요시 ScoringResult.total_score >= 의 값 수정하여 사용)
    try:
        results = db.query(ScoringResult, Company.name)\
            .join(Company, ScoringResult.ticker == Company.ticker) \
            .filter(ScoringResult.total_score >= 27)\
            .order_by(ScoringResult.total_score.desc(), ScoringResult.grade.asc())\
            .limit(limit).all()

        if not results:
            print("❌ 표시할 평가 결과가 없습니다. (27점 이상인 종목이 없거나 평가를 진행하지 않았습니다.)")
            return

        print(f"\n=====================================================================================================================================")
        print(f"🏆 우량주 평가 리더보드 (Top {limit}) - 정성평가 만점 시 70점 이상 가능한 종목만 표시 (현재 27점 이상)")
        print(f"=====================================================================================================================================")
        
        # 헤더 출력
        header = f"{pad_string('순위', 5)} | {pad_string('종목명', 16)} | {pad_string('종목코드', 8)} | {pad_string('총점', 5)} | {pad_string('등급', 4)} | {pad_string('PER', 4)} | {pad_string('PBR', 4)} | {pad_string('배당수익', 8)} | {pad_string('분기배당', 8)} | {pad_string('배당인상', 8)} | {pad_string('자사주비율', 10)} | {pad_string('매입소각', 8)}"
        print(header)
        print("-" * 133)

        for i, (score, company_name) in enumerate(results, 1):
            # 종목명이 너무 길면 자르기 (한글 너비 고려)
            name_display = company_name
            if get_display_width(name_display) > 14:
                # 대략적으로 자르기
                temp_width = 0
                temp_name = ""
                for char in name_display:
                    w = 2 if unicodedata.east_asian_width(char) in ('W', 'F') else 1
                    if temp_width + w > 11:
                        break
                    temp_name += char
                    temp_width += w
                name_display = temp_name + "..."

            row = f"{pad_string(i, 5)} | {pad_string(name_display, 16)} | {pad_string(score.ticker, 8)} | {pad_string(score.total_score, 5)} | {pad_string(score.grade, 4)} | {pad_string(score.score_per, 4)} | {pad_string(score.score_pbr, 4)} | {pad_string(score.score_div_yield, 8)} | {pad_string(score.score_div_quarter, 8)} | {pad_string(score.score_div_inc, 8)} | {pad_string(score.score_treasury_ratio, 10)} | {pad_string(score.score_buyback, 8)}"
            print(row)
            
        print("=====================================================================================================================================\n")
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
            print(f"❌ DB에서 종목코드 {ticker}를 찾을 수 없습니다.")
            return

        raw = db.query(RawFinancialData).filter(RawFinancialData.ticker == ticker).order_by(RawFinancialData.record_date.desc()).first()
        processed = db.query(ProcessedFinancialData).filter(ProcessedFinancialData.ticker == ticker).order_by(ProcessedFinancialData.record_date.desc()).first()
        score = db.query(ScoringResult).filter(ScoringResult.ticker == ticker).order_by(ScoringResult.score_date.desc()).first()

        if not (raw and processed and score):
            print("❌ 평가 결과 데이터가 충분하지 않습니다. 수집/가공/평가 단계를 모두 거쳤는지 확인해 주세요.")
            return

        print(f"========================================")
        print(f"📊 [{company.name} ({ticker})] 평가 결과 보고서")
        print(f"   기준일자: {raw.record_date} / 평가일자: {score.score_date}")
        print(f"========================================\n")

        print("1️⃣ [수집된 원본 데이터 (Raw Data)]")
        print(f"  - 현재 주가: {raw.current_price:,.0f} 원")
        print(f"  - 당기순이익: {raw.net_income:,.0f} 원")
        print(f"  - 자본총계(순자산): {raw.total_equity:,.0f} 원")
        print(f"  - 총 유통주식수: {raw.total_shares:,.0f} 주 (자사주 제외)")
        print(f"  - 자기주식수(자사주): {raw.treasury_shares:,.0f} 주")
        print(f"  - 1주당 연간 배당금: {raw.dividend_per_share:,.0f} 원")
        print(f"  - 분기 배당 실시: {'예' if raw.quarterly_dividend else '아니오'}")
        print(f"  - 배당 연속 인상: {raw.dividend_increase_years} 년")
        print(f"  - 정기적 자사주 매입 및 소각: {'예' if raw.share_buyback_cancel else '아니오'}\n")

        print("2️⃣ [가공된 평가 지표 (Processed Data)]")
        print(f"  - EPS (주당순이익): {processed.eps:,.0f} 원 (당기순이익 / 유통주식수)")
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
        print(f"  - 최종 투자 등급: [{score.grade}] 등급\n")

    finally:
        db.close()

def export_data():
    """
    5단계: 정성적 평가를 수행할 수 있도록 현재 27점 이상인 종목들을 CSV(엑셀 호환) 파일로 내보냅니다.
    """
    import csv
    from datetime import datetime
    
    db = SessionLocal()
    try:
        results = db.query(ScoringResult, Company.name)\
            .join(Company, ScoringResult.ticker == Company.ticker)\
            .filter(ScoringResult.total_score >= 27)\
            .order_by(ScoringResult.total_score.desc())\
            .all()

        if not results:
            print("❌ 내보낼 데이터가 없습니다. (27점 이상인 종목이 없습니다.)")
            return

        filename = f"holdit_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        
        # utf-8-sig를 사용하여 엑셀에서 한글이 깨지지 않도록 함
        with open(filename, 'w', newline='', encoding='utf-8-sig') as f:
            writer = csv.writer(f)
            
            # 헤더 작성
            headers = [
                '순위', '종목명', '종목코드', '현재_정량점수', 
                '이익_지속가능성(5)', '중복_상장여부(5)', '연간_소각비율(8)', 
                '미래_성장잠재력(10)', '기업_경영(10)', '세계적_브랜드(5)', '최종_예상점수'
            ]
            writer.writerow(headers)
            
            # 데이터 작성
            for i, (score, name) in enumerate(results, 1):
                # 엑셀 수식: 현재 정량점수(D열) + 정성평가 항목들(E~J열) 합산
                # 엑셀의 행은 1부터 시작하고 헤더가 1행이므로, 데이터는 2행부터 시작 (i+1)
                row_num = i + 1
                excel_formula = f"=D{row_num}+SUM(E{row_num}:J{row_num})"
                
                row = [
                    i, name, score.ticker, score.total_score,
                    '', '', '', '', '', '', excel_formula
                ]
                writer.writerow(row)
                
        print(f"\n========================================")
        print(f"✅ 데이터 내보내기 완료: {filename}")
        print(f"   - 총 {len(results)}개 종목 (정량점수 27점 이상)")
        print(f"   - 엑셀에서 파일을 열어 E열~J열에 정성적 평가 점수를 기입하시면,")
        print(f"   - K열(최종_예상점수)에 총점이 자동으로 계산됩니다!")
        print(f"========================================\n")
        
    finally:
        db.close()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        command = sys.argv[1]
        
        if command == "fetch":
            arg = sys.argv[2] if len(sys.argv) > 2 else "10"
            limit = None if arg.lower() == "all" else int(arg)
            fetch_data(limit)
            
        elif command == "process":
            process_data()
            
        elif command == "score":
            score_data()
            
        elif command == "view":
            limit = int(sys.argv[2]) if len(sys.argv) > 2 else 50
            show_leaderboard(limit)
            
        elif command == "detail":
            target_ticker = sys.argv[2] if len(sys.argv) > 2 else '005930'
            show_results(target_ticker)
            
        elif command == "export":
            export_data()
            
        else:
            print("❌ 알 수 없는 명령어입니다.")
    else:
        print("사용법:")
        print("  python3 main.py fetch <개수|all> : [1단계] DART/yfinance에서 원본 데이터 수집 (예: fetch 100, fetch all)")
        print("  python3 main.py process          : [2단계] DB의 원본 데이터를 바탕으로 가공 지표 계산")
        print("  python3 main.py score            : [3단계] 가공된 지표를 바탕으로 점수 산정 및 등급 부여")
        print("  python3 main.py view <개수>      : [4단계] 전체 종목 리더보드 조회 (기본: 50개)")
        print("  python3 main.py detail <종목>    : 특정 종목의 상세 평가 결과 조회 (예: 005930)")
        print("  python3 main.py export           : [5단계] 정성 평가용 엑셀(CSV) 파일 내보내기 (27점 이상 종목)")