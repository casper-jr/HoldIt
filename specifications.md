# 기능 명세
## 개요
국내, 해외의 저평가된 우량주를 scoring_guideline.md의 기준에 따라 평가하여 조회할 수 있게 하는 것을 목표로 함
- **한국 주식**: DART API + yfinance를 통해 데이터 수집
- **미국 주식**: yfinance만으로 재무제표 + 시장 데이터를 모두 수집 (yfinance screener로 미국 증시 전체를 시가총액 순으로 조회)
scoring_guideline.md에 의한 평가를 진행한 뒤 점수 기준으로 정렬해서 보는 것과 같은 기능 제공

## 필요 정보
아래 정보들 중 정량적으로 얻을 수 있는 정보들을 프로그램을 통해 얻고, 이후에 이외 판단이 필요한 값들은 사용자가 직접 고려하여 선정

국내, 해외 주식에 대해서 다음 값들이 필요
- 현재값(계산 용이성을 위해 종가 기준과 같이 선정 가능) - **[구현 완료: yfinance]**
- EPS(가장 최근 분기 또는 작년 값 사용, 당기순이익/발행주식 으로 계산) - **[구현 완료: KR=DART API 사업보고서(연간) / US=yfinance quarterly_income_stmt 최근 4분기 합산(TTM)]**
  * *KR 참고: 장기투자 관점에서 분기별 실적 변동에 의한 왜곡을 막기 위해 항상 가장 최근의 '사업보고서(1년 치 최종 확정 실적)'를 기준으로 계산.*
  * *US 참고: 미국 주식은 Naver 증권 등 국내 금융 사이트의 EPS 계산 기준("최근 분기 합산 순이익")에 맞춰 최근 4개 분기 합산(TTM) 방식을 사용. 연간 보고서 단일 값은 Seagen 인수 후 상각 등 대규모 일회성 비용이 집중된 해에 TTM과 크게 달라질 수 있어 TTM 채택.*
  * *데이터 출처 간 오차 허용 범위: yfinance와 외부 사이트 간 EPS/PER의 차이가 발생할 수 있음. 원인은 (1) TTM 집계 구간 타이밍 차이 — 양쪽의 데이터 갱신 시점에 따라 "최근 4분기"에 포함되는 분기가 다를 수 있음, (2) 비지배지분 처리 방식 차이. PBR/BPS는 분기 말 시점 기준이라 사이트 간 거의 일치함. 점수 산정 시 PER 구간 경계선 근처 종목은 출처에 따라 다른 점수가 나올 수 있음.*
- PER(현재값/EPS 로 계산) - **[구현 완료]**
  * *US ADR 통화 불일치 보정: 주가 통화(`currency`)와 재무제표 통화(`financialCurrency`)가 다른 종목(ITUB 등)은 자체 계산 대신 yfinance `info`의 `trailingPE` / `priceToBook` 직접 사용 (`processor.py`에서 처리)*
- PBR(가장 최근 분기 값) - **[구현 완료: KR=DART API 최근 분기 자본총계 / US=yfinance quarterly_balance_sheet]**
- 배당수익률(한 주당 연간 배당금 / 현재 주가 x 100) - **[구현 완료: yfinance 작년도 연간 총 배당금]**
  * *참고: 배당금은 장기투자 관점에서의 일관성을 위해 최근 1년(TTM) 기준이 아닌 '작년도 연간 총 배당금'을 확정값으로 사용합니다.*
- 분기 배당 여부 - **[구현 완료: yfinance 배당 내역 분석]**
- 배당 인상 기록(지급 기록) - **[구현 완료: yfinance 연도별 배당금 분석]**
- 정기적 자사주 매입 및 소각 여부 - **[구현 완료: KR=DART API 공시검색 / US=yfinance cashflow Repurchase 확인]**
- 매입 및 소각을 하는 경우 연간 소각 비율(소각 계획 주식 수, 총 주식수로 계산) - **[미구현: API 한계로 수동 입력 또는 보류]**
- 자사주 보유 비율 - **[구현 완료: KR=DART API 주식총수현황 / US=yfinance balance_sheet Treasury Shares]**

이외 판단이 필요한 값들
- 기업의 이익 지속 가능성
- 중복 상장 여부
- 세계적 브랜드 보유 여부

## 기술적 요구사항
사용 언어
- 파이썬
환경
- 도커에서 PostgreSQL 사용
- API나 크롤링을 통해 위의 필요 정보들을 가져와 DB에 저장할 수 있도록 함
- DB에 저장할 때 날짜 정보를 포함하여 분기, 월 단위와 같이 주기적으로 날짜를 기준으로 저장할 수 있게 하여 이후에 날짜에 따른 변화를 확인할 수 있도록 함

## 기능적 요구사항
1. 국내/해외 상장된 주식 정보들을 불러와서 DB에 불러온 날짜 정보와 함께 저장
   - **한국 (KR)**: 시가총액 기준 상위 300~500개 종목 권장 (배당 없는 종목 자동 제외 → 실제 100~200개 내외로 압축). `FinanceDataReader.StockListing('KRX')`로 KRX 전체 종목을 시가총액(`Marcap`) 순으로 정렬.
   - **미국 (US)**: yfinance screener(`yf.screen()`)를 통해 NYSE(NYQ)/NASDAQ(NMS) 전체를 시가총액(`intradaymarketcap`) 순으로 수집 (S&P 500에 한정되지 않음, 배당 없는 종목 자동 제외)
   - **미국 우선주 자동 제외**: 티커에 `-P` 패턴이 포함된 종목(`JPM-PC`, `WFC-PY`, `BAC-PE` 등)은 fetch 단계에서 수집 대상에서 제외. S&P 500 등 주요 지수와 동일하게 보통주(Common Stock)만 평가 대상으로 함. 우선주는 고정 배당 상품으로 PER/PBR 계산 모델의 전제와 맞지 않아 점수가 왜곡됨.
2. 불러온 정보를 기반으로 이후에 진행할 평가에 실제로 사용될 값들만 따로 테이블을 만들어서 저장(가공 과정)
3. 가공된 정보들을 이용하여 scoring_guidelines에 따른 점수 계산을 수행하여 결과를 날짜와 함께 저장
4. 이후에 점수 계산 결과를 조회할 수 있도록 하고, 필요시 각 종목의 세부지표들의 값과 지표별 점수 추이를 조회할 수 있도록 함
   - **시장 필터링**: `view kr`, `view us`, `view`(전체)로 시장별 리더보드 조회 가능. 개수 지정도 가능 (예: `view us 100`). 시장 지정 시 기본 50개, 전체 조회 시 기본 100개.
   - **조회 필터링**: 정성평가 만점(43점)을 획득하더라도 70점(B등급) 미만인 종목은 리더보드(`view`)에서 자동으로 제외됨 (현재 정량점수 27점 이상만 표시)
   - **중복 방지**: `score`를 여러 날짜에 실행해도 `view`/`export`에서는 종목당 가장 최근 평가일의 결과 1건만 표시
   - **통화 표시**: `detail` 조회 시 한국 종목은 `원`, 미국 종목은 `$`로 자동 표시
5. **데이터 내보내기**: 수집 시점에서의 결과를 엑셀(CSV) 형식으로 Export하여, 사용자가 직접 정성적 평가 항목의 점수를 기입하고 최종 점수를 확인할 수 있도록 지원
   - **시장 필터링**: `export kr`, `export us`, `export`(전체) 지원
   - **저장 위치**: `export/holdit_<market>_<YYYYMMDD>.csv` (동일 날짜 재실행 시 덮어쓰기)
   - **항목 구성**: 정량 항목별로 측정값과 점수를 분리 출력 (예: PER 값 + PER점수(/20)), 정성 평가 6개 항목은 빈칸, 마지막 열에 엑셀 수식(정량합계 + 정성항목 SUM) 자동 삽입

## 사용 DB 스키마

기능적 요구사항에 맞추어 데이터의 수집(Raw), 가공(Processed), 정성적 평가(Qualitative), 점수 결과(Scoring)를 분리하여 이력을 관리할 수 있도록 설계합니다.

### 1. `companies` (기업 마스터 테이블)
- `ticker` (VARCHAR, PK): 종목코드 (예: '005930', 'AAPL')
- `name` (VARCHAR): 기업명
- `market` (VARCHAR): 시장 구분 (KOSPI, KOSDAQ, NYSE, NASDAQ 등)
- `created_at` (TIMESTAMP): 등록일시

### 2. `raw_financial_data` (원본 재무/주가 데이터 테이블)
API나 크롤링을 통해 수집한 가공 전 원본 데이터를 날짜별로 저장합니다.
- `id` (SERIAL, PK)
- `ticker` (VARCHAR, FK -> companies.ticker)
- `record_date` (DATE): 수집/기준 일자 (가장 최근 분기/반기/사업보고서 기준일)
- `current_price` (NUMERIC): 현재가(종가) (yfinance)
- `net_income` (NUMERIC): 당기순이익 (KR: DART API 사업보고서 연간 / US: yfinance quarterly_income_stmt 최근 4분기 합산 TTM)
- `total_equity` (NUMERIC): 자본총계 (KR: DART API 최근 분기 보고서 / US: yfinance quarterly_balance_sheet) - PBR 계산용
- `total_shares` (NUMERIC): 총 주식수. yfinance `quarterly_income_stmt`의 `Diluted Average Shares` 우선 사용, 없으면 `info['sharesOutstanding']` 폴백. 이중 주식 구조(MKC-V 등 Dual-Class)에서 `sharesOutstanding`은 해당 클래스 주식수만 반환하므로 전사 합산 희석주식수를 우선 사용함.
- `dividend_per_share` (NUMERIC): 1주당 연간 배당금 (yfinance)
- `quarterly_dividend` (BOOLEAN): 분기 배당 실시 여부 (yfinance 배당 내역 분석)
- `dividend_increase_years` (INTEGER): 배당 연속 인상 연수 (yfinance 배당 내역 분석)
- `share_buyback_cancel` (BOOLEAN): 정기적 자사주 매입 및 소각 여부
- `cancel_shares` (NUMERIC): 소각 (계획) 주식 수
- `treasury_shares` (NUMERIC): 자사주 보유 수 (KR: DART API 주식총수현황 / US: yfinance quarterly_balance_sheet)
- `created_at` (TIMESTAMP): 수집일시

### 3. `processed_financial_data` (가공된 평가 지표 테이블)
원본 데이터를 바탕으로 평가에 직접 사용될 비율 및 지표를 계산하여 저장합니다.
- `id` (SERIAL, PK)
- `ticker` (VARCHAR, FK -> companies.ticker)
- `record_date` (DATE): 기준 일자
- `eps` (NUMERIC): 주당순이익 (당기순이익 / 유통주식수)
- `per` (NUMERIC): 주가수익비율 (현재가 / EPS)
- `pbr` (NUMERIC): 주가순자산비율 (현재가 / (자본총계 / 유통주식수))
- `dividend_yield` (NUMERIC): 배당수익률 (%), (주당 연간 배당금 / 현재 주가) * 100 으로 계산
- `cancel_ratio` (NUMERIC): 연간 소각 비율 (%), (매입 및 소각 계획 주식 수 / 총 주식 수) * 100 으로 계산
- `treasury_share_ratio` (NUMERIC): 자사주 보유 비율 (%), (자사주 / (유통주식수 + 자사주)) * 100 으로 계산
- `created_at` (TIMESTAMP): 가공일시

### 4. `qualitative_assessments` (정성적 평가 데이터 테이블)
사용자가 직접 판단하여 입력하는 지표를 이력으로 관리합니다. (우선은 값을 사용하지 않고 비워둘 예정)
- `id` (SERIAL, PK)
- `ticker` (VARCHAR, FK -> companies.ticker)
- `assessment_date` (DATE): 평가 일자
- `profit_sustainability` (VARCHAR): 이익 지속 가능성 (예: SUSTAINABLE, UNSTABLE)
- `multiple_listing` (VARCHAR): 중복 상장 여부 (예: SINGLE, MULTIPLE)
- `future_growth` (VARCHAR): 미래 성장 잠재력 (예: VERY_HIGH, HIGH, NORMAL, LOW)
- `management_quality` (VARCHAR): 기업 경영 (예: EXCELLENT, PROFESSIONAL, POOR)
- `global_brand` (BOOLEAN): 세계적 브랜드 보유 여부
- `created_at` (TIMESTAMP): 입력일시

### 5. `scoring_results` (최종 평가 점수 및 등급 테이블)
가공된 지표와 정성적 평가를 종합하여 가이드라인에 따른 점수와 등급을 계산해 저장합니다.
- `id` (SERIAL, PK)
- `ticker` (VARCHAR, FK -> companies.ticker)
- `score_date` (DATE): 점수 산정 일자
- `score_per` (INTEGER): PER 점수 (최대 20)
- `score_pbr` (INTEGER): PBR 점수 (최대 5)
- `score_profit_sus` (INTEGER): 이익 지속성 점수 (최대 5)
- `score_listing` (INTEGER): 중복 상장 점수 (최대 5)
- `score_div_yield` (INTEGER): 배당수익률 점수 (최대 10)
- `score_div_quarter` (INTEGER): 분기 배당 점수 (최대 5)
- `score_div_inc` (INTEGER): 배당 인상 점수 (최대 5)
- `score_buyback` (INTEGER): 자사주 매입/소각 점수 (최대 7)
- `score_cancel_ratio` (INTEGER): 소각 비율 점수 (최대 8)
- `score_treasury_ratio` (INTEGER): 자사주 보유 비율 점수 (최대 5)
- `score_growth` (INTEGER): 미래 성장 점수 (최대 10)
- `score_management` (INTEGER): 기업 경영 점수 (최대 10)
- `score_brand` (INTEGER): 브랜드 점수 (최대 5)
- `total_score` (INTEGER): 총점 (최대 100)
- `grade` (VARCHAR): 투자 등급 (A, B, C, D)
- `created_at` (TIMESTAMP): 산정일시

### 6. `fetch_history` (수집 이력 관리 테이블)
수집 시도한 종목의 결과(성공, 실패, 스킵 등)를 날짜별로 기록하여 무한 재수집을 방지합니다.
- `id` (SERIAL, PK)
- `ticker` (VARCHAR, FK -> companies.ticker)
- `fetch_date` (DATE): 수집 시도 일자
- `status` (VARCHAR): 결과 상태 (SUCCESS, SKIP_NO_DIVIDEND, FAIL_NO_DATA, ERROR 등)
- `message` (VARCHAR): 상세 메시지
- `created_at` (TIMESTAMP): 기록일시
