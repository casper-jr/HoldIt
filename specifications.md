# 기능 명세
## 개요
국내, 해외의 저평가된 우량주를 scoring_guideline.md의 기준에 따라 평가하여 조회할 수 있게 하는 것을 목표로 함
국내 주식 정보의 경우 DART API를 통해 조회할 수 있을 것 같으나, 해외의 경우 API의 유/무료 여부에 따라서 해외 주식의 경우 이후에 구현할 수 있음
scoring_guideline.md에 의한 평가를 진행한 뒤 점수 기준으로 정렬해서 보는 것과 같은 기능 제공

## 필요 정보
아래 정보들 중 정량적으로 얻을 수 있는 정보들을 프로그램을 통해 얻고, 이후에 이외 판단이 필요한 값들은 사용자가 직접 고려하여 선정

국내, 해외 주식에 대해서 다음 값들이 필요
- 현재값(계산 용이성을 위해 종가 기준과 같이 선정 가능) - **[구현 완료: yfinance]**
- EPS(가장 최근 분기 또는 작년 값 사용, 당기순이익/발행주식 으로 계산) - **[구현 완료: DART API 당기순이익 / yfinance 유통주식수]**
- PER(현재값/EPS 로 계산) - **[구현 완료]**
- PBR(가장 최근 분기 값) - **[구현 완료: DART API 자본총계 활용 계산]**
- 배당수익률(한 주당 연간 배당금 / 현재 주가 x 100) - **[구현 완료: yfinance 배당금]**
- 분기 배당 여부 - **[구현 완료: yfinance 배당 내역 분석]**
- 배당 인상 기록(지급 기록) - **[구현 완료: yfinance 연도별 배당금 분석]**
- 정기적 자사주 매입 및 소각 여부 - **[구현 완료: DART API 공시검색 활용 (최근 1년 내 취득/소각 공시 동시 존재 여부)]**
- 매입 및 소각을 하는 경우 연간 소각 비율(소각 계획 주식 수, 총 주식수로 계산) - **[미구현: API 한계로 수동 입력 또는 보류]**
- 자사주 보유 비율 - **[구현 완료: DART API 주식총수현황]**

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
1. 국내 상장된 주식 정보들을 불러와서 DB에 불러온 날짜 정보와 함께 저장
   - **권장 수집 범위**: 시가총액 기준 상위 300~500개 종목 (배당이 없는 종목은 자동 제외되므로, 실제 DB에 저장되는 유효 배당 우량주는 100~200개 내외로 압축됨)
2. 불러온 정보를 기반으로 이후에 진행할 평가에 실제로 사용될 값들만 따로 테이블을 만들어서 저장(가공 과정)
3. 가공된 정보들을 이용하여 scoring_guidelines에 따른 점수 계산을 수행하여 결과를 날짜와 함께 저장
4. 이후에 점수 계산 결과를 조회할 수 있도록 하고, 필요시 각 종목의 세부지표들의 값과 지표별 점수 추이를 조회할 수 있도록 함
   - **조회 필터링**: 정성평가 만점(43점)을 획득하더라도 70점(B등급) 미만인 종목은 리더보드(`view`)에서 자동으로 제외됨 (현재 정량점수 27점 이상만 표시)
5. **데이터 내보내기**: 수집 시점에서의 결과를 엑셀(CSV) 형식으로 Export하여, 사용자가 직접 정성적 평가 항목(이익 지속성, 경영자 평가 등)의 점수를 기입하고 최종 점수를 확인할 수 있도록 지원

## 사용 DB 스키마

기능적 요구사항에 맞추어 데이터의 수집(Raw), 가공(Processed), 정성적 평가(Qualitative), 점수 결과(Scoring)를 분리하여 이력을 관리할 수 있도록 설계합니다.

### 1. `companies` (기업 마스터 테이블)
- `ticker` (VARCHAR, PK): 종목코드 (예: '005930')
- `name` (VARCHAR): 기업명
- `market` (VARCHAR): 시장 구분 (KOSPI, KOSDAQ 등)
- `created_at` (TIMESTAMP): 등록일시

### 2. `raw_financial_data` (원본 재무/주가 데이터 테이블)
API나 크롤링을 통해 수집한 가공 전 원본 데이터를 날짜별로 저장합니다.
- `id` (SERIAL, PK)
- `ticker` (VARCHAR, FK -> companies.ticker)
- `record_date` (DATE): 수집/기준 일자 (가장 최근 분기/반기/사업보고서 기준일)
- `current_price` (NUMERIC): 현재가(종가) (yfinance)
- `net_income` (NUMERIC): 당기순이익 (DART API)
- `total_equity` (NUMERIC): 자본총계 (DART API) - PBR 계산용
- `total_shares` (NUMERIC): 총 유통 주식 수 (yfinance, 자사주 제외)
- `dividend_per_share` (NUMERIC): 1주당 연간 배당금 (yfinance)
- `quarterly_dividend` (BOOLEAN): 분기 배당 실시 여부 (yfinance 배당 내역 분석)
- `dividend_increase_years` (INTEGER): 배당 연속 인상 연수 (yfinance 배당 내역 분석)
- `share_buyback_cancel` (BOOLEAN): 정기적 자사주 매입 및 소각 여부
- `cancel_shares` (NUMERIC): 소각 (계획) 주식 수
- `treasury_shares` (NUMERIC): 자사주 보유 수 (DART API 주식총수현황)
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
