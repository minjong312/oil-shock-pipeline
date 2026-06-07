- 데이터 안내
1. 대용량 데이터 제외 (.gitignore)
본 프로젝트에서 수집한 GDELT 뉴스 원본 파일과 거시경제 데이터 파일은 전체 기간을 합산할 경우 GitHub 저장소에 업로드하기에 무리가 있어
과제 요구사항에 맞게 .gitignore를 설정하여 대용량 raw 데이터는 저장소에 포함되지 않도록 하였다.
데이터의 형태를 확인할 수 있도록 sample_data/ 폴더 안에 전체 데이터 중 500행을 추출한 샘플 파일을 커밋했다.
샘플 파일명 설명
- yfinance_sample.csv : FRED API 수집 거시경제 지표 샘플
- gdelt_sample.csvGDELT 뉴스 이벤트 샘플

3. 데이터 출처
본 프로젝트의 환율 급등 예측 모델은 두 가지 외부 데이터를 매일 수집하여 병합(Join)하는 방식으로 구성된다.
거시경제 지표 데이터 (FRED API)

출처: 미국 연방준비은행 경제 데이터 (Federal Reserve Economic Data)
수집 내용: 원/달러 환율(DEXKOUS), WTI 유가(DCOILWTICO)
수집 스크립트: src/ingest/get_yfinance.py

- 정치·사회 이벤트 데이터 (GDELT Project 1.0)

출처: GDELT Project 공식 서버
수집 내용: 전 세계의 분쟁, 시위, 외교적 마찰 등의 뉴스 이벤트를 수치화한 데이터
도입 이유: 환율 시장에 급격한 충격을 주는 외부 요인을 모델에 반영하기 위해 도입


3. 핵심 데이터 스키마 (Schema)
Spark를 이용해 두 데이터를 날짜 기준으로 조인하고 전처리를 거친 뒤, 최종 Random Forest 학습에 사용되는 주요 컬럼은 다음과 같습니다.
컬럼명타입설명DateSTRING기준 날짜 (YYYY-MM-DD)DEXKOUSDOUBLE당일 원/달러 환율 종가DCOILWTICODOUBLE당일 WTI 유가AvgToneDOUBLEGDELT 당일 주요 뉴스의 평균 긍정/부정 어조GoldsteinScaleDOUBLEGDELT 이벤트의 국가 간 안정성 영향력 지수lag_1_DEXKOUS ~ lag_5_DEXKOUSDOUBLE과거 1~5일 전 환율 타임래그(Time-lag) 파생 변수targetINT다음 날 환율 급등 여부 (급등: 1, 평탄: 0)

 target 컬럼은 분류 모델의 최종 예측 대상(Label)입니다.
