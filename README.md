# 환율 급등 예측 자동화 파이프라인 프로젝트

## 1. 프로젝트 개요 및 문제 정의
이번 유가 파동이 국내 경제에 미친 영향을 보고 국제 사회의 이벤트가 국내 경제에 영향을 미치기까지 걸리는 시간을 측정하고자 하는 목적이 생겼다. 
따라서 국내 경제 지표인 원/달러 환율의 움직임을 예측하는 모델을 만들고 변수로 유가지수, 달러인덱스 그리고 GDELT를 사용했다.

##2. 질문
- 뉴스 데이터는 환율 예측에 어느정도 도움을 주는가? 
- 경제 데이터의 변동이 환율에 영향을 주는데 걸리는 시차는 얼마인가?
- 환율 급등일의 공통 특징은 무엇인가?



## 3. 기술 스택
- 언어: Python 3
- 데이터 처리: Apache Spark (PySpark), Hadoop HDFS
- 인프라 및 자동화: GCP (HDP Sandbox), GitHub Actions, Bash Shell

## 4. 구현 계획 및 파이프라인 구조
시간을 고려하여 파이프라인을 학습과 추론 두 단계로 분리하여 설계했다.
- 학습 파이프라인 (run_pipeline.sh) : 전체 GDELT 데이터와 거시경제 데이터를 GDFS에 적재하고, SPAKR로 TIME LAG 전처리를 한 뒤, RF모델을 학습시켜 HDFS에 저장했다.
- 추론 파이프라인 (run_inference.sh): 매일 실행되는 스크립트로, 최근 5일치 데이터를 수집하여 저장된 모델로 당일 환율 예측값을 산출한다.

## 5. 실행 방법

방법 1: Github에서 실행
1. 깃허브 ACTION으로 이동.
2. 왼쪽 메뉴에서 'Manual Train Pipeline'(학습) 또는 'Daily FX Inference Pipeline'(예측)을 선택.
3. Run workflow 버튼을 클릭하여 서버의 SPARK 작업 실행.

방법 2: 서버에서 직접 실행
GCP 서버의 maria_dev 계정으로 접속한 뒤 프로젝트 폴더로 이동해서 아래 스크립트를 실행하면 됩니다.
- 모델 전체 학습 시: ./run_pipeline.sh
- 당일 환율 예측 시: ./run_inference.sh

## 6. 결과 요약
FRED API를 통해 수집한 거시경제 데이터(환율, 유가, 달러인덱스)와 GDELT 프로젝트의 뉴스 데이터를 이용해 학습 데이터를 구성했다.
RF를 이용했고 GITHUB Action스케쥴러를 통해 매일 정해진 시간에 데이터를 수집하고 환율 급등 여부를 예측하는 자동화 시스템을 구축했다.


## 7. 참고 자료 및 레퍼런스
- 데이터 출처: GDELT Project 1.0, FRED API
- AI 도구 활용: 프로젝트 진행 중 GitHub Actions에서 GCP 샌드박스로 이중 접속(Proxy)하는 yml 설정 코드를 작성할 때와, 스파크 Window 함수 사용 중 발생한 Timeout 에러 원인을 분석할 때 Gemini의 도움을 받았습니다.
- 노재확, "머신러닝을 이용한 우리나라 환율 예측 연구" (ML을 사용한 이유, RF 선택 이유, 평가지표의 선택의 인사이트를 얻었습니다.)
- Github 프로젝트 : ganjjiang, "1차 프로젝트 - 데이터분석_경제지표를 통한 화율 예측 모델 생성"(https://github.com/ganjjiang/first_project) (환율 예측 프로젝트의 전반적 흐름을 파악했습니다.)
- Hexanika, "Hadoop in Banking : The Game Changer" (https://hexanika.com/hadoop-in-banking-the-game-changer/) (경제 분야에서 Hdoop을 어떻게 활용할 수 있는지 아이디어를 얻었습니다.)
