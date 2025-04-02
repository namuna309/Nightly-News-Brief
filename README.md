# Nightly News Brief

## 개요

**Nightly News Brief**는 미국 뉴스, 경제 지표, 실적 발표 데이터를 자동으로 수집, 처리 및 제공하는 시스템입니다. 이 프로젝트는 미국 경제 관련 데이터들을 크롤링하고, 이를 가공하여 사용자에게 전달하는 것을 목표로 합니다.
<br/>
<br/>
## 주요 기술 스택
| Field | Stack |
|:---:|:---|
| 사용 언어 | <img src="https://img.shields.io/badge/python-3776AB?style=for-the-badge&logo=python&logoColor=white"> <img src="https://img.shields.io/badge/html5-E34F26?style=for-the-badge&logo=html5&logoColor=white"> <img src="https://img.shields.io/badge/css-1572B6?style=for-the-badge&logo=css3&logoColor=white"> <img src="https://img.shields.io/badge/javascript-F7DF1E?style=for-the-badge&logo=javascript&logoColor=black">  |
| 웹 | <img src="https://img.shields.io/badge/flask-000000?style=for-the-badge&logo=flask&logoColor=white"/> <img src="https://img.shields.io/badge/amazon_ec2-FF9900?style=for-the-badge&logo=amazonec2&logoColor=white"/> | 
|데이터<br/>수집| <img src="https://img.shields.io/badge/selenium-43B02A?style=for-the-badge&logo=selenium&logoColor=white"/> <img src="https://img.shields.io/badge/amazon_lambda-FF9900?style=for-the-badge&logo=awslambda&logoColor=white"/> <img src="https://img.shields.io/badge/beautifulsoup4-000000?style=for-the-badge&logoColor=white"/>| 
|데이터<br/>정제| <img src="https://img.shields.io/badge/amazone_emr-8C4FFF?style=for-the-badge&logo=amazonwebservices&logoColor=white"/> <img src="https://img.shields.io/badge/amazon_lambda-FF9900?style=for-the-badge&logo=awslambda&logoColor=white"/> <img src="https://img.shields.io/badge/apache_spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white"/> <img src="https://img.shields.io/badge/beautifulsoup4-000000?style=for-the-badge&logoColor=white"/> <img src="https://img.shields.io/badge/pandas-150458?style=for-the-badge&logo=pandas&logoColor=white"/> |
| 데이터<br/>적재 | <img src="https://img.shields.io/badge/amazon_lambda-FF9900?style=for-the-badge&logo=awslambda&logoColor=white"/> <img src="https://img.shields.io/badge/amazon_s3-569A31?style=for-the-badge&logo=amazons3&logoColor=white"/> <img src="https://img.shields.io/badge/amazon_redshift-8C4FFF?style=for-the-badge&logo=amazonredshift&logoColor=white"/> <img src="https://img.shields.io/badge/amazon_rds-527FFF?style=for-the-badge&logo=amazonrds&logoColor=white"/>|
| Workflow | <img src="https://img.shields.io/badge/apache_airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white"/> <img src="https://img.shields.io/badge/gcp vm-4285F4?style=for-the-badge&logo=googlecloud&logoColor=white"/> |
| Container | <img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white"/> |
| 메일 발송 | <img src="https://img.shields.io/badge/amazon_lambda-FF9900?style=for-the-badge&logo=awslambda&logoColor=white"/> <img src="https://img.shields.io/badge/amazon_sqs-FF4F8B?style=for-the-badge&logo=amazonsqs&logoColor=white"/> <img src="https://img.shields.io/badge/amazon_rds-527FFF?style=for-the-badge&logo=amazonrds&logoColor=white"/> |
| 회원 관리 | <img src="https://img.shields.io/badge/google_forms-7248B9?style=for-the-badge&logo=googleforms&logoColor=white"/> <img src="https://img.shields.io/badge/google_sheets-34A853?style=for-the-badge&logo=googlesheets&logoColor=white"/> <img src="https://img.shields.io/badge/google_apps_script-4285F4?style=for-the-badge&logo=googleappsscript&logoColor=white"/> <img src="https://img.shields.io/badge/amazon_lambda-FF9900?style=for-the-badge&logo=awslambda&logoColor=white"/> <img src="https://img.shields.io/badge/amazon_rds-527FFF?style=for-the-badge&logo=amazonrds&logoColor=white"/>|
<br/>
<br/>
## 데이터 파이프라인
![Image](https://github.com/user-attachments/assets/6a86d53a-39f7-4b30-ab3f-ff0cf8269563)
### 데이터 수집
- 데이터 소스
    - 경제 뉴스: Yahoo Finance
    - 경제 지표: investing.com
    - 실적 발표: inversting.com
- 데이터 수집 방식
    - Selenium과 BeautifulSoup4를 사용하여 각 웹 페이지의 데이터를 크롤링합니다.
    - 집한 원시 데이터는 AWS S3의 `RAW`에 저장됩니다.
### 데이터 정제
- S3에 저장된 원시 페이지 소스에서 필요한 정보만 추출하여 Parquet 형식으로 재가공 후 S3에 저장합니다.
- 경제 뉴스
    - 원시 데이터의 크기가 GB 단위이므로, Apache Spark를 활용하여 뉴스 제목, 본문 등을 추출합니다.
    - 정제된 데이터는 S3의 `TRANSFORMED`에 Parquet 형식으로 저장됩니다.
- 경제 지표
    - 원시 데이터가 MB 단위이므로, AWS Lambda와 Pandas를 사용하여 지표 내용, 발표 시간 등을 추출합니다.
    - 정제된 데이터는 S3의 `TRANSFORMED`에 Parquet 형식으로 저장됩니다.
- 실적 발표
    - 데이터 크기가 MB 단위이므로, AWS Lambda와 Pandas를 사용하여 회사명 및 실적 정보를 추출합니다.
    - Redshift에 적재될 데이터는 S3의 `TRANSFORMED`에 Parquet 형식으로, RDS에 적재될 데이터는 S3의 `OLTP`에 JSON 형식으로 S3에 저장됩니다.

### 데이터 적재
- S3의 `TRANSFORMED` 및 `OLTP` 경로에 저장된 데이터를 각각 Amazon Redshift와 Amazon RDS로 적재합니다.
- 특히 실적 발표 데이터는 구조의 복잡성으로 인해, `OLTP` 에 저장된 JSON 데이터를 기준으로 RDS에 적재합니다.
<br/>
<br/>
## 메일 전송 및 회원 등록
### 메일 전송
![Image](https://github.com/user-attachments/assets/1a3f9e64-edef-419c-957a-8bdb42f7fc4a)
1. **요약 파일 생성 (Lambda 실행)**  
   - **RDS에 저장된 데이터를 기반으로** 텍스트 파일(`.txt`)로 변환하여 `S3/SUMMARY`에 저장합니다.
     - **경제 뉴스**: 저장된 뉴스 데이터를 기반으로 **ChatGPT**를 활용해 주요 뉴스 10건으로 요약 후 `.txt` 파일로 저장합니다.
     - **경제 지표 및 실적 발표**: 과거 데이터 및 기대치와 비교할 수 있도록 내용을 종합한 후 `.txt` 파일로 저장합니다.

2. **초안 메일 발송 (Lambda 실행)**  
   - 위 단계에서 생성된 각 `.txt` 파일을 기반으로 **초안 이메일을 작성자에게 Gmail로 전송**합니다.

3. **최종본 저장 (Flask 서버)**  
   - 작성자가 Gmail 초안을 확인하고 최종본을 확정하면, 이를 **Flask 기반 서버를 통해 `S3/FINAL`에 저장**합니다.

4. **회원별 메시지 큐 전송 (Lambda 실행)**  
   - `S3/FINAL`에 저장된 최종본을 기반으로, `RDS/web_data` 스키마 내 **회원 테이블을 조회**하여  
     **회원 수만큼 최종본과 수신자 정보를 포함한 메시지를 SQS에 전송**합니다.

5. **회원별 발송 처리 (Lambda 실행)**  
   - SQS에 등록된 메시지 수만큼 **Lambda가 실행**되어, 각 회원에게 최종본을 전송합니다.

### 회원 등록
![Image](https://github.com/user-attachments/assets/f751350c-76d5-401d-aac6-09437d210666)
1. **이메일 입력 (Google Form)**  
   - 사용자가 **Google Form**을 통해 이메일 주소를 입력합니다.

2. **이메일 저장 (Google Sheet)**  
   - 입력된 이메일 주소는 **Google Sheet**에 자동 저장됩니다.

3. **이메일 전송 (Google Apps Script)**  
   - **Google Apps Script**를 통해 Google Sheet에 **추가되거나 수정된 이메일 주소를 AWS Lambda로 전송**합니다.

4. **RDS 저장 (Lambda)**  
   - Lambda는 전달받은 이메일 주소를 **RDS의 `web_data` 스키마 내 회원 테이블에 저장**합니다.
<br/>
<br/>

## 결과물(발송되는 최종본)
![Image](https://github.com/user-attachments/assets/059c8547-dbb7-4523-a6cb-53183606d7fc)