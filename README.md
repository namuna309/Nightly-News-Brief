# Nightly News Brief

## 개요

**Nightly News Brief**는 미국 뉴스, 경제 지표, 실적 발표 데이터를 자동으로 수집, 처리 및 제공하는 시스템입니다. 이 프로젝트는 미국 경제 관련 데이터들을 크롤링하고, 이를 가공하여 사용자에게 전달하는 것을 목표로 합니다.

## 주요 기술 스택
| Field | Stack |
|:---:|:---|
| 사용 언어 | <img src="https://img.shields.io/badge/python-3776AB?style=for-the-badge&logo=python&logoColor=white"> <img src="https://img.shields.io/badge/html5-E34F26?style=for-the-badge&logo=html5&logoColor=white"> <img src="https://img.shields.io/badge/css-1572B6?style=for-the-badge&logo=css3&logoColor=white"> <img src="https://img.shields.io/badge/javascript-F7DF1E?style=for-the-badge&logo=javascript&logoColor=black">  |
| 웹 | <img src="https://img.shields.io/badge/flask-000000?style=for-the-badge&logo=flask&logoColor=white"/> <img src="https://img.shields.io/badge/amazon_ec2-FF9900?style=for-the-badge&logo=amazonec2&logoColor=white"/> | 
|데이터 수집| <img src="https://img.shields.io/badge/selenium-43B02A?style=for-the-badge&logo=selenium&logoColor=white"/> <img src="https://img.shields.io/badge/amazon_lambda-FF9900?style=for-the-badge&logo=awslambda&logoColor=white"/> <img src="https://img.shields.io/badge/beautifulsoup4-000000?style=for-the-badge&logoColor=white"/>| 
|데이터 정제| <img src="https://img.shields.io/badge/amazone_emr-8C4FFF?style=for-the-badge&logo=amazonwebservices&logoColor=white"/> <img src="https://img.shields.io/badge/amazon_lambda-FF9900?style=for-the-badge&logo=awslambda&logoColor=white"/> <img src="https://img.shields.io/badge/apache_spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white"/> <img src="https://img.shields.io/badge/beautifulsoup4-000000?style=for-the-badge&logoColor=white"/> <img src="https://img.shields.io/badge/pandas-150458?style=for-the-badge&logo=pandas&logoColor=white"/> |
| 데이터 적재 | <img src="https://img.shields.io/badge/amazon_lambda-FF9900?style=for-the-badge&logo=awslambda&logoColor=white"/> <img src="https://img.shields.io/badge/amazon_s3-569A31?style=for-the-badge&logo=amazons3&logoColor=white"/> <img src="https://img.shields.io/badge/amazon_redshift-8C4FFF?style=for-the-badge&logo=amazonredshift&logoColor=white"/> <img src="https://img.shields.io/badge/amazon_rds-527FFF?style=for-the-badge&logo=amazonrds&logoColor=white"/>|
| 워크플로우 | <img src="https://img.shields.io/badge/apache_airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white"/> <img src="https://img.shields.io/badge/gcp vm-4285F4?style=for-the-badge&logo=googlecloud&logoColor=white"/> |
| 컨테이너화 | <img src="https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white"/> |
| 메일 발송 | <img src="https://img.shields.io/badge/amazon_lambda-FF9900?style=for-the-badge&logo=awslambda&logoColor=white"/> <img src="https://img.shields.io/badge/amazon_sqs-FF4F8B?style=for-the-badge&logo=amazonsqs&logoColor=white"/> <img src="https://img.shields.io/badge/amazon_rds-527FFF?style=for-the-badge&logo=amazonrds&logoColor=white"/> |
| 회원 관리 | <img src="https://img.shields.io/badge/google_forms-7248B9?style=for-the-badge&logo=googleforms&logoColor=white"/> <img src="https://img.shields.io/badge/google_sheets-34A853?style=for-the-badge&logo=googlesheets&logoColor=white"/> <img src="https://img.shields.io/badge/google_apps_script-4285F4?style=for-the-badge&logo=googleappsscript&logoColor=white"/> <img src="https://img.shields.io/badge/amazon_lambda-FF9900?style=for-the-badge&logo=awslambda&logoColor=white"/> <img src="https://img.shields.io/badge/amazon_rds-527FFF?style=for-the-badge&logo=amazonrds&logoColor=white"/>|

## 아키텍처
### Data Flow
![Image](https://github.com/user-attachments/assets/6a86d53a-39f7-4b30-ab3f-ff0cf8269563)
### Mail Flow
![Image](https://github.com/user-attachments/assets/09bf68d4-e451-4877-a732-6f289bd3de5d)
### Member Register Flow
![Image](https://github.com/user-attachments/assets/f751350c-76d5-401d-aac6-09437d210666)