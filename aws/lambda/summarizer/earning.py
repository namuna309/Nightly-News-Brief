import os
import json
import boto3
from sqlalchemy import create_engine, text
from urllib.parse import unquote
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

load_dotenv()

# 환경 변수 로드
S3_REGION = unquote(os.environ.get('S3_REGION'))
BUCKET_NAME = unquote(os.environ.get('BUCKET_NAME')) 
DB_NAME = unquote(os.environ.get('DB_NAME'))
EARNING_TABLE_NAME = unquote(os.environ.get('EARNING_TABLE_NAME'))
COMPANY_TABLE_NAME = unquote(os.environ.get('COMPANY_TABLE_NAME'))
PERIOD_TABLE_NAME = unquote(os.environ.get('PERIOD_TABLE_NAME'))
RDS_HOST = unquote(os.environ.get('RDS_HOST'))
RDS_PORT = unquote(os.environ.get('RDS_PORT'))
RDS_USER = unquote(os.environ.get('RDS_USER'))
RDS_PASSWORD = unquote(os.environ.get('RDS_PASSWORD'))

QUERY = f"""
SELECT DISTINCT c.name, c.symbol, p.year, p.quarter, e.revenue_forecast, e.revenue_forecast_unit, e.revenue_actual, e.revenue_actual_unit
FROM {DB_NAME}.{EARNING_TABLE_NAME} AS e
JOIN {DB_NAME}.{COMPANY_TABLE_NAME} AS c ON e.company_id = c.company_id
JOIN {DB_NAME}.{PERIOD_TABLE_NAME} AS p ON e.period_id = p.period_id
WHERE DATE(e.call_date) = CURDATE() - INTERVAL 1 DAY
AND (e.revenue_actual != 0 AND e.revenue_forecast != 0)
ORDER BY e.call_date;
"""

today = datetime.now(ZoneInfo("Asia/Seoul")).date()

def fetch_earnings_from_rds(conn_str):
    print(f"RDS 연결 시도: {conn_str}")
    engine = create_engine(conn_str)
    try:
        with engine.connect() as conn:
            sql = QUERY
            print("SQL 쿼리 실행 중...")
            results = conn.execute(text(sql)).fetchall()
            print(f"RDS에서 가져온 기사 수: {len(results)}")
            return results
    finally:
        print("RDS 연결 종료")
        conn.close()

def list_to_txt(lst):
    print("실적 리스트를 텍스트로 변환 중...")
    new_str = ''
    for i, l in enumerate(lst):
        # 실적과 예상 비교
        actual_earnings = float(l[6])  # 실제 실적
        forecast_earnings = float(l[4])  # 예상 실적

        if actual_earnings > forecast_earnings:
            performance_status = "▲"
        elif actual_earnings < forecast_earnings:
            performance_status = "▼"
        else:
            performance_status = "="
        
        new_str += f'{l[0]}({l[1]}): {actual_earnings} {l[7]} / {forecast_earnings} {l[5]} → {performance_status}\n'

    print(f"변환된 텍스트 길이: {len(new_str)}자")
    return new_str

def get_filename_with_date_hour(prefix="earnings", format=".txt"):
    timestamp = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%y%m%d%H")
    filename = f"{prefix}_{timestamp}{format}"
    print(f"생성된 파일명: {filename}")
    return filename

def get_prefix():
    filename = get_filename_with_date_hour()
    day = datetime.now(ZoneInfo("Asia/Seoul")).date()
    prefix = f"SUMMARY/EARNINGS/year={day.year}/month={day.strftime('%m')}/day={day.strftime('%d')}/{filename}"
    print(f"생성된 S3 prefix: {prefix}")
    return prefix


def upload_text_to_s3(text):
    print("S3 업로드 시작...")
    s3_client = boto3.client(
        service_name='s3',
        region_name=S3_REGION
    )
    prefix = get_prefix()
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=prefix,
        Body=text.encode("utf-8"),
        ContentType="text/plain"
    )
    print(f"업로드 완료: s3://{BUCKET_NAME}/{prefix}")


def lambda_handler(event, context):
    print("Lambda 함수 실행 시작")
    conn_str = f'mysql+pymysql://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{DB_NAME}'
    print("RDS에서 실적 발표 가져오는 중...")
    earnings_lst = fetch_earnings_from_rds(conn_str)

    print("뉴스 텍스트로 변환 중...")
    earnings_str = list_to_txt(earnings_lst)

    print("S3에 요약본 업로드 중...")
    upload_text_to_s3(earnings_str)


    print("Lambda 함수 실행 완료")
    return {
        'statusCode': 200,
        'body': json.dumps('Earnings summary generation and S3 upload have been completed successfully.')
    }

if __name__ == "__main__":
    print("로컬 실행 시작")
    lambda_handler([], [])
    print("로컬 실행 종료")