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
EVENT_TABLE_NAME = unquote(os.environ.get('EVENT_TABLE_NAME'))
RDS_HOST = unquote(os.environ.get('RDS_HOST'))
RDS_PORT = unquote(os.environ.get('RDS_PORT'))
RDS_USER = unquote(os.environ.get('RDS_USER'))
RDS_PASSWORD = unquote(os.environ.get('RDS_PASSWORD'))

QUERY = f"""
SELECT *
FROM {DB_NAME}.{EVENT_TABLE_NAME} AS f
WHERE DATE(f.release_time) = CURDATE()
ORDER BY f.release_time AND f.title;
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
    timestamp = datetime.now(ZoneInfo("Asia/Seoul"))
    curr_hour = timestamp.hour
    new_str = ''
    for i, l in enumerate(lst):
        release_time, timezone, country, volatility, title, actual, forecast, previous, unit = l[0], l[1], l[2], l[3], l[4], [5], l[6], l[7], l[8]
        unit = unit if unit else ''
        forecast = f"예상: {forecast}{unit}" if forecast else ""
        previous = f"이전: {previous}{unit}" if previous else ""
        
        release_time_ny = release_time.replace(tzinfo=ZoneInfo(timezone))
        release_time_seoul = release_time_ny.astimezone(ZoneInfo("Asia/Seoul"))
        release_time = release_time_seoul.strftime("%H:%M")

        if curr_hour > 12:
            stars = '★' * volatility
            if "Speaks" in title:
                economic_values_str = ""  
            else:
                if forecast and previous:
                    economic_values_str = f"({forecast}, {previous})"
                elif forecast:
                    economic_values_str = f"({forecast})"
                elif previous:
                    economic_values_str = f"({previous})"
                else:
                    economic_values_str = ""

            new_str += f'{release_time} - {title} {stars} {economic_values_str}\n'
        elif curr_hour < 12 and actual != 0:
            # 실적과 예상 비교
            if actual > forecast:
                status_vs_forecast = "▲"
            elif actual < forecast:
                status_vs_forecast = "▼"
            else:
                status_vs_forecast = "="

            if actual > previous:
                status_vs_previous = "▲"
            elif actual < previous:
                status_vs_previous = "▼"
            else:
                status_vs_previous = "="
            
            if "Speaks" in title:
                economic_values_str = ""  
            else:
                if forecast and previous:
                    economic_values_str = f" {actual}{unit} → 예상({forecast}{unit}): {status_vs_forecast}, 이전({previous}{unit}): {status_vs_previous}"
                elif forecast:
                    economic_values_str = f" {actual}{unit} → 예상({forecast}{unit}): {status_vs_forecast}"
                elif previous:
                    economic_values_str = f" {actual}{unit} → 이전({previous}{unit}): {status_vs_previous}"
                else:
                    economic_values_str = f" {actual}{unit}"
            
            nnew_str += f'{release_time} - {title} {stars} {economic_values_str}\n'

    print(f"변환된 텍스트 길이: {len(new_str)}자")
    return new_str

def get_filename_with_date_hour(prefix="events", format=".txt"):
    timestamp = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%y%m%d%H")
    filename = f"{prefix}_{timestamp}{format}"
    print(f"생성된 파일명: {filename}")
    return filename

def get_prefix():
    filename = get_filename_with_date_hour()
    day = datetime.now(ZoneInfo("Asia/Seoul")).date()
    prefix = f"SUMMARY/EVENTS/year={day.year}/month={day.strftime('%m')}/day={day.strftime('%d')}/{filename}"
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
    print("RDS에서 경제 지표 발표 가져오는 중...")
    earnings_lst = fetch_earnings_from_rds(conn_str)

    print("경제 지표 텍스트로 변환 중...")
    earnings_str = list_to_txt(earnings_lst)

    print("S3에 요약본 업로드 중...")
    upload_text_to_s3(earnings_str)


    print("Lambda 함수 실행 완료")
    return {
        'statusCode': 200,
        'body': json.dumps('Earnings summary generation and S3 upload have been completed successfully.')
    }

if __name__ == '__main__':
    print(lambda_handler([], []))