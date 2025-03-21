import os
import json
from datetime import datetime, timedelta
import pandas as pd
import pyarrow.parquet as pq
from io import BytesIO
from zoneinfo import ZoneInfo
from urllib.parse import unquote
import boto3
import psycopg2
from dotenv import load_dotenv

load_dotenv()
# 환경 변수 설정
S3_REGION = unquote(os.environ.get('S3_REGION'))
BUCKET_NAME = unquote(os.environ.get('BUCKET_NAME')) 
REDSHIFT_HOST = unquote(os.environ.get('REDSHIFT_HOST'))
REDSHIFT_DB = unquote(os.environ.get('REDSHIFT_DB'))
REDSHIFT_USER = unquote(os.environ.get('REDSHIFT_USER'))
REDSHIFT_PW = unquote(os.environ.get('REDSHIFT_PW'))
REDSHIFT_PORT = unquote(os.environ.get('REDSHIFT_PORT'))
REDSHIFT_IAM_ROLE = unquote(os.environ.get('REDSHIFT_IAM_ROLE'))

s3_client = boto3.client(  # S3 클라이언트 생성
    service_name='s3',
    region_name=S3_REGION
)

def load_parquet_data(prefix):
    file_objects = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)['Contents']
    dfs = []
    for file_object in file_objects:
        file_key = file_object['Key']
        file_obj = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_key)
        parquet_file = pq.ParquetFile(BytesIO(file_obj['Body'].read()))
        df = parquet_file.read().to_pandas()
        dfs.append(df)
    return pd.concat(dfs)


def set_day(daydelta):
    today = datetime.now(ZoneInfo("America/New_York")).date()
    day = today - timedelta(days = daydelta)
    return day

def get_prefix(day):
        return f"TRANSFORMED/EARNING_CALLS/year={day.year}/month={day.strftime('%m')}/day={day.strftime('%d')}/earning.parquet"

def load_files():
    """테마별 JSON 파일 경로 로드"""
    try:
        url_per_day = []
        for d in [-1, 0, 1]:
            day = set_day(daydelta=d)
            prefix = get_prefix(day)
            response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
            if "Contents" in response:
                obj = response["Contents"][0]
                if prefix == obj['Key']:
                    url_per_day.append(prefix)
    except Exception as e:
        print(f"prefix 추출 실패: {e}")
    else:
        print("parqeut 파일 s3 prefix 추출 완료")
        print(url_per_day)
        return url_per_day

def save_to_redshift(parquet_paths):
    """
    S3의 Parquet 데이터를 Redshift의 financial_articles 테이블에 안전하게 Append하는 함수.
    중복 제거 후, 임시 테이블을 활용하여 데이터 정합성을 유지하면서 적재함.
    """
    # Redshift 연결
    conn = psycopg2.connect(
        dbname=REDSHIFT_DB,
        user=REDSHIFT_USER,
        password=REDSHIFT_PW,
        host=REDSHIFT_HOST,
        port=REDSHIFT_PORT
    )
    cur = conn.cursor()

    # 1. 메인 테이블 (`earning_calls`)이 존재하지 않으면 생성
    print('1. 메인 테이블 (`earning_calls`)이 없으면 생성')
    create_og_table_query = """
        CREATE TABLE IF NOT EXISTS raw_data.earning_calls (
            company_name VARCHAR(100),
            symbol VARCHAR(10),
            country VARCHAR(100),
            year INT2,
            quarter INT2,
            eps_actual_value FLOAT4,
            eps_actual_unit CHAR,
            eps_forecast_value FLOAT4,
            eps_forecast_unit CHAR,
            revenue_actual_value FLOAT4,
            revenue_actual_unit CHAR,
            revenue_forecast_value FLOAT4,
            revenue_forecast_unit CHAR,
            market_cap_value FLOAT4,
            market_cap_unit CHAR,
            call_date TIMESTAMP,
            timezone VARCHAR(100)
        );
    """
    cur.execute(create_og_table_query)
    conn.commit()
    
    # 2. S3에서 가져온 데이터를 임시로 저장할 테이블 (`s3_import_earning_calls_table`)이 존재하지 않으면 생성
    print('2. 임시 적재 테이블 (`s3_import_earning_calls_table`)이 없으면 생성')
    create_data_table_query = """
        CREATE TABLE IF NOT EXISTS raw_data.s3_import_earning_calls_table (
            company_name VARCHAR(100),
            symbol VARCHAR(10),
            country VARCHAR(100),
            year INT2,
            quarter INT2,
            eps_actual_value FLOAT4,
            eps_actual_unit CHAR,
            eps_forecast_value FLOAT4,
            eps_forecast_unit CHAR,
            revenue_actual_value FLOAT4,
            revenue_actual_unit CHAR,
            revenue_forecast_value FLOAT4,
            revenue_forecast_unit CHAR,
            market_cap_value FLOAT4,
            market_cap_unit CHAR,
            call_date TIMESTAMP,
            timezone VARCHAR(100)
        );
    """
    cur.execute(create_data_table_query)
    conn.commit()

    for parquet_path in parquet_paths:
        df = load_parquet_data(parquet_path)
        if df.empty:
            continue
        folder_path = parquet_path.rsplit("/", 1)[0] + "/"
         # 3. S3에서 Parquet 데이터를 `s3_import_earning_calls_table`에 적재
        print('3. S3의 Parquet 데이터를 `s3_import_earning_calls_table`에 적재')
        print(f"s3://{BUCKET_NAME}/{folder_path}")
        import_data_to_data_table_query = f"""
            COPY raw_data.s3_import_earning_calls_table
            FROM 's3://{BUCKET_NAME}/{folder_path}'
            IAM_ROLE '{REDSHIFT_IAM_ROLE}'
            FORMAT AS PARQUET;
        """
        cur.execute(import_data_to_data_table_query)
        conn.commit()
        
        # 4. 기존 데이터 중 업데이트 대상 필터링하여 새로운 테이블(`earning_calls_new`) 생성
        print('4. 기존 데이터 중 업데이트 대상 필터링하여 새로운 테이블(`earning_calls_new`) 생성')
        filtered_earning_calls_creation_query = """
            CREATE TABLE raw_data.earning_calls_new AS
            SELECT *
            FROM raw_data.earning_calls
            WHERE NOT EXISTS (
                SELECT 1
                FROM raw_data.s3_import_earning_calls_table s
                WHERE earning_calls.company_name = s.company_name
                AND earning_calls.symbol = s.symbol
                AND earning_calls.year = s.year
                AND earning_calls.quarter = s.quarter
            );
        """
        cur.execute(filtered_earning_calls_creation_query)
        conn.commit()

        # 5. 새로운 데이터를 추가 (중복 제거 후 Append)
        print('5. 새로운 데이터를 추가 (중복 제거 후 Append)')
        insert_new_earnings_data_query = """
            INSERT INTO raw_data.earning_calls_new
            SELECT * FROM raw_data.s3_import_earning_calls_table;
        """
        cur.execute(insert_new_earnings_data_query)
        conn.commit()

        # 6. 기존 `earning_calls` 테이블을 삭제하고, 새로운 테이블(`earning_calls_new`)로 교체
        print('6. 기존 `earning_calls` 테이블을 삭제하고, 새로운 테이블(`earning_calls_new`)로 교체')
        replace_earning_calls_table_query = """
            DROP TABLE raw_data.earning_calls;
            ALTER TABLE raw_data.earning_calls_new RENAME TO earning_calls;
        """
        cur.execute(replace_earning_calls_table_query)
        conn.commit()

        # 7. 임시 테이블 (`s3_import_earning_calls_table`) 데이터 삭제
        print('7. 임시 테이블 (`s3_import_earning_calls_table`) 데이터 삭제')
        truncate_data_table_query = """
            TRUNCATE TABLE raw_data.s3_import_earning_calls_table;
        """
        cur.execute(truncate_data_table_query)
        conn.commit()


    # 8. 데이터 적재 결과 확인
    print('8. 데이터 적재 결과 확인')
    cur.execute("SELECT COUNT(*) FROM raw_data.earning_calls;")
    count = cur.fetchone()[0]
    print(f"earning_calls 총 데이터 개수: {count}")

    # 9. Redshift 연결 종료
    print('9. Redshift 연결 종료')
    cur.close()
    conn.close()
    print("Redshift 연결 종료")

def lambda_handler(event, context):
    parquet_paths = load_files()
    save_to_redshift(parquet_paths)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }