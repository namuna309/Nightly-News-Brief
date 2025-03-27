import os
import json
import boto3
import pandas as pd
import pyarrow.parquet as pq
from io import BytesIO
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from urllib.parse import unquote
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


load_dotenv()
# 환경 변수 설정
S3_REGION = unquote(os.environ.get('S3_REGION'))
BUCKET_NAME = unquote(os.environ.get('BUCKET_NAME')) 
DB_NAME = unquote(os.environ.get('DB_NAME'))
TABLE_NAME = unquote(os.environ.get('TABLE_NAME'))
RDS_HOST = unquote(os.environ.get('RDS_HOST'))
RDS_PORT = unquote(os.environ.get('RDS_PORT'))
RDS_USER = unquote(os.environ.get('RDS_USER'))
RDS_PASSWORD = unquote(os.environ.get('RDS_PASSWORD'))

# S3 bucket and file details
s3_client = boto3.client(  # S3 클라이언트 생성
    service_name='s3',
    region_name=S3_REGION
)

def set_day(daydelta):
    today = datetime.now(ZoneInfo("America/New_York")).date()
    day = today - timedelta(days = daydelta)
    return day

def get_prefix(day):
        return f"TRANSFORMED/EVENTS/year={day.year}/month={day.strftime('%m')}/day={day.strftime('%d')}/event.parquet"

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

def save_to_rds(parquet_paths):
    conn_str = f'mysql+pymysql://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{DB_NAME}'
    engine = create_engine(conn_str)

    with engine.connect() as conn:
        print('1. 메인 테이블 (`financial_events`)이 없으면 생성')
        create_og_table_query = """
            CREATE TABLE IF NOT EXISTS raw_data.financial_events (
                release_time TIMESTAMP,
                timezone VARCHAR(100),
                country VARCHAR(100),
                volatility SMALLINT,  
                title VARCHAR(500),
                actual FLOAT,
                forecast FLOAT,
                previous FLOAT,
                unit CHAR
            );
        """
        conn.execute(text(create_og_table_query))

        print('2. 임시 적재 테이블 (`s3_import_events_table`)이 없으면 생성')
        create_data_table_query = """
            CREATE TABLE IF NOT EXISTS raw_data.s3_import_events_table (
                release_time TIMESTAMP,
                timezone VARCHAR(100),
                country VARCHAR(100),
                volatility SMALLINT,  
                title VARCHAR(500),
                actual FLOAT,  
                forecast FLOAT, 
                previous FLOAT,  
                unit CHAR  
            );
        """
        conn.execute(text(create_data_table_query))

    for parquet_path in parquet_paths:
        print(parquet_path)
        df = load_parquet_data(parquet_path)
        df.drop_duplicates()
        df["release_time"] = pd.to_datetime(df["release_time"], unit="ms")  # Unix Timestamp 변환
        # DataFrame을 MySQL RDS에 저장
        df.to_sql(name="s3_import_events_table", con=engine, schema="raw_data", if_exists="append", index=False)

        with engine.begin() as conn:
            print('3. 기존 데이터 Update')
            update_data_query = """
                UPDATE raw_data.financial_events AS o
                JOIN raw_data.s3_import_events_table AS n
                ON o.title = n.title AND o.release_time = n.release_time
                SET 
                    o.release_time = n.release_time,
                    o.timezone = n.timezone,
                    o.country = n.country,
                    o.volatility = n.volatility,
                    o.actual = n.actual,
                    o.forecast = n.forecast,
                    o.previous = n.previous,
                    o.unit = n.unit;
            """
            conn.execute(text(update_data_query))
            conn.commit()

        with engine.begin() as conn:
            print('4. 중복 제거 후 데이터 Append')
            append_data_query = """
                INSERT INTO raw_data.financial_events
                SELECT DISTINCT *
                FROM raw_data.s3_import_events_table s
                WHERE NOT EXISTS (
                    SELECT 1 
                    FROM raw_data.financial_events f
                    WHERE s.title = f.title AND s.release_time = f.release_time
                );
            """
            conn.execute(text(append_data_query))
            conn.commit()
    
    with engine.connect() as conn:
        total_financial_events = "SELECT COUNT(*) FROM raw_data.financial_events;"
        result = conn.execute(text(total_financial_events))
        count = result.fetchone()[0]
        print(f"financial_events 총 데이터 개수: {count}")
        
    print(f"Event Data inserted successfully!")


    
def lambda_handler(event, context):
    parquet_paths = load_files()
    save_to_rds(parquet_paths)
