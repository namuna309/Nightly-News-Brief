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

# 테마 설정
THEMES = ['Stock_Market', 'Original', 'Economies', 'Earning', 'Tech', 'Housing', 'Crypto', 'Latest']

# 현재 날짜 (Eastern Time 기준)
today = datetime.now(ZoneInfo("America/New_York")).date()

def get_prefix(data_stage, theme):
    """테마별 JSON 및 Parquet 저장 경로 반환"""
    return f"{data_stage}/ARTICLES/{theme.upper()}/year={today.year}/month={today.strftime('%m')}/day={today.strftime('%d')}"

def load_files(stage, format):
    """테마별 JSON 파일 경로 로드"""
    try:
        url_per_themes = {theme: None for theme in THEMES}
        for theme in THEMES:
            prefix = get_prefix(stage, theme)
            response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
            if "Contents" in response:
                for obj in response["Contents"]:
                    if obj["Key"].endswith(f".{format}"):  # JSON 파일만 필터링
                        url_per_themes[theme] = f"{obj['Key']}"
    except Exception as e:
        print(f"prefix 추출 실패: {e}")
    else:
        print(f"{format}파일 s3 prefix 추출 완료")
        for t, u in url_per_themes.items():
            print(f"{t}: {u}")
        return url_per_themes
    

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

def save_to_rds(file_path):
    conn_str = f'mysql+pymysql://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{DB_NAME}'
    engine = create_engine(conn_str)

    with engine.connect() as conn:
        create_og_table_query = """
            CREATE TABLE IF NOT EXISTS raw_data.financial_articles (
                article_publisher VARCHAR(255),
                authors VARCHAR(500),
                date TIMESTAMP,
                text TEXT,  -- VARCHAR(65535) 대신 TEXT 사용
                theme VARCHAR(100),
                timezone VARCHAR(100),
                title VARCHAR(500),
                url TEXT  -- VARCHAR(1000) 대신 TEXT 사용 고려 가능
            );
        """
        conn.execute(text(create_og_table_query))

        create_data_table_query = """
            CREATE TABLE IF NOT EXISTS raw_data.s3_import_table (
                article_publisher VARCHAR(255),
                authors VARCHAR(500),
                date TIMESTAMP,
                text TEXT,  -- VARCHAR(65535) 대신 TEXT 사용
                theme VARCHAR(100),
                timezone VARCHAR(100),
                title VARCHAR(500),
                url TEXT  -- VARCHAR(1000) 대신 TEXT 사용 고려 가능
            );
        """
        conn.execute(text(create_data_table_query))

        for theme in THEMES:
            df = load_parquet_data(file_path[theme])
            # DataFrame을 MySQL RDS에 저장
            df.to_sql(name="financial_articles", con=engine, if_exists="append", index=False)
            
            print(f"{theme} Data inserted successfully!")


    
if __name__ == "__main__":
    parquet_paths = load_files('TRANSFORMED', 'parquet')
    save_to_rds(parquet_paths)
