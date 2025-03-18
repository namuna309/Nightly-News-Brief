import os
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from urllib.parse import unquote
import boto3
import psycopg2
from dotenv import load_dotenv

load_dotenv()


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

    # 1. 메인 테이블 (`financial_events`)이 없으면 생성
    print('1. 메인 테이블 (`financial_events`)이 없으면 생성')
    create_og_table_query = """
        CREATE TABLE IF NOT EXISTS raw_data.financial_events (
            time TIMESTAMP,
            timezone VARCHAR(100),
            country VARCHAR(100),
            volatility INT2,
            title VARCHAR(500),
            actual FLOAT4,
            forecast FLOAT4,
            previous FLOAT4,
            unit CHAR
        );
    """
    cur.execute(create_og_table_query)
    conn.commit()
    
    # 2. 임시 적재 테이블 (`s3_import_events_table`)이 없으면 생성
    print('2. 임시 적재 테이블 (`s3_import_events_table`)이 없으면 생성')
    create_data_table_query = """
        CREATE TABLE IF NOT EXISTS raw_data.s3_import_events_table (
            time TIMESTAMP,
            timezone VARCHAR(100),
            country VARCHAR(100),
            volatility INT2,
            title VARCHAR(500),
            actual FLOAT4,
            forecast FLOAT4,
            previous FLOAT4,
            unit CHAR
        );
    """
    cur.execute(create_data_table_query)
    conn.commit()

    for parquet_path in parquet_paths:
        folder_path = parquet_path.rsplit("/", 1)[0] + "/"
        # 3. S3의 Parquet 데이터를 `s3_import_table`에 적재
        print('3. S3의 Parquet 데이터를 `s3_import_table`에 적재')
        print(f"s3://{BUCKET_NAME}/{folder_path}")
        import_data_to_data_table_query = f"""
            COPY raw_data.s3_import_events_table
            FROM 's3://{BUCKET_NAME}/{folder_path}'
            IAM_ROLE '{REDSHIFT_IAM_ROLE}'
            FORMAT AS PARQUET;
        """
        cur.execute(import_data_to_data_table_query)
        conn.commit()
        
        # 4. 기존 데이터 Update
        print('4. 기존 데이터 Update')
        update_data_query = """
            UPDATE raw_data.financial_events AS o
            SET
                time = n.time,
                timezone = n.timezone,
                country = n.country,
                volatility = n.volatility,
                title = n.title,
                actual = n.actual,
                forecast = n.forecast,
                previous = n.previous,
                unit = n.unit
            FROM raw_data.s3_import_events_table AS n
            WHERE o.title = n.title
        """
        cur.execute(update_data_query)
        conn.commit()

        # 5. 중복 제거 후 데이터 Append
        print('5. 중복 제거 후 데이터 Append')
        append_data_query = """
            INSERT INTO raw_data.financial_events
            SELECT DISTINCT *
            FROM raw_data.s3_import_events_table s
            WHERE NOT EXISTS (
                SELECT 1 
                FROM raw_data.financial_events f
                WHERE s.title = f.title AND s.previous = f.previous
            );
        """
        cur.execute(append_data_query)
        conn.commit()

        # 6. `s3_import_events_table` 비우기
        print('6. `s3_import_events_table` 비우기')
        truncate_data_table_query = """
            TRUNCATE TABLE raw_data.s3_import_events_table;
        """
        cur.execute(truncate_data_table_query)
        conn.commit()


    # 7. 데이터 적재 결과 확인
    print('7. 데이터 적재 결과 확인')
    cur.execute("SELECT COUNT(*) FROM raw_data.financial_events;")
    count = cur.fetchone()[0]
    print(f"financial_events 총 데이터 개수: {count}")

    # 8. Redshift 연결 종료
    print('8. Redshift 연결 종료')
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
