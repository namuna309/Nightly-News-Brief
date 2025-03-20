import os
import json
from datetime import datetime
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

# 현재 날짜 (Eastern Time 기준)
today = datetime.now(ZoneInfo("America/New_York")).date()

# 테마 설정
THEMES = ['Stock_Market', 'Original', 'Economies', 'Earning', 'Tech', 'Housing', 'Crypto', 'Latest']


def get_prefix(data_stage, theme):
    """테마별 JSON 및 Parquet 저장 경로 반환"""
    return f"{data_stage}/ARTICLES/{theme.upper()}/year={today.year}/month={today.strftime('%m')}/day={today.strftime('%d')}"

def load_files(stage, format):
    """테마별 JSON 파일 경로 로드"""
    try:
        url_per_themes = {theme: [] for theme in THEMES}
        for theme in THEMES:
            prefix = get_prefix(stage, theme)
            response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
            if "Contents" in response:
                for obj in response["Contents"]:
                    if obj["Key"].endswith(f".{format}"):  # JSON 파일만 필터링
                        url_per_themes[theme].append(f"{obj['Key']}")
    except Exception as e:
        print(f"prefix 추출 실패: {e}")
    else:
        print(f"{format}파일 s3 prefix 추출 완료")
        for t, u in url_per_themes.items():
            print(f"{t}: {u}")
        return url_per_themes

def save_to_redshift(file_path):
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

    # 1. 메인 테이블 (`financial_articles`)이 없으면 생성
    create_og_table_query = """
        CREATE TABLE IF NOT EXISTS raw_data.financial_articles (
            article_publisher VARCHAR(255),
            authors VARCHAR(500),
            date TIMESTAMP,
            text VARCHAR(65535),
            theme VARCHAR(100),
            timezone VARCHAR(100),
            title VARCHAR(500),
            url VARCHAR(1000)
        );
    """
    cur.execute(create_og_table_query)
    conn.commit()
    
    # 2. 임시 적재 테이블 (`s3_import_table`)이 없으면 생성
    create_data_table_query = """
        CREATE TABLE IF NOT EXISTS raw_data.s3_import_table (
            article_publisher VARCHAR(255),
            authors VARCHAR(500),
            date TIMESTAMP,
            text VARCHAR(65535),
            theme VARCHAR(100),
            timezone VARCHAR(100),
            title VARCHAR(500),
            url VARCHAR(1000)
        );
    """
    cur.execute(create_data_table_query)
    conn.commit()

    for theme in THEMES:
        folder_path = file_path[theme][0].rsplit("/", 1)[0] + "/"
        # 3. S3의 Parquet 데이터를 `s3_import_table`에 적재
        import_data_to_data_table_query = f"""
            COPY raw_data.s3_import_table
            FROM 's3://{BUCKET_NAME}/{folder_path}'
            IAM_ROLE '{REDSHIFT_IAM_ROLE}'
            FORMAT AS PARQUET;
        """
        cur.execute(import_data_to_data_table_query)
        conn.commit()

        # 4. 중복 제거 후 데이터 Append
        append_data_query = """
            INSERT INTO raw_data.financial_articles
            SELECT DISTINCT * FROM raw_data.s3_import_table
            WHERE url NOT IN (SELECT url FROM raw_data.financial_articles);
        """
        cur.execute(append_data_query)
        conn.commit()

        # 5. `s3_import_table` 비우기 (성능 최적화)
        truncate_data_table_query = """
            TRUNCATE TABLE raw_data.s3_import_table;
        """
        cur.execute(truncate_data_table_query)
        conn.commit()


    # 6. 데이터 적재 결과 확인
    cur.execute("SELECT COUNT(*) FROM raw_data.financial_articles;")
    count = cur.fetchone()[0]
    print(f"financial_articles의 총 데이터 개수: {count}")

    # 7. Redshift 연결 종료
    cur.close()
    conn.close()
    print("Redshift 연결 종료")

def lambda_handler(event, context):
    parquet_paths = load_files('TRANSFORMED', 'parquet')
    save_to_redshift(parquet_paths)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }