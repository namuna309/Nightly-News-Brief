import sys
import json
from datetime import datetime
from zoneinfo import ZoneInfo
from bs4 import BeautifulSoup
from pyspark.sql import SparkSession, functions as F
import concurrent.futures
from urllib.parse import unquote
import boto3
import psycopg2

# 환경 변수 설정
json_args = sys.argv[1]  
args = json.loads(json_args)

# 값 추출
S3_REGION = args["S3_REGION"]
BUCKET_NAME = args["BUCKET_NAME"]
REDSHIFT_HOST = args['REDSHIFT_HOST']
REDSHIFT_DB = args['REDSHIFT_DB']
REDSHIFT_USER = args['REDSHIFT_USER']
REDSHIFT_PW = args['REDSHIFT_PW']
REDSHIFT_PORT = args['REDSHIFT_PORT']
REDSHIFT_IAM_ROLE = args['REDSHIFT_IAM_ROLE']

s3_client = boto3.client(  # S3 클라이언트 생성
    service_name='s3',
    region_name=S3_REGION
)

# SparkSession 생성
spark = SparkSession.builder.appName("Yahoo_Finanace_Json_to_Parquet").getOrCreate()

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

def extract_article_data(file_path, theme):
    """JSON 파일에서 HTML 파싱 및 기사 데이터 추출"""
    object = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_path)
    print(f"{file_path}: s3 object read 완료")
    article = json.loads(object['Body'].read())
    print(f"{file_path}: object -> json 추출 완료")
    soup = BeautifulSoup(article['content'], "html.parser")
    article_data = {
        'title': '',
        'date': '',
        'authors': [],
        'article_publisher': '',
        'url': article['url'],
        'text': '',
        'theme': theme
    }

    article_wrap = soup.find("div", class_=lambda c: c and "article-wrap" in c)
    if not article_wrap:
        print(f"'article-wrap' 요소를 찾을 수 없습니다. URL: {article['url']}")
        return article_data

    # 제목 추출
    cover_title = article_wrap.find("div", class_=lambda c: c and "cover-title" in c)
    article_data['title'] = cover_title.get_text(strip=True) if cover_title else ""

    top_header = soup.find(class_="top-header")
    if top_header:
        subtle_link = top_header.find(class_="subtle-link")
        if subtle_link and "title" in subtle_link.attrs:
            article_publisher = subtle_link["title"]
            if article_publisher:
                article_data['article_publisher'] = article_publisher

    # 저자 추출
    byline_author = article_wrap.find("div", class_=lambda c: c and "byline-attr-author" in c)
    if byline_author:
        try:
            author_links = byline_author.find_all("a")
            if author_links:
                article_data['authors'] = [a.get_text(strip=True) for a in author_links]
                if article_data['authors'] == ['']:
                    author_img = author_links[0].find('img')
                    article_data['authors'] = [author_img['alt'].split(', ')[0]]
            else:
                authors_text = byline_author.text
                article_data['authors'] = [author.strip() for author in authors_text.split(', ')] if ',' in authors_text else [authors_text.strip()]
            if article_data['authors']:
                tmp = []
                for i in range(len(article_data['authors'])):
                    if i > 0 and article_data['authors'][i] in article_data['article_publisher']:
                        continue
                    elif " and " in article_data['authors'][i]:
                        tmp += [author.strip() for author in article_data['authors'][i].split(' and ')]
                    else:
                        tmp.append(article_data['authors'][i])
                article_data['authors'] = tmp.copy()
        except Exception as e:
            print(f"'authors' 파싱 실패: {e}. URL: {article_data['url']}")

    # 날짜 추출
    meta_time = article_wrap.find("time", class_=lambda c: c and "byline-attr-meta-time" in c)
    article_data['date'] = meta_time.get("data-timestamp", "") if meta_time else ""


    # 본문 추출
    body_wrap = soup.find("div", class_=lambda c: c and "body-wrap" in c)
    if body_wrap:
        body_div = body_wrap.find("div", class_=lambda c: c and "body" in c)
        article_data['text'] = body_div.get_text(separator="\n", strip=True) if body_div else ""

    print(f"{article_data['url']} 기사 데이터 추출 완료")
    return article_data

def normalized_1(df, columns):
    # 제1정규화 (1NF) - authors 리스트를 개별 행으로 변환
    for colunm in columns:
        df = df.withColumn(colunm, F.explode(F.col(colunm)))
    return df

def save_to_s3(df, theme):
    """기사 데이터 리스트를 Parquet 형식으로 저장 (pandas-on-Spark 사용)"""
    # Python 리스트를 Spark DataFrame으로 변환
    df = df.coalesce(1)  # 단일 Parquet 파일로 저장
    prefix = get_prefix('TRANSFORMED', theme)

    df.write.option('header', 'true').mode('overwrite').parquet(f's3a://{BUCKET_NAME}/{prefix}')
    print(f"Parquet 저장 완료: f's3a://{BUCKET_NAME}/{prefix}'")

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
            date TIMESTAMPTZ,
            text VARCHAR(65535),
            theme VARCHAR(100),
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
            date TIMESTAMPTZ,
            text VARCHAR(65535),
            theme VARCHAR(100),
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

def transform_to_parquet(url_per_themes):
    """테마별 JSON 파일을 처리 후 리스트로 저장 (병렬 처리)"""
    for theme, file_paths in url_per_themes.items():
        article_list = []
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            futures = {executor.submit(extract_article_data, file_path, theme): file_path for file_path in file_paths}
            for future in concurrent.futures.as_completed(futures):
                try:
                    article_data = future.result()
                    article_list.append(article_data)
                except Exception as e:
                    print(f"기사 데이터 추출 중 오류 발생: {e}")
        
        if article_list:
            spark_df = spark.createDataFrame(article_list)
            # 제1정규화 (1NF) - authors 리스트를 개별 행으로 변환
            normalized_df = normalized_1(spark_df, ["authors"])
            # ISO 8601 형식의 문자열을 unix_timestamp로 변환
            df = normalized_df.withColumn("date", F.to_timestamp(F.col("date"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"))
            df.printSchema()
            save_to_s3(df, theme)

if __name__ == "__main__":
    file_paths = load_files('RAW', 'json')
    transform_to_parquet(file_paths)
    print('데이터 적재 시작')
    parquet_paths = load_files('TRANSFORMED', 'parquet')
    save_to_redshift(parquet_paths)
    spark.stop()  # SparkSession 종료