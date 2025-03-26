import os
import json
import boto3
from sqlalchemy import create_engine, text
from urllib.parse import unquote
from openai import OpenAI
from time import sleep
from datetime import datetime
from zoneinfo import ZoneInfo


S3_REGION = unquote(os.environ.get('S3_REGION'))
BUCKET_NAME = unquote(os.environ.get('BUCKET_NAME')) 
DB_NAME = unquote(os.environ.get('DB_NAME'))
TABLE_NAME = unquote(os.environ.get('TABLE_NAME'))
RDS_HOST = unquote(os.environ.get('RDS_HOST'))
RDS_PORT = unquote(os.environ.get('RDS_PORT'))
RDS_USER = unquote(os.environ.get('RDS_USER'))
RDS_PASSWORD = unquote(os.environ.get('RDS_PASSWORD'))
OPENAI_API_KEY = unquote(os.environ.get('OPENAI_API_KEY'))
VECTOR_STORE_ID = unquote(os.environ.get('VECTOR_STORE_ID'))
ASSISTANT_ID = unquote(os.environ.get('ASSISTANT_ID'))

client = OpenAI(api_key=OPENAI_API_KEY)
today = datetime.now().date()


    
def fetch_news_from_rds(conn_str):
    engine = create_engine(conn_str)
    try:
        with engine.connect() as conn:
            sql = """
                SELECT a.title, a.text FROM raw_data.financial_articles as a
                WHERE DATE(a.date) = CURDATE() - INTERVAL 1 DAY
                ORDER BY a.date;
            """
            results = conn.execute(text(sql)).fetchall()
            return results
    finally:
        conn.close()

def list_to_txt(lst):
    new_str = ''
    for l in lst:
        new_str += f'title: {l[0]}\ncontent\n{l[1]}\n\n'
    return new_str

def get_filename_with_date_hour(prefix="articles", format=".txt"):
    timestamp = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%y%m%d%H")  # YYMMDDhh
    return f"{prefix}_{timestamp}{format}"

def get_prefix():
    filename = get_filename_with_date_hour()
    day = datetime.now(ZoneInfo("Asia/Seoul")).date()
    return f"SUMMARY/ARTICLES/year={day.year}/month={day.strftime('%m')}/day={day.strftime('%d')}/{filename}"

def upload_txt_file(text):
    uploaded_filename = "/tmp/" + get_filename_with_date_hour()

    with open(uploaded_filename, "w", encoding="utf-8") as f:
        f.write(text)
    
    files_file = client.files.create(
        file=open(uploaded_filename, "rb"),
        purpose="assistants"
    )
 
    vector_store_file = client.vector_stores.files.create(
    vector_store_id=VECTOR_STORE_ID,
    file_id=files_file.id
    )

    return vector_store_file

def summarize_with_llm(file_obj):
    thread = client.beta.threads.create(
        tool_resources={
            "file_search": {
                "vector_store_ids": [VECTOR_STORE_ID]
            }
        }
    )
    client.beta.threads.messages.create(
        thread_id=thread.id,
        role="user",
        content="""
다음 규칙에 따라 자주 빈출된 내용을 중심으로 뉴스 브리핑을 작성해
규칙
1. 한국어로 작성할 것
2. 다음 양식을 지킬 것
    - 양식
        제목
        내용

        제목
        내용
3. 총 15개 내외 주요 뉴스로 작성할 것
4. 종목 추천 및 비추천 기사 제외
""",
        attachments=[{"file_id": file_obj.id, "tools": [{"type": "file_search"}]}]
    )

    # 3. Assistant 실행 및 응답 확인
    run = client.beta.threads.runs.create(
        thread_id=thread.id,
        assistant_id=ASSISTANT_ID
    )
    while True:
        run_status = client.beta.threads.runs.retrieve(thread_id=thread.id, run_id=run.id)
        if run_status.status == "completed":
            break
        sleep(1)

    messages = client.beta.threads.messages.list(thread_id=thread.id)
    summary = messages.data[0].content[0].text.value
    return summary

def upload_text_to_s3(text):
    s3_client = boto3.client(  # S3 클라이언트 생성
        service_name='s3',
        region_name=S3_REGION
    )
    prefix = get_prefix()
    # 문자열을 바이트로 인코딩하여 업로드
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=prefix,
        Body=text.encode("utf-8"),
        ContentType="text/plain"
    )

    print(f"업로드 완료: s3://{BUCKET_NAME}/{prefix}")

def lambda_handler(event, context):
    # TODO implement
    conn_str = f'mysql+pymysql://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{DB_NAME}'
    news_articles = fetch_news_from_rds(conn_str)

    news_str = list_to_txt(news_articles)
    file_obj = upload_txt_file(news_str)

    # 2. LLM으로 뉴스 요약 생성
    summary = summarize_with_llm(file_obj)

    # 3. 뉴스 요약본 S3에 저장
    upload_text_to_s3(summary)

    return {
        'statusCode': 200,
        'body': json.dumps('News summary generation and S3 upload have been completed successfully.')
    }
