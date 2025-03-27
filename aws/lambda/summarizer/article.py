import os
import re
import json
import boto3
from sqlalchemy import create_engine, text
from urllib.parse import unquote
from openai import OpenAI
from time import sleep
from datetime import datetime
from zoneinfo import ZoneInfo
from dotenv import load_dotenv

load_dotenv()

# 환경 변수 로드
S3_REGION = unquote(os.environ.get('S3_REGION'))
BUCKET_NAME = unquote(os.environ.get('BUCKET_NAME')) 
DB_NAME = unquote(os.environ.get('DB_NAME'))
ARTICLE_TABLE_NAME = unquote(os.environ.get('ARTICLE_TABLE_NAME'))
RDS_HOST = unquote(os.environ.get('RDS_HOST'))
RDS_PORT = unquote(os.environ.get('RDS_PORT'))
RDS_USER = unquote(os.environ.get('RDS_USER'))
RDS_PASSWORD = unquote(os.environ.get('RDS_PASSWORD'))
OPENAI_API_KEY = unquote(os.environ.get('OPENAI_API_KEY'))
VECTOR_STORE_ID = unquote(os.environ.get('VECTOR_STORE_ID'))
ASSISTANT_ID = unquote(os.environ.get('ASSISTANT_ID'))

QUERY = f"""
SELECT DISTINCT a.title, a.text
FROM {DB_NAME}.{ARTICLE_TABLE_NAME} AS a
WHERE a.date >= CONVERT_TZ(
    DATE_SUB(DATE_FORMAT(NOW(), '%Y-%m-%d %H:00:00'), INTERVAL 10 HOUR),
    'UTC', 'America/New_York'
) 
AND a.date < CONVERT_TZ(
    NOW(),
    'UTC', 'America/New_York'
)
ORDER BY a.date;
"""

client = OpenAI(api_key=OPENAI_API_KEY)
today = datetime.now(ZoneInfo("Asia/Seoul")).date()

def fetch_news_from_rds(conn_str):
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
    print("기사 리스트를 텍스트로 변환 중...")
    new_str = ''
    for i, l in enumerate(lst):
        new_str += f'title: {l[0]}\ncontent\n{l[1]}\n\n'
    print(f"변환된 텍스트 길이: {len(new_str)}자")
    return new_str

def get_filename_with_date_hour(prefix="articles", format=".txt"):
    timestamp = datetime.now(ZoneInfo("Asia/Seoul")).strftime("%y%m%d%H")
    filename = f"{prefix}_{timestamp}{format}"
    print(f"생성된 파일명: {filename}")
    return filename

def get_prefix():
    filename = get_filename_with_date_hour()
    day = datetime.now(ZoneInfo("Asia/Seoul")).date()
    prefix = f"SUMMARY/ARTICLES/year={day.year}/month={day.strftime('%m')}/day={day.strftime('%d')}/{filename}"
    print(f"생성된 S3 prefix: {prefix}")
    return prefix

def upload_txt_file(text):
    uploaded_filename = "/tmp/" + get_filename_with_date_hour()
    print(f"로컬에 텍스트 파일 저장 중: {uploaded_filename}")
    
    with open(uploaded_filename, "w", encoding="utf-8") as f:
        f.write(text)
    print(f"파일 저장 완료: {uploaded_filename}")
    
    print("OpenAI에 파일 업로드 시작...")
    files_file = client.files.create(
        file=open(uploaded_filename, "rb"),
        purpose="assistants"
    )
    print(f"OpenAI 파일 업로드 완료, file_id: {files_file.id}")
 
    print(f"Vector Store에 파일 연결 중, vector_store_id: {VECTOR_STORE_ID}")
    vector_store_file = client.vector_stores.files.create(
        vector_store_id=VECTOR_STORE_ID,
        file_id=files_file.id
    )
    print(f"Vector Store 연결 완료, vector_store_file_id: {vector_store_file.id}")
    
    return vector_store_file

def summarize_with_llm(file_obj):
    print(f"LLM 요약 시작, file_id: {file_obj.id}")
    thread = client.beta.threads.create(
        tool_resources={
            "file_search": {
                "vector_store_ids": [VECTOR_STORE_ID]
            }
        }
    )
    print(f"Thread 생성 완료, thread_id: {thread.id}")
    
    print("메시지 생성 및 파일 첨부 중...")
    client.beta.threads.messages.create(
        thread_id=thread.id,
        role="user",
        content="파일에 있는 기사 자료를 설명해",
        attachments=[{"file_id": file_obj.id, "tools": [{"type": "file_search"}]}]
    )
    print("메시지 전송 완료")

    print(f"Assistant 실행 중, assistant_id: {ASSISTANT_ID}")
    run = client.beta.threads.runs.create(
        thread_id=thread.id,
        assistant_id=ASSISTANT_ID
    )
    print(f"Run 생성, run_id: {run.id}")
    
    while True:
        run_status = client.beta.threads.runs.retrieve(thread_id=thread.id, run_id=run.id)
        print(f"Run 상태 확인: {run_status.status}")
        if run_status.status == "completed":
            break
        sleep(1)

    print("응답 가져오는 중...")
    messages = client.beta.threads.messages.list(thread_id=thread.id)
    summary = messages.data[0].content[0].text.value
    print(f"요약 완료, 요약본 길이: {len(summary)}자")
    print(f"요약본 미리보기: {summary[:100]}...")
    return summary + '\n'

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

def clean_text(text):
    # '【숫자:숫자†문자열】' 패턴 제거
    res = re.sub(r'【\d+:\d+†.*?】', '', text)
    res = re.sub(r'^\s+', '', res, flags=re.MULTILINE)
    print(f'cleaning 완료')
    return res

def clean_storage():
    files = client.files.list().data
    if not files:
        print("삭제할 파일이 없습니다.")
    else:
        # 첫 번째 파일 기준
        for file_to_delete in files:
            file_id = file_to_delete.id
            print(f"삭제할 파일 ID: {file_id}")


            # Vector Store에서 파일 detach
            client.vector_stores.files.delete(
                vector_store_id=VECTOR_STORE_ID,
                file_id=file_id
            )
            print("Vector Store에서 파일 detach")

            # (2) Storage에서 파일 삭제
            client.files.delete(file_id=file_id)
        print("삭제 완료")

def lambda_handler(event, context):
    print("Lambda 함수 실행 시작")
    conn_str = f'mysql+pymysql://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{DB_NAME}'
    print("RDS에서 뉴스 가져오는 중...")
    news_articles = fetch_news_from_rds(conn_str)

    print("뉴스 텍스트로 변환 중...")
    news_str = list_to_txt(news_articles)
    
    print("텍스트 파일 업로드 중...")
    file_obj = upload_txt_file(news_str)

    print("LLM으로 뉴스 요약 생성 중...")
    summary = summarize_with_llm(file_obj)

    print("불필요한 요소 제거 중...")
    cleaned_summary = clean_text(summary)

    print("S3에 요약본 업로드 중...")
    upload_text_to_s3(cleaned_summary)

    print("OpenAI Storage Files 삭제 중...")
    clean_storage()

    print("Lambda 함수 실행 완료")
    return {
        'statusCode': 200,
        'body': json.dumps('News summary generation and S3 upload have been completed successfully.')
    }

if __name__ == "__main__":
    print("로컬 실행 시작")
    lambda_handler([], [])
    print("로컬 실행 종료")