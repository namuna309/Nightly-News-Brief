import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from urllib.parse import unquote
from dotenv import load_dotenv
from openai import OpenAI
load_dotenv()

S3_REGION = unquote(os.environ.get('S3_REGION'))
BUCKET_NAME = unquote(os.environ.get('BUCKET_NAME')) 
DB_NAME = unquote(os.environ.get('DB_NAME'))
TABLE_NAME = unquote(os.environ.get('TABLE_NAME'))
RDS_HOST = unquote(os.environ.get('RDS_HOST'))
RDS_PORT = unquote(os.environ.get('RDS_PORT'))
RDS_USER = unquote(os.environ.get('RDS_USER'))
RDS_PASSWORD = unquote(os.environ.get('RDS_PASSWORD'))
OPENAI_API_KEY = unquote(os.environ.get('OPENAI_API_KEY'))



def fetch_news_from_rds(conn_str):
    engine = create_engine(conn_str)
    try:
        with engine.connect() as conn:
            sql = "SELECT title, text FROM raw_data.financial_articles ORDER BY date DESC LIMIT 20"
            results = conn.execute(text(sql)).fetchall()
            return results
    finally:
        conn.close()

def summarize_with_llm(news_text):
    prompt = f"""다음은 뉴스 기사 모음이야. 주식 종목 추천 기사는 제외한 나머지 기사들의 내용을 요약해서 간단한 뉴스 브리핑 형식으로 만들어줘.
    
    {news_text}
    """

    # 📡 OpenAI API 호출
    client = OpenAI()

    completion = client.chat.completions.create(
        model="gpt-4o",
        messages=[
            {"role": "system", "content": "당신은 뉴스 요약을 잘하는 어시스턴트입니다."},
            {"role": "user", "content": prompt}
        ],
        temperature=0.7
    )
    
    # 📝 요약 결과 출력
    summary = completion.choices[0].message.content
    print("요약 결과")
    print(summary)

    return summary

def send_email(content):
    # 이메일 전송 로직
    pass

def process_and_send_news(event, context):
    # 1. RDS에서 뉴스기사 가져오기
    conn_str = f'mysql+pymysql://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{DB_NAME}'
    news_articles = fetch_news_from_rds(conn_str)

    formatted_text = ""
    for news_article in news_articles:
        formatted_text += f"제목: {news_article[0]}\n내용\n{news_article[1]}\n\n"

    with open("news_summary_output.txt", "w", encoding="utf-8") as f:
        f.write(formatted_text)
    # 2. LLM으로 뉴스 요약 생성
    # summary = summarize_with_llm(formatted_text)

    # print(summary)
    # 3. 이메일로 전송
    # send_email(summary)

if __name__ == "__main__":
    process_and_send_news([],[])