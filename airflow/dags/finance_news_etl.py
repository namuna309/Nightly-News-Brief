from airflow import DAG
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.utils.task_group import TaskGroup
from airflow.operators.empty import EmptyOperator
from datetime import datetime

# AWS Lambda 설정
LAMBDA_FUNCTION_NAME = "article_scraper"

# 크롤링할 URL 목록
THEME_URLS = {
    "Stock_Market": "https://finance.yahoo.com/topic/stock-market-news/",
    "Original": "https://finance.yahoo.com/topic/yahoo-finance-originals/",
    "Economies": "https://finance.yahoo.com/topic/economic-news/",
    "Earning": "https://finance.yahoo.com/topic/earnings/",
    "Tech": "https://finance.yahoo.com/topic/tech/",
    "Housing": "https://finance.yahoo.com/topic/housing-market/",
    "Crypto": "https://finance.yahoo.com/topic/crypto/"
}

default_args = {
    'start_date': datetime(2025, 3, 7),
    'catchup': False
}

with DAG('finance_news_etl', default_args=default_args, schedule_interval='@daily') as dag:
    import json

    lambda_tasks = []
    for theme, url in THEME_URLS.items():
        invoke_lambda_function = LambdaInvokeFunctionOperator(
            task_id=f"invoke_lambda_{theme}",
            function_name=LAMBDA_FUNCTION_NAME,
            payload=json.dumps({"theme": theme, "url": url}),
            aws_conn_id="aws_conn",  # AWS 연결 ID (Airflow에서 설정 필요)
            invocation_type="RequestResponse"  # 동기 실행
        )
        lambda_tasks.append(invoke_lambda_function)
    next_task = EmptyOperator(task_id="next_task")

    # DAG 실행 순서 정의
    lambda_tasks >> next_task