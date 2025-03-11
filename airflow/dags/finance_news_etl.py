from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator, EmrServerlessStopApplicationOperator
from airflow.providers.amazon.aws.sensors.emr import EmrServerlessJobSensor
from airflow.operators.empty import EmptyOperator
from datetime import datetime
from airflow.models import Variable
import pendulum

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

# 시간 조건 체크 함수
def check_time(**kwargs):
    # 현재 시간을 로컬 타임존으로 가져오기 (예: Asia/Seoul)
    local_tz = pendulum.timezone("Asia/Seoul")
    now = pendulum.now(local_tz)
    hour = now.hour
    
    # 오전 7시 (07:00) 또는 오후 9시 (21:00) 체크
    if (hour == 7) or (hour == 21):
        return "extracting_articles_content"
    else:
        return "skip_emr"

with DAG('finance_news_etl', default_args=default_args, schedule_interval='@daily') as dag:
    import json

    lambda_tasks = []
    for theme, url in THEME_URLS.items():
        invoke_lambda_function = LambdaInvokeFunctionOperator(
            task_id = f"invoke_lambda_{theme}",
            function_name = LAMBDA_FUNCTION_NAME,
            payload = json.dumps({"theme": theme, "url": url}),
            aws_conn_id = "aws_conn",  # AWS 연결 ID (Airflow에서 설정 필요)
            invocation_type = "RequestResponse"  # 동기 실행
        )
        lambda_tasks.append(invoke_lambda_function)

    # 시간 조건 분기
    check_time_branch = BranchPythonOperator(
        task_id="check_time",
        python_callable=check_time,
        provide_context=True,
    )
    

    emr_job_driver_str = Variable.get("EMR_SERVERLESS_JOB_DRIVER")
    EMR_SERVERLESS_JOB_DRIVER = json.loads(emr_job_driver_str)

    emr_serverless_task = EmrServerlessStartJobOperator(
        task_id = "extracting_articles_content",
        application_id = Variable.get('EMR_SERVERLESS_APPLICATION_ID'),
        execution_role_arn = Variable.get('EMR_EXECUTION_ROLE_ARN'),
        job_driver = EMR_SERVERLESS_JOB_DRIVER,
        aws_conn_id = "aws_conn",
    )

    stop_emr_serverless_task = EmrServerlessStopApplicationOperator(
        task_id = "stopping_extracting_articles_application",
        application_id = Variable.get('EMR_SERVERLESS_APPLICATION_ID'),
        aws_conn_id = "aws_conn",
        force_stop = False,
    )

    # 건너뛰기용 더미 태스크
    skip_emr = DummyOperator(task_id="skip_emr")

    next_task = EmptyOperator(task_id="next_task")

    # DAG 실행 순서 정의
    lambda_tasks >> check_time_branch
    check_time_branch >> [emr_serverless_task, skip_emr]
    emr_serverless_task >>  stop_emr_serverless_task >> next_task
    skip_emr >> next_task