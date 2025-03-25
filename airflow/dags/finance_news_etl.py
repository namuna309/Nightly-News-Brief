from airflow import DAG
from airflow.operators.python import BranchPythonOperator
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator, EmrServerlessStopApplicationOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta 
from airflow.models import Variable
import pendulum

# AWS Lambda 설정
EXTRACT_LAMBDA_FUNCTION_NAME = "article_scraper"
LOAD_TO_REDSHIFT_LAMBDA_FUNCTION_NAME = "article_loader"
LOAD_TO_RDS_LAMBDA_FUNCTION_NAME = "article_loader_to_rds"

# 크롤링할 URL 목록
THEME_URLS = {
    "Stock_Market": "https://finance.yahoo.com/topic/stock-market-news/",
    "Original": "https://finance.yahoo.com/topic/yahoo-finance-originals/",
    "Economies": "https://finance.yahoo.com/topic/economic-news/",
    "Earning": "https://finance.yahoo.com/topic/earnings/",
    "Tech": "https://finance.yahoo.com/topic/tech/",
    "Housing": "https://finance.yahoo.com/topic/housing-market/",
    "Crypto": "https://finance.yahoo.com/topic/crypto/",
    "Latest": "https://finance.yahoo.com/topic/latest-news/"
}

default_args = {
    'start_date': datetime(2025, 3, 25),
    'catchup': False,
    'retries': 4,
    'retry_delay': timedelta(minutes=3)
}

# 시간 조건 체크 함수
def check_scraping_time(**kwargs):
    # 현재 시간을 로컬 타임존으로 가져오기 (예: Asia/Seoul)
    local_tz = pendulum.timezone("Asia/Seoul")
    now = pendulum.now(local_tz)
    hour = now.hour
    
    # 오전 7시 (07:00) 또는 오후 9시 (21:00) 체크
    if (hour == 7) or (hour == 21):
        return [f"invoke_lambda_{theme}" for theme in THEME_URLS.keys()]
    else:
        return [f"invoke_lambda_Latest"]

def check_transforming_time(**kwargs):
    # 현재 시간을 로컬 타임존으로 가져오기 (예: Asia/Seoul)
    local_tz = pendulum.timezone("Asia/Seoul")
    now = pendulum.now(local_tz)
    hour = now.hour
    
    # 오전 7시 (07:00) 또는 오후 9시 (21:00) 체크
    if (hour == 7) or (hour == 21):
        return "extracting_articles_content"
    else:
        return "skip_emr"

with DAG('finance_news_etl', default_args=default_args, schedule_interval='@hourly') as dag:
    import json

    lambda_tasks = []
    for theme, url in THEME_URLS.items():
        invoke_lambda_function = LambdaInvokeFunctionOperator(
            task_id = f"invoke_lambda_{theme}",
            function_name = EXTRACT_LAMBDA_FUNCTION_NAME,
            payload = json.dumps({"theme": theme, "url": url}),
            aws_conn_id = "aws_conn",  # AWS 연결 ID (Airflow에서 설정 필요)
            invocation_type = "RequestResponse"  # 동기 실행
        )
        lambda_tasks.append(invoke_lambda_function)

    # 시간 조건 분기
    check_scraping_time_branch = BranchPythonOperator(
        task_id="check_scraping_time",
        python_callable=check_scraping_time,
        provide_context=True,
    )

    check_transforming_time_branch = BranchPythonOperator(
        task_id="check_transforming_time",
        python_callable=check_transforming_time,
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

    parquet_to_redshift_financial_articles_task = LambdaInvokeFunctionOperator(
        task_id = "invoke_loading_data_to_redshift_lambda",
        function_name= LOAD_TO_REDSHIFT_LAMBDA_FUNCTION_NAME,
        aws_conn_id = "aws_conn",  # AWS 연결 ID (Airflow에서 설정 필요)
        invocation_type = "RequestResponse"  # 동기 실행
    )

    parquet_to_rds_financial_articles_task = LambdaInvokeFunctionOperator(
        task_id = "invoke_loading_data_to_rds_lambda",
        function_name= LOAD_TO_RDS_LAMBDA_FUNCTION_NAME,
        aws_conn_id = "aws_conn",  # AWS 연결 ID (Airflow에서 설정 필요)
        invocation_type = "RequestResponse"  # 동기 실행
    )

    # 건너뛰기용 더미 태스크
    skip_emr = EmptyOperator(task_id="skip_emr")

    end = EmptyOperator(task_id='end')




    # DAG 실행 순서 정의
    check_scraping_time_branch >> lambda_tasks >> check_transforming_time_branch
    check_transforming_time_branch >> [emr_serverless_task, skip_emr]
    emr_serverless_task >> stop_emr_serverless_task >> [parquet_to_redshift_financial_articles_task, parquet_to_rds_financial_articles_task] >> end