from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from datetime import timedelta
import pendulum


# AWS Lambda 설정
SUMMARIZE_ARTICLE_LAMBDA_FUNCTION_NAME = 'summarizer_article'
SUMMARIZE_EARNING_LAMBDA_FUNCTION_NAME = 'summarizer_earning'
SUMMARIZE_EVENT_LAMBDA_FUNCTION_NAME = 'summarizer_event'
SEND_EMAIL_LAMBDA_FUNCTION_NAME = 'email_sender'



# 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
with DAG(
    dag_id='summarize_and_send',
    default_args=default_args,
    description='Summarize and send after ETLs are complete',
    schedule_interval='30 7,21 * * *',  # 매일 07:00과 21:00에 실행
    start_date=days_ago(1).replace(tzinfo=pendulum.timezone("Asia/Seoul")),
    catchup=False,
) as dag:

    summarize_article = LambdaInvokeFunctionOperator(
        task_id = "summarize_finanacial_aritlces",
        function_name= SUMMARIZE_ARTICLE_LAMBDA_FUNCTION_NAME,
        aws_conn_id = "aws_conn",
        invocation_type = "RequestResponse"  # 동기 실행
    )

    summarize_earning = LambdaInvokeFunctionOperator(
        task_id = "summarize_finanacial_earnings",
        function_name= SUMMARIZE_EARNING_LAMBDA_FUNCTION_NAME,
        aws_conn_id = "aws_conn",
        invocation_type = "RequestResponse"  # 동기 실행
    )

    summarize_event = LambdaInvokeFunctionOperator(
        task_id = "summarize_finanacial_events",
        function_name= SUMMARIZE_EVENT_LAMBDA_FUNCTION_NAME,
        aws_conn_id = "aws_conn",
        invocation_type = "RequestResponse"  # 동기 실행
    )

    send_email = LambdaInvokeFunctionOperator(
        task_id = "send_email",
        function_name= SEND_EMAIL_LAMBDA_FUNCTION_NAME,
        aws_conn_id = "aws_conn", 
        invocation_type = "RequestResponse"  # 동기 실행
    )

    end = EmptyOperator(task_id='end')

    # 의존성 설정
    # 1. 모든 요약 태스크가 완료된 후 이메일 전송
    [summarize_article, summarize_earning, summarize_event] >> send_email
    # 2. 이메일 전송 후 종료
    send_email >> end