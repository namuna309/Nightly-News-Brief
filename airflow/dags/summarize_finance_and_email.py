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
    schedule_interval='0 7,21 * * *',  # 매일 07:00과 21:00에 실행
    start_date=days_ago(1).replace(tzinfo=pendulum.timezone("Asia/Seoul")),
    catchup=False,
) as dag:

    # 각 ETL DAG의 성공 여부를 감지하는 센서
    wait_for_earning_calls = ExternalTaskSensor(
        task_id='wait_for_earning_calls_etl',
        external_dag_id='earning_calls_etl',
        external_task_ids=['invoke_loading_data_to_redshift_lambda', 'invoke_loading_data_to_rds_lambda'],  # ETL DAG의 마지막 태스크 ID (가정)
        execution_delta=timedelta(minutes=0),  # 동일한 스케줄 시간 기준
        timeout=3600,  # 1시간 타임아웃
        mode='poke',
        poke_interval=60,  # 60초마다 확인
    )

    wait_for_finance_events = ExternalTaskSensor(
        task_id='wait_for_finance_events_etl',
        external_dag_id='finance_events_etl',
        external_task_ids=['invoke_loading_data_to_redshift_lambda', 'invoke_loading_data_to_rds_lambda'],  # ETL DAG의 마지막 태스크 ID (가정)
        execution_delta=timedelta(minutes=0),
        timeout=3600,
        mode='poke',
        poke_interval=60,
    )

    wait_for_finance_news = ExternalTaskSensor(
        task_id='wait_for_finance_news_etl',
        external_dag_id='finance_news_etl',
        external_task_ids=['invoke_loading_data_to_redshift_lambda', 'invoke_loading_data_to_rds_lambda'],  # ETL DAG의 마지막 태스크 ID (가정)
        execution_delta=timedelta(minutes=0),
        timeout=3600,
        mode='poke',
        poke_interval=60,
    )

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

    bridge = end = EmptyOperator(task_id='bridge')
    end = EmptyOperator(task_id='end')

    # 의존성 설정
    # 1. 모든 센서가 완료된 후 요약 태스크 실행
    [wait_for_earning_calls, wait_for_finance_events, wait_for_finance_news] >> bridge
    # 2. 모든 요약 태스크가 완료된 후 이메일 전송
    bridge >> [summarize_article, summarize_earning, summarize_event] >> send_email
    # 3. 이메일 전송 후 종료
    send_email >> end