from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta 
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator

# AWS Lambda 설정
LAMBDA_FUNCTION_NAME = 'earning_call_scraper'

default_args = {
    'start_date': datetime(2025, 3, 14),
    'catchup': False
}

with DAG(dag_id='earning_calls_etl',
        default_args=default_args,
        description='기업 earning call 데이터 ETL 파이프라인 workflow',
        schedule_interval='0 7,21 * * *',
        catchup=False,
):

    start = EmptyOperator(task_id='start')

    scarping_carning_calls_task = LambdaInvokeFunctionOperator(
            task_id = f"invoke_lambda_earning_calls",
            function_name = LAMBDA_FUNCTION_NAME,
            aws_conn_id = "aws_conn",  # AWS 연결 ID (Airflow에서 설정 필요)
            invocation_type = "RequestResponse"  # 동기 실행
        )

    end = EmptyOperator(task_id='end')

    start >> scarping_carning_calls_task >> end