from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta 
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from zoneinfo import ZoneInfo
import pendulum

# AWS Lambda 설정
EXTRACT_LAMBDA_FUNCTION_NAME = 'event_scraper'
TRANSFORM_LAMBDA_FUNCTION_NAME = 'event_transformer'
LOAD_TO_REDSHIFT_LAMBDA_FUNCTION_NAME = 'event_loader'
LOAD_TO_RDS_LAMBDA_FUNCTION_NAME = "event_loader_to_rds"

default_args = {
    'start_date': datetime(2025, 3, 25, tzinfo=pendulum.timezone("Asia/Seoul")),
    'catchup': False,
    'retries': 4,
}

with DAG(dag_id='finance_events_etl',
        default_args=default_args,
        description='경제 지표 발표 데이터 ETL 파이프라인 workflow',
        schedule_interval='0 7,21 * * *',
        catchup=False,
):

    start = EmptyOperator(task_id='start')

    scarping_financial_events_task = LambdaInvokeFunctionOperator(
            task_id = "invoke_extracting_data_lambda",
            function_name = EXTRACT_LAMBDA_FUNCTION_NAME,
            aws_conn_id = "aws_conn",  # AWS 연결 ID (Airflow에서 설정 필요)
            invocation_type = "RequestResponse"  # 동기 실행
        )
    
    json_to_parquet_financial_events_task = LambdaInvokeFunctionOperator(
        task_id = "invoke_transforming_data_lambda",
        function_name = TRANSFORM_LAMBDA_FUNCTION_NAME,
        aws_conn_id = "aws_conn",  # AWS 연결 ID (Airflow에서 설정 필요)
        invocation_type = "RequestResponse"  # 동기 실행
    )

    parquet_to_redshift_financial_evnets_task = LambdaInvokeFunctionOperator(
        task_id = "invoke_loading_data_to_redshift_lambda",
        function_name= LOAD_TO_REDSHIFT_LAMBDA_FUNCTION_NAME,
        aws_conn_id = "aws_conn",  # AWS 연결 ID (Airflow에서 설정 필요)
        invocation_type = "RequestResponse"  # 동기 실행
    )

    parquet_to_rds_financial_evnets_task = LambdaInvokeFunctionOperator(
        task_id = "invoke_loading_data_to_rds_lambda",
        function_name= LOAD_TO_RDS_LAMBDA_FUNCTION_NAME,
        aws_conn_id = "aws_conn",  # AWS 연결 ID (Airflow에서 설정 필요)
        invocation_type = "RequestResponse"  # 동기 실행
    )

    end = EmptyOperator(task_id='end')

    start >> scarping_financial_events_task >> json_to_parquet_financial_events_task
    json_to_parquet_financial_events_task >> [parquet_to_redshift_financial_evnets_task, parquet_to_rds_financial_evnets_task] >> end