from airflow import DAG
from airflow.decorators import task
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta 
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
import pendulum


# AWS Lambda 설정
EXTRACT_LAMBDA_FUNCTION_NAME = 'earning_call_scraper'
TRANSFORM_LAMBDA_FUNCTION_NAME = 'earning_call_transformer'
LOAD_TO_REDSHIFT_LAMBDA_FUNCTION_NAME = 'earning_call_loader'
LOAD_TO_RDS_LAMBDA_FUNCTION_NAME = "earning_call_loader_to_rds"


default_args = {
    'start_date': datetime(2025, 3, 25, tzinfo=pendulum.timezone("Asia/Seoul")),
    'catchup': False,
    'retries': 4,
    'retry_delay': timedelta(minutes=3)
}

with DAG(dag_id='earning_calls_etl',
        default_args=default_args,
        description='기업 earning call 데이터 ETL 파이프라인 workflow',
        schedule_interval='0 7,21 * * *',
        catchup=False,
):

    start = EmptyOperator(task_id='start')

    scarping_earning_calls_task = LambdaInvokeFunctionOperator(
            task_id = f"invoke_extracting_data_lambda",
            function_name = EXTRACT_LAMBDA_FUNCTION_NAME,
            aws_conn_id = "aws_conn",  # AWS 연결 ID (Airflow에서 설정 필요)
            invocation_type = "RequestResponse"  # 동기 실행
        )
    
    transforming_earning_calls_task = LambdaInvokeFunctionOperator(
        task_id = f"invoke_transforming_data_lambda",
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

    oltp_json_to_rds_financial_evnets_task = LambdaInvokeFunctionOperator(
        task_id = "invoke_loading_data_to_rds_lambda",
        function_name= LOAD_TO_RDS_LAMBDA_FUNCTION_NAME,
        aws_conn_id = "aws_conn",  # AWS 연결 ID (Airflow에서 설정 필요)
        invocation_type = "RequestResponse"  # 동기 실행
    )

    end = EmptyOperator(task_id='end')

    start >> scarping_earning_calls_task >> transforming_earning_calls_task
    transforming_earning_calls_task >> [parquet_to_redshift_financial_evnets_task, oltp_json_to_rds_financial_evnets_task] >> end