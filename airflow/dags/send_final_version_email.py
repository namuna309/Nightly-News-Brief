from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.dates import days_ago
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator
from datetime import timedelta
import pendulum


# AWS Lambda 설정
SEND_EMAIL_LAMBDA_FUNCTION_NAME = 'text_to_sqs'


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
    dag_id='send_final_version_email',
    default_args=default_args,
    description='Send final version brief document email',
    schedule_interval='0 22 * * *',  # 매일 07:00과 21:00에 실행
    start_date=days_ago(1).replace(tzinfo=pendulum.timezone("Asia/Seoul")),
    catchup=False,
) as dag:

    send_final_version_email = LambdaInvokeFunctionOperator(
        task_id = "send_final_version_email",
        function_name= SEND_EMAIL_LAMBDA_FUNCTION_NAME,
        aws_conn_id = "aws_conn",
        invocation_type = "RequestResponse"  # 동기 실행
    )

    end = EmptyOperator(task_id='end')

    send_final_version_email >> end