import os
import json
import boto3
import pymysql
from urllib.parse import unquote
from datetime import datetime
from zoneinfo import ZoneInfo
import time

sqs = boto3.client('sqs')
s3 = boto3.client('s3')

BUCKET_NAME = unquote(os.environ.get('BUCKET_NAME'))
QUEUE_URL = unquote(os.environ.get('QUEUE_URL')) # 실제 SQS 큐 URL
DB_NAME = unquote(os.environ.get('DB_NAME'))
TABLE_NAME = unquote(os.environ.get('TABLE_NAME'))
RDS_HOST = unquote(os.environ.get('RDS_HOST'))
RDS_PORT = unquote(os.environ.get('RDS_PORT'))
RDS_USER = unquote(os.environ.get('RDS_USER'))
RDS_PASSWORD = unquote(os.environ.get('RDS_PASSWORD'))

def get_prefix():
    day = datetime.now(ZoneInfo("Asia/Seoul")).date()
    return f"FINAL/year={day.year}/month={day.strftime('%m')}/day={day.strftime('%d')}/"

def get_latest_s3_file(prefix):
    # 객체 목록 가져오기
    response = s3.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)

    if "Contents" not in response:
        print("S3에 해당 prefix에 파일이 없습니다.")
        return None

    # 가장 최근에 업로드된 객체 선택 (LastModified 기준 정렬)
    latest_file = max(response["Contents"], key=lambda x: x["LastModified"])

    print(f"가장 최근 파일: {latest_file['Key']}")
    return latest_file["Key"]

def read_txt(prefix):
    response = s3.get_object(Bucket=BUCKET_NAME, Key=prefix)
    file_content = response['Body'].read().decode('utf-8')
    return file_content

def lambda_handler(event, context):
     
    prefix = get_prefix()
    latest_file_key = get_latest_s3_file(prefix=prefix)
    text = read_txt(latest_file_key)

    if not text:
        return {
            'statusCode': 400,
            'body': json.dumps('No text provided')
        }
    
    # RDS 연결
    try:
        connection = pymysql.connect(
            host=RDS_HOST,
            user=RDS_USER,
            password=RDS_PASSWORD,
            database=DB_NAME,
            connect_timeout=5
        )
        
        with connection.cursor() as cursor:
            # receiver 테이블에서 이메일 주소 조회
            cursor.execute(f"SELECT email_address FROM {DB_NAME}.{TABLE_NAME}")
            emails = [row[0] for row in cursor.fetchall()]
        
        connection.close()
        
        if not emails:
            return {
                'statusCode': 404,
                'body': json.dumps('No email addresses found in receiver table')
            }
        for address in emails:
            MessageGroupId=f'group-{int(time.time())}'
            # SQS로 메시지 전송
            message_body = {
                'text': text,
                'address': address
            }
            print(message_body['address'])
            sqs.send_message(
                QueueUrl=QUEUE_URL,
                MessageBody=json.dumps(message_body),
                MessageGroupId=MessageGroupId
            )
        
        return {
            'statusCode': 200,
            'body': json.dumps(f'Text and emails sent to SQS')
        }
    
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }