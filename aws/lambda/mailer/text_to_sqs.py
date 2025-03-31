import os
import json
import boto3
import pymysql
from urllib.parse import unquote
from datetime import datetime
import time

sqs = boto3.client('sqs')
QUEUE_URL = unquote(os.environ.get('QUEUE_URL')) # 실제 SQS 큐 URL
DB_NAME = unquote(os.environ.get('DB_NAME'))
TABLE_NAME = unquote(os.environ.get('TABLE_NAME'))
RDS_HOST = unquote(os.environ.get('RDS_HOST'))
RDS_PORT = unquote(os.environ.get('RDS_PORT'))
RDS_USER = unquote(os.environ.get('RDS_USER'))
RDS_PASSWORD = unquote(os.environ.get('RDS_PASSWORD'))


def lambda_handler(event, context):
    text = event.get('text', '')
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
            print(message_body)
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