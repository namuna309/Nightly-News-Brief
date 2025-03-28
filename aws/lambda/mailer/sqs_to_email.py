import os
import re
import json
import boto3
import smtplib
from email.mime.text import MIMEText
from email.header import Header
from urllib.parse import unquote
from datetime import datetime
from zoneinfo import ZoneInfo

sqs = boto3.client('sqs')
SENDER_EMAIL = unquote(os.environ.get('SENDER_EMAIL')) 
GMAIL_KEY = unquote(os.environ.get('GMAIL_KEY')) 
SMTP_HOST = unquote(os.environ.get('SMTP_HOST'))
SMTP_PORT = unquote(os.environ.get('SMTP_PORT'))


def send_email(text, addr):
    now = datetime.now(ZoneInfo("Asia/Seoul"))
    current_hour = now.hour
    msg = MIMEText(text, 'plain', 'utf-8')
    msg['Subject'] = Header(f'{current_hour}시 뉴스 요약', 'utf-8')
    msg['From'] = SENDER_EMAIL
    msg['To'] = addr

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as connection:
            print("SMTP 서버 연결 성공")
            connection.starttls() #Transport Layer Security : 메시지 암호화
            print("TLS 시작")
            connection.login(user=SENDER_EMAIL, password=GMAIL_KEY)
            print("로그인 성공")
            connection.send_message(msg)  # MIMEText 객체 전송
    except Exception as e:
        print(f"이메일 전송 실패: {e}")
        return '이메일 전송 실패: {e}'
    else:
        print("이메일 전송 완료")
        return 'email sending completed successfully.'
    finally:
        print("SMTP 연결 종료")

def lambda_handler(event, context):
    # TODO implement
    record = event['Records'][0]
    print(record)
    message_body = json.loads(record['body'])
    text = message_body.get('text', '')
    address = message_body.get('address', '')

    res = send_email(text, address)

    return {
        'statusCode': 200,
        'body': json.dumps(res)
    }