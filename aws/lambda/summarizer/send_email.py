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
from dotenv import load_dotenv
load_dotenv()


S3_REGION = unquote(os.environ.get('S3_REGION'))
BUCKET_NAME = unquote(os.environ.get('BUCKET_NAME')) 
SENDER_EMAIL = unquote(os.environ.get('SENDER_EMAIL')) 
RECEIVER_EMAIL = unquote(os.environ.get('RECEIVER_EMAIL')) 
GMAIL_KEY = unquote(os.environ.get('GMAIL_KEY')) 
SMTP_HOST = unquote(os.environ.get('SMTP_HOST'))
SMTP_PORT = unquote(os.environ.get('SMTP_PORT'))


s3_client = boto3.client("s3")

def get_prefix(theme):
    day = datetime.now(ZoneInfo("Asia/Seoul")).date()
    return f"SUMMARY/{theme}/year={day.year}/month={day.strftime('%m')}/day={day.strftime('%d')}/"

def get_latest_s3_file(prefix):
    # 객체 목록 가져오기
    response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)

    if "Contents" not in response:
        print("S3에 해당 prefix에 파일이 없습니다.")
        return None

    # 가장 최근에 업로드된 객체 선택 (LastModified 기준 정렬)
    latest_file = max(response["Contents"], key=lambda x: x["LastModified"])

    print(f"가장 최근 파일: {latest_file['Key']}")
    return latest_file["Key"]

def read_txt(title, prefix):
    response = s3_client.get_object(Bucket=BUCKET_NAME, Key=prefix)
    file_content = response['Body'].read().decode('utf-8')
    return f"【{title}】\n"+file_content

def clean_text(text):
    # '【숫자:숫자†문자열】' 패턴 제거
    return re.sub(r'【\d+:\d+†.*?】', '', text)



def send_email(summary):
    now = datetime.now(ZoneInfo("Asia/Seoul"))
    current_hour = now.hour
    msg = MIMEText(summary, 'plain', 'utf-8')
    msg['Subject'] = Header(f'{current_hour}시 뉴스 요약', 'utf-8')
    msg['From'] = SENDER_EMAIL
    msg['To'] = RECEIVER_EMAIL

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
        return 'News summary email sending completed successfully.'
    finally:
        print("SMTP 연결 종료")


def lambda_handler(event, context):
    email_context = ""
    themes = [("ARTICLES", "미국 뉴스"), ("EVENTS", "지표 발표"), ("EARNINGS", "실적 발표")]
    for i in range(len(themes)):
        theme_eng, theme_kor = themes[i]
        prefix = get_prefix(theme_eng)
        latest_file_key = get_latest_s3_file(prefix=prefix)
        summary = read_txt(theme_kor, latest_file_key)
        email_context += summary
        email_context += '' if i == len(themes) - 1 else '\n--------------------------------------------------------------------------------------------------------------------\n\n'

    res = send_email(email_context)

    return {
        'statusCode': 200,
        'body': json.dumps(res)
    }

if __name__ == "__main__":
    print(lambda_handler([],[]))