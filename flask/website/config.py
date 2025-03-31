import os
from dotenv import load_dotenv
from urllib.parse import unquote

# 환경 변수 로드
load_dotenv()

# 환경 변수 설정
AWS_ACCESS_KEY = unquote(os.environ.get('AWS_ACCESS_KEY'))
AWS_SECRET_KEY = unquote(os.environ.get('AWS_SECRET_KEY'))
REGION = unquote(os.environ.get('REGION'))
DB_NAME = unquote(os.environ.get('DB_NAME'))
TABLE_NAME = unquote(os.environ.get('TABLE_NAME'))
RDS_HOST = unquote(os.environ.get('RDS_HOST'))
RDS_PORT = unquote(os.environ.get('RDS_PORT'))
RDS_USER = unquote(os.environ.get('RDS_USER'))
RDS_PASSWORD = unquote(os.environ.get('RDS_PASSWORD'))
SECRET_KEY = unquote(os.environ.get('SECRET_KEY'))
BUCKET_NAME = unquote(os.environ.get('BUCKET_NAME'))

class Config:
    AWS_ACCESS_KEY = AWS_ACCESS_KEY
    AWS_SECRET_KEY = AWS_SECRET_KEY
    REGION = REGION
    SQLALCHEMY_DATABASE_URI = f"mysql+pymysql://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{DB_NAME}"
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    SECRET_KEY = SECRET_KEY  # 세션 보안을 위한 시크릿 키
    BUCKET_NAME=BUCKET_NAME