import os
import json
import boto3
from sqlalchemy import create_engine, Column, Integer, String, DECIMAL, ForeignKey, TIMESTAMP, inspect
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
from decimal import Decimal
from dotenv import load_dotenv
from urllib.parse import unquote
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

# 환경 변수 로드
load_dotenv()

# 환경 변수 설정
S3_REGION = unquote(os.environ.get('S3_REGION'))
BUCKET_NAME = unquote(os.environ.get('BUCKET_NAME')) 
DB_NAME = unquote(os.environ.get('DB_NAME'))
TABLE_NAME = unquote(os.environ.get('TABLE_NAME'))
RDS_HOST = unquote(os.environ.get('RDS_HOST'))
RDS_PORT = unquote(os.environ.get('RDS_PORT'))
RDS_USER = unquote(os.environ.get('RDS_USER'))
RDS_PASSWORD = unquote(os.environ.get('RDS_PASSWORD'))

# S3 클라이언트 생성
s3_client = boto3.client(  # S3 클라이언트 생성
    service_name='s3',
    region_name=S3_REGION
)

# SQLAlchemy 설정
Base = declarative_base()
RDS_URL = f"mysql+pymysql://{RDS_USER}:{RDS_PASSWORD}@{RDS_HOST}:{RDS_PORT}/{DB_NAME}"
engine = create_engine(RDS_URL)
Session = sessionmaker(bind=engine)
session = Session()

# 테이블 정의
class Company(Base):
    __tablename__ = 'companies'
    company_id = Column(Integer, primary_key=True)
    name = Column(String(255))
    symbol = Column(String(50))
    country = Column(String(50))
    market_cap = Column(DECIMAL(20, 2))
    market_cap_unit = Column(String(5))
    earnings = relationship("Earning", back_populates="company")

class EarningPeriod(Base):
    __tablename__ = 'earning_periods'
    period_id = Column(Integer, primary_key=True)
    year = Column(Integer)
    quarter = Column(Integer)
    earnings = relationship("Earning", back_populates="period")

class Earning(Base):
    __tablename__ = 'earnings'
    earning_id = Column(Integer, primary_key=True)
    company_id = Column(Integer, ForeignKey('companies.company_id'))
    period_id = Column(Integer, ForeignKey('earning_periods.period_id'))
    eps_forecast = Column(DECIMAL(10, 4), nullable=True)
    eps_actual = Column(DECIMAL(10, 4), nullable=True)
    revenue_forecast = Column(DECIMAL(20, 2), nullable=True)
    revenue_forecast_unit = Column(String(5), nullable=True)
    revenue_actual = Column(DECIMAL(20, 2), nullable=True)
    revenue_actual_unit = Column(String(5), nullable=True)
    company = relationship("Company", back_populates="earnings")
    period = relationship("EarningPeriod", back_populates="earnings")
    call_date = Column(TIMESTAMP)
    timezone = Column(String(50))

# 테이블이 존재하지 않을 경우에만 생성
inspector = inspect(engine)
existing_tables = inspector.get_table_names()
required_tables = {'companies', 'earning_periods', 'earnings'}

if not required_tables.issubset(set(existing_tables)):
    print("테이블이 존재하지 않아 생성합니다.")
    Base.metadata.create_all(engine)
else:
    print("테이블이 이미 존재하므로 생략합니다.")


def set_day(daydelta):
    # 기준 날짜 설정 (뉴욕 시간 기준)
    today = datetime.now(ZoneInfo("America/New_York")).date()
    day = today - timedelta(days = daydelta)
    return day

def get_prefix(day):
    # S3에서 가져올 파일 경로 생성
    return f"OLTP/EARNING_CALLS/year={day.year}/month={day.strftime('%m')}/day={day.strftime('%d')}/earning.json"

def load_files():
    # S3에 존재하는 파일 경로 리스트 가져오기
    try:
        url_per_day = []
        for d in [-1, 0, 1]:
            day = set_day(daydelta=d)
            prefix = get_prefix(day)
            response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
            if "Contents" in response:
                obj = response["Contents"][0]
                if prefix == obj['Key']:
                    url_per_day.append(prefix)
    except Exception as e:
        print(f"prefix 추출 실패: {e}")
    else:
        print("parqeut 파일 s3 prefix 추출 완료")
        print(url_per_day)
        return url_per_day
    

def load_json_data(prefix):
    # S3에서 JSON 데이터 로드
    try:
        response = s3_client.get_object(Bucket=BUCKET_NAME, Key=prefix)
        data = json.loads(response['Body'].read())
        return data['data']
    except Exception as e:
        print(f"JSON 로드 실패: {e}")
        return []
    
def save_to_rds(json_paths):
    if not json_paths:
        print("처리할 JSON 경로가 없습니다. 종료합니다.")
        return
    
    for path in json_paths:
        print(f"{path} 처리 시작")
        records = load_json_data(path)

        for record in records:
            company_info = record["company"]
            company = session.query(Company).filter_by(
                name=company_info["name"],
                symbol=company_info["symbol"]
            ).first()

            if not company:
                company = Company(
                    name=company_info["name"],
                    symbol=company_info["symbol"],
                    country=record.get("country"),
                    market_cap=Decimal(record["market_cap"]["value"] or 0),
                    market_cap_unit=record["market_cap"]["unit"]
                )
                session.add(company)
                session.flush()
                print(f"신규 회사 추가: {company.name} ({company.symbol})")

            period = session.query(EarningPeriod).filter_by(
                year=record["year"],
                quarter=record["quarter"],
            ).first()

            if not period:
                period = EarningPeriod(
                    year=record["year"],
                    quarter=record["quarter"],
                )
                session.add(period)
                session.flush()
                print(f"신규 분기 추가: {period.year}년 {period.quarter}분기")

            existing_earning = session.query(Earning).filter_by(
                company_id=company.company_id,
                period_id=period.period_id
            ).first()

            call_date = record["call_date"]
            call_timestamp = datetime.fromtimestamp(call_date / 1000)

            new_earning = {
                "eps_forecast": Decimal(record["eps"]["forecast"]["value"] or 0),
                "eps_actual": Decimal(record["eps"]["actual"]["value"] or 0),
                "revenue_forecast": Decimal(record["revenue"]["forecast"]["value"] or 0),
                "revenue_forecast_unit": record["revenue"]["forecast"]["unit"],
                "revenue_actual": Decimal(record["revenue"]["actual"]["value"] or 0),
                "revenue_actual_unit": record["revenue"]["actual"]["unit"],
                "call_date": call_timestamp,
                "timezone": record["timezone"]
            }

            if existing_earning:
                # 최신 데이터와 기존 데이터가 다르면 업데이트
                updated=False
                for field, new_value in new_earning.items():
                    if getattr(existing_earning, field) != new_value:
                        setattr(existing_earning, field, new_value)
                        updated = True
                if updated:
                    print(f"업데이트됨: {company.name}, {period.year} Q{period.quarter}")
            else:
                earning = Earning(
                    company=company,
                    period=period,
                    **new_earning
                )
                session.add(earning)
                print(f"신규 earning 삽입: {company.name}, {period.year} Q{period.quarter}")

    session.commit()
    print("RDS 저장 완료")

def lambda_handler(event, context):
    json_paths = load_files()
    save_to_rds(json_paths)

    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }


