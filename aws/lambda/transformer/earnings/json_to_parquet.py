import os
import json
from datetime import datetime
from zoneinfo import ZoneInfo
from bs4 import BeautifulSoup
import pandas as pd
import boto3
from urllib.parse import unquote



S3_REGION = unquote(os.environ.get('S3_REGION'))
BUCKET_NAME = unquote(os.environ.get('BUCKET_NAME')) 

s3_client = boto3.client(  # S3 클라이언트 생성
    service_name='s3',
    region_name=S3_REGION
)

def get_prefix(data_stage):
    """테마별 JSON 및 Parquet 저장 경로 반환"""
    return f"{data_stage}/EARNING_CALLS/year={today.year}/month={today.strftime('%m')}/day={today.strftime('%d')}/earning.json"


def parse_value(cell_text, lstrip_chars= ""):
        """
        셀의 텍스트를 파싱하여 숫자값과 단위를 추출합니다.
        
        :param cell_text: 셀의 원본 텍스트
        :param lstrip_chars: 왼쪽에서 제거할 문자(예: '/')
        :return: (value, unit) 튜플, 값이 없으면 (None, None)을 반환
        """
        if lstrip_chars:
            cell_text = cell_text.lstrip(lstrip_chars)
        text = cell_text.strip()
        if text == '--':
            return None, None
        unit = None
        # 마지막 문자가 숫자가 아니면 단위로 간주
        if text and not text[-1].isdigit():
            unit = text[-1]
            # 단위를 제거한 후 쉼표 제거
            text = text[:-1].replace(',', '')
        else:
            text = text.replace(',', '')
        try:
            value = float(text) if text else None
        except ValueError:
            value = None
        return value, unit

today = datetime.now(ZoneInfo("America/New_York")).date()
prefix = get_prefix('RAW')
file_path = ''
response = s3_client.list_objects_v2(Bucket=BUCKET_NAME, Prefix=prefix)
if "Contents" in response:
    obj = response["Contents"]
    if obj["Key"].endswith(f".{format}"):  # JSON 파일만 필터링
        file_path = f"{obj['Key']}"
object = s3_client.get_object(Bucket=BUCKET_NAME, Key=file_path)
events = json.loads(object['Body'].read())
html = events['content']
soup = BeautifulSoup(html, "html.parser")

earning_datas = []

# 클래스가 'js-event-item'인 모든 <tr> 요소를 찾고 데이터를 추출합니다.
earning_rows = soup.select("#earningsCalendarData tbody tr")

print("\n추출된 이벤트 데이터:")
for row in earning_rows[1:]:
    earning = {
                'company': {
                    'name': None,
                    'symbol': None
                },
                'country': None,
                'eps': {
                    'actual': {
                        'value': None,
                        'unit': None
                    },
                    'forecast': {
                        'value': None,
                        'unit': None
                    },
                },
                'revenue': {
                    'actual': {
                        'value': None,
                        'unit': None
                    },
                    'forecast': {
                        'value': None,
                        'unit': None
                    },
                },
                'market cap': {
                    'value': None,
                    'unit': None
                },
            }
    try:
        country_tag = row.select_one("td.flag span")
        earning['country'] = country_tag.get("title") if country_tag else None

        company_tag = row.select_one("td.left.noWrap.earnCalCompany")
        earning['company']['name'] = company_tag.get("title") if company_tag else None
        
        symbol_tag = row.select_one("td.left.noWrap.earnCalCompany a")
        earning['company']['symbol'] = symbol_tag.text.strip() if symbol_tag else None

        # EPS Actual / Forecast 처리
        eps_actual_tag = row.find_all("td")[2]  # 인덱스 2 = 3번째 <td>

        if eps_actual_tag:
            eps_actual_text = eps_actual_tag.text.strip()
            eps_actual_value, eps_actual_unit = parse_value(eps_actual_text)
            earning['eps']['actual']['value'] = eps_actual_value

            if eps_actual_unit:
                earning['eps']['actual']['unit'] = eps_actual_unit

        eps_forecast_tag = row.find_all("td")[3]  # 인덱스 3 = 4번째 <td>

        if eps_forecast_tag:
            eps_forecast_text = eps_forecast_tag.text.strip()
            eps_forecast_value, eps_forecast_unit = parse_value(eps_forecast_text, lstrip_chars='/')
            earning['eps']['forecast']['value'] = eps_forecast_value

            if eps_forecast_unit:
                earning['eps']['forecast']['unit'] = eps_forecast_unit

        # Revenue Actual / Forecast 처리
        revenue_actual_tag = row.find_all("td")[4]

        if revenue_actual_tag:
            revenue_actual_text = revenue_actual_tag.text.strip()
            revenue_actual_value, revenue_actual_unit = parse_value(revenue_actual_text)
            earning['revenue']['actual']['value'] = revenue_actual_value
            if revenue_actual_unit:
                earning['revenue']['actual']['unit'] = revenue_actual_unit
        
        revenue_forecast_tag = row.find_all("td")[5]
        if revenue_actual_tag:
            revenue_forecast_text = revenue_forecast_tag.text.strip()
            revenue_forecast_value, revenue_forecast_unit = parse_value(revenue_forecast_text, lstrip_chars='/')
            earning['revenue']['forecast']['value'] = revenue_forecast_value
            if revenue_forecast_unit:
                earning['revenue']['forecast']['unit'] = revenue_forecast_unit
        
        # Market Cap 처리
        market_cap_tag = row.find_all("td")[6]
        if market_cap_tag:
            market_cap_text = market_cap_tag.text.strip()
            market_cap_value, market_cap_unit = parse_value(market_cap_text)
            earning['market cap']['value'] = market_cap_value
            if market_cap_unit:
                earning['market cap']['unit'] = market_cap_unit

        earning_datas.append(earning)

    except Exception as e:
        print(f"Error processing row: {e}")
        continue

# Pandas DataFrame 변환
df = pd.DataFrame(earning_datas)

print(df)
# # Parquet 저장 경로 설정
# folder_name = os.path.join(
#     'parquet',
#     "earning_calls".upper(),  # "earnings"를 대문자로 변환
#     f"year={today.year}",
#     f"month={today.strftime('%m')}",
#     f"day={today.strftime('%d')}"
# )
# os.makedirs(folder_name, exist_ok=True)
# parquet_file_path = os.path.join(folder_name, "earning_calls.parquet")


# # Parquet 파일로 저장 (Snappy 압축 적용)
# df.to_parquet(parquet_file_path, engine="pyarrow", compression="snappy", index=False)

# print(f"\n🎯 Parquet 파일 저장 완료: {parquet_file_path}")

# # 결과 확인 (처음 5개 출력)
# print(df.head())
