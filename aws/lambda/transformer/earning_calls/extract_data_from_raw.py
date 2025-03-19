import os
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from bs4 import BeautifulSoup
import pandas as pd
import boto3
from urllib.parse import unquote
import awswrangler as wr

class S3Manager:
    def __init__(self):
        self.s3_region = unquote(os.environ.get('S3_REGION'))
        self.bucket_name = unquote(os.environ.get('BUCKET_NAME'))
        self.s3_client = boto3.client('s3', region_name=self.s3_region)
    
    def get_object(self, prefix):
        response = self.s3_client.get_object(Bucket=self.bucket_name, Key=prefix)
        return json.loads(response['Body'].read())
    
    def save_to_s3(self, data, prefix, format):
        if format == 'parquet':
            wr.s3.to_parquet(
                df=data,
                path=f's3://{self.bucket_name}/{prefix}'
            )
        else:
            self.s3_client.put_object(
                Body=json.dumps(data),
                Bucket=self.bucket_name,
                Key=prefix
            )

class EarningsParser:
    def __init__(self, html_content):
        self.soup = BeautifulSoup(html_content, "html.parser")
    
    def parse_value(self, cell_text, lstrip_chars=""):
        if lstrip_chars:
            cell_text = cell_text.lstrip(lstrip_chars)
        text = cell_text.strip()
        if text == '--':
            return None, None
        unit = None
        if text and not text[-1].isdigit():
            unit = text[-1]
            text = text[:-1].replace(',', '')
        else:
            text = text.replace(',', '')
        try:
            value = float(text) if text else None
        except ValueError:
            value = None
        return value, unit
    
    def extract_earnings(self, daydelta):
        earnings = []
        rows = self.soup.select("#earningsCalendarData tbody tr")[1:]
        current_year = datetime.now().year
        current_quarter = (datetime.now().month - 1) // 3 + 1
        today = datetime.now(ZoneInfo("UTC")).replace(hour=0, minute=0, second=0, microsecond=0)
        call_date = (today - timedelta(days=daydelta)).timestamp()

        for row in rows:
            earning = {
                'company': {'name': None, 'symbol': None},
                'country': None,
                'eps': {'actual': {'value': None, 'unit': None}, 'forecast': {'value': None, 'unit': None}},
                'revenue': {'actual': {'value': None, 'unit': None}, 'forecast': {'value': None, 'unit': None}},
                'market_cap': {'value': None, 'unit': None},
                'year': current_year,
                'quarter' : current_quarter,
                'call_date': int(call_date * 1000),
                'timezone': 'America/New_York',
            }
            try:
                earning['country'] = row.select_one("td.flag span").get("title") if row.select_one("td.flag span") else None
                earning['company']['name'] = row.select_one("td.left.noWrap.earnCalCompany").get("title") if row.select_one("td.left.noWrap.earnCalCompany") else None
                earning['company']['symbol'] = row.select_one("td.left.noWrap.earnCalCompany a").text.strip() if row.select_one("td.left.noWrap.earnCalCompany a") else None
                
                td_cells = row.find_all("td")
                earning['eps']['actual']['value'], earning['eps']['actual']['unit'] = self.parse_value(td_cells[2].text.strip())
                earning['eps']['forecast']['value'], earning['eps']['forecast']['unit'] = self.parse_value(td_cells[3].text.strip(), lstrip_chars='/')
                earning['revenue']['actual']['value'], earning['revenue']['actual']['unit'] = self.parse_value(td_cells[4].text.strip())
                earning['revenue']['forecast']['value'], earning['revenue']['forecast']['unit'] = self.parse_value(td_cells[5].text.strip(), lstrip_chars='/')
                earning['market_cap']['value'], earning['market_cap']['unit'] = self.parse_value(td_cells[6].text.strip())
                
                earnings.append(earning)
            except Exception as e:
                print(f"Error processing row: {e}")
                continue
        return earnings

class EarningsTransformer:
    def __init__(self, earnings_data):
        self.df = pd.DataFrame(earnings_data)

    def convert_OLAP_df(self):
        # OLAP 분석용 테이블 변환
        # 현재 연도와 분기 계산
        df_olap = pd.DataFrame([
            {
                "company_name": row["company"]["name"],
                "symbol": row["company"]["symbol"],
                "country": row["country"],
                "year": row['year'],
                "quarter": row['quarter'],
                "eps_actual_value": row["eps"]["actual"]["value"],
                "eps_actual_unit": row["eps"]["actual"]["unit"],
                "eps_forecast_value": row["eps"]["forecast"]["value"],
                "eps_forecast_unit": row["eps"]["forecast"]["unit"],
                "revenue_actual_value": row["revenue"]["actual"]["value"],
                "revenue_actual_unit": row["revenue"]["actual"]["unit"],
                "revenue_forecast_value": row["revenue"]["forecast"]["value"],
                "revenue_forecast_unit": row["revenue"]["forecast"]["unit"],
                "market_cap_value": row["market_cap"]["value"],
                "market_cap_unit": row["market_cap"]["unit"],
                'call_date': row['call_date'],
                'timezone': row['timezone'],
            }
            for _, row in self.df.iterrows()
        ])
        return df_olap
    
    def convert_dataframe_for_redshift(self, df):
        dtype_mapping = {
            "company_name": "string",     # VARCHAR(100)
            "symbol": "string",           # VARCHAR(10)
            "country": "string",          # VARCHAR(100)
            "year": "int16",              # INT2
            "quarter": "int16",           # INT2
            "eps_actual_value": "float32",    # FLOAT4
            "eps_actual_unit": "string",  # CHAR
            "eps_forecast_value": "float32",  # FLOAT4
            "eps_forecast_unit": "string",    # CHAR
            "revenue_actual_value": "float32",  # FLOAT4
            "revenue_actual_unit": "string",  # CHAR
            "revenue_forecast_value": "float32",  # FLOAT4
            "revenue_forecast_unit": "string",  # CHAR
            "market_cap_value": "float32",  # FLOAT4
            "market_cap_unit": "string",    # CHAR
            "timezone": "string"
        }

        # 컬럼이 부족한 경우 대비하여 공통 컬럼만 변환
        common_columns = df.columns.intersection(dtype_mapping.keys())
        
        # 데이터 타입 변환
        df = df.astype({col: dtype_mapping[col] for col in common_columns})
        
        return df
    
    def convert_OLTP_json(self, earnings_data):
        json_oltp = {'data': earnings_data}
        return json_oltp
    

    def save_data(self, s3_manager, data, prefix, format):
        if format == 'parqeut':
            s3_manager.save_to_s3(data, prefix, format)
        else:
            s3_manager.save_to_s3(data, prefix, format)
        print(f"{format} 파일 저장 완료: {prefix}")

class EarningsPipeline:
    def __init__(self):
        self.s3_manager = S3Manager()
    
    def set_day(self, daydelta):
        today = datetime.now(ZoneInfo("America/New_York")).date()
        self.day = today - timedelta(days = daydelta)
    
    def get_prefix(self, data_stage, format):
        return f"{data_stage}/EARNING_CALLS/year={self.day.year}/month={self.day.strftime('%m')}/day={self.day.strftime('%d')}/earning.{format}"
        
    def run(self, daydelta):
        raw_prefix = self.get_prefix('RAW', 'json')
        events = self.s3_manager.get_object(raw_prefix)
        parser = EarningsParser(events['content'])
        earnings_data = parser.extract_earnings(daydelta)
        
        transformer = EarningsTransformer(earnings_data)

        oltp_json = transformer.convert_OLTP_json(earnings_data)
        json_prefix = self.get_prefix('OLTP', 'json')
        transformer.save_data(s3_manager=self.s3_manager, data=oltp_json, prefix=json_prefix, format='json')

        olap_df = transformer.convert_OLAP_df()
        olap_df = transformer.convert_dataframe_for_redshift(olap_df)
        parquet_prefix = self.get_prefix('TRANSFORMED', 'parquet')
        transformer.save_data(s3_manager=self.s3_manager, data=olap_df, prefix=parquet_prefix, format='parquet')



def lambda_handler(event, context):
    pipeline = EarningsPipeline()
    for d in [-1, 0, 1]:
        pipeline.set_day(daydelta=d)
        pipeline.run(daydelta=d)
    return {"statusCode": 200, "body": "Processing completed successfully"}