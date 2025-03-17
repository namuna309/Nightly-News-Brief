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
    
    def save_to_s3(self, df, prefix):
        wr.s3.to_parquet(
            df=df,
            path=f's3://{self.bucket_name}/{prefix}'
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
    
    def extract_earnings(self):
        earnings = []
        rows = self.soup.select("#earningsCalendarData tbody tr")[1:]
        for row in rows:
            earning = {
                'company': {'name': None, 'symbol': None},
                'country': None,
                'eps': {'actual': {'value': None, 'unit': None}, 'forecast': {'value': None, 'unit': None}},
                'revenue': {'actual': {'value': None, 'unit': None}, 'forecast': {'value': None, 'unit': None}},
                'market cap': {'value': None, 'unit': None},
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
                earning['market cap']['value'], earning['market cap']['unit'] = self.parse_value(td_cells[6].text.strip())
                
                earnings.append(earning)
            except Exception as e:
                print(f"Error processing row: {e}")
                continue
        return earnings

class EarningsTransformer:
    def __init__(self, earnings_data):
        self.df = pd.DataFrame(earnings_data)
    
    def save_as_parquet(self, s3_manager, prefix):
        s3_manager.save_to_s3(self.df, prefix)
        print(f"Parquet 파일 저장 완료: {prefix}")

class EarningsPipeline:
    def __init__(self):
        self.s3_manager = S3Manager()
    
    def set_day(self, daydelta):
        today = datetime.now(ZoneInfo("America/New_York")).date()
        self.day = today - timedelta(days = daydelta)
    
    def get_prefix(self, data_stage, format):
        return f"{data_stage}/EARNING_CALLS/year={self.day.year}/month={self.day.strftime('%m')}/day={self.day.strftime('%d')}/earning.{format}"
    
    def run(self):
        json_prefix = self.get_prefix('RAW', 'json')
        events = self.s3_manager.get_object(json_prefix)
        parser = EarningsParser(events['content'])
        earnings_data = parser.extract_earnings()
        
        transformer = EarningsTransformer(earnings_data)
        parquet_prefix = self.get_prefix('TRANSFORMED', 'parquet')
        transformer.save_as_parquet(self.s3_manager, parquet_prefix)

def lambda_handler(event, context):
    pipeline = EarningsPipeline()
    for d in [-1, 0, 1]:
        pipeline.set_day(daydelta=d)
        pipeline.run()
    return {"statusCode": 200, "body": "Processing completed successfully"}