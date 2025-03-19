import os
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from bs4 import BeautifulSoup
import pandas as pd
import boto3
import awswrangler as wr
from urllib.parse import unquote

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
            path=f's3://{self.bucket_name}/{prefix}',
            index=False
        )

class EventParser:
    def __init__(self, html_content):
        self.soup = BeautifulSoup(html_content, "html.parser")
    
    def extract_events(self, daydelta):
        event_datas = []
        event_rows = self.soup.find_all("tr", class_="js-event-item")
        date = (datetime.today() - timedelta(days = daydelta)).date()

        for row in event_rows:
            event = {
                'release_time': None,
                'timezone': 'America/New_York',
                'country': None,
                'volatility': None,
                'title': None,
                'actual': None,
                'forecast': None,
                'previous': None,
                'unit': None
            }
            try:
                time_tag = row.find("td", class_="time")
                if time_tag:
                    time_str = time_tag.text.strip()
                    event_time = datetime.strptime(time_str, "%H:%M").time()
                    event['release_time'] = int(datetime.combine(date, event_time).timestamp() * 1000)

                country_tag = row.select_one("td.left.flagCur.noWrap span")
                if country_tag:
                    event['country'] = country_tag.get("title")

                volatility_icons = row.find_all("i", class_="grayFullBullishIcon")
                event['volatility'] = len(volatility_icons)

                title_tag = row.find("td", class_="event")
                if title_tag:
                    event['title'] = title_tag.text.strip()

                actual_tag = row.find("td", class_="act")
                forecast_tag = row.find("td", class_="fore")
                previous_tag = row.find("td", class_="prev")

                actual = actual_tag.text.strip() if actual_tag else ""
                forecast = forecast_tag.text.strip() if forecast_tag else ""
                previous = previous_tag.text.strip() if previous_tag else ""

                if previous and previous[-1] not in '0123456789':
                    event['unit'] = previous[-1]
                    actual = actual[:-1] if actual else actual
                    forecast = forecast[:-1] if forecast else forecast
                    previous = previous[:-1] if previous else previous

                event['actual'] = float(actual.replace(',', '')) if actual else None
                event['forecast'] = float(forecast.replace(',', '')) if forecast else None
                event['previous'] = float(previous.replace(',', '')) if previous else None

                event_datas.append(event)
            except Exception as e:
                print(f"Error processing row: {e}")
                continue
        return event_datas

class EventTransformer:
    def __init__(self, event_data):
        self.df = pd.DataFrame(event_data)
    
    def save_as_parquet(self, s3_manager, prefix):
        if self.df.empty:  # DataFrame이 비어 있는지(지표 발표 일정이 있는지) 확인
            return
        s3_manager.save_to_s3(self.df, prefix)
    
    def convert_type(self, cols, d_types):
        if self.df.empty:  # DataFrame이 비어 있는지(지표 발표 일정이 있는지) 확인
            return

        for col, dtype in zip(cols, d_types):
            if col in self.df.columns:
                self.df[col] = self.df[col].astype(dtype)
            else:
                print(f"Column '{col}' not found.")

class EventPipeline:
    def __init__(self):
        self.s3_manager = S3Manager()
    
    def set_day(self, daydelta):
        today = datetime.now(ZoneInfo("America/New_York")).date()
        self.day = today - timedelta(days = daydelta)

    def get_prefix(self, data_stage, format):
        return f"{data_stage}/EVENTS/year={self.day.year}/month={self.day.strftime('%m')}/day={self.day.strftime('%d')}/event.{format}"

    def run(self, daydelta):
        json_prefix = self.get_prefix('RAW', 'json')
        response = self.s3_manager.s3_client.get_object(Bucket=self.s3_manager.bucket_name, Key=json_prefix)
        events = json.loads(response['Body'].read())
        
        parser = EventParser(events['content'])
        event_data = parser.extract_events(daydelta)
        
        transformer = EventTransformer(event_data)
        parquet_prefix = self.get_prefix('TRANSFORMED', 'parquet')
        transformer.convert_type(cols=['volatility', 'actual', 'forecast', 'previous'], d_types=['int16', 'float32', 'float32', 'float32'])
        transformer.save_as_parquet(self.s3_manager, parquet_prefix)

def lambda_handler(event, context):
    pipeline = EventPipeline()
    for d in [-1, 0, 1]:
        pipeline.set_day(daydelta=d)
        pipeline.run(daydelta=d)
    return {"statusCode": 200, "body": "Processing completed successfully"}