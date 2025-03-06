import os
import re
import time
import boto3
import json
import subprocess
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from zoneinfo import ZoneInfo
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv


class WebScraper:
    def __init__(self, url):
        self.url = url
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                          "AppleWebKit/537.36 (KHTML, like Gecko) "
                          "Chrome/90.0.4430.93 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.google.com/",
            "Connection": "keep-alive"
        }

    def scrape(self):
        page_data = {
            'url': self.url,
            'timestamp': datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            'content': None
        }
        
        for attempt in range(10):
            try:
                response = requests.get(self.url, headers=self.headers)
                if response.status_code == 200:
                    page_data['content'] = BeautifulSoup(response.text, "html.parser").prettify()
                    return page_data
            except Exception as e:
                print(f"Attempt {attempt + 1} failed for {self.url}: {e}")
                time.sleep(1)
        
        print(f"Failed to fetch data from {self.url}")
        return page_data


class S3Uploader:
    def __init__(self, bucket_name):
        self.s3 = boto3.client("s3")
        self.bucket_name = bucket_name
    
    def upload(self, theme, page_data):
        today = datetime.now(ZoneInfo("America/New_York")).date()
        title = page_data["url"].rsplit("/", 1)[-1].replace(".html", "").replace("-", "_")
        
        s3_folder_path = (
            f"RAW/ARTICLES/{theme.replace(' ', '_').upper()}/"
            f"year={today.year}/month={today.strftime('%m')}/day={today.strftime('%d')}/"
        )
        s3_file_path = f"{s3_folder_path}{title}.json"
        json_data = json.dumps(page_data, ensure_ascii=False, indent=4)
        
        try:
            self.s3.put_object(
                Bucket=self.bucket_name,
                Key=s3_file_path,
                Body=json_data.encode('utf-8'),
                ContentType="application/json",
            )
            print(f"Uploaded to s3://{self.bucket_name}/{s3_file_path}")
        except Exception as e:
            print(f"S3 Upload Error: {e}")


class ChromeDriverManager:
    def __init__(self):
        self.driver = None
        self.options = Options()
        self._setup_options()

    def _setup_options(self):
        self.options.add_argument("--headless=new")
        self.options.add_argument("--no-sandbox")
        self.options.add_argument("--disable-dev-shm-usage")
        self.options.add_argument("--disable-gpu")
        self.options.binary_location = "/opt/chrome/chrome-linux64/chrome"
        
    def start_driver(self):
        service = Service("/opt/chrome-driver/chromedriver-linux64/chromedriver")
        self.driver = webdriver.Chrome(service=service, options=self.options)
        return self.driver
    
    def quit_driver(self):
        if self.driver:
            self.driver.quit()


def lambda_handler(event, context):
    load_dotenv()
    bucket_name = os.getenv("S3_BUCKET")
    theme = event["theme"]
    url = event["url"]
    s3_uploader = S3Uploader(bucket_name)
    
    scraper = WebScraper(url)
    driver_manager = ChromeDriverManager()
    driver = driver_manager.start_driver()
    
    try:
        driver.get(url)
        WebDriverWait(driver, 5).until(EC.presence_of_element_located((By.CLASS_NAME, "publishing")))
        article_links = [a.get_attribute("href") for a in driver.find_elements(By.XPATH, "//div[contains(@class, 'content')]//a")]
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
    finally:
        driver_manager.quit_driver()
    
    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(lambda l: s3_uploader.upload(theme, WebScraper(l).scrape()), link) for link in article_links]
        for future in as_completed(futures):
            future.result()
    
    return {"statusCode": 200, "body": json.dumps({"message": "Scraping completed"})}
