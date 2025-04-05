import os
import re
import time
import boto3
import json
import re
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
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


class ChromeDriver:
    def __init__(self):
        
        self.driver = None
        self.options = Options()
        self._setup_options()

    def _setup_options(self):
        self.options.add_argument("--headless=new")
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--disable-gpu')
        self.options.add_argument("--disable-dev-shm-usage")
        self.options.add_argument('--window-size=1280x1696')
        self.options.add_argument('--single-process')
        self.options.binary_location = "/opt/chrome/chrome-linux64/chrome"
        
    def start_driver(self):
        service = Service("/opt/chrome-driver/chromedriver-linux64/chromedriver")
        print("Starting ChromeDriver...")
        self.driver = webdriver.Chrome(service=service, options=self.options)
        print(f"ChromeDriver started, Chrome version: {self.driver.capabilities['browserVersion']}")
    
    def quit_driver(self):
        if self.driver:
            self.driver.quit()
    
    def scroll_down(self, max_scrolls=20, wait_time=2, until=None):
        """
        스크롤을 내리면서 'publishing' 클래스를 포함한 태그 내부의 텍스트를 확인.
        날짜 정보에 숫자가 포함되지 않으면 스크롤을 중단하며,
        최대 max_scrolls 횟수까지 스크롤합니다.
        """
        previous_date_texts = []
        for scroll in range(max_scrolls):
            time.sleep(wait_time)  # 페이지 로드 대기
            try:
                date_elements = self.driver.find_elements(By.XPATH, "//div[contains(@class, 'publishing')]")
                date_texts = [el.get_attribute("textContent").split("•")[-1].strip() for el in date_elements]
            except Exception as e:
                print("❌ XPath 오류 발생:", e)
                return

            # 새로 로드된 날짜 텍스트 확인
            new_date_texts = date_texts[len(previous_date_texts):]

            # 날짜 정보에 숫자가 없는 경우 (ex. "yesterday" 등) 스크롤 중단
            contains_number = any(re.search(r'\d', text) for text in new_date_texts)
            if not contains_number:
                return

            previous_date_texts.extend(new_date_texts)
            # 페이지 하단으로 스크롤
            self.driver.find_element(By.TAG_NAME, "body").send_keys(Keys.END)
                # print(f"⬇ 스크롤 {scroll + 1}회 실행")

            print("스크롤 도달, 중지")
        
        
    def extract_article_links(self, until=None) -> list:
        """
        - 'content' 클래스를 포함한 div 요소들을 찾고,
        - 내부의 footer > publishing 영역에서 날짜 텍스트를 확인하여,
        - 날짜가 "yesterday"가 아닌 경우 해당 content 내부의 a 태그 href를 추출합니다.
        """
        article_links = []
        try:
            content_divs = self.driver.find_elements(By.XPATH, "//div[contains(@class, 'content')]")
        except Exception as e:
            print("content 요소 찾기 실패:", e)
            return article_links

        for content in content_divs:
            try:
                # 날짜 정보 추출 (footer > publishing)
                date_element = content.find_element(
                    By.XPATH, ".//div[contains(@class, 'footer')]//div[contains(@class, 'publishing')]"
                )
                date_text = date_element.get_attribute("textContent").split("•")[-1].strip()
            except Exception:
                continue  # 날짜 정보가 없으면 스킵

            # 원래 코드의 로직: 날짜 정보가 "yesterday"가 아닌 경우 링크 추출
            if until == 'yesterday':
                if "yesterday" != date_text:
                    try:
                        article_link = content.find_element(By.XPATH, ".//a").get_attribute("href")
                        article_links.append(article_link)
                    except Exception:
                        continue
            else:
                if re.search(r'(\d+)\s*(minutes?)', date_text):
                    try:
                        article_link = content.find_element(By.XPATH, ".//a").get_attribute("href")
                        article_links.append(article_link)
                    except Exception:
                        continue

        return article_links

    def get_article_links(self, theme, url):
        for i in range(5):
            try:
                print(f"Attempt {i+1}: Loading {url}")
                self.driver.get(url)
                print(f'{url}로 이동')
                WebDriverWait(self.driver, 5).until(EC.presence_of_element_located((By.CLASS_NAME, "publishing")))
                break
            except Exception as e:
                print(f"Attempt {i+1} failed: {e}")
                time.sleep(2)
        else:
            raise Exception("All attempts failed")
        time.sleep(3)
        print(f'{url}웹페이지 로딩 성공')
        until = 'yesterday' if theme != 'Latest' else None
        self.scroll_down(max_scrolls=20, wait_time=2, until=until)
        links = self.extract_article_links(until=until)
        return links
    


def lambda_handler(event, context):
    load_dotenv()
    bucket_name = os.getenv("S3_BUCKET")
    theme = event["theme"]
    url = event["url"]
    s3_uploader = S3Uploader(bucket_name)
    print("theme: " + theme)
    print("url: " + url)

    
    scraper = WebScraper(url)
    driver_manager = ChromeDriver()
    driver_manager.start_driver()
    print('드라이버 생성')
    try:
        article_links = driver_manager.get_article_links(theme, url)
    except Exception as e:
        print(f'{e}')
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
    finally:
        driver_manager.quit_driver()
    try:
        with ThreadPoolExecutor() as executor:
            futures = [executor.submit(lambda l: s3_uploader.upload(theme, WebScraper(l).scrape()), link) for link in article_links]
            for future in as_completed(futures):
                future.result()
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}         
    
    return {"statusCode": 200, "body": json.dumps({"message": "Scraping completed"})}
