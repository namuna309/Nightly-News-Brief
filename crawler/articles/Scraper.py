import os
import re
import time
import boto3
import json
from botocore.config import Config
from botocore.exceptions import ClientError
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
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
    def __init__(self, bucket_name, max_retries=3, timeout=10):
        # S3 클라이언트에 타임아웃 설정 추가
        config = Config(connect_timeout=timeout, retries={'max_attempts': max_retries})
        self.s3 = boto3.client("s3", config=config)
        self.bucket_name = bucket_name
        self.max_retries = max_retries
        print(f"S3Uploader 초기화 - 버킷: {self.bucket_name}, 최대 재시도: {max_retries}, 타임아웃: {timeout}초")
    
    def upload(self, theme, page_data):
        print(f"S3 업로드 시작: {page_data['url']}")  # 업로드 시작 표시 추가
        today = datetime.now(ZoneInfo("America/New_York")).date()
        title = page_data["url"].rsplit("/", 1)[-1].replace(".html", "").replace("-", "_")
        
        s3_folder_path = (
            f"RAW/ARTICLES/{theme.replace(' ', '_').upper()}/"
            f"year={today.year}/month={today.strftime('%m')}/day={today.strftime('%d')}/"
        )
        s3_file_path = f"{s3_folder_path}{title}.json"
        print(f"S3 경로 생성: {s3_file_path}")
        
        json_data = json.dumps(page_data, ensure_ascii=False, indent=4)
        print(f"JSON 데이터 크기: {len(json_data.encode('utf-8'))} 바이트")

        # 재시도 로직
        for attempt in range(self.max_retries):
            try:
                print(f"S3 업로드 시도 {attempt + 1}/{self.max_retries}: {s3_file_path}")
                self.s3.put_object(
                    Bucket=self.bucket_name,
                    Key=s3_file_path,
                    Body=json_data.encode('utf-8'),
                    ContentType="application/json",
                )
                print(f"업로드 성공: s3://{self.bucket_name}/{s3_file_path}")
                print(f"S3 업로드 완료: {page_data['url']}")
                return True  # 성공 시 종료
            except ClientError as e:
                if "Network" in str(e) or "Timeout" in str(e):  # 네트워크 관련 오류 체크
                    print(f"네트워크 오류 발생 - 시도 {attempt + 1} 실패: {e}")
                    if attempt < self.max_retries - 1:
                        wait_time = 2 ** attempt  # 지수 백오프: 1초, 2초, 4초
                        print(f"{wait_time}초 대기 후 재시도...")
                        time.sleep(wait_time)
                    else:
                        print(f"최대 재시도 횟수 초과 - 업로드 실패: {s3_file_path}")
                else:
                    print(f"기타 S3 오류: {e}")
                    break
            except Exception as e:
                print(f"예상치 못한 오류: {e}")
                break


class ChromeDriver:
    def __init__(self):
        self.driver = None
        self.options = Options()
        self._setup_options()

    def _setup_options(self):
        options = webdriver.ChromeOptions()
        self.options.add_argument("--headless")
        self.options.add_argument("--enable-unsafe-swiftshader")
        self.options.add_argument('--ignore-certificate-errors')
        self.options.add_argument('--ignore-certificate-errors-spki-list')
        self.options.add_argument('--disable-gpu')
        self.options.add_argument('--disable-dev-shm-usage')
        self.options.add_argument(f'user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/80.0.3987.87 Safari/537.36')
        self.options.add_argument('--no-sandbox')
        self.options.add_argument('--ignore-ssl-errors')
        self.options.add_experimental_option('excludeSwitches', ['enable-logging'])
        print("ChromeDriver 옵션 설정 완료")

    def start_driver(self):
        service = Service(ChromeDriverManager().install())
        print("ChromeDriver 시작 중...")
        self.driver = webdriver.Chrome(service=service, options=self.options)
        print(f"ChromeDriver 시작 완료 - Chrome 버전: {self.driver.capabilities['browserVersion']}")
    
    def quit_driver(self):
        if self.driver:
            print("ChromeDriver 종료 중...")
            self.driver.quit()
            print("ChromeDriver 종료 완료")
    
    def scroll_down(self, max_scrolls=20, wait_time=2, until=None):
        print(f"스크롤 시작 - 최대 {max_scrolls}회, 대기 시간 {wait_time}초")
        previous_date_texts = []
        for scroll in range(max_scrolls):
            time.sleep(wait_time)
            try:
                date_elements = self.driver.find_elements(By.XPATH, "//div[contains(@class, 'publishing')]")
                date_texts = [el.get_attribute("textContent").split("•")[-1].strip() for el in date_elements]
                print(f"스크롤 {scroll + 1}: {len(date_texts)}개의 날짜 요소 발견")
            except Exception as e:
                print(f"❌ XPath 오류 발생: {e}")
                return

            new_date_texts = date_texts[len(previous_date_texts):]
            print(f"새로 로드된 날짜 텍스트: {new_date_texts}")

            contains_number = any(re.search(r'\d', text) for text in new_date_texts)
            if not contains_number:
                print("날짜에 숫자가 없음 - 스크롤 중단")
                return

            previous_date_texts.extend(new_date_texts)
            self.driver.find_element(By.TAG_NAME, "body").send_keys(Keys.END)
            print(f"⬇ 스크롤 {scroll + 1}회 실행")

        print("최대 스크롤 도달, 중지")
        
    def extract_article_links(self, until=None) -> list:
        print("기사 링크 추출 시작")
        article_links = []
        try:
            content_divs = self.driver.find_elements(By.XPATH, "//div[contains(@class, 'content')]")
            print(f"발견된 콘텐츠 요소 수: {len(content_divs)}")
        except Exception as e:
            print(f"콘텐츠 요소 찾기 실패: {e}")
            return article_links

        for idx, content in enumerate(content_divs):
            try:
                date_element = content.find_element(
                    By.XPATH, ".//div[contains(@class, 'footer')]//div[contains(@class, 'publishing')]"
                )
                date_text = date_element.get_attribute("textContent").split("•")[-1].strip()
                print(f"콘텐츠 {idx + 1}: 날짜 텍스트 - {date_text}")
            except Exception:
                print(f"콘텐츠 {idx + 1}: 날짜 정보 없음, 스킵")
                continue

            if until == 'yesterday':
                if "yesterday" != date_text:
                    try:
                        article_link = content.find_element(By.XPATH, ".//a").get_attribute("href")
                        article_links.append(article_link)
                        print(f"링크 추가: {article_link}")
                    except Exception:
                        print(f"콘텐츠 {idx + 1}: 링크 추출 실패")
                        continue
            else:
                if re.search(r'(\d+)\s*(minutes?)', date_text):
                    try:
                        article_link = content.find_element(By.XPATH, ".//a").get_attribute("href")
                        article_links.append(article_link)
                        print(f"링크 추가: {article_link}")
                    except Exception:
                        print(f"콘텐츠 {idx + 1}: 링크 추출 실패")
                        continue

        print(f"추출된 기사 링크 수: {len(article_links)}")
        return article_links

    def get_article_links(self, theme, url):
        for i in range(5):
            try:
                print(f"시도 {i+1}: {url} 로드 중...")
                self.driver.get(url)
                print(f"{url}로 이동 완료")
                WebDriverWait(self.driver, 5).until(EC.presence_of_element_located((By.CLASS_NAME, "publishing")))
                break
            except Exception as e:
                print(f"시도 {i+1} 실패: {e}")
                time.sleep(2)
        else:
            raise Exception("모든 시도 실패")
        time.sleep(3)
        print(f"{url} 웹페이지 로딩 성공")
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
    print(f"theme: {theme}")
    print(f"url: {url}")

    scraper = WebScraper(url)
    driver_manager = ChromeDriver()
    driver_manager.start_driver()
    print("드라이버 생성 완료")
    try:
        print("기사 링크 수집 시작")
        article_links = driver_manager.get_article_links(theme, url)
        print(f"총 기사 링크 수: {len(article_links)}")
        print(f"수집된 링크 목록: {article_links}")
    except Exception as e:
        print(f"기사 링크 수집 실패: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}
    finally:
        driver_manager.quit_driver()

    failed_uploads = []  # 실패한 업로드 추적
    batch_size = 10
    try:
        for i in range(0, len(article_links), batch_size):
            batch = article_links[i:i + batch_size]
            print("S3 업로드 시작 - 스레드 풀 사용")
            with ThreadPoolExecutor() as executor:
                futures = [executor.submit(lambda l: s3_uploader.upload(theme, WebScraper(l).scrape()), link) 
                        for link in batch]
                print(f"업로드 작업 수: {len(futures)}")
                for idx, future in enumerate(as_completed(futures)):
                    try:
                        result = future.result()
                        if not result:
                            failed_uploads.append(article_links[idx])
                        print(f"업로드 작업 {idx + 1} 완료 - 성공 여부: {result}")
                    except Exception as e:
                        print(f"업로드 작업 {idx + 1} 실패: {e}")
                        failed_uploads.append(article_links[idx])
    except Exception as e:
        print(f"S3 업로드 중 전체 오류: {e}")
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}

    if failed_uploads:
        print(f"실패한 업로드 수: {len(failed_uploads)} - 링크: {failed_uploads}")
        # 여기에 실패한 작업 재처리 로직 추가 가능 (예: 별도 큐에 저장)   
    
    print("스크레이핑 및 업로드 완료")
    return {"statusCode": 200, "body": json.dumps({"message": "Scraping completed"})}

if __name__ == '__main__':
    THEME_URLS = {
        "Stock_Market": "https://finance.yahoo.com/topic/stock-market-news/",
        # "Original": "https://finance.yahoo.com/topic/yahoo-finance-originals/",
        # "Economies": "https://finance.yahoo.com/topic/economic-news/",
        # "Earning": "https://finance.yahoo.com/topic/earnings/",
        # "Tech": "https://finance.yahoo.com/topic/tech/",
        # "Housing": "https://finance.yahoo.com/topic/housing-market/",
        # "Crypto": "https://finance.yahoo.com/topic/crypto/",
        # "Latest": "https://finance.yahoo.com/topic/latest-news/"
    }

    for theme, url in THEME_URLS.items():
        print(f"테마 처리 시작: {theme}")
        event = {"theme": theme, "url": url}
        lambda_handler(event, [])
        print(f"테마 처리 완료: {theme}")