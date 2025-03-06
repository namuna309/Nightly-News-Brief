import os
import json
import boto3
import time
import subprocess
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from tempfile import mkdtemp
import time
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()
S3_BUCKET = os.getenv("S3_BUCKET")

class InvestingCalendarScraper:
    def __init__(self, headless=False):
        # Chrome 옵션 설정
        
        chrome_binary_location = "/opt/chrome/chrome-linux64/chrome"
        print("Chrome 버전 확인")
        chrome_version = subprocess.getoutput(f"{chrome_binary_location} --version")
        print(f"Chrome Version: {chrome_version}")

        options = Options()
        options.add_argument("--headless=new")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-gpu")
        options.add_argument("--disable-dev-tools")
        options.add_argument("--no-zygote")
        options.add_argument("--single-process")
        options.add_argument(f"--user-data-dir={mkdtemp()}")
        options.add_argument(f"--data-path={mkdtemp()}")
        options.add_argument(f"--disk-cache-dir={mkdtemp()}")
        options.add_argument(f'user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/{chrome_version} Safari/537.36')
        options.add_argument("--remote-debugging-pipe")
        options.add_argument("--verbose")
        options.add_argument("--log-path=/tmp")
        options.binary_location = chrome_binary_location

        service = Service(
            executable_path="/opt/chrome-driver/chromedriver-linux64/chromedriver",
            service_log_path="/tmp/chromedriver.log"
        )
        
        # WebDriver 실행
        self.driver = webdriver.Chrome(
            service=service,
            options=options
        )

        # 명시적 대기 객체 (최대 10초)
        self.wait = WebDriverWait(self.driver, 10)
        self.earnings = []
        self.page = {
            'url': "https://www.investing.com/earnings-calendar/",
            'timestamp': datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            'content': None
        }
    
    def window_maximize(self):
        self.driver.maximize_window()


    def check_and_close_popup(self):
        """
        팝업(클래스에 auth_popup_darkBackground___oAw5 포함)이 나타나면,
        팝업 내 close 버튼(i.popupCloseIcon.largeBannerCloser)을 클릭합니다.
        """
        try:
            # 팝업이 있는지 검사 (부분 클래스명 사용)
            popups = self.driver.find_elements(By.XPATH, "//*[contains(@class, 'js-gen-popup dark_graph')]")
            if popups:
                # 오버레이 요소 제거 시도 (닫기 버튼이 가려진 경우 대비)
                try:
                    overlay = WebDriverWait(self.driver, 5).until(
                        EC.presence_of_element_located((By.CLASS_NAME, "js-general-overlay"))
                    )
                    self.driver.execute_script("arguments[0].style.display = 'none';", overlay)
                except Exception as overlay_err:
                    print("Overlay not found or already hidden:", overlay_err)
                    return True
                    # 오버레이 제거에 실패해도 닫기 버튼이 클릭 가능할 수 있으므로 계속 진행
                
                try:
                    # 닫기 버튼이 클릭 가능할 때까지 대기
                    close_button = WebDriverWait(self.driver, 5).until(
                        EC.element_to_be_clickable((By.CSS_SELECTOR, "i.popupCloseIcon.largeBannerCloser"))
                    )
                    # 요소가 화면에 보이도록 스크롤
                    self.driver.execute_script("arguments[0].scrollIntoView(true);", close_button)
                    # JavaScript 클릭 실행 (기본 click()이 동작하지 않을 경우 대비)
                    self.driver.execute_script("arguments[0].click();", close_button)
                    print("Popup가 발견되어 닫았습니다.")
                    time.sleep(1)  # 닫은 후 안정화 대기
                except Exception as click_err:
                    print("Close button not interactable:", click_err)
                return True
        except Exception as e:
            print("팝업 닫기 중 에러:", e)
        return False

    def execute_step(self, step_func, description, max_retries=5, retry_interval=1):
        """
        전달된 step_func(단계 함수)를 실행하기 전에 팝업 여부를 확인하며,
        실행 도중 에러가 발생하면 팝업을 닫고 재시도합니다.
        """
        retries = 0
        while True:
            try:
                self.check_and_close_popup()  # 단계 시작 전에 팝업이 있으면 닫기
                step_func()  # 단계 실행
                return
            except Exception as e:
                self.check_and_close_popup()  # 예외 발생 시 팝업 닫기 시도
                retries += 1
                print(f"{description} 단계 실행 중 에러 발생: {e}. 재시도 {retries}회")
                time.sleep(retry_interval)
                if retries >= max_retries:
                    raise Exception(f"{description} 단계 재시도 실패: {e}")

    # 단계별 함수들 (각 단계가 완료되어야 다음 단계로 진행)
    def step_load_page(self):
        self.driver.get("https://www.investing.com/earnings-calendar/")
        time.sleep(2)  # 페이지 로딩 대기

    def step_click_day(self, day):
        # "Yesterday" 버튼 클릭 (버튼 ID가 "timeFrame_yesterday")
        day_button = self.wait.until(EC.element_to_be_clickable((By.ID, f"timeFrame_{day}")))
        day_button.click()
        print(f"{day} 버튼 클릭 완료")
        time.sleep(1)

    def step_click_filters(self):
        filters_button = self.wait.until(EC.element_to_be_clickable((By.ID, "filterStateAnchor")))
        filters_button.click()
        print("Filters 버튼 클릭 완료")
        time.sleep(1)

    def step_click_clear(self):
        clear_button = self.wait.until(EC.element_to_be_clickable(
            (By.XPATH, "//a[@onclick=\"clearAll('country[]');\"]")
        ))
        clear_button.click()
        print("Clear 버튼 클릭 완료")
        time.sleep(1)

    def step_click_us_checkbox(self):
        us_checkbox = self.wait.until(EC.presence_of_element_located((By.ID, "country5")))
        us_checkbox.click()
        print("United States 체크박스 클릭 완료")
        time.sleep(1)

    def step_click_apply(self):
        apply_button = self.wait.until(EC.presence_of_element_located((By.ID, "ecSubmitButton")))
        apply_button.click()
        print("Apply 버튼 클릭 완료")
        time.sleep(1)

    def step_extract_earnings(self):
        # 클래스가 'js-event-item'인 모든 <tr> 요소를 찾고 데이터를 추출합니다.
        event_rows = self.driver.find_elements(By.XPATH, "//*[@id=\"earningsCalendarData\"]/tbody/tr")
        print("\n추출된 실적 데이터:")
        for row in event_rows[1:]:
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
                earning['country'] = row.find_element(By.XPATH, ".//td[@class='flag']/span").get_attribute("title")

                earning['company']['name'] = row.find_element(
                    By.XPATH, ".//td[@class='left noWrap earnCalCompany']"
                ).get_attribute("title")
                earning['company']['symbol'] = row.find_element(
                    By.XPATH, ".//td[@class='left noWrap earnCalCompany']/a"
                ).text.strip()

                # EPS Actual / Forecast 처리
                eps_actual_text = row.find_element(By.XPATH, ".//td[3]").text
                eps_actual_value, eps_actual_unit = self.parse_value(eps_actual_text)
                earning['eps']['actual']['value'] = eps_actual_value
                if eps_actual_unit:
                    earning['eps']['actual']['unit'] = eps_actual_unit

                eps_forecast_text = row.find_element(By.XPATH, ".//td[4]").text
                eps_forecast_value, eps_forecast_unit = self.parse_value(eps_forecast_text, lstrip_chars='/')
                earning['eps']['forecast']['value'] = eps_forecast_value
                if eps_forecast_unit:
                    earning['eps']['forecast']['unit'] = eps_forecast_unit

                # Revenue Actual / Forecast 처리
                revenue_actual_text = row.find_element(By.XPATH, ".//td[5]").text
                revenue_actual_value, revenue_actual_unit = self.parse_value(revenue_actual_text)
                earning['revenue']['actual']['value'] = revenue_actual_value
                if revenue_actual_unit:
                    earning['revenue']['actual']['unit'] = revenue_actual_unit

                revenue_forecast_text = row.find_element(By.XPATH, ".//td[6]").text
                revenue_forecast_value, revenue_forecast_unit = self.parse_value(revenue_forecast_text, lstrip_chars='/')
                earning['revenue']['forecast']['value'] = revenue_forecast_value
                if revenue_forecast_unit:
                    earning['revenue']['forecast']['unit'] = revenue_forecast_unit

                # Market Cap 처리
                market_cap_text = row.find_element(By.XPATH, ".//td[7]").text
                market_cap_value, market_cap_unit = self.parse_value(market_cap_text)
                earning['market cap']['value'] = market_cap_value
                if market_cap_unit:
                    earning['market cap']['unit'] = market_cap_unit
                self.earnings.append(earning)
            except Exception as e:
                print(e)
                continue

    def parse_value(self, cell_text, lstrip_chars= ""):
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

    def step_extract_page_source(self):
        try:
            self.page['content'] = self.driver.page_source
        except Exception as e:
            print(e)

    def save_to_json(self, day):
        """추출한 데이터를 JSON 파일로 저장하는 메서드"""
        now = datetime.now(ZoneInfo("America/New_York"))
        if day == 'today':
            today = now
        elif day == 'yesterday':
            today = now - timedelta(days=1)
        elif day == 'tomorrow':
            today = now + timedelta(days=1)
        folder_name = os.path.join(
            'json',
            "earning_calls".upper(),  # "earnings"를 대문자로 변환
            f"year={today.year}",
            f"month={today.strftime('%m')}",
            f"day={today.strftime('%d')}"
        )
        os.makedirs(folder_name, exist_ok=True)
        file_path = os.path.join(folder_name, "earning.json")

        # with open(file_path, "w", encoding="utf-8") as f:
        #     json.dump({'earnings': self.earnings}, f, ensure_ascii=False, indent=4)
        # print(f"earnings saved to {file_path}")

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(self.page, f, ensure_ascii=False, indent=4)
        print(f"Events saved to {file_path}")
    
    def save_to_s3(self, day):
        """추출한 데이터를 JSON 파일로 저장하는 대신 S3 버킷에 업로드하는 메서드"""
        s3 = boto3.client("s3")
        now = datetime.now(ZoneInfo("America/New_York"))

        if day == 'today':
            today = now
        elif day == 'yesterday':
            today = now - timedelta(days=1)
        elif day == 'tomorrow':
            today = now + timedelta(days=1)

        # S3 저장 경로 설정
        s3_folder_path = (
            f"RAW/EARNING_CALLS/"  # 'earning_calls'를 대문자로 변환
            f"year={today.year}/month={today.strftime('%m')}/day={today.strftime('%d')}/"
        )
        s3_file_path = f"{s3_folder_path}earning.json"

        # JSON 변환
        json_data = json.dumps(self.page, ensure_ascii=False, indent=4)

        try:
            response = s3.put_object(
                Bucket=S3_BUCKET,
                Key=s3_file_path,
                Body=json_data.encode("utf-8"),
                ContentType="application/json",
            )
            print(f"  → [EARNING_CALLS] 데이터가 s3://{S3_BUCKET}/{s3_file_path} 에 업로드되었습니다.")
        except Exception as e:
            print(f"  → S3 업로드 중 오류 발생: {e}")

    


    def scrape_page_source(self, day):
        try:
            self.window_maximize()
            # 1. 페이지 접속
            self.execute_step(self.step_load_page, "페이지 접속")
            # 2. Tomorrow 버튼 클릭
            self.execute_step(lambda: self.step_click_day(day), f"{day} 버튼 클릭")
            # 3. Filters 버튼 클릭
            self.execute_step(self.step_click_filters, "Filters 버튼 클릭")
            # 4. Clear 버튼 클릭
            self.execute_step(self.step_click_clear, "Clear 버튼 클릭")
            # 5. United States 체크박스 클릭
            self.execute_step(self.step_click_us_checkbox, "United States 체크박스 클릭")
            # 6. Apply 버튼 클릭
            self.execute_step(self.step_click_apply, "Apply 버튼 클릭")
            # 7. 페이지 소스 추출
            self.execute_step(self.step_extract_page_source, "Page Source 추출")
            
            return self.page

        except Exception as e:
            print("스크래핑 도중 에러 발생:", e)
            return None
        finally:
            self.driver.quit()

def lambda_handler(event, context):
    for day in ['yesterday', 'today', 'tomorrow']:
        scraper = InvestingCalendarScraper(headless=True)
        page = scraper.scrape_page_source(day)
        if page is not None:
            scraper.save_to_s3(day)