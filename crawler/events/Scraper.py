import os
import json
import time
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import time
from datetime import datetime, timedelta
from decimal import Decimal  # 필요시 사용

from bs4 import BeautifulSoup

class InvestingCalendarScraper:
    def __init__(self, headless=False):
        # Chrome 옵션 설정
        options = webdriver.ChromeOptions()
        if headless:
            options.add_argument("--headless")
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--ignore-certificate-errors-spki-list')
        options.add_argument('--ignore-ssl-errors')
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        options.add_experimental_option("detach", True)  # 브라우저가 자동으로 닫히지 않도록 설정

        # WebDriver 실행
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        # 명시적 대기 객체 (최대 10초)
        self.wait = WebDriverWait(self.driver, 10)
        self.events = []
        self.page = {
            'url': "https://www.investing.com/economic-calendar/",
            'timestamp': datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            'content': None
        }

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
        self.driver.get(self.page['url'])
        time.sleep(2)  # 페이지 로딩 대기

    def step_click_day(self, txt):
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

    def step_extract_events(self):
        # 클래스가 'js-event-item'인 모든 <tr> 요소를 찾고 데이터를 추출합니다.
        event_rows = self.wait.until(EC.presence_of_all_elements_located((By.CSS_SELECTOR, "tr.js-event-item")))
        now = datetime.now(ZoneInfo("America/New_York"))
        if day == 'today':
            today = now
        elif day == 'yesterday':
            today = now - timedelta(days=1)
        elif day == 'tomorrow':
            today = now + timedelta(days=1)
        print("\n추출된 이벤트 데이터:")
        for row in event_rows:
            event = {
                'time': None,
                'utc': 'ET',
                'country': None,
                'volatility': None,
                'title': None,
                'actual': None,
                'forecast': None,
                'previous': None,
                'unit': None
            }
            try:
                # 시간은 24시간 형식 ("%H:%M")
                time_str = row.find_element(By.CLASS_NAME, "time").text.strip()
                event_time = datetime.strptime(time_str, "%H:%M").time()
                event['time'] = datetime.combine(today, event_time).isoformat()
                # 국가명 추출
                event['country'] = row.find_element(By.XPATH, "//td[@class='left flagCur noWrap']/span").get_attribute("title")
                # 변동성: 아이콘 개수
                event['volatility'] = len(row.find_elements(By.CLASS_NAME, "grayFullBullishIcon"))
                # 제목 추출
                event['title'] = row.find_element(By.CLASS_NAME, 'event').text.strip()
                # Actual, Forecast, Previous 값 추출
                actual = row.find_element(By.CLASS_NAME, 'act').text.strip()
                forecast = row.find_element(By.CLASS_NAME, 'fore').text.strip()
                previous = row.find_element(By.CLASS_NAME, 'prev').text.strip()
                
                # 단위가 포함된 경우 (마지막 문자가 숫자가 아니면)
                if previous and previous[-1] not in '0123456789': 
                    event['unit'] = previous[-1]               
                    actual = actual[:-1] if actual else actual
                    forecast = forecast[:-1] if forecast else forecast
                    previous = previous[:-1] if previous else previous
                else:
                    actual = actual if actual else ''
                    forecast = forecast if forecast else ''
                    previous = previous if previous else ''
                actual = actual.replace(',' ,'') if ',' in actual else actual
                forecast = forecast.replace(',' ,'') if ',' in forecast else forecast
                previous = previous.replace(',' ,'') if ',' in previous else previous

                event['actual'] = float(actual) if actual else None
                event['forecast'] = float(forecast) if forecast else None
                event['previous'] = float(previous) if previous else None
                self.events.append(event)
            except Exception as e:
                print(e)
                continue

    def step_extract_page_source(self):
        try:
            html = self.driver.page_source
            soup = BeautifulSoup(html, "html.parser").prettify()
            self.page['content'] = soup
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
            "events".upper(),  # "events"를 대문자로 변환
            f"year={today.year}",
            f"month={today.strftime('%m')}",
            f"day={today.strftime('%d')}"
        )
        os.makedirs(folder_name, exist_ok=True)
        file_path = os.path.join(folder_name, "events.json")

        # with open(file_path, "w", encoding="utf-8") as f:
        #     json.dump({'events': self.events}, f, ensure_ascii=False, indent=4)
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(self.page, f, ensure_ascii=False, indent=4)
        print(f"Events saved to {file_path}")

    def scrape_events(self, day):
        try:
            self.window_maximize()
            # 1. 페이지 접속
            self.execute_step(self.step_load_page, "페이지 접속")
            # 2. Tomorrow 버튼 클릭
            self.execute_step(self.step_click_day, f"{day} 버튼 클릭")
            # 3. Filters 버튼 클릭
            self.execute_step(self.step_click_filters, "Filters 버튼 클릭")
            # 4. Clear 버튼 클릭
            self.execute_step(self.step_click_clear, "Clear 버튼 클릭")
            # 5. United States 체크박스 클릭
            self.execute_step(self.step_click_us_checkbox, "United States 체크박스 클릭")
            # 6. Apply 버튼 클릭
            self.execute_step(self.step_click_apply, "Apply 버튼 클릭")
            # 7. 이벤트 데이터 추출
            self.execute_step(self.step_extract_events, "이벤트 데이터 추출")
            
            return self.events

        except Exception as e:
            print("스크래핑 도중 에러 발생:", e)
            return None
        finally:
            self.driver.quit()
    
    def scrape_page_source(self, day):
        try:
            # 1. 페이지 접속
            self.execute_step(self.step_load_page, "페이지 접속")
            # 2. Tomorrow 버튼 클릭
            self.execute_step(self.step_click_day, f"{day} 버튼 클릭")
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

if __name__ == '__main__':
    for day in ['yesterday', 'today', 'tomorrow']:
        scraper = InvestingCalendarScraper(headless=True)
        # events = scraper.scrape_events(day)
        # if events is not None:
        #     scraper.save_to_json(day)
        page = scraper.scrape_page_source(day)
        if page is not None:
            scraper.save_to_json(day)