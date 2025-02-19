import re
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager

class LinksScraper:
    def __init__(self, url: str):
        self.url = url
        # Selenium 옵션 설정 (헤드리스 모드)
        options = webdriver.ChromeOptions()
        options.add_argument("--headless")  # 브라우저 창을 띄우지 않음
        options.add_argument('--ignore-certificate-errors')
        options.add_argument('--ignore-certificate-errors-spki-list')
        options.add_argument('--ignore-ssl-errors')
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    def scroll_down_until_yesterday(self, max_scrolls=20, wait_time=2):
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

        # print("🚨 최대 스크롤 횟수 도달, 중지")

    def extract_article_links(self) -> list:
        """
        - 'content' 클래스를 포함한 div 요소들을 찾고,
        - 내부의 footer > publishing 영역에서 날짜 텍스트를 확인하여,
        - 날짜가 "yesterday"가 아닌 경우 해당 content 내부의 a 태그 href를 추출합니다.
        """
        article_links = []
        try:
            content_divs = self.driver.find_elements(By.XPATH, "//div[contains(@class, 'content')]")
        except Exception as e:
            print("❌ content 요소 찾기 실패:", e)
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
            if "yesterday" != date_text:
                try:
                    article_link = content.find_element(By.XPATH, ".//a").get_attribute("href")
                    article_links.append(article_link)
                except Exception:
                    continue

        return article_links

    def get_article_links(self) -> list:
        """
        입력 URL로 접속 후, 스크롤을 내려 날짜 정보를 업데이트하고,
        기사 링크(article_links)를 추출하여 반환합니다.
        """
        
        for i in range(5):
            self.driver.get(self.url)
            try:
                WebDriverWait(self.driver, 5).until(EC.presence_of_element_located((By.CLASS_NAME, "publishing")))
            except:
                print(f'{self.url}웹페이지 로딩 실패. 재시도 실행')
                continue
            else:
                time.sleep(3)  # 페이지 초기 로딩 대기
                self.scroll_down_until_yesterday(max_scrolls=20, wait_time=2)
                links = self.extract_article_links()
                self.driver.quit()
                return links
            

# 사용 예제:
if __name__ == "__main__":
    URL = "https://finance.yahoo.com/topic/stock-market-news/"
    scraper = LinksScraper(URL)
    article_links = scraper.get_article_links()
    if article_links:
        print("🔹 오늘 기사 링크:")
        for link in article_links:
            print(link)
    else:
        print("🚨 'yesterday'에 해당하는 기사 링크 없음.")
