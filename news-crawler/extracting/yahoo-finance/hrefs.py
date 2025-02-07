import re
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from webdriver_manager.chrome import ChromeDriverManager
import time

# Selenium 설정
options = webdriver.ChromeOptions()
options.add_argument("--headless")  # 브라우저 창을 띄우지 않음
options.add_argument('--ignore-certificate-errors')
options.add_argument('--ignore-certificate-errors-spki-list')
options.add_argument('--ignore-ssl-errors')

# ChromeDriver 실행
driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

# Yahoo Finance 뉴스 페이지 열기
URL = "https://finance.yahoo.com/topic/stock-market-news/"
driver.get(URL)
time.sleep(3)  # 페이지 로딩 대기

def extract_article_links():
    """
    - content 클래스를 포함한 div 요소들을 찾음
    - 해당 div 내부에서 footer > publishing > i 태그 바로 다음의 텍스트 확인
    - 해당 텍스트가 'yesterday'면 content 내 a 태그의 href를 추출하여 출력
    """
    try:
        content_divs = driver.find_elements(By.XPATH, "//div[contains(@class, 'content')]")
    except Exception as e:
        print("❌ content 요소 찾기 실패:", e)
        return

    article_links = []

    for content in content_divs:
        try:
            # 해당 content 내부에서 날짜 정보 찾기
            date_element = content.find_element(By.XPATH, ".//div[contains(@class, 'footer')]//div[contains(@class, 'publishing')]")
            date_text = date_element.get_attribute("textContent").split("•")[-1].strip()
        except Exception:
            continue  # 날짜 정보가 없으면 스킵

        if "yesterday" != date_text:
            try:
                # 해당 content 내부의 a 태그에서 href 추출
                article_link = content.find_element(By.XPATH, ".//a").get_attribute("href")
                article_links.append(article_link)
            except Exception:
                continue  # a 태그가 없으면 스킵

    if article_links:
        print("🔹 오늘 기사 링크:")
        for link in article_links:
            print(link)
    else:
        print("🚨 'yesterday'에 해당하는 기사 링크 없음.")

# 스크롤을 내리면서 'yesterday' 여부를 확인하는 함수
def scroll_down_until_yesterday(max_scrolls=20, wait_time=2):
    """
    - 스크롤을 내리면서 'publishing' 클래스를 포함한 태그 내 i 태그 텍스트를 확인
    - 'yesterday'가 포함되면 스크롤을 멈춤
    - 최대 max_scrolls 횟수까지 스크롤
    """
    previous_date_texts = []
    for _ in range(max_scrolls):
        time.sleep(wait_time)  # 페이지 로드 대기
        
        # XPath를 사용하여 'publishing' 클래스를 가진 태그 내부의 <i> 태그 찾기
        try:
            date_elements = driver.find_elements(By.XPATH, "//div[contains(@class, 'publishing')]")
            date_texts = [el.get_attribute("textContent").split("•")[-1].strip() for el in date_elements]
        except Exception as e:
            print("❌ XPath 오류 발생:", e)
            return
        
        new_date_texts = date_texts[len(previous_date_texts):]
        for new_date_text in new_date_texts:
            print(new_date_text)
        print()
        ontains_number = any(re.search(r'\d', new_date_text) for new_date_text in new_date_texts)

        if not ontains_number:
            print("🚨 날짜 정보에 숫자가 포함되지 않음. 스크롤 중단.")
            return  # 숫자가 없는 경우 함수 종료

        previous_date_texts.extend(new_date_texts)
        # 'yesterday'가 없으면 스크롤 다운
        driver.find_element(By.TAG_NAME, "body").send_keys(Keys.END)
        print(f"⬇ 스크롤 {_ + 1}회 실행")

    print("🚨 최대 스크롤 횟수 도달, 중지")


# 'yesterday'가 나올 때까지 스크롤 실행
scroll_down_until_yesterday(max_scrolls=20, wait_time=2)

# 결과 출력
print("📌 스크롤 완료. 기사 링크 수집 시작")

# 기사 링크 추출 실행
extract_article_links()


driver.quit()