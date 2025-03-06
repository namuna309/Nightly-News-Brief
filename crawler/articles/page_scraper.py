import requests
import time
from bs4 import BeautifulSoup
import threading
from datetime import datetime
class TimeoutException(Exception):
    pass

def timeout_handler():
    raise TimeoutException("Scraper operation timed out after 60 seconds.")


class PagesScraper:
    def __init__(self, url: str):
        self.url = url
        self.headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/90.0.4430.93 Safari/537.36"
            }
        self.page = {
            'url': url,
            'timestamp': datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
            'content': None
        }
    
    def scrape(self, max_attempts=30) -> dict:
        for i in range(5):
            timer = threading.Timer(60, timeout_handler)
            timer.start()

            try:
                # 1. URL 접속하여 HTML 소스 가져오기 (200 응답을 받을 때까지 최대 10번 재시도)

                attempt = 0
                response = None

                while attempt < max_attempts:
                    try:
                        response = requests.get(self.url, headers=self.headers)
                        if response.status_code == 200:
                            break  # 200 응답이면 루프 종료
                        else:
                            pass
                    except Exception as e:
                        print(f"Attempt {attempt + 1} failed with error: {e}")
                    attempt += 1
                    time.sleep(1)  # 재시도 전에 1초 대기

                if not response or response.status_code != 200:
                    print(f"Failed to get 200 OK response after {max_attempts} attempts. url: {self.url}")
                    return self.page

                html = response.text
                soup = BeautifulSoup(html, "html.parser").prettify()
                self.page['content'] = soup
                
                return self.page
            except TimeoutException:
                print(f"{i+1} try: Scraper operation timed out after 60 seconds. url: {self.url}")
            finally:
                timer.cancel()
        return self.page


if __name__ == "__main__":
    url = "https://finance.yahoo.com/news/5-things-know-stock-market-132442666.html"
    scraper = PagesScraper(url)
    page = scraper.scrape()
    print(page)