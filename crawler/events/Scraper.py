import os
import json
import requests
import time
from datetime import datetime
from bs4 import BeautifulSoup

class EconodayScraper:
    """Econoday 웹사이트에서 경제 이벤트 데이터를 수집하는 클래스"""

    BASE_URL = 'https://us.econoday.com/'
    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                      "AppleWebKit/537.36 (KHTML, like Gecko) "
                      "Chrome/90.0.4430.93 Safari/537.36"
    }
    MAX_ATTEMPTS = 10

    def __init__(self, save_dir="json"):
        self.save_dir = save_dir
        self.soup = None
        self.events = []

    def fetch_html(self):
        """웹사이트에서 HTML을 가져오는 메서드"""
        attempt = 0
        while attempt < self.MAX_ATTEMPTS:
            try:
                response = requests.get(self.BASE_URL, headers=self.HEADERS)
                if response.status_code == 200:
                    self.soup = BeautifulSoup(response.text, "html.parser")
                    return True
                else:
                    print(f"Attempt {attempt + 1} failed: Status {response.status_code}")
            except Exception as e:
                print(f"Attempt {attempt + 1} failed with error: {e}")

            attempt += 1
            time.sleep(1)  # 재시도 전에 1초 대기

        print(f"Failed to fetch HTML after {self.MAX_ATTEMPTS} attempts.")
        return False

    def parse_events(self):
        """HTML을 파싱하여 경제 이벤트 데이터를 추출하는 메서드"""
        if not self.soup:
            print("No HTML data to parse.")
            return

        event_cell = self.soup.find(id="highlight_today")
        if not event_cell:
            print("No event data found.")
            return

        event_datas = event_cell.find_all(attrs={'class': 'econoevents', 'data-country': 'US'})

        for event in event_datas:
            event_title = event.find('a').get_text(strip=True) if event.find('a') else "Unknown Event"
            event_time_raw = event.find('span').get_text(strip=True) if event.find('span') else "12:00 AM ET"

            # " ET" 제거 후 시간 변환
            try:
                event_time_obj = datetime.strptime(event_time_raw[:-3], "%I:%M %p")
                today_date = datetime.today().date()
                event_datetime = datetime.combine(today_date, event_time_obj.time())
                event_time = event_datetime.strftime("%Y-%m-%d %H:%M:%S")
            except ValueError:
                event_time = "Invalid Time"

            self.events.append({'title': event_title, 'at': event_time})

    def save_to_json(self):
        """추출한 데이터를 JSON 파일로 저장하는 메서드"""
        today = datetime.today()
        folder_name = os.path.join(
            self.save_dir,
            "events".upper(),  # 공백을 언더바로 변경
            f"year={today.year}",
            f"month={today.strftime('%m')}",
            f"day={today.strftime('%d')}"
        )
        os.makedirs(folder_name, exist_ok=True)
        file_path = os.path.join(folder_name, "events.json")

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(self.events, f, ensure_ascii=False, indent=4)

        print(f"  → 경제 이벤트 데이터가 {file_path} 에 저장되었습니다.")

    def run(self):
        """스크래핑 전체 과정을 실행하는 메서드"""
        if self.fetch_html():
            self.parse_events()
            self.save_to_json()
        else:
            print("스크래핑에 실패했습니다.")


if __name__ == "__main__":
    scraper = EconodayScraper()
    scraper.run()
