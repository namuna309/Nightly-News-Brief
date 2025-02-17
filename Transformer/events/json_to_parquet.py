import os
import json
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from bs4 import BeautifulSoup
import pandas as pd


today = datetime.now(ZoneInfo("America/New_York")).date()
folder_name = os.path.join(
    'json',
    "events".upper(),  # "earnings"를 대문자로 변환
    f"year={today.year}",
    f"month={today.strftime('%m')}",
    f"day={today.strftime('%d')}"
)
os.makedirs(folder_name, exist_ok=True)
file_path = os.path.join(folder_name, "events.json")

with open(file_path, 'r', encoding='utf-8') as f:
    events = json.load(f)

html = events['content']
soup = BeautifulSoup(html, "html.parser")

event_datas = []

# 클래스가 'js-event-item'인 모든 <tr> 요소를 찾고 데이터를 추출합니다.
event_rows = soup.find_all("tr", class_="js-event-item")
tomorrow_date = (datetime.today() + timedelta(days=1)).date()

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
        # ✅ 시간 추출 (24시간 형식 "%H:%M")
        time_tag = row.find("td", class_="time")
        if time_tag:
            time_str = time_tag.text.strip()
            event_time = datetime.strptime(time_str, "%H:%M").time()
            event['time'] = datetime.combine(tomorrow_date, event_time).isoformat()

        # ✅ 국가명 추출 (국기 아이콘의 title 속성 사용)
        country_tag = row.select_one("td.left.flagCur.noWrap span")
        if country_tag:
            event['country'] = country_tag.get("title")

        # ✅ 변동성 (아이콘 개수 계산)
        volatility_icons = row.find_all("i", class_="grayFullBullishIcon")
        event['volatility'] = len(volatility_icons)

        # ✅ 이벤트 제목 추출
        title_tag = row.find("td", class_="event")
        if title_tag:
            event['title'] = title_tag.text.strip()

        # ✅ Actual, Forecast, Previous 값 추출
        actual_tag = row.find("td", class_="act")
        forecast_tag = row.find("td", class_="fore")
        previous_tag = row.find("td", class_="prev")

        actual = actual_tag.text.strip() if actual_tag else ""
        forecast = forecast_tag.text.strip() if forecast_tag else ""
        previous = previous_tag.text.strip() if previous_tag else ""

        # ✅ 단위 처리 (마지막 문자가 숫자가 아니면 단위로 간주)
        if previous and previous[-1] not in '0123456789':
            event['unit'] = previous[-1]
            actual = actual[:-1] if actual else actual
            forecast = forecast[:-1] if forecast else forecast
            previous = previous[:-1] if previous else previous

        # ✅ 숫자 변환 및 데이터 정리
        actual = actual.replace(',', '') if ',' in actual else actual
        forecast = forecast.replace(',', '') if ',' in forecast else forecast
        previous = previous.replace(',', '') if ',' in previous else previous

        event['actual'] = float(actual) if actual else None
        event['forecast'] = float(forecast) if forecast else None
        event['previous'] = float(previous) if previous else None

        event_datas.append(event)

    except Exception as e:
        print(f"Error processing row: {e}")
        continue

# ✅ Pandas DataFrame 변환
df = pd.DataFrame(event_datas)

# ✅ Parquet 저장 경로 설정
folder_name = os.path.join(
    'parquet',
    "events".upper(),  # "earnings"를 대문자로 변환
    f"year={today.year}",
    f"month={today.strftime('%m')}",
    f"day={today.strftime('%d')}"
)
os.makedirs(folder_name, exist_ok=True)
parquet_file_path = os.path.join(folder_name, "events.parquet")


# ✅ Parquet 파일로 저장 (Snappy 압축 적용)
df.to_parquet(parquet_file_path, engine="pyarrow", compression="snappy", index=False)

print(f"\n🎯 Parquet 파일 저장 완료: {parquet_file_path}")

# ✅ 결과 확인 (처음 5개 출력)
print(df.head())
