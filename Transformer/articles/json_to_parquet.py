import os
import json
import glob
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
from bs4 import BeautifulSoup
import concurrent.futures


# ✅ 현재 날짜 (Eastern Time 기준)
today = datetime.now(ZoneInfo("America/New_York")).date()

# ✅ 테마 설정
THEMES = ['Stock_Market', 'Original', 'Economies', 'Earning', 'Tech', 'Housing', 'Crypto']

def get_folder_path(base_dir, theme):
    """테마별 JSON 및 Parquet 저장 경로 반환"""
    return os.path.join(
        base_dir,
        "ARTICLES",
        theme.upper(),
        f"year={today.year}",
        f"month={today.strftime('%m')}",
        f"day={today.strftime('%d')}"
    )

def load_json_files():
    """테마별 JSON 파일 경로 로드"""
    url_per_themes = {theme: [] for theme in THEMES}
    for theme in THEMES:
        folder_path = get_folder_path('json', theme)
        json_files = glob.glob(os.path.join(folder_path, '*.json'))
        url_per_themes[theme].extend(json_files)
    return url_per_themes

def extract_article_data(file_path):
    """JSON 파일에서 HTML 파싱 및 기사 데이터 추출"""
    with open(file_path, 'r', encoding='utf-8') as f:
        article = json.load(f)

    soup = BeautifulSoup(article['content'], "html.parser")
    article_data = {
        'title': '',
        'date': '',
        'authors': [],
        'url': article['url'],
        'text': ''
    }

    article_wrap = soup.find("div", class_=lambda c: c and "article-wrap" in c)
    if not article_wrap:
        print(f"❌ 'article-wrap' 요소를 찾을 수 없습니다. URL: {article['url']}")
        return article_data

    # ✅ 제목 추출
    cover_title = article_wrap.find("div", class_=lambda c: c and "cover-title" in c)
    article_data['title'] = cover_title.get_text(strip=True) if cover_title else ""

    # ✅ 저자 추출
    byline_author = article_wrap.find("div", class_=lambda c: c and "byline-attr-author" in c)
    if byline_author:
        try:
            author_links = byline_author.find_all("a")
            if author_links:
                article_data['authors'] = [a.get_text(strip=True) for a in author_links]
                if article_data['authors'] == ['']:
                    author_img = author_links[0].find('img')
                    article_data['authors'] = [author_img['alt'].split(', ')[0]]
            else:
                authors_text = byline_author.text
                article_data['authors'] = [author.strip() for author in authors_text.split(', ')] if ',' in authors_text else [authors_text.strip()]
        except Exception as e:
            print(f"❌ 'authors' 파싱 실패: {e}. URL: {article_data['url']}")

    # ✅ 날짜 추출
    meta_time = article_wrap.find("time", class_=lambda c: c and "byline-attr-meta-time" in c)
    article_data['date'] = meta_time.get("data-timestamp", "") if meta_time else ""

    # ✅ 본문 추출
    body_wrap = soup.find("div", class_=lambda c: c and "body-wrap" in c)
    if body_wrap:
        body_div = body_wrap.find("div", class_=lambda c: c and "body" in c)
        article_data['text'] = body_div.get_text(separator="\n", strip=True) if body_div else ""

    print(f"✅ {article_data['url']} 기사 데이터 추출 완료")
    return article_data

def save_to_parquet(article_data, theme):
    """기사 데이터를 Parquet 형식으로 저장"""
    df = pd.DataFrame([article_data])  # 단일 기사이므로 리스트로 감싸서 DataFrame 생성

    title = article_data['url'].rsplit("/", 1)[-1].replace(".html", "").replace('-', '_')
    folder_name = get_folder_path('parquet', theme)

    os.makedirs(folder_name, exist_ok=True)
    parquet_file_path = os.path.join(folder_name, f"{title}.parquet")

    df.to_parquet(parquet_file_path, engine="pyarrow", compression="snappy", index=False)
    print(f"🎯 Parquet 저장 완료: {parquet_file_path}")

def process_article(theme, file_path):
    """JSON 파일을 읽고, 기사 데이터 추출 후 Parquet 변환"""
    article_data = extract_article_data(file_path)
    save_to_parquet(article_data, theme)

def process_articles(theme, file_paths):
    """한 개의 테마에 속한 모든 JSON 파일을 순차적으로 처리"""
    for file_path in file_paths:
        process_article(theme, file_path)


def process_articles_in_parallel(url_per_themes):
    """테마 단위로 병렬 처리"""
    max_workers = min(len(url_per_themes), os.cpu_count() // 2)  # 프로세스 개수 제한

    with concurrent.futures.ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(process_articles, theme, file_paths): theme
            for theme, file_paths in url_per_themes.items()
        }

        # ✅ 모든 작업 완료 대기
        for future in concurrent.futures.as_completed(futures):
            try:
                future.result()  # 예외 발생 시 출력
            except Exception as e:
                print(f"❌ 테마 '{futures[future]}' 처리 중 오류 발생: {e}")

if __name__ == "__main__":
    url_per_themes = load_json_files()
    process_articles_in_parallel(url_per_themes)
