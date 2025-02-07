# main.py
import os
import json
from links_scraper import LinksScraper
from articles_scraper import ArticlesScraper
import concurrent.futures

def scrape_article(link: str) -> dict:
    """
    개별 기사 링크에 대해 ArticleScraper를 실행하여 기사를 스크랩한 후 딕셔너리 반환.
    """
    scraper = ArticlesScraper(link)
    return scraper.scrape()

def save_articles_to_json(theme: str, articles: list):
    """
    주어진 테마 이름의 폴더를 생성하고, 해당 폴더 내에 articles.json 파일로 기사 데이터를 저장함.
    """
    # 테마명에 공백이 있을 경우 그대로 사용하거나, 필요에 따라 슬러그화 가능
    folder_name = f'json\{theme}'
    os.makedirs(folder_name, exist_ok=True)
    file_path = os.path.join(folder_name, "articles.json")
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(articles, f, ensure_ascii=False, indent=4)
    print(f"  → [{theme}] 기사 데이터가 {file_path} 에 저장되었습니다.")

def main():
    # 테마별 Yahoo Finance 뉴스 페이지 URL 목록
    theme_urls = {
        "Stock Market": "https://finance.yahoo.com/topic/stock-market-news/",
        "Original": "https://finance.yahoo.com/topic/yahoo-finance-originals/",
        "Economies": "https://finance.yahoo.com/topic/economic-news/",
        "Earning": "https://finance.yahoo.com/topic/earnings/",
        "Tech": "https://finance.yahoo.com/topic/tech/",
        "Housing": "https://finance.yahoo.com/topic/housing-market/",
        "Crypto": "https://finance.yahoo.com/topic/crypto/"
    }
    
    articles_by_theme = {}
    
    # 각 테마별로 기사 링크 수집 및 병렬 스크랩 실행
    for theme, url in theme_urls.items():
        print(f"\n▶ [{theme}] 기사 링크 수집 중...")
        links_scraper = LinksScraper(url)
        article_links = links_scraper.get_article_links()
        print(f"  → {len(article_links)}개의 기사 링크 추출됨.")
        
        theme_articles = []
        # ThreadPoolExecutor를 사용하여 기사 스크랩 병렬 처리
        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            results = list(executor.map(scrape_article, article_links))
            theme_articles.extend(results)
        
        articles_by_theme[theme] = theme_articles
        # 각 테마별 폴더를 생성하고 JSON 파일로 저장
        save_articles_to_json(theme, theme_articles)

if __name__ == "__main__":
    main()
