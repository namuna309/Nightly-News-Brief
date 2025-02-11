import os
import json
import concurrent.futures
from datetime import datetime
from links_scraper import LinksScraper
from articles_scraper import ArticlesScraper

class YahooFinanceScraper:
    """Yahoo Finance에서 뉴스 링크를 수집하고 기사를 스크랩하는 클래스"""

    def __init__(self, save_dir="json"):
        self.save_dir = save_dir

    def get_links(self, theme, base_url):
        """테마별 기사 링크를 수집하는 메서드"""
        print(f"\n▶ [{theme}] 기사 링크 수집 중...")
        links_scraper = LinksScraper(base_url)
        article_links = links_scraper.get_article_links()
        print(f"  → {len(article_links)}개의 [{theme}] 기사 링크 추출됨.")
        return article_links

    def scrape_article(self, link):
        """개별 기사 스크랩을 실행하는 메서드"""
        scraper = ArticlesScraper(link)
        return scraper.scrape()

    def scrape_articles(self, theme, article_links):
        """각 테마별 기사 링크를 수집하고 기사 데이터를 병렬 스크랩하는 메서드"""
        theme_articles = []

        with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
            results = list(executor.map(self.scrape_article, article_links))
            theme_articles.extend(results)

        self.save_to_json(theme, theme_articles)

    def save_to_json(self, theme, articles):
        """기사 데이터를 JSON 파일로 저장하는 메서드"""
        today = datetime.today()
        folder_name = os.path.join(
            self.save_dir,
            theme.replace(" ", "_").upper(),  # 공백을 언더바로 변경
            f"year={today.year}",
            f"month={today.strftime('%m')}",
            f"day={today.strftime('%d')}"
        )

        # 디렉토리 생성 (존재하지 않으면 생성)
        os.makedirs(folder_name, exist_ok=True)

        file_path = os.path.join(folder_name, "articles.json")

        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(articles, f, ensure_ascii=False, indent=4)

        print(f"  → [{theme}] 기사 데이터가 {file_path} 에 저장되었습니다.")


if __name__ == "__main__":
    import time
    start = time.time()
    THEME_URLS = {
        "Stock_Market": "https://finance.yahoo.com/topic/stock-market-news/",
        "Original": "https://finance.yahoo.com/topic/yahoo-finance-originals/",
        "Economies": "https://finance.yahoo.com/topic/economic-news/",
        "Earning": "https://finance.yahoo.com/topic/earnings/",
        "Tech": "https://finance.yahoo.com/topic/tech/",
        "Housing": "https://finance.yahoo.com/topic/housing-market/",
        "Crypto": "https://finance.yahoo.com/topic/crypto/"
    }

    scraper = YahooFinanceScraper()
    theme_links = {theme: [] for theme in THEME_URLS.keys()}

    def fetch_links(theme, base_url):
        theme_links[theme] = scraper.get_links(theme, base_url)

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(THEME_URLS)) as executor:
        futures = [executor.submit(fetch_links, theme, base_url) for theme, base_url in THEME_URLS.items()]
        for future in futures:
            future.result()

    for theme, article_links in theme_links.items():
        scraper.scrape_articles(theme, article_links)
    print('time: ', time.time() - start)