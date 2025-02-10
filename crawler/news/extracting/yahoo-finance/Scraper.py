import os
import json
import concurrent.futures
from datetime import datetime
from links_scraper import LinksScraper
from articles_scraper import ArticlesScraper

class YahooFinanceScraper:
    """Yahoo Finance에서 뉴스 링크를 수집하고 기사를 스크랩하는 클래스"""

    THEME_URLS = {
        "Stock_Market": "https://finance.yahoo.com/topic/stock-market-news/",
        "Original": "https://finance.yahoo.com/topic/yahoo-finance-originals/",
        "Economies": "https://finance.yahoo.com/topic/economic-news/",
        "Earning": "https://finance.yahoo.com/topic/earnings/",
        "Tech": "https://finance.yahoo.com/topic/tech/",
        "Housing": "https://finance.yahoo.com/topic/housing-market/",
        "Crypto": "https://finance.yahoo.com/topic/crypto/"
    }

    def __init__(self, save_dir="json"):
        self.save_dir = save_dir
        self.articles_by_theme = {}

    def get_article_links(self, theme, url):
        """테마별 기사 링크를 수집하는 메서드"""
        print(f"\n▶ [{theme}] 기사 링크 수집 중...")
        links_scraper = LinksScraper(url)
        article_links = links_scraper.get_article_links()
        print(f"  → {len(article_links)}개의 기사 링크 추출됨.")
        return article_links

    def scrape_article(self, link):
        """개별 기사 스크랩을 실행하는 메서드"""
        scraper = ArticlesScraper(link)
        return scraper.scrape()

    def scrape_articles(self):
        """각 테마별 기사 링크를 수집하고 기사 데이터를 병렬 스크랩하는 메서드"""
        for theme, url in self.THEME_URLS.items():
            article_links = self.get_article_links(theme, url)
            theme_articles = []

            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                results = list(executor.map(self.scrape_article, article_links))
                theme_articles.extend(results)

            self.articles_by_theme[theme] = theme_articles
            self.save_to_json(theme, theme_articles)

    def save_to_json(self, theme, articles):
        import os
import json
import concurrent.futures
from links_scraper import LinksScraper
from articles_scraper import ArticlesScraper
from datetime import datetime

class YahooFinanceScraper:
    """Yahoo Finance에서 뉴스 링크를 수집하고 기사를 스크랩하는 클래스"""

    THEME_URLS = {
        "Stock Market": "https://finance.yahoo.com/topic/stock-market-news/",
        "Original": "https://finance.yahoo.com/topic/yahoo-finance-originals/",
        "Economies": "https://finance.yahoo.com/topic/economic-news/",
        "Earning": "https://finance.yahoo.com/topic/earnings/",
        "Tech": "https://finance.yahoo.com/topic/tech/",
        "Housing": "https://finance.yahoo.com/topic/housing-market/",
        "Crypto": "https://finance.yahoo.com/topic/crypto/"
    }

    def __init__(self, save_dir="json"):
        self.save_dir = save_dir
        self.articles_by_theme = {}

    def get_article_links(self, theme, url):
        """테마별 기사 링크를 수집하는 메서드"""
        print(f"\n▶ [{theme}] 기사 링크 수집 중...")
        links_scraper = LinksScraper(url)
        article_links = links_scraper.get_article_links()
        print(f"  → {len(article_links)}개의 기사 링크 추출됨.")
        return article_links

    def scrape_article(self, link):
        """개별 기사 스크랩을 실행하는 메서드"""
        scraper = ArticlesScraper(link)
        return scraper.scrape()

    def scrape_articles(self):
        """각 테마별 기사 링크를 수집하고 기사 데이터를 병렬 스크랩하는 메서드"""
        for theme, url in self.THEME_URLS.items():
            article_links = self.get_article_links(theme, url)
            theme_articles = []

            with concurrent.futures.ThreadPoolExecutor(max_workers=5) as executor:
                results = list(executor.map(self.scrape_article, article_links))
                theme_articles.extend(results)

            self.articles_by_theme[theme] = theme_articles
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

    def run(self):
        """전체 스크래핑 실행"""
        self.scrape_articles()

if __name__ == "__main__":
    scraper = YahooFinanceScraper()
    scraper.run()
