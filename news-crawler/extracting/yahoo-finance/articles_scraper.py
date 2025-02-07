import requests
import time
from bs4 import BeautifulSoup

class ArticlesScraper:
    def __init__(self, url: str):
        self.url = url
        self.headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                            "AppleWebKit/537.36 (KHTML, like Gecko) "
                            "Chrome/90.0.4430.93 Safari/537.36"
            }
        self.article = {
            'title': '',
            'date': '',
            'authors': [],
            'url': url
            }
    
    def scrape(self, max_attempts=30) -> dict:
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
            return self.article

        html = response.text
        soup = BeautifulSoup(html, "html.parser")

        # 2. 'article-wrap'을 포함하는 클래스를 갖는 div 요소들 중 첫번째 요소 찾기
        article_wrap = soup.find("div", class_=lambda c: c and "article-wrap" in c)
        if not article_wrap:
            print("❌ 'article-wrap' 요소를 찾을 수 없습니다.")
            return self.article

        # 3. 'cover-title'을 포함하는 클래스를 갖는 div 요소를 찾아 제목 추출
        cover_title = article_wrap.find("div", class_=lambda c: c and "cover-title" in c)
        if cover_title:
            self.article['title'] = cover_title.get_text(strip=True)
        else:
            self.article['title'] = ""

        # 4. 'byline-attr-author' 클래스를 갖는 div 요소 밑에 있는 a 태그들의 텍스트들을 추출하여 저자 저장
        byline_author = article_wrap.find("div", class_=lambda c: c and "byline-attr-author" in c)
        if byline_author:
            author_links = byline_author.find_all("a")
            if author_links:
                self.article['authors'] = [a.get_text(strip=True) for a in author_links]
            else:
                authors_text = byline_author.get_text(strip=True)
                self.article['authors'] = [author.strip() for author in authors_text.split(', ')] if ',' in authors_text else [authors_text.strip()]
        else:
            self.article['authors'] = []

        # 5. 'byline-attr-meta-time' 클래스를 갖는 div의 data-timestamp 속성을 추출하여 저장
        meta_time = article_wrap.find("time", class_=lambda c: c and "byline-attr-meta-time" in c)
        if meta_time:
            self.article['date'] = meta_time.get("data-timestamp", "")
        else:
            self.article['date'] = ""

        # 6. 'body-wrap' 클래스를 포함한 div 요소를 찾아 그 내부의 본문 텍스트 추출
        body_wrap = soup.find("div", class_=lambda c: c and "body-wrap" in c)
        if body_wrap:
            # 6-1. body_wrap 내에서 'body' 클래스를 포함한 div 요소 찾기
            body_div = body_wrap.find("div", class_=lambda c: c and "body" in c)
            if body_div:
                # 줄바꿈(separator="\n")을 적용하여 텍스트 추출
                self.article['text'] = body_div.get_text(separator="\n", strip=True)
            else:
                self.article['text'] = ""
        else:
            self.article['text'] = ""
        return self.article


if __name__ == "__main__":
    url = "https://finance.yahoo.com/news/entegris-nasdaq-entg-q4-beats-110506608.html"
    scraper = ArticlesScraper(url)
    article = scraper.scrape()
    print(article)