from newspaper import Article

# 대상 URL
url = "https://finance.yahoo.com/news/stock-market-today-asian-shares-031117951.html"

# Article 객체 생성
article = Article(url)

# 기사 다운로드 및 파싱
article.download()
article.parse()

# 기사 정보 출력
print("제목:", article.title)
print("저자:", article.authors)
print("게시 날짜:", article.publish_date)
print("\n본문:")
print(article.text)
