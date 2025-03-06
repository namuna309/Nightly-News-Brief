# Python 3.11
FROM --platform=linux/amd64 python:3.11

WORKDIR /usr/src
RUN apt-get -y update
RUN apt install wget
RUN apt install unzip  
RUN wget https://dl.google.com/linux/direct/google-chrome-stable_current_amd64.deb
RUN apt -y install ./google-chrome-stable_current_amd64.deb
RUN wget -O /tmp/chromedriver.zip http://chromedriver.storage.googleapis.com/` curl -sS chromedriver.storage.googleapis.com/LATEST_RELEASE`/chromedriver_linux64.zip
RUN mkdir chrome
RUN unzip /tmp/chromedriver.zip chromedriver -d /usr/src/chrome

WORKDIR /app

# requirements.txt를 복사하여 의존성 설치
COPY ./requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# crawler 디렉토리 생성
RUN mkdir -p /app/crawler

# articles 폴더 내 Python 파일을 컨테이너 내 /app/crawler 폴더로 복사
COPY . /app/crawler/