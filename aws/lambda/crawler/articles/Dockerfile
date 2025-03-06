FROM amazon/aws-lambda-python:3.11

# Install chrome dependencies
RUN yum install -y atk cups-libs gtk3 libXcomposite alsa-lib \
    libXcursor libXdamage libXext libXi libXrandr libXScrnSaver \
    libXtst pango at-spi2-atk libXt xorg-x11-server-Xvfb \
    xorg-x11-xauth dbus-glib dbus-glib-devel nss mesa-libgbm jq unzip

# Copy and run the chrome installer script
COPY ./chrome-installer.sh ./chrome-installer.sh
RUN chmod +x ./chrome-installer.sh
RUN ./chrome-installer.sh
RUN rm ./chrome-installer.sh

RUN pip install requests
RUN pip install beautifulsoup4
# RUN pip install pymysql
RUN pip install selenium
RUN pip install python-dotenv

COPY *.py /var/task/
COPY .env /var/task/

WORKDIR /var/task

CMD [ "Scraper.lambda_handler" ]
