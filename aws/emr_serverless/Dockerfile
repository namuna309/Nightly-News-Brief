# Dockerfile
FROM public.ecr.aws/emr-serverless/spark/emr-7.7.0:latest

USER root

# install python 3
RUN yum install -y python3-pip && \
    pip3 install beautifulsoup4 lxml psycopg2


# EMRS will run the image as hadoop
USER hadoop:hadoop