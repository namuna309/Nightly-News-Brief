# Start from lambda Python3.8 image
FROM public.ecr.aws/lambda/python:3.11

# install python 3
RUN yum install -y gcc postgresql-devel

RUN yum install -y python3-pip && \
    pip3 install psycopg2-binary python-dotenv

COPY *.py /var/task/
COPY .env /var/task/

WORKDIR /var/task

# Command can be overwritten by providing a different command in the template directly.
CMD ["parquet_to_redshift.lambda_handler"]