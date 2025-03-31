import os
import json
import pymysql
from urllib.parse import unquote

DB_NAME = unquote(os.environ.get('DB_NAME'))
TABLE_NAME = unquote(os.environ.get('TABLE_NAME'))
RDS_HOST = unquote(os.environ.get('RDS_HOST'))
RDS_PORT = int(unquote(os.environ.get('RDS_PORT')))
RDS_USER = unquote(os.environ.get('RDS_USER'))
RDS_PASSWORD = unquote(os.environ.get('RDS_PASSWORD'))

def lambda_handler(event, context):
    try:
        conn = pymysql.connect(host=RDS_HOST, user=RDS_USER, passwd=RDS_PASSWORD, db=DB_NAME, connect_timeout=5)
        body = json.loads(event['body'])
        response_id = body['responseId']
        timestamp = body['timestamp']
        email = body['email']

        with conn.cursor() as cur:
            cur.execute(f"""
                INSERT INTO {DB_NAME}.{TABLE_NAME} (response_id, timestamp, email_address)
                VALUES ('{response_id}', '{timestamp}', '{email}')
                ON DUPLICATE KEY UPDATE timestamp = '{timestamp}', email_address = '{email}'
            """)
            conn.commit()

        return {
            'statusCode': 200,
            'body': json.dumps('Email processed successfully')
        }

    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        }

    finally:
        conn.close()