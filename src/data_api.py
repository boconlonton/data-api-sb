import os
import boto3

from dotenv import load_dotenv

from aurora_data_api import connect

load_dotenv()

_DATABASE = os.getenv('AURORA_ARN')
_CREDENTIALS = os.getenv('SECRET_ARN')
rds_data_client = boto3.client('rds-data')


def execute_statement(sql, sql_parameters=[]):
    response = rds_data_client.execute_statement(
        secretArn=_CREDENTIALS,
        database='test_euni',
        resourceArn=_DATABASE,
        sql=sql,
        parameters=sql_parameters
    )
    return response


# res = execute_statement('CREATE TABLE test_table(test_id SERIAL PRIMARY KEY, '
#                         'test_name varchar(20));')
# print(res)

# with connect(aurora_cluster_arn=_DATABASE,
#              secret_arn=_CREDENTIALS,
#              database='test_euni') as conn:
#     with conn.cursor() as cursor:
#         cursor.execute(
#             'INSERT INTO test_table (test_id, test_name) VALUES (:test_id,:test_name);',
#             {'test_id': 3, 'test_name': 'a'}
#         )
with connect(aurora_cluster_arn=_DATABASE,
             secret_arn=_CREDENTIALS,
             database='test_euni') as conn:
    with conn.cursor() as cursor:
        cursor.execute(
            'SELECT * FROM test_table;'
        )
        print(cursor.fetchall())
