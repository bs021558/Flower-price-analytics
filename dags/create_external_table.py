from airflow.decorators import dag
from airflow.models import Variable
from airflow.providers.postgres.operators.postgres import PostgresOperator
from utils import FileManager
from datetime import datetime

file_path = Variable.get('oss_path_flower_raw_data')
table_name = 'external_flower'
server_for_fdw = 'oss_server'
columns = '''
    saleDate date,	
    flowerGubn varchar,
    pumName int,
    goodName varchar,
    lvNm varchar,
    maxAmt int,
    minAmt int,
    avgAmt int,
    totAmt int,
    totQty int
'''
header = True

# extension_name = 'oss_fdw'

# storage_endpoint = Variable.get('oss_endpoint')
# bucket_name = Variable.get('oss_bucket_name')
# key_id = Variable.get('oss_access_key_id')
# key_secret = Variable.get('oss_access_key_secret')



default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    
}

@dag(
    schedule_interval=None,
    catchup=False,
    default_args=default_args,
    template_searchpath = [FileManager.getcwd()]
)
def create_external_table():
    
    # create_extension = PostgresOperator(
    #     task_id='create_extension',
    #     postgres_conn_id='postgres_conn',
    #     sql='sql/create_extension.sql',
    #     parameters={
    #         'extension_name':extension_name
    #     }
    # )

    # create_server_with_fdw = PostgresOperator(
    #     task_id='create_server_with_fdw',
    #     postgres_conn_id='postgres_conn',
    #     sql='sql/create_server_with_fdw.sql',
    #     parameters={
    #         'server_name':server_for_fdw,
    #         'extension_name':extension_name,
    #         'storage_endpoint':storage_endpoint,
    #         'bucket_name':bucket_name,
    #         'key_id':key_id,
    #         'key_secret':key_secret
    #     }
    # )

    create_external_table_task = PostgresOperator(
        task_id='create_external_table_task',
        postgres_conn_id='postgres_conn',
        sql='sql/create_external_table.sql',
        parameters={
            'table_name':table_name,
            'server_name':server_for_fdw,
            'file_path': f'{file_path}*',
            'columns': columns,
            'header' : 'HEADER' if header else ''
        }
    )

    create_external_table_task

# DAG instance
create_external_table = create_external_table()
