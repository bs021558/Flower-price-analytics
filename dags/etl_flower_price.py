from airflow.decorators import dag, task
from airflow.models import Variable

from airflow.providers.alibaba.cloud.operators.oss import OSSUploadObjectOperator
from utils import RequestTool, FileManager
from datetime import datetime, timedelta
import logging
import pandas as pd

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_date': '{{ds}}'
}


@dag(
    schedule_interval='@daily',
    catchup=True,
    default_args=default_args
)
def etl_flower_price():

    @task()
    def prepare(execution_date: str):
        service_key = Variable.get('api_auth_code')
        flower_types = [1, 2, 3, 4]
        list_params = []
        for flower_type in flower_types:
            params = {
                "kind": "f001",
                "serviceKey": service_key,
                "baseDate": f"{execution_date}",
                "flowerGubn": str(flower_type),
                "dataType": "json",
                "countPerPage": "5",
            }
            list_params.append(params)
        logging.info('Parameters for request is ready.')
        return list_params

    @task()
    def extract(list_params: list, execution_date: str):
        api_url = Variable.get('api_url')

        # Api request multiple time with various parameters
        list_json = [RequestTool.api_request(
            api_url, params) for params in list_params]
        logging.info('JSON data has been extracted.')
        return list_json

    @task()
    def transform(list_json: list, execution_date: str):
        # filename of the csv file to be finally saved
        filename = f'temp/flower/{execution_date}.csv'

        # JSON to DataFrame
        flower_data = [json_data['response']['items']
                       for json_data in list_json]
        concat_data = sum(flower_data, [])
        df = pd.DataFrame(concat_data)

        # Cleansing
        df.dropna(inplace=True)
        df.drop_duplicates(inplace=True)

        # Save as CSV
        df.to_csv(filename, index=False, encoding="utf-8-sig")

        logging.info(
            f'Data has been transformed to CSV. The filename is {filename}')

        return filename

    @task()
    def load(filename: str, execution_date: str, **context):
        destination_keyname = f'raw_data/flower_price/{execution_date}.csv'
        oss_uploader = OSSUploadObjectOperator(
            task_id='load_to_OSS',
            bucket_name='bs021558',
            key=destination_keyname,
            file=filename,
            region='cn-hongkong',
            oss_conn_id='oss_conn_id'
        )
        try:
            oss_uploader.execute(context)
        except Exception as e:
            logging.error(f'Error occured during loading to OSS: {str(e)}')
            raise
        logging.info('CSV file has been loaded to OSS.')
        FileManager.remove(filename)

    # TaskFlow
    list_params = prepare()
    list_json = extract(list_params)
    filename = transform(list_json)
    load(filename)


# DAG instance
etl_flower_price = etl_flower_price()
