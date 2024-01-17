from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.plugins.utils import RequestTool, FileManager

from airflow.providers.alibaba.cloud.operators.oss import OSSUploadObjectOperator

from datetime import datetime
import logging
import pandas as pd
import numpy as np


@dag(
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=True
)
def etl_flower_price_data():

    @task()
    def extract(api_url: str, list_params: list):
        list_json = [api_request(api_url, params) for params in list_params]
        logging.info('JSON data has been extracted.')
        return list_json

    @task()
    def transform(list_json: list, key_name: str):
        flower_data = [json_data[key_name] for json_data in list_json]
        np_array = np.concatenate(flower_data)
        df = pd.dataFrame(np_array)
        logging.info('Data has been transformed to dataframe.')
        return df

    @task()
    def cleanse(df):
        df.dropna(inplace=True)
        df.drop_duplicates(inplace=True)
        logging.info('Nulls and duplicates have been dropped.')
        return df_cleansed

    @task()
    def load(df):
        filename = 'flower-price-{{ds}}.csv'
        df.to_csv(filename, index=False)
        try:
            OSSUploadObjectOperator(
                task_id='load_to_OSS',
                key='/raw_data/flower_price/',
                file=filename,
                region='oss-cn-hongkong',
                oss_conn_id='oss_conn_id'
            ).execute()
        except Exception as e:
            logging.error(f'Error occured during loading to OSS: {str(e)}')
            raise
        logging.info('CSV file has been loaded to OSS.')
        FileManager.remove(filename)

    request_tool = RequestTool(
        Variable.get('api_url'),
        Variable.get('api_auth_code'),
        {{ds}}
    )
    list_params = []
    for flower_type in [1, 2, 3, 4]:
        params = request_tool.prepare_params_for_flower(flower_type)
        list.params.append(params)

    list_json = extract(list_params)
    df = transform(list_json)
    df_cleansed = cleanse(df)
    load(df_cleansed)


# DAG 인스턴스 생성
etl_flower_price_data = etl_flower_price_data()
