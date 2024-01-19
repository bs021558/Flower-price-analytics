from airflow.plugins_manager import AirflowPlugin

import requests
import os


class RequestTool(AirflowPlugin):
    name = "request_tool"

    def api_request(api_url: str, params: dict):
        try:
            response = requests.get(api_url, params=params)
            response.raise_for_status()

            return response.json()

        except requests.exceptions.HTTPError as e:
            # HTTP 오류 (예: 404, 500 등)
            raise e

        except requests.exceptions.RequestException as e:
            # 그 외의 예외 처리
            raise e


class FileManager(AirflowPlugin):
    name='file_manager'
    def remove(filename: str):
        os.remove(filename)
