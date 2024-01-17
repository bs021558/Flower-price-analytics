import requests
import os


class RequestTool:
    def __init__(self, api_url: str, service_key: str, base_date: str):
        self.api_url = api_url
        self.service_key = service_key,
        self.base_date = base_date

    def prepare_params_for_flower(self, flower_type: int):
        params = {
            "kind": "f001",
            "serviceKey": self.service_key,
            "baseDate": self.base_date,
            "flowerGubn": str(flower_type),
            "dataType": "json",
            "countPerPage": "999",
            "currentPage": "1"
        }
        return params

    def api_request(self, params):
        try:
            response = requests.get(self.api_url, params=params)
            response.raise_for_status()

            return response.json()

        except requests.exceptions.HTTPError as e:
            # HTTP 오류 (예: 404, 500 등)
            raise e

        except requests.exceptions.RequestException as e:
            # 그 외의 예외 처리
            raise e


class FileManager:
    def remove(filename: str):
        os.remove(filename)
