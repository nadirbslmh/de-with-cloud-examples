import requests
import pandas as pd
import firebase_admin

from firebase_admin import credentials
from firebase_admin import storage


class ReqresETL:
    def __init__(self, base_url: str, cert_path: str, storage_bucket: str) -> None:
        self.__base_url = base_url
        self.__cert_path = cert_path
        self.__storage_bucket = storage_bucket

    # extract from API
    def extract_from_api(self) -> list:
        try:
            response = requests.get(self.__base_url)

            if response.status_code == 200:
                return response.json()["data"]
            else:
                print(f"failed to fetching data: {response.status_code}")
                return []

        except requests.RequestException as e:
            print(f"An error occurred when sending request: {e}")
            return []

    # transform
    def transform(self, data: list) -> pd.DataFrame:
        result = []

        for dt in data:
            result.append(
                {
                    "id": dt["id"],
                    "email": dt["email"],
                }
            )

        return pd.DataFrame(result)

    # load to google storage
    def load(self, df: pd.DataFrame) -> None:
        try:
            cred = credentials.Certificate(self.__cert_path)

            storage_bucket = self.__storage_bucket

            firebase_admin.initialize_app(cred, {"storageBucket": storage_bucket})

            bucket = storage.bucket()

            filename = "result.parquet"

            df.to_parquet(filename)

            blob = bucket.blob(blob_name=filename)

            blob.upload_from_filename(filename)

            print("Data loaded successfully")
        except Exception as e:
            print(f"An error occurred when loading data to the storage: {e}")
