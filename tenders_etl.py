import requests
import pandas as pd
from supabase.client import create_client


class TendersETL:
    def __init__(
        self,
        base_url: str,
        supabase_url: str,
        supabase_key: str,
        supabase_storage_bucket: str,
    ) -> None:
        self.__base_url = base_url
        self.__supabase_url = supabase_url
        self.__supabase_key = supabase_key
        self.__supabase_storage_bucket = supabase_storage_bucket

    # extract from API
    def extract_from_api(self) -> list:
        try:
            response = requests.get(self.__base_url)

            if response.status_code == 200:
                return response.json()["data"]
            else:
                print(f"Failed to fetch data: {response.status_code}")
                return []

        except requests.RequestException as e:
            print(f"An error occurred when extracting data: {e}")
            return []

    # transform
    def transform(self, data: list) -> pd.DataFrame:
        result = []

        for dt in data:
            result.append(
                {
                    "document_url": dt["src_url"],
                    "project_name": dt["title"],
                }
            )

        return pd.DataFrame(result)

    # load
    def load(self, df: pd.DataFrame) -> None:
        try:
            url: str = self.__supabase_url
            key: str = self.__supabase_key
            supabase = create_client(url, key)

            bucket_name: str = self.__supabase_storage_bucket

            filename = "result.parquet"

            df.to_parquet(filename)

            with open(filename, "rb") as f:
                supabase.storage.from_(bucket_name).upload(file=f, path=filename)

            print("Data loaded successfully")
        except Exception as e:
            print(f"An error occurred when loading data: {e}")
