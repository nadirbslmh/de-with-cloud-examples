import firebase_admin
import pandas as pd

from firebase_admin import credentials
from firebase_admin import storage


class CoffeeETL:
    def __init__(self, cert_path: str, storage_bucket: str) -> None:
        self.__cert_path = cert_path
        self.__storage_bucket = storage_bucket

    # extract from storage
    def extract_from_storage(self) -> pd.DataFrame:
        try:
            cred = credentials.Certificate(self.__cert_path)

            storage_bucket = self.__storage_bucket

            firebase_admin.initialize_app(cred, {"storageBucket": storage_bucket})

            bucket = storage.bucket()

            filename = "coffee_shop.csv"

            blob = bucket.get_blob(filename)

            with blob.open(mode="r") as file:
                df = pd.read_csv(file)

            return df
        except Exception as e:
            print(f"An error occurred when extracting data from storage: {e}")
            return pd.DataFrame()

    # transform
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df[(df["menu"] != "green tea") & (df["menu"] != "jasmine tea")]
        df = df.reset_index(drop=True)
        return df

    # load
    def load(self, df: pd.DataFrame) -> None:
        try:
            df.to_csv("coffee_orders.csv")
            print("Data loaded successfully")
        except Exception as e:
            print(f"An error occurred when loading data: {e}")
