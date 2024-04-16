import os

from coffee_etl import CoffeeETL

etl_app = CoffeeETL(
    os.environ["CERTIFICATE_PATH"],
    os.environ["GOOGLE_STORAGE_BUCKET"],
)

df = etl_app.extract_from_storage()
result = etl_app.transform(df)
etl_app.load(result)
