import os

from reqres_etl import ReqresETL

etl_app = ReqresETL(
    "https://reqres.in/api/users?page=2",
    os.environ["CERTIFICATE_PATH"],
    os.environ["GOOGLE_STORAGE_BUCKET"],
)

result = etl_app.extract_from_api()
df = etl_app.transform(result)
etl_app.load(df)
