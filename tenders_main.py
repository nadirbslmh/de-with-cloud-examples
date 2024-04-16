from tenders_etl import TendersETL
import os

etl_app = TendersETL(
    "https://tenders.guru/api/es/tenders",
    os.environ["SUPABASE_URL"],
    os.environ["SUPABASE_KEY"],
    os.environ["SUPABASE_STORAGE_BUCKET"],
)

result = etl_app.extract_from_api()
df = etl_app.transform(result)
etl_app.load(df)
# break dulu...
