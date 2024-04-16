import os
import pandas as pd
from supabase.client import create_client

url = os.environ["SUPABASE_URL"]
key = os.environ["SUPABASE_KEY"]
supabase = create_client(url, key)

filename = "result.parquet"

bucket_name = os.environ["SUPABASE_STORAGE_BUCKET"]

with open(filename, "wb+") as f:
    res = supabase.storage.from_(bucket_name).download(filename)
    f.write(res)


df = pd.read_parquet(filename)

print(df.head())
