from airflow.hooks.base import BaseHook
from minio import Minio
from io import BytesIO
import requests
import json
from datetime import datetime, timedelta
from airflow.exceptions import AirflowNotFoundException

# -------------------------
# GLOBALS
# -------------------------
BUCKET_NAME = "stock-market"


# -------------------------
# MINIO CLIENT
# -------------------------
def _get_minio_client():
    """
    Returns a MinIO client using Airflow connection 'minio'.
    """
    minio_conn = BaseHook.get_connection('minio')
    client = Minio(
        endpoint=minio_conn.extra_dejson["endpoint_url"].split("//")[1],
        access_key=minio_conn.login,
        secret_key=minio_conn.password,
        secure=False,
    )
    return client


# -------------------------
# GET STOCK PRICES (multiple symbols, multiple days)
# -------------------------
def _get_stock_prices(url, symbols):
    """
    Calls stock API and fetches 90 days of data for each symbol.
    Returns dictionary {symbol: [ {date: result}, ... ]}
    """
    api = BaseHook.get_connection('stock_api')
    results = {}

    end_date = datetime.now()
    start_date = end_date - timedelta(days=90)

    for symbol in symbols:
        results[symbol] = []
        date = start_date

        while date <= end_date:
            date_str = date.strftime('%Y-%m-%d')
            full_url = f"{url}{symbol}?metrics=high&interval=1d&range=1d&date={date_str}"
            response = requests.get(full_url, headers=api.extra_dejson['headers'])

            if response.status_code != 200:
                date += timedelta(days=1)
                continue

            data = response.json().get("chart", {}).get("result", [])
            if not data:
                date += timedelta(days=1)
                continue

            results[symbol].append({date_str: data[0]})
            date += timedelta(days=1)

    return json.dumps(results)


# -------------------------
# STORE PRICES → MINIO
# -------------------------
def _store_prices(stock):
    """
    Stores JSON stock records into MinIO bucket:
       stock-market/SYMBOL/YYYY-MM-DD/prices.json

    Returns: list of symbols
    """
    client = _get_minio_client()

    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)

    stock_data = json.loads(stock)
    uploaded_symbols = []

    for symbol, records in stock_data.items():
        for daily_data in records:
            date_str, daily_record = next(iter(daily_data.items()))
            file_bytes = json.dumps(daily_record).encode('utf8')

            client.put_object(
                bucket_name=BUCKET_NAME,
                object_name=f"{symbol}/{date_str}/prices.json",
                data=BytesIO(file_bytes),
                length=len(file_bytes),
            )

        uploaded_symbols.append(symbol)
        print(f"✅ Uploaded: {symbol}")

    return uploaded_symbols


# -------------------------
# FIND PROCESSED PARQUET
# -------------------------
def _get_formatted_parquet(symbol: str):
    """
    Looks under:
       stock-market/processed/<SYMBOL>/
    and finds a parquet file.

    Returns parquet path string or throws AirflowNotFoundException
    """
    client = _get_minio_client()

    prefix = f"processed/{symbol}/"

    objects = client.list_objects(
        bucket_name=BUCKET_NAME,
        prefix=prefix,
        recursive=True
    )

    for obj in objects:
        if obj.object_name.endswith(".parquet"):
            return obj.object_name

    raise AirflowNotFoundException(
        f"❌ No parquet file found for symbol={symbol}"
    )
