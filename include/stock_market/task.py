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
# GET STOCK PRICES (with better error handling)
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
            
            try:
                print(f"📡 Fetching data for {symbol} on {date_str}")
                response = requests.get(
                    full_url, 
                    headers=api.extra_dejson['headers'], 
                    timeout=30
                )
                
                # Check if response is successful
                if response.status_code != 200:
                    print(f"⚠️ API returned status {response.status_code} for {symbol} on {date_str}")
                    date += timedelta(days=1)
                    continue
                
                # Check if response content is valid
                if not response.content:
                    print(f"⚠️ Empty response for {symbol} on {date_str}")
                    date += timedelta(days=1)
                    continue
                
                # Try to parse JSON with better error handling
                try:
                    data = response.json()
                except json.JSONDecodeError as e:
                    print(f"❌ JSON decode error for {symbol} on {date_str}: {e}")
                    print(f"Response text: {response.text[:200]}")  # First 200 chars for debugging
                    date += timedelta(days=1)
                    continue

                # Check if we have the expected data structure
                chart_data = data.get("chart", {}).get("result", [])
                if not chart_data:
                    print(f"⚠️ No chart data for {symbol} on {date_str}")
                    date += timedelta(days=1)
                    continue

                results[symbol].append({date_str: chart_data[0]})
                print(f"✅ Successfully fetched data for {symbol} on {date_str}")
                
            except requests.exceptions.RequestException as e:
                print(f"❌ Request failed for {symbol} on {date_str}: {e}")
            
            date += timedelta(days=1)

        print(f"📊 Total records for {symbol}: {len(results[symbol])}")

    return json.dumps(results)

# -------------------------
# STORE PRICES → MINIO
# -------------------------
def _store_prices(stock_json):
    """
    Stores JSON stock records into MinIO bucket:
       stock-market/SYMBOL/YYYY-MM-DD/prices.json

    Returns: list of symbols
    """
    client = _get_minio_client()

    if not client.bucket_exists(BUCKET_NAME):
        client.make_bucket(BUCKET_NAME)

    try:
        stock_data = json.loads(stock_json)
    except json.JSONDecodeError as e:
        print(f"❌ Failed to parse stock_json: {e}")
        raise

    uploaded_symbols = []

    for symbol, records in stock_data.items():
        if not records:
            print(f"⚠️ No records found for symbol: {symbol}")
            continue
            
        for daily_data in records:
            date_str, daily_record = next(iter(daily_data.items()))
            file_bytes = json.dumps(daily_record).encode('utf8')

            try:
                client.put_object(
                    bucket_name=BUCKET_NAME,
                    object_name=f"{symbol}/{date_str}/prices.json",
                    data=BytesIO(file_bytes),
                    length=len(file_bytes),
                )
                print(f"✅ Uploaded: {symbol}/{date_str}/prices.json")
            except Exception as e:
                print(f"❌ Failed to upload {symbol}/{date_str}: {e}")

        uploaded_symbols.append(symbol)

    print(f"🎯 Completed upload for symbols: {uploaded_symbols}")
    return uploaded_symbols

# -------------------------
# FIND PROCESSED PARQUET
# -------------------------
def _get_formatted_parquet(symbol: str):
    """
    Returns ALL parquet files for a symbol.
    Since we have multiple files per symbol, we return the list.
    """
    client = _get_minio_client()
    prefix = f"processed/{symbol}/"

    try:
        objects = client.list_objects(
            bucket_name=BUCKET_NAME,
            prefix=prefix,
            recursive=True
        )

        parquet_files = [
            obj.object_name for obj in objects if obj.object_name.endswith(".parquet")
        ]

        if not parquet_files:
            raise AirflowNotFoundException(f"❌ No parquet file found for symbol={symbol}")
        
        print(f"📁 Found {len(parquet_files)} parquet files for {symbol}: {parquet_files}")
        return parquet_files  # Return the LIST of files
        
    except Exception as e:
        print(f"❌ Error listing objects for {symbol}: {e}")
        raise AirflowNotFoundException(f"Error finding parquet for symbol={symbol}: {e}")