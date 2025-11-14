from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow import XComArg
from datetime import datetime
import requests
import json
from io import BytesIO
import pandas as pd
from minio import Minio

from include.stock_market.task import (
    _get_stock_prices,
    _store_prices,
    _get_formatted_parquet,
    BUCKET_NAME
)

# -------------------------
# SYMBOLS
# -------------------------
SYMBOLS = ['NVDA', 'AAPL', 'GOOG', 'MSFT', 'AMZN']

# -------------------------
# DAG DEFINITION
# -------------------------
@dag(
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['stock_market'],
)
def stock_market():

    # ------------------------------------------
    # SENSOR: Check API availability
    # ------------------------------------------
    @task.sensor(poke_interval=30, timeout=300, mode='poke')
    def is_api_available() -> PokeReturnValue:
        api = BaseHook.get_connection('stock_api')
        base_url = api.host.rstrip('/')
        endpoint = api.extra_dejson['endpoint'].lstrip('/')
        url = f"{base_url}/{endpoint}"

        test_symbol = "NVDA"
        full_url = f"{url}{test_symbol}?metrics=high&interval=1d&range=1d"
        
        try:
            print(f"🔍 Testing API availability: {full_url}")
            response = requests.get(
                full_url, 
                headers=api.extra_dejson['headers'], 
                timeout=30
            )
            
            condition = response.status_code == 200
            
            if condition:
                try:
                    data = response.json()
                    print("✅ API is available and returning valid JSON")
                except json.JSONDecodeError:
                    print("❌ API returned invalid JSON")
                    condition = False
            else:
                print(f"❌ API returned status code: {response.status_code}")
                    
        except requests.exceptions.RequestException as e:
            print(f"❌ API request failed: {e}")
            condition = False
            
        return PokeReturnValue(is_done=condition, xcom_value=url)

    # ------------------------------------------
    # FETCH STOCK PRICES
    # ------------------------------------------
    get_stock_prices = PythonOperator(
        task_id='get_stock_prices',
        python_callable=_get_stock_prices,
        op_kwargs={
            'url': "{{ ti.xcom_pull(task_ids='is_api_available') }}",
            'symbols': SYMBOLS
        }
    )

    # ------------------------------------------
    # STORE JSON FILES TO MINIO
    # ------------------------------------------
    store_prices = PythonOperator(
        task_id='store_prices',
        python_callable=_store_prices,
        op_kwargs={'stock_json': "{{ ti.xcom_pull(task_ids='get_stock_prices') }}"}
    )

    stored_syms = XComArg(store_prices)

    # ------------------------------------------
    # FORMAT JSON → PARQUET (via Spark in Docker)
    # ------------------------------------------
    format_prices = (
        DockerOperator.partial(
            task_id="format_prices",
            image="airflow/stock-app",
            container_name="format_prices__{{ ds_nodash }}__{{ ti.map_index }}",
            api_version="auto",
            auto_remove='success',
            docker_url="tcp://docker-proxy:2375",
            network_mode="container:spark-master",
            tty=True,
            mount_tmp_dir=False,
        )
        .expand(
            environment=stored_syms.map(
                lambda symbol: {
                    "STOCK_SYMBOL": symbol,
                    "S3_BUCKET": BUCKET_NAME,
                    "AWS_ACCESS_KEY_ID": "minio",
                    "AWS_SECRET_ACCESS_KEY": "minio123",
                    "ENDPOINT": "http://minio:9000"
                }
            )
        )
    )

    # ------------------------------------------
    # GET ALL PARQUET FILE PATHS PER SYMBOL
    # ------------------------------------------
    get_formatted_parquet = PythonOperator.partial(
        task_id='get_formatted_parquet',
        python_callable=_get_formatted_parquet,
    ).expand(
        op_kwargs=stored_syms.map(lambda symbol: {"symbol": symbol})
    )

    # ------------------------------------------
    # PROCESS PARQUET FILES
    # ------------------------------------------
    @task
    def process_parquet_files(parquet_files_list):
        """
        Process the list of parquet files for a symbol.
        This task receives the list from get_formatted_parquet and processes each file.
        """
        if not parquet_files_list:
            print("⚠️ No parquet files to process")
            return "No files processed"
        
        # Ensure we have a list (handle case where it might be a single string)
        if isinstance(parquet_files_list, str):
            parquet_files = [parquet_files_list]
        else:
            parquet_files = parquet_files_list
        
        print(f"📦 Processing {len(parquet_files)} parquet files")
        
        # MinIO client
        minio_conn = BaseHook.get_connection('minio')
        client = Minio(
            endpoint=minio_conn.extra_dejson["endpoint_url"].split("//")[1],
            access_key=minio_conn.login,
            secret_key=minio_conn.password,
            secure=False,
        )

        # Postgres connection - NOW USING stock_market DATABASE
        pg_hook = PostgresHook(postgres_conn_id="postgres")
        engine = pg_hook.get_sqlalchemy_engine()
        
        total_records = 0
        processed_files = []
        
        for parquet_path in parquet_files:
            try:
                # Read parquet from MinIO
                print(f"📥 Reading {parquet_path}")
                response = client.get_object(
                    bucket_name=BUCKET_NAME,
                    object_name=parquet_path
                )
                parquet_bytes = BytesIO(response.read())
                df = pd.read_parquet(parquet_bytes)
                
                # Load into Postgres
                df.to_sql(
                    name="stock_market",
                    con=engine,
                    schema="public",
                    if_exists="append",
                    index=False
                )
                
                total_records += len(df)
                processed_files.append(parquet_path)
                print(f"✅ Loaded {len(df)} records from {parquet_path}")
                
            except Exception as e:
                print(f"❌ Failed to process {parquet_path}: {e}")
                # Continue with other files even if one fails
                continue
        
        result = f"Processed {len(processed_files)} files with {total_records} total records"
        print(f"🎯 {result}")
        return result

    # ------------------------------------------
    # DAG FLOW (SIMPLIFIED - NO DATABASE CREATION)
    # ------------------------------------------
    (is_api_available() >> get_stock_prices >> store_prices >> 
     format_prices >> get_formatted_parquet >> 
     process_parquet_files.expand(parquet_files_list=XComArg(get_formatted_parquet)))

stock_market_dag = stock_market()