from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator
from airflow.providers.docker.operators.docker import DockerOperator
from airflow import XComArg
from datetime import datetime
import requests
from include.stock_market.task import _get_stock_prices, _store_prices

SYMBOLS = ['NVDA', 'AAPL', 'GOOG', 'MSFT', 'AMZN']

@dag(
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['stock_market'],
)
def stock_market():

    @task.sensor(poke_interval=30, timeout=300, mode='poke')
    def is_api_available() -> PokeReturnValue:
        api = BaseHook.get_connection('stock_api')
        base_url = api.host.rstrip('/')
        endpoint = api.extra_dejson['endpoint'].lstrip('/')
        url = f"{base_url}/{endpoint}"

        test_symbol = "NVDA"
        full_url = f"{url}{test_symbol}?metrics=high&interval=1d&range=1d"
        response = requests.get(full_url, headers=api.extra_dejson['headers'])
        condition = response.status_code == 200
        return PokeReturnValue(is_done=condition, xcom_value=url)

    get_stock_prices = PythonOperator(
        task_id='get_stock_prices',
        python_callable=_get_stock_prices,
        op_kwargs={
            'url': "{{ ti.xcom_pull(task_ids='is_api_available') }}",
            'symbols': SYMBOLS
        }
    )

    store_prices = PythonOperator(
        task_id='store_prices',
        python_callable=_store_prices,
        op_kwargs={'stock': "{{ ti.xcom_pull(task_ids='get_stock_prices') }}"}
    )

    stored_syms = XComArg(store_prices)

    # DockerOperator to run PySpark for each symbol
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
                    "S3_BUCKET": "stock-market",
                    "AWS_ACCESS_KEY_ID": "minio",
                    "AWS_SECRET_ACCESS_KEY": "minio123",
                    "ENDPOINT": "http://minio:9000"
                }
            )
        )
    )

    get_formatted_parquet = PythonOperator.partial(
    task_id='get_formatted_parquet',
    python_callable=_get_formatted_parquet,
    ).expand(
        op_kwargs=stored_syms.map(lambda symbol: {"symbol": symbol})
    )

    is_api_available() >> get_stock_prices >> store_prices >> format_prices >> get_formatted_parquet

stock_market()
