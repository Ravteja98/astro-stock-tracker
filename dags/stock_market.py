from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.sensors.base import PokeReturnValue
from airflow.operators.python import PythonOperator
from datetime import datetime
from include.stock_market.task import _get_stock_prices

SYMBOLS = [ 'NVDA', 'AAPL', 'GOOG', 'MSFT', 'AMZN' ]

@dag(
    start_date=datetime(2023, 1, 1),
    schedule='@daily',
    catchup=False,
    tags=['stock_market'],
)
def stock_market():
    
    @task.sensor(poke_interval=30, timeout=300, mode='poke')
    def is_api_available() -> PokeReturnValue:
        import requests
        
        api = BaseHook.get_connection('stock_api')
        url = f"{api.host}{api.extra_dejson['endpoint']}"
        print(url)
        response = requests.get(url, headers=api.extra_dejson['headers'])
        condition = response.json()['finance']['result'] is None
        return PokeReturnValue(is_done=condition, xcom_value=url)
    
    get_stock_prices = PythonOperator(
        task_id='get_stock_prices',
        python_callable= _get_stock_prices,
        op_kwargs={'url': "{{ ti.xcom_pull(task_ids='is_api_available') }}", 'symbols': SYMBOLS}
    )
    
    is_api_available() >> get_stock_prices
        

stock_market()
