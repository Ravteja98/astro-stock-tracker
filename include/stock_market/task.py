from airflow.hooks.base import BaseHook
import requests
import json

def _get_stock_prices(url, symbols):
    api = BaseHook.get_connection('stock_api')
    results = {}

    for symbol in symbols:
        full_url = f"{url}{symbol}?metrics=high?&interval=1d&range=1y"
        response = requests.get(full_url, headers=api.extra_dejson['headers'])
        results[symbol] = response.json()['chart']['result'][0]

    return json.dumps(results)