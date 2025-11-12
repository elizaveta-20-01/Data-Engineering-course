from typing import List, Dict, Any
import requests
from loguru import logger
import pandas as pd


API_URL = 'https://fake-api-vycpfa6oca-uc.a.run.app/sales'
AUTH_TOKEN = '2b8d97ce57d401abd89f45b0079d8790edd940e6'

def get_sales(purchase_date: str) -> List[Dict[str, Any]]:
    """
    Get data from sales API for specified date.

    :param date: data retrieve the data from
    :return: list of records
    """
    base_url: str = API_URL
    headers: dict[str, str] = {"Authorization": AUTH_TOKEN}

    all_sales: List[Dict[str, Any]] = []
    page = 1

    while True: #цикл по сторінкам
        params: dict[str, str] = {"date": purchase_date, "page": str(page)}  

        try:
            request_url: str = requests.Request('GET', base_url, params=params, headers=headers).prepare().url
            logger.info(f"Fetching from URL: {request_url}")

            response: requests.Response = requests.get(base_url, params=params,headers=headers)
            if response.status_code == 404:
                logger.info(f"No more data found for {purchase_date} at page {page}. Ending pagination.")
                break
            response.raise_for_status()
            data = response.json()
            all_sales.extend(data)
            page += 1
        except requests.exceptions.RequestException as e:
             logger.error(f"Error fetching data for {purchase_date}: {e}")
             break
 
    return all_sales
