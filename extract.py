# importing necessary libraries

import pandas as pd
import requests
import json
import time
from datetime import datetime   
from dotenv import load_dotenv 
import os
import logging

def extraction_layer(**kwargs):

    app_id = os.getenv('API_ID')
    app_key = os.getenv('API_KEY')
    POLL_INTERVAL = 60  # Time in seconds between requests

    # Define the endpoint and parameters for scheduled data
    station_code = 'RMD'  # Example station code (London Waterloo)
    url = f'https://transportapi.com/v3/uk/train/station/{station_code}/live.json'

    params = {
        'app_id': app_id,
        'app_key': app_key,
        'darwin': 'false',  
        'train_status': 'passenger',  
        'live' :'True',
        
    }
    response = requests.get(url, params=params)

    if response.status_code == 200:
        response= response.json()
        print("Data successfully extracted for train_departures")
    else:
        print(f"Failed to fetch data: {response.status_code}")
        print(response.text)

        logging.info("kwargs: %s", kwargs)
        kwargs['ti'].xcom_push(key="raw_data", value=response)
        print("Data pushed to xcom")
        return response

if __name__ == "__main__":
    extraction_layer()