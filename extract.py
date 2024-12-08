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

    app_id = '4f292769'
    app_key = '3e6756c7b1419aaf07d4370c42cb58d2'
    POLL_INTERVAL = 60  # Time in seconds between requests

    # Define the endpoint and parameters for scheduled data
    station_code = 'RMD'  # Example station code (London Waterloo)
    url = f'https://transportapi.com/v3/uk/train/station/{station_code}/live.json'

    params = {
        'app_id': app_id,
        'app_key': app_key,
        #'time_of_day': '19:00',
        #'request_time': '2024-10-30T18:50:00+00:00',
        'darwin': 'false',  
        'train_status': 'passenger',  # Status filter, e.g., passenger trains only
        'live' :'True',
        #'station_detail': 'destination'
    }
    response = requests.get(url, params=params)

    if response.status_code == 200:
        response= response.json()
        #kwargs['ti'].xcom_push(key='raw_response', value=response_json)
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