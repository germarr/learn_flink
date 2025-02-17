import pandas as pd
import requests
import os
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import numpy as np
from sklearn.linear_model import LinearRegression
from datetime import datetime
import pytz
from quixstreams import Application
import json
import time
import logging

load_dotenv()

alpaca_endpoint = os.getenv('alpaca_endpoint')
alpaca_key = os.getenv('alpaca_key') 
alpaca_secret = os.getenv('alpaca_secret')

def transform_to_nyc_time(data):
    """
    Transforms the 'timestamp' field to NYC time and ensures both 'timestamp' and 'current_time'
    are formatted as '0000-01-01 00:00:00.000000000'.

    Args:
        data (dict): A dictionary with 'timestamp' (str in ISO 8601 format) and 'current_time' (datetime object).

    Returns:
        dict: The transformed dictionary with both fields formatted.
    """
    # Parse the UTC timestamp
    utc_timestamp = datetime.strptime(data["timestamp"], "%Y-%m-%dT%H:%M:%SZ")
    utc_timestamp = pytz.utc.localize(utc_timestamp)  # Localize to UTC

    # Convert timestamp to NYC time
    nyc_timezone = pytz.timezone("America/New_York")
    nyc_timestamp = utc_timestamp.astimezone(nyc_timezone)

    # Convert current_time to NYC time if it's naive
    # current_time = data["current_time"]
    # if current_time.tzinfo is None:
    #     current_time = pytz.utc.localize(current_time)  # Assume it's UTC if naive
    # nyc_current_time = current_time.astimezone(nyc_timezone)

    # Format both timestamps to the desired format
    formatted_timestamp = nyc_timestamp.strftime("%Y-%m-%d %H:%M:%S.%f000")
    formatted_current_time  = data["current_time"].strftime("%Y-%m-%d %H:%M:%S.%f000")

    # Return the updated dictionary
    return {
        "timestamp": formatted_timestamp,
        "current_time": formatted_current_time
    }

def get_quote(stock:str='AAPL'):
    url = f"https://data.alpaca.markets/v2/stocks/bars/latest?symbols={stock}"
    headers = {
        "accept": "application/json",
        "APCA-API-KEY-ID": alpaca_key,
        "APCA-API-SECRET-KEY": alpaca_secret
    }
    response = requests.get(url, headers=headers).json()['bars'][stock]

    dates_d = transform_to_nyc_time(data={
        "timestamp": response['t'],
        "current_time": datetime.now()
    })


    export_data = {
        "stock_name": stock,
        "stock_value": response['o']
    }
    return export_data

def main(kafka_broker:str='kafka-broker:9092'):
    app = Application(
        broker_address=kafka_broker,
        loglevel="DEBUG"
    )

    with app.get_producer() as producer:
        while True:
            quote = get_quote()
            print(f"Sending STOCK data: {quote}")

            producer.produce(
                topic = "aapl_price",
                key="Stocks",
                value=json.dumps(quote)
            )
            print("Bitcoin data sent")
            time.sleep(30)

if __name__ == "__main__":
    logging.basicConfig(level="DEBUG")
    main(kafka_broker = "kaf.gerardomarr.com:19092")
