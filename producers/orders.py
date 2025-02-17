import pandas as pd
import requests
import os
from dotenv import load_dotenv
import matplotlib.pyplot as plt
import numpy as np
from sklearn.linear_model import LinearRegression
from datetime import datetime, timedelta
import pytz
from quixstreams import Application
import json
import time
import logging
import string
import random

load_dotenv()

def generate_order():
    """
    Generates a JSON object with random values based on the given schema.
    """
    # Generate a random string for 'a' (VARCHAR)
    a_value = ''.join(random.choices(string.ascii_letters + string.digits, k=10))
    
    # Generate random BIGINT values for 'b' and 'c'
    b_value = random.randint(10**12, 10**15)  # Random large integer
    c_value = random.randint(10**12, 10**15)  # Random large integer
    
    # Generate a timestamp with 3 decimal places (TIMESTAMP(3))
    rowtime_value = datetime.now(pytz.utc)\
        .strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]  # Keep milliseconds
    
    # Create JSON object
    data = {
        "a": a_value,
        "b": b_value,
        "c": c_value,
        "rowtime": rowtime_value
    }
    
    return data

def main(kafka_broker:str='kafka-broker:9092'):
    app = Application(
        broker_address=kafka_broker,
        loglevel="DEBUG"
    )

    with app.get_producer() as producer:
        while True:
            quote = generate_order()
            print(f"Sending Order data: {quote}")
            
            producer.produce(
                topic = "orders",
                key="sales",
                value=json.dumps(quote)
            )
            
            print("Order data sent")
            
            time.sleep(5)

if __name__ == "__main__":
    logging.basicConfig(level="DEBUG")
    main(kafka_broker = "localhost:9092")
