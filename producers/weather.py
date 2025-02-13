import requests
from quixstreams import Application
import json
import logging
import time
from datetime import datetime

def get_weather():
    response = requests.get(
        "https://api.open-meteo.com/v1/forecast", 
        params={
            "latitude": 40.3639956, 
            "longitude": -74.6609918,
            "current":"temperature_2m"
        }
    ).json()

    # Get the current time
    now = datetime.now()

    # Round down to the nearest minute (set seconds and microseconds to zero)
    rounded_minute = now.replace(second=0, microsecond=0)

    # Round down to the nearest hour (set minutes, seconds, and microseconds to zero)
    rounded_hour = now.replace(minute=0, second=0, microsecond=0)

    # Convert to string format with microseconds precision 
    response['current_time'] = now.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "000"
    response['rounded_minute'] = rounded_minute.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "000"
    response['rounded_hour'] = rounded_hour.strftime("%Y-%m-%d %H:%M:%S.%f")[:-3] + "000"


    return response

def main():
    app = Application(
        broker_address="localhost:9092",
        loglevel="DEBUG"
    )

    with app.get_producer() as producer:
        for i in range(120):
            weather = get_weather()
            logging.debug(f"Sending weather data: {weather}")
            producer.produce(
                topic = "princeton-weather",
                key="Princeton",
                value=json.dumps(weather)
            )
            logging.info("Weather data sent")
            time.sleep(10)

if __name__ == "__main__":
    logging.basicConfig(level="DEBUG")
    main()