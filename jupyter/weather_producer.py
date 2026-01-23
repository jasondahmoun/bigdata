import requests
import json
import time
import os
from kafka import KafkaProducer

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:29092').strip() or 'kafka:29092'
KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'weather_data').strip() or 'weather_data'
WEATHER_API_URL = os.getenv('WEATHER_API_URL', 'https://api.open-meteo.com/v1/forecast').strip() or 'https://api.open-meteo.com/v1/forecast'
WEATHER_LATITUDE = float((os.getenv('WEATHER_LATITUDE') or '52.52').strip() or '52.52')
WEATHER_LONGITUDE = float((os.getenv('WEATHER_LONGITUDE') or '13.41').strip() or '13.41')
FETCH_INTERVAL = int((os.getenv('FETCH_INTERVAL') or '30').strip() or '30')

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

while True:
    try:
        response = requests.get(
            WEATHER_API_URL,
            params={"latitude": WEATHER_LATITUDE, "longitude": WEATHER_LONGITUDE, "current_weather": "true"}
        )
        data = response.json()["current_weather"]
        
        producer.send(KAFKA_TOPIC, data)
        print(f"Envoyer: {data}")
        
    except Exception as e:
        print(f"Erreur: {e}")
    
    time.sleep(FETCH_INTERVAL)
