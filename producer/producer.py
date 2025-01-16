import requests
import json
import time
from kafka import KafkaProducer
import os
from datetime import datetime, timedelta
import kafka.errors
from json import dumps

# Kafka konfiguracija
KAFKA_BROKER = os.environ["KAFKA_BROKER"]
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "earthquakes-topic")

KAFKA_CONFIGURATION = {
    "bootstrap_servers": os.environ.get("KAFKA_BROKER", "kafka1:19092").split(","),
    "key_serializer": lambda x: str.encode("" if not x else x, encoding='utf-8'),
    "value_serializer": lambda x: dumps(dict() if not x else x).encode(encoding='utf-8'),
    "reconnect_backoff_ms": int(100)
}

# Funkcija za preuzimanje podataka sa USGS API-ja
def fetch_earthquake_data(start_time, end_time):
    base_url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    params = {
        "format": "geojson",
        "starttime": start_time,
        "endtime": end_time
    }
    response = requests.get(base_url, params=params)
    if response.status_code == 200:
        return response.json()  # Vraća podatke kao Python dict
    else:
        print(f"Failed to fetch data: {response.status_code}")
        return None

# Funkcija za slanje podataka u Kafka ******ovo izmenitit
def send_to_kafka(producer, topic, data):
    for feature in data.get("features", []):
        properties = feature["properties"]
        earthquake = {
            "id": feature["id"],
            "magnitude": properties["mag"],
            "place": properties["place"],
            "time": properties["time"],
            "updated": properties.get("updated", None),
            "tz": properties.get("tz", None),
            "detail": properties.get("detail", None),
            "felt": properties.get("felt", None),
            "cdi": properties.get("cdi", None),
            "mmi": properties.get("mmi", None),
            "alert": properties.get("alert", None),
            "status": properties["status"],
            "tsunami": properties["tsunami"],
            "sig": properties["sig"],
            "net": properties["net"],
            "code": properties["code"],
            "ids": properties["ids"],
            "sources": properties["sources"],
            "types": properties["types"],
            "nst": properties["nst"],
            "dmin": properties["dmin"],
            "rms": properties["rms"],
            "gap": properties["gap"],
            "magType": properties["magType"],
            "type": properties["type"],
            "title": properties["title"],
            "latitude": feature["geometry"]["coordinates"][1],
            "longitude": feature["geometry"]["coordinates"][0],
            "depth": feature["geometry"]["coordinates"][2]
        }
        producer.send(topic, value=earthquake)  #ovde nema key proveriti ovo kasnije
        print(f"Sent earthquake data: {earthquake}") 


# Glavna petlja za periodično preuzimanje podataka

while True:
    try:
        producer = KafkaProducer(**KAFKA_CONFIGURATION)
        print("Connected to Kafka!")
        break
    except kafka.errors.NoBrokersAvailable as e:
        print(e)
        time.sleep(3)

# Početno vreme
current_time = datetime.utcnow().replace(minute=0, second=0, microsecond=0) - timedelta(days=(1)) 
print("Trenutno vreme: ",current_time)
interval = timedelta(minutes=15)  # Interval od 15 minuta

while True:
    # Postavljanje start i end vremena
    start_time = current_time.strftime("%Y-%m-%dT%H:%M:%S")
    end_time = (current_time + interval).strftime("%Y-%m-%dT%H:%M:%S")

    print(f"Fetching data for {start_time} to {end_time}")
    data = fetch_earthquake_data(start_time, end_time)

    if data:
        send_to_kafka(producer, KAFKA_TOPIC, data)

    # Ažuriranje vremena za sledeći interval
    current_time += interval

    # Čeka pola minuta pre sledeće iteracije
    time.sleep(30)
