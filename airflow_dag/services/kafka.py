from datetime import datetime
from json import dumps, loads
import logging
import os

from dotenv import load_dotenv
import joblib
from kafka import KafkaConsumer, KafkaProducer
import pandas as pd
import requests


load_dotenv()
API_ENDPOINT = os.getenv('API_POWERBI')

joblib_file = "./model/random_forest_regressor.pkl"
model = joblib.load(joblib_file)


def kafka_producer(row):
    producer = KafkaProducer(
        value_serializer=lambda m: dumps(m).encode('utf-8'),
        bootstrap_servers=['localhost:9092'],
    )

    message = row.to_dict()
    producer.send('kafka-used-cars', value=message)
    logging.info("Message sent")


def kafka_consumer():
    consumer = KafkaConsumer(
        'kafka-used-cars',
        enable_auto_commit=True,
        group_id='my-group-1',
        value_deserializer=lambda m: loads(m.decode('utf-8')),
        bootstrap_servers=['localhost:9092']
    )

    for message in consumer:
        df = pd.json_normalize(data=message.value)
        df = df.rename(columns={"vehicle_year": "year"})
        df['price_prediction'] = model.predict(df[["horsepower", "mileage", "year"]])
        current_time = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        df['time'] = current_time
        data = bytes(df[['price', 'price_prediction', 'time']].to_json(orient='records'), 'utf-8')
        print(f"Sending data: {data}")
        req = requests.post(API_ENDPOINT, data)