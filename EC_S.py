import time
from kafka import KafkaProducer
import json
import random

class EC_S:
    def __init__(self, kafka_server, taxi_id):
        self.kafka_server = kafka_server
        self.taxi_id = taxi_id
        self.producer = KafkaProducer(bootstrap_servers=[self.kafka_server],
                                      value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    def send_sensor_data(self):
        while True:
            status = "OK" if random.random() > 0.1 else "KO"
            self.producer.send('sensor_data', {'taxi_id': self.taxi_id, 'status': status})
            print(f"Sensor data sent: {status}")
            time.sleep(1)

if __name__ == "__main__":
    sensor = EC_S(kafka_server="localhost:9092", taxi_id="T123")
    sensor.send_sensor_data()
