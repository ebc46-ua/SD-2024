from kafka import KafkaProducer
import json
import time

class Customer:
    def __init__(self, kafka_server, customer_id, services_file):
        self.kafka_server = kafka_server
        self.customer_id = customer_id
        self.services = self.read_services(services_file)
        self.producer = KafkaProducer(bootstrap_servers=[self.kafka_server],
                                      value_serializer=lambda v: json.dumps(v).encode('utf-8'))

    def read_services(self, services_file):
        with open(services_file, 'r') as f:
            return json.load(f)

    def request_service(self):
        for service in self.services:
            print(f"Requesting service: {service}")
            self.producer.send('taxi_requests', {'customer_id': self.customer_id, 'destination': service['destination']})
            time.sleep(4)

if __name__ == "__main__":
    customer = Customer(kafka_server="localhost:9092", customer_id="C123", services_file="services.json")
    customer.request_service()
