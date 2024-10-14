from kafka import KafkaProducer, KafkaConsumer
import json

class EC_DE:
    def __init__(self, central_ip, kafka_server, sensor_ip, taxi_id):
        self.central_ip = central_ip
        self.kafka_server = kafka_server
        self.sensor_ip = sensor_ip
        self.taxi_id = taxi_id
        self.producer = KafkaProducer(bootstrap_servers=[self.kafka_server],
                                      value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        self.consumer = KafkaConsumer('taxi_response', bootstrap_servers=[self.kafka_server],
                                      value_deserializer=lambda m: json.loads(m.decode('utf-8')))

    def authenticate(self):
        print(f"Authenticating taxi {self.taxi_id}")
        self.producer.send('taxi_requests', {'taxi_id': self.taxi_id})

    def receive_service(self):
        for message in self.consumer:
            data = message.value
            print(f"Received service response: {data}")
            # Lógica para moverse o actualizar estado del taxi
            
    def run(self):
        self.authenticate()
        self.receive_service()

if __name__ == "__main__":
    taxi = EC_DE(central_ip="localhost", kafka_server="localhost:9092", sensor_ip="localhost", taxi_id="T123")
    taxi.run()
