from kafka import KafkaConsumer, KafkaProducer
import sqlite3
import json

class CentralSystem:
    def __init__(self, listen_port, kafka_server, db_path):
        self.listen_port = listen_port
        self.kafka_server = kafka_server
        self.db_path = db_path
        self.producer = KafkaProducer(bootstrap_servers=[self.kafka_server],
                                      value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        self.consumer = KafkaConsumer('taxi_requests', bootstrap_servers=[self.kafka_server],
                                      value_deserializer=lambda m: json.loads(m.decode('utf-8')))
        self.init_db()

    def init_db(self):
        self.conn = sqlite3.connect(self.db_path)
        self.cursor = self.conn.cursor()
        self.cursor.execute('''CREATE TABLE IF NOT EXISTS taxis (id TEXT, estado TEXT)''')
        self.conn.commit()

    def handle_taxi_request(self):
        for message in self.consumer:
            data = message.value
            print(f"Received taxi request: {data}")
            # lógica para asignar taxi y devolver el estado
            self.producer.send('taxi_response', {'status': 'Assigned', 'taxi_id': 'T123'})
            
    def run(self):
        print(f"Central system listening on port {self.listen_port}")
        self.handle_taxi_request()

if __name__ == "__main__":
    central = CentralSystem(listen_port=8080, kafka_server="localhost:9092", db_path="taxis.db")
    central.run()
