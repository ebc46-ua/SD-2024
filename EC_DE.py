from kafka import KafkaProducer, KafkaConsumer
import json
import time

class EC_DE:
    def __init__(self, central_ip, kafka_server, sensor_ip, taxi_id):
        self.central_ip = central_ip
        self.kafka_server = kafka_server
        self.sensor_ip = sensor_ip
        self.taxi_id = taxi_id
        self.position = [1, 1]  # Todos los taxis arrancan en [1, 1]
        self.producer = KafkaProducer(bootstrap_servers=[self.kafka_server],
                                      value_serializer=lambda v: json.dumps(v).encode('utf-8'))
        self.consumer = KafkaConsumer('taxi_response', bootstrap_servers=[self.kafka_server],
                                    value_deserializer=lambda m: json.loads(m.decode('utf-8'))) 

        self.authenticated = False

    def authenticate(self):
        # Se solicita autenticación a la central
        print(f"Authenticating taxi {self.taxi_id} with EC_Central...")
        self.producer.send('taxi_requests', {'type': 'auth', 'taxi_id': self.taxi_id})
        time.sleep(1)  # No es necesario pero simulamos un pequeño delay

    def receive_service(self):
        for message in self.consumer:
            data = message.value
            print(f"Received service response: {data}")
            
            # Verifica si la respuesta indica un fallo en la autenticación
            if 'estado' in data:
                if data['estado'] == 'OK':
                    print(f"Taxi {self.taxi_id} autenticado correctamente. Esperando servicio...")
                    # Aquí puedes implementar la lógica para esperar servicios
                elif data['estado'] == 'KO':
                    print(f"Taxi {self.taxi_id} no fue autenticado. Saliendo...")
                    break  # Salir del bucle si la autenticación falla
    
    def run(self):
        self.authenticate()
        self.receive_service()

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 5:
        print("Usage: python3 EC_DE.py <central_ip> <kafka_server> <sensor_ip> <taxi_id>")
        sys.exit(1)

    central_ip = sys.argv[1]
    kafka_server = sys.argv[2]
    sensor_ip = sys.argv[3]
    taxi_id = sys.argv[4]

    taxi = EC_DE(central_ip=central_ip, kafka_server=kafka_server, sensor_ip=sensor_ip, taxi_id=taxi_id)
    taxi.run()
