# -*- coding: utf-8 -*-
import json
import time
from kafka import KafkaProducer, KafkaConsumer
import argparse

class ECCustomer:
    def __init__(self, broker_ip, requests_path, cliente_id):
        self.broker_ip = broker_ip
        self.requests_path = requests_path
        self.producer = KafkaProducer(bootstrap_servers=[self.broker_ip])
        self.consumer = KafkaConsumer('respuestas_clientes', bootstrap_servers=[self.broker_ip], group_id='clientes')
        self.cliente_id = cliente_id  # Asignamos un ID �nico al cliente
        self.solicitudes_pendientes = []  # Lista de solicitudes pendientes por cliente
        
    def cargar_solicitudes(self):
        try:
            with open(self.requests_path, 'r') as archivo_requests:
                requests_data = json.load(archivo_requests)
                self.solicitudes_pendientes = requests_data['Requests']
        except Exception as e:
            print(f"Error al cargar solicitudes: {e}")
        
    def enviar_solicitud(self):
        if self.solicitudes_pendientes:
            request = self.solicitudes_pendientes.pop(0)
            destino_id = request['Id']
            mensaje = {
                'cliente_id': self.cliente_id,
                'destino': destino_id 
            }
            self.producer.send('solicitudes', json.dumps(mensaje).encode())
            print(f"[CLIENTE {self.cliente_id}] Solicitud enviada para destino {destino_id}")
        else:
            print(f"[CLIENTE {self.cliente_id}] No hay m�s solicitudes pendientes.")


    def escuchar_respuestas(self):
        print(f"[CLIENTE {self.cliente_id}] Esperando respuestas de la CENTRAL...")
        for mensaje in self.consumer:
            respuesta = json.loads(mensaje.value.decode())
            cliente_id_respuesta = respuesta.get('cliente_id')
            estado = respuesta.get('estado')
            if cliente_id_respuesta == self.cliente_id:
                if estado == 'OK':
                    print(f"[CLIENTE {cliente_id_respuesta}] Su solicitud ha sido aceptada. Un taxi est� en camino.")
                elif estado == 'KO':
                    print(f"[CLIENTE {cliente_id_respuesta}] Lo sentimos, no hay taxis disponibles en este momento.")
                elif estado == 'COMPLETED':
                    print(f"[CLIENTE {cliente_id_respuesta}] Su servicio ha finalizado.")
                    if self.solicitudes_pendientes:
                        print(f"[CLIENTE {cliente_id_respuesta}] Esperando 4 segundos para solicitar un nuevo servicio...")
                        time.sleep(4)
                        self.enviar_solicitud()
                    else:
                        print(f"[CLIENTE {cliente_id_respuesta}] No hay m�s solicitudes pendientes.")
                else:
                    print(f"[CLIENTE {cliente_id_respuesta}] Estado desconocido: {estado}")

    
    def iniciar(self):
        self.cargar_solicitudes()
        self.enviar_solicitud()
        self.escuchar_respuestas()
                


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Ejecutar EC_Customer con parámetros de conexión y autenticación.")

    parser.add_argument('broker_ip', type=str, default='localhost:9092', help='IP del Broker de Kafka')       # Broker IP y Puerto
    parser.add_argument('cliente_id', type=int, default=1, help='ID del cliente') 

    args = parser.parse_args()

    broker_ip = args.broker_ip
    cliente_id = args.cliente_id

    requests_path = "EC_Requests.json"

    ec_customer = ECCustomer(broker_ip, requests_path, cliente_id)
    
    # ec_customer.enviar_solicitud()
    ec_customer.iniciar()
