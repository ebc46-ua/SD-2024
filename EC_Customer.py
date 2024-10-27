# -*- coding: utf-8 -*-
import json
import time
import os
from kafka import KafkaProducer, KafkaConsumer
import argparse

class ECCustomer:
    def __init__(self, broker_ip, requests_path, cliente_id):
        self.broker_ip = broker_ip
        self.requests_path = requests_path
        self.producer = KafkaProducer(bootstrap_servers=[self.broker_ip])
        self.consumer = KafkaConsumer(
            'respuestas_clientes',
            bootstrap_servers=[self.broker_ip],
            group_id='clientes',
            auto_offset_reset='earliest'
        )
        self.cliente_id = cliente_id  # cliente_id es ahora un string
        self.solicitudes_pendientes = []  # Lista de solicitudes pendientes por cliente
        self.solicitud_enviada = False
        self.cargar_solicitudes()

    def cargar_solicitudes(self):
        try:
            with open(self.requests_path, 'r') as archivo_requests:
                requests_data = json.load(archivo_requests)
                total_requests = requests_data['Requests']

                # Mapeo de cliente_id (letra) a índice
                # 'a' -> 0, 'b' -> 1, 'c' -> 2, etc.
                index = ord(self.cliente_id) - ord('a')

                # Verificar que el índice sea válido
                if 0 <= index < len(total_requests):
                    solicitud = total_requests[index]
                    self.solicitudes_pendientes = [solicitud]
                    print(f"[CLIENTE {self.cliente_id}] Solicitud asignada: {solicitud}")
                else:
                    print(f"[CLIENTE {self.cliente_id}] No hay solicitudes disponibles para este cliente.")
                    self.solicitudes_pendientes = []
        except Exception as e:
            print(f"Error al cargar solicitudes: {e}")

    def enviar_solicitud(self):
        if not self.solicitud_enviada:  # Solo envía si no hay una solicitud en curso
            if self.solicitudes_pendientes:
                self.solicitud_enviada = True  # Marca que una solicitud está en curso
                request = self.solicitudes_pendientes.pop(0)
                destino_id = request['Id']
                origen_coord = request['Start']
                mensaje = {
                    'cliente_id': self.cliente_id,  # cliente_id sigue siendo un string
                    'origen': origen_coord,
                    'destino': destino_id
                }
                self.producer.send('solicitudes', json.dumps(mensaje).encode())
                print(f"[CLIENTE {self.cliente_id}] Solicitud enviada para destino {destino_id}")
            else:
                print(f"[CLIENTE {self.cliente_id}] No hay más solicitudes pendientes.")
                exit(0)
        else:
            print(f"[CLIENTE {self.cliente_id}] Una solicitud ya está en curso. Esperando respuesta.")

    def escuchar_respuestas(self):
        print(f"[CLIENTE {self.cliente_id}] Esperando respuestas de la CENTRAL...")
        for mensaje in self.consumer:
            respuesta = json.loads(mensaje.value.decode())
            cliente_id_respuesta = respuesta.get('cliente_id')
            estado = respuesta.get('estado')

            if cliente_id_respuesta == self.cliente_id:  # Comparación con cliente_id como string
                if estado == 'OK':
                    print(f"[CLIENTE {cliente_id_respuesta}] Su solicitud ha sido aceptada. Un taxi está en camino.")
                elif estado == 'KO':
                    print(f"[CLIENTE {cliente_id_respuesta}] Lo sentimos, no hay taxis disponibles en este momento.")
                    self.solicitud_enviada = False  # Permite intentar enviar nuevamente
                elif estado == 'COMPLETED':
                    print(f"[CLIENTE {cliente_id_respuesta}] Su servicio ha finalizado.")
                    print(f"[CLIENTE {cliente_id_respuesta}] Esperando 4 segundos para solicitar un nuevo servicio...")
                    time.sleep(4)
                    self.solicitud_enviada = False  # Permite enviar una nueva solicitud
                    self.enviar_solicitud()  # Envía la siguiente solicitud si hay una pendiente
                else:
                    print(f"[CLIENTE {cliente_id_respuesta}] Estado desconocido: {estado}")

    def iniciar(self):
        if self.solicitudes_pendientes:
            self.enviar_solicitud()
            self.escuchar_respuestas()
        else:
            print(f"[CLIENTE {self.cliente_id}] No hay solicitudes para procesar.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ejecutar EC_Customer con parámetros de conexión y autenticación.")

    parser.add_argument('broker_ip', type=str, help='IP del Broker de Kafka')  # Broker IP y Puerto
    parser.add_argument('cliente_id', type=str, help='ID del cliente (como letra)')

    args = parser.parse_args()

    broker_ip = args.broker_ip
    cliente_id = args.cliente_id 

    requests_path = "EC_Requests.json"

    ec_customer = ECCustomer(broker_ip, requests_path, cliente_id)
    ec_customer.iniciar()

