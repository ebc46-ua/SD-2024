import json
import time
import threading
import signal
from kafka import KafkaProducer, KafkaConsumer
import argparse
import tkinter as tk
from tkinter import scrolledtext

class EC_Customer:
    def __init__(self, broker_ip, requests_path, cliente_id):
        self.broker_ip = broker_ip
        self.requests_path = requests_path
        self.producer = KafkaProducer(bootstrap_servers=[self.broker_ip])
        self.consumer = KafkaConsumer(
            'respuestas_clientes',
            bootstrap_servers=[self.broker_ip],
            group_id=f'cliente_{cliente_id}',
            auto_offset_reset='earliest'
        )
        self.cliente_id = cliente_id
        self.solicitudes_pendientes = []
        self.solicitud_enviada = False
        self.taxi_asignado = None  # Para almacenar el taxi asignado
        self.cargar_solicitudes()

        # Asignar el manejador de señales
        signal.signal(signal.SIGINT, self.on_closing)
        signal.signal(signal.SIGTERM, self.on_closing)

    def cargar_solicitudes(self):
        try:
            with open(self.requests_path, 'r') as archivo_requests:
                requests_data = json.load(archivo_requests)
                total_requests = requests_data['Requests']
                self.solicitudes_pendientes = total_requests
                self.log(f"[CLIENTE {self.cliente_id}] Solicitudes cargadas: {self.solicitudes_pendientes}")
                if self.solicitudes_pendientes:
                    self.enviar_solicitud()
                else:
                    self.log(f"[CLIENTE {self.cliente_id}] No hay solicitudes disponibles.")
        except Exception as e:
            self.log(f"Error al cargar solicitudes: {e}")

    def enviar_solicitud(self):
        # Verificar que no haya una solicitud en curso y que el taxi esté asignado
        if not self.solicitud_enviada and (self.taxi_asignado is None or self.solicitudes_pendientes):
            if self.solicitudes_pendientes:
                self.solicitud_enviada = True
                request = self.solicitudes_pendientes.pop(0)
                destino_id = request['Id']
                origen_coord = request['Start']
                mensaje = {
                    'cliente_id': self.cliente_id,
                    'origen': origen_coord,
                    'destino': destino_id,
                    'taxi_asignado': self.taxi_asignado
                }
                try:
                    self.producer.send('solicitudes', json.dumps(mensaje).encode()).get(timeout=10)
                    self.log(f"[CLIENTE {self.cliente_id}] Solicitud enviada para destino {destino_id}")
                except Exception as e:
                    self.log(f"[CLIENTE {self.cliente_id}] Error al enviar solicitud: {e}")
                    self.solicitud_enviada = False
            else:
                self.log(f"[CLIENTE {self.cliente_id}] No hay más solicitudes pendientes.")
        else:
            self.log(f"[CLIENTE {self.cliente_id}] Una solicitud ya está en curso. Esperando respuesta.")

    def escuchar_respuestas(self):
        self.log(f"[CLIENTE {self.cliente_id}] Esperando respuestas de la CENTRAL...")
        start_time = time.time()
        timeout = 120

        while True:
            if time.time() - start_time > timeout:
                self.log(f"[CLIENTE {self.cliente_id}] Tiempo de espera excedido, cerrando conexión.")
                break

            try:
                mensaje = next(self.consumer)
                respuesta = json.loads(mensaje.value.decode())
                cliente_id_respuesta = respuesta.get('cliente_id')
                estado = respuesta.get('estado')

                if cliente_id_respuesta == self.cliente_id:
                    start_time = time.time()
                    if estado == 'OK':
                        self.taxi_asignado = respuesta.get('taxi_id') 
                        self.log(f"[CLIENTE {cliente_id_respuesta}] Su solicitud ha sido aceptada. Taxi asignado: {self.taxi_asignado}")
                    
                    elif estado == 'KO':
                        self.log(f"[CLIENTE {cliente_id_respuesta}] Lo sentimos, no hay taxis disponibles en este momento.")
                        self.solicitud_enviada = False
                    
                    elif estado == 'RECOGIDO':
                        self.log(f"[CLIENTE {cliente_id_respuesta}] Dirigiéndose a su destino con taxi {self.taxi_asignado}.")
                    
                    elif estado == 'COMPLETED':
                        self.log(f"[CLIENTE {cliente_id_respuesta}] Su servicio ha finalizado.")
                        self.solicitud_enviada = False
                        if self.solicitudes_pendientes:
                            self.log(f"[CLIENTE {cliente_id_respuesta}] Esperando 4 segundos antes de nueva solicitud.")
                            time.sleep(4)
                            self.enviar_solicitud()
                        else:
                            self.log(f"[CLIENTE {cliente_id_respuesta}] No hay más solicitudes pendientes. Taxi liberado.")
                            self.taxi_asignado = None
                    else:
                        self.log(f"[CLIENTE {cliente_id_respuesta}] Estado desconocido: {estado}")
            
            except StopIteration:
                self.log(f"[CLIENTE {self.cliente_id}] No hay más mensajes disponibles.")
                break
            except Exception as e:
                self.log(f"[CLIENTE {self.cliente_id}] Ocurrió un error: {e}")
                break

    def iniciar(self):
        self.init_gui()
        hilo_kafka = threading.Thread(target=self.escuchar_respuestas, daemon=True)
        hilo_kafka.start()
        self.root.mainloop()

    def init_gui(self):
        self.root = tk.Tk()
        self.root.title(f"Cliente {self.cliente_id}")
        self.text_area = scrolledtext.ScrolledText(self.root, width=60, height=20)
        self.text_area.grid(row=0, column=0, columnspan=2, padx=10, pady=10)
        self.label_destino = tk.Label(self.root, text="Esperando respuestas...")
        self.label_destino.grid(row=1, column=0, padx=5, pady=5, sticky="e")
        self.button_solicitar = tk.Button(self.root, text="Cerrar", command=self.on_closing)
        self.button_solicitar.grid(row=2, column=0, columnspan=2, pady=10)

    def log(self, message):
        try:
            if hasattr(self, 'text_area'):
                self.text_area.insert(tk.END, message + "\n")
                self.text_area.see(tk.END)
            else:
                print(message)
        except Exception as e:
            print(f"Error en log: {e}")

    def on_closing(self):
        self.producer.close()
        self.consumer.close()
        self.cerrar_conexion()
        self.root.quit()
        self.root.destroy()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ejecutar EC_Customer con parámetros de conexión y autenticación.")
    parser.add_argument('broker_ip', type=str, help='IP del Broker de Kafka')
    parser.add_argument('cliente_id', type=str, help='ID del cliente (como letra)')
    parser.add_argument('requests', type=str, help='Archivo requests')

    args = parser.parse_args()
    broker_ip = args.broker_ip
    cliente_id = args.cliente_id
    requests_path = args.requests

    ec_customer = EC_Customer(broker_ip, requests_path, cliente_id)
    ec_customer.iniciar()
