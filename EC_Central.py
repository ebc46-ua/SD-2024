import json
import os
import re
import socket
import time
import kafka
import sqlite3
from pygame import *
import pygame
import threading
from kafka import KafkaProducer, KafkaConsumer 
import argparse
from prettytable import PrettyTable
 
class ECCentral:
    def __init__(self, puerto_escucha, broker_ip, db_path, map_path):
        self.puerto_escucha = puerto_escucha
        self.broker_ip = broker_ip
        self.db_path = db_path
        self.map_path = map_path
        self.mapa = [[' ' for _ in range(20)] for _ in range(20)]  # Mapa 20x20 vacío
        self.taxis_disponibles = {}
        self.clientes_activos = {}
        self.taxi_cliente = {}  # Diccionario para asociar taxi_id con cliente_id
        self.taxis_autenticados = {}
        self.sockets_taxis = {}  # Almacenar los sockets de los taxis
        self.lock = threading.Lock()  # Lock para proteger acceso a datos compartidos
        self.quit = False  # Flag para indicar salida del programa
        self.actualizar_mapa = False  # Flag para indicar actualización del mapa
        self.actualizar_tabla = False
        
        self.cargar_localizaciones()  # Carga las localizaciones desde el archivo JSON
        self.cargar_taxis_desde_bd()
        self.inicializar_kafka()
        self.iniciar_servidor_sockets()
        self.localizaciones_clientes = {}  # Diccionario para almacenar el origen de cada cliente

        
        pygame.init()
        self.ancho_ventana = 900  # Ancho de la ventana
        self.alto_ventana = 400   # Alto de la ventana
        self.tamaño_celda = 20    # Tamaño de cada celda en píxeles
        self.ventana = pygame.display.set_mode((self.ancho_ventana, self.alto_ventana))
        pygame.display.set_caption("Mapa de Taxis")
        self.font = pygame.font.SysFont(None, 24)
        self.actualizar_mapa = True
        self.actualizar_tabla = True
        self.screen = pygame.display.set_mode((1250, 400))
        pygame.display.set_caption("Estado de Taxis y Clientes")
        self.clock = pygame.time.Clock()  # Para manejar la tasa de refresc
 
    def iniciar_servidor_sockets(self):
        self.servidor_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.servidor_socket.bind(('localhost', self.puerto_escucha))
        self.servidor_socket.listen(5)
        threading.Thread(target=self.aceptar_conexiones_taxis, daemon=True).start()
        print(f"[CENTRAL] Servidor de sockets iniciado en el puerto {self.puerto_escucha}")

    def aceptar_conexiones_taxis(self):
        while True:
            cliente_socket, direccion = self.servidor_socket.accept()
            threading.Thread(target=self.procesar_conexion_taxi, args=(cliente_socket,), daemon=True).start()
            print(f"[CENTRAL] Conexión entrante de {direccion}")

    def procesar_conexion_taxi(self, cliente_socket):
        try:
            mensaje = cliente_socket.recv(1024).decode()
            if mensaje == 'ENQ':
                cliente_socket.send('ACK'.encode())
            else:
                cliente_socket.send('NACK'.encode())
                cliente_socket.close()
                return

            mensaje = cliente_socket.recv(1024).decode()
            stx_index = mensaje.find('<STX>')
            etx_index = mensaje.find('<ETX>')
            lrc_index = mensaje.find('<LRC>')

            if stx_index != -1 and etx_index != -1 and lrc_index != -1:
                data = mensaje[stx_index+5:etx_index]
                lrc = mensaje[lrc_index+5:]
                if self.verificar_lrc(data, lrc):
                    campos = data.split('#')
                    if campos[0] == 'AUTH':
                        taxi_id = campos[1]
                        datos_taxi = {'id': taxi_id}
                        if self.autentifica(datos_taxi):
                            cliente_socket.send('ACK'.encode())
                            self.sockets_taxis[taxi_id] = cliente_socket
                            threading.Thread(target=self.gestionar_taxi, args=(cliente_socket, taxi_id), daemon=True).start()
                            
                            # Enviar el mapa actualizado después de iniciar el hilo de gestión
                            self.enviar_mapa_actualizado()
                        else:
                            cliente_socket.send('NACK'.encode())
                    else:
                        cliente_socket.send('NACK'.encode())
                else:
                    cliente_socket.send('NACK'.encode())
            else:
                cliente_socket.send('NACK'.encode())
                cliente_socket.close()
        except Exception as e:
            print(f"[CENTRAL] Error al procesar conexión de taxi: {e}")
            cliente_socket.close()


    def verificar_lrc(self, data, lrc):
        # Implementar la verificación del LRC
        calculated_lrc = self.calcular_lrc(data)
        return str(calculated_lrc) == lrc.strip()  # Compara correctamente

    def calcular_lrc(self, data):
        lrc = 0
        for byte in data.encode():
            lrc ^= byte
        return str(lrc)  

    def enviar_mapa_actualizado(self):
        # Preparar los datos a enviar: posiciones y estados de todos los taxis
        taxis_estado = {}
        for taxi_id, taxi_info in self.taxis_autenticados.items():
            taxis_estado[taxi_id] = {
                'posicion': taxi_info['posicion'],
                'estado': taxi_info['estado']
            }
        data = 'MAP#' + json.dumps(taxis_estado)
        lrc = self.calcular_lrc(data)
        mensaje = f'<STX>{data}<ETX><LRC>{lrc}'
        # Enviar a todos los taxis
        for taxi_socket in self.sockets_taxis.values():
            try:
                taxi_socket.send(mensaje.encode())
            except Exception as e:
                print(f"[CENTRAL] Error al enviar mapa actualizado: {e}")

    


    def gestionar_taxi(self, cliente_socket, taxi_id):
        try:
            buffer = ""  
            pattern = re.compile(r'<STX>(.*?)<ETX><LRC>(.*)')

            while True:
                parte_mensaje = cliente_socket.recv(1024).decode()
                if not parte_mensaje:
                    break  

                buffer += parte_mensaje 

            
                while True:
                    match = pattern.search(buffer)
                    if match:
                        data = match.group(1)
                        lrc = match.group(2).strip()
                        mensaje_completo = match.group(0)
                        buffer = buffer[buffer.find(mensaje_completo) + len(mensaje_completo):]  

                        print(f"[CENTRAL] Mensaje recibido de taxi {taxi_id}: {mensaje_completo}")

                        # Verifica LRC
                        if self.verificar_lrc(data, lrc):
                            campos = data.split('#')

                            if campos[0] == 'POS':
                                x, y = int(campos[1]), int(campos[2])
                                with self.lock:
                                    self.taxis_autenticados[taxi_id]['posicion'] = (x, y)
                                    self.actualizar_mapa = True
                                self.enviar_respuesta(cliente_socket, 'ACK')
                                self.enviar_mapa_actualizado()

                            elif campos[0] == 'ARRIVED':
                                
                                self.procesar_arrived(taxi_id, cliente_socket)
                            else:
                                self.enviar_respuesta(cliente_socket, 'NACK')
                        else:
                            print(f"[CENTRAL] LRC incorrecto para el mensaje: {mensaje_completo}")
                            self.enviar_respuesta(cliente_socket, 'NACK')
                    else:
                        break  

        except Exception as e:
            print(f"[CENTRAL] Conexión con taxi {taxi_id} cerrada: {e}")
            cliente_socket.close()
            threading.Thread(target=self.esperar_reconexion_taxi, args=(taxi_id,), daemon=True).start()

    def procesar_arrived(self, taxi_id, cliente_socket):
        cliente_id = self.taxi_cliente.get(taxi_id)
        if cliente_id:
            with self.lock:
                cliente_info = self.localizaciones_clientes[cliente_id]
                destino_cliente = cliente_info['destino']
                estado_cliente = cliente_info.get('estado')

                if estado_cliente == 'EN RUTA':
                    # The taxi has arrived at the final destination
                    print(f"[CENTRAL] Taxi {taxi_id} ha llegado al destino final con el cliente {cliente_id}.")
                    # Actualizar estados del cliente
                    self.taxis_autenticados[taxi_id]['estado'] = 'FREE'
                    self.taxis_autenticados[taxi_id].pop('destino', None)
                    self.actualizar_mapa = True
                    self.actualizar_tabla = True
                    self.enviar_mensaje_cliente(cliente_id, 'COMPLETED')
                    # Borrar el cliente 
                    del self.localizaciones_clientes[cliente_id]
                    del self.taxi_cliente[taxi_id]
                else:
                    print(f"[CENTRAL] Taxi {taxi_id} ha recogido al cliente {cliente_id}. Dirigiéndose al destino {destino_cliente}.")
                    cliente_info['estado'] = 'EN RUTA'
                    self.enviar_mensaje_cliente(cliente_id, 'RECOGIDO')
                    self.actualizar_mapa = True
                    # Mandar instrucciones del destino final al taxi
                    data = f'GO#{destino_cliente[0]}#{destino_cliente[1]}'
                    lrc = self.calcular_lrc(data)
                    mensaje = f"<STX>{data}<ETX><LRC>{lrc}"
                    cliente_socket.send(mensaje.encode())
            self.enviar_respuesta(cliente_socket, 'ACK')
        else:
            self.enviar_respuesta(cliente_socket, 'NACK')


    def enviar_respuesta(self, cliente_socket, respuesta):
        """Enviar una respuesta al cliente."""
        try:
            cliente_socket.send(respuesta.encode())
        except Exception as e:
            print(f"[ERROR] Al enviar respuesta: {e}")
           
    
    def esperar_reconexion_taxi(self, taxi_id):
        print(f"[CENTRAL] Esperando 10 segundos por reconexión del taxi {taxi_id}...")
        time_start = time.time()
        while time.time() - time_start < 10:
            if taxi_id in self.taxis_autenticados:
                print(f"[CENTRAL] Taxi {taxi_id} se ha reconectado.")
                return
            time.sleep(1)
        # Si no se reconecta, marcar incidencia
        print(f"[CENTRAL] Taxi {taxi_id} no se ha reconectado en 10 segundos. Marcando incidencia.")
        with self.lock:
            if taxi_id in self.taxis_autenticados:
                del self.taxis_autenticados[taxi_id]
            if taxi_id in self.sockets_taxis:
                del self.sockets_taxis[taxi_id]
            if taxi_id in self.taxis_disponibles:
                del self.taxis_disponibles[taxi_id]
            self.actualizar_mapa = True
        self.enviar_mapa_actualizado()
        # Notificar al cliente si el taxi estaba asignado
        with self.lock:
            cliente_id = self.taxi_cliente.get(taxi_id)
            if cliente_id:
                self.enviar_mensaje_cliente(cliente_id, 'TAXI_DISCONNECTED')
                del self.taxi_cliente[taxi_id]
            
    def run(self):
        # Iniciar hilos para manejar conexiones y Kafka
        threading.Thread(target=self.procesar_comandos_arbitrarios, daemon=True).start()
        threading.Thread(target=self.procesar_peticiones_kafka, daemon=True).start()

        clock = pygame.time.Clock()
        while not self.quit:
            for evento in pygame.event.get():
                if evento.type == pygame.QUIT:
                    self.quit = True
                    pygame.quit()
                    break

            if self.actualizar_mapa:
                self.dibujar_mapa()
                self.dibujar_tabla_estados()
                pygame.display.flip()
                self.actualizar_mapa = False

            clock.tick(60)  # Limitar a 60 FPS
    
     
    def dibujar_mapa(self):
        self.ventana.fill((255, 255, 255))  # Limpiar la pantalla con color blanco

        # Dibujar la cuadrícula
        for x in range(0, 400, self.tamaño_celda):
            for y in range(0, 400, self.tamaño_celda):
                rect = pygame.Rect(x, y, self.tamaño_celda, self.tamaño_celda)
                pygame.draw.rect(self.ventana, (200, 200, 200), rect, 1)

        # Dibujar las localizaciones
        for id_localizacion, coord in self.localizaciones.items():
            x, y = coord
            rect = pygame.Rect(x * self.tamaño_celda, y * self.tamaño_celda, self.tamaño_celda, self.tamaño_celda)
            pygame.draw.rect(self.ventana, (23, 196, 247), rect)  # Color azul para las localizaciones
            img = self.font.render(id_localizacion, True, (0, 0, 0))
            self.ventana.blit(img, (x * self.tamaño_celda + 4, y * self.tamaño_celda + 3))

        # Dibujar los taxis
        with self.lock:
            for taxi_id, taxi_info in self.taxis_autenticados.items():
                x, y = taxi_info['posicion']
                rect = pygame.Rect(x * self.tamaño_celda, y * self.tamaño_celda, self.tamaño_celda, self.tamaño_celda)
                color_taxi = (0, 255, 0) if taxi_info['estado'] == 'RUN' or taxi_info['estado'] == 'BUSY' else (255, 0, 0)
                pygame.draw.rect(self.ventana, color_taxi, rect)
                img = self.font.render(str(taxi_id), True, (0, 0, 0))
                self.ventana.blit(img, (x * self.tamaño_celda + 4, y * self.tamaño_celda + 3))

        # Dibujar los clientes
        for cliente_id, info_cliente in self.localizaciones_clientes.items():
            origen = info_cliente.get('origen')
            if isinstance(origen, tuple) and len(origen) == 2:
                x, y = origen
                rect = pygame.Rect(x * self.tamaño_celda, y * self.tamaño_celda, self.tamaño_celda, self.tamaño_celda)
                pygame.draw.rect(self.ventana, (255, 255, 0), rect)  
                img = self.font.render(f"{cliente_id}", True, (0, 0, 0))
                self.ventana.blit(img, (x * self.tamaño_celda + 4, y * self.tamaño_celda + 3))
            else:
                print(f"[ERROR] Cliente {cliente_id} tiene un origen no válido: {origen}")

        pygame.display.flip()


    def int_a_char(cliente_id):
        # Asumimos que cliente_id es un entero a partir de 1
        return chr(ord('a') + cliente_id - 1)  # Mapea 1 -> 'a', 2 -> 'b', ...

    def dibujar_tabla_estados(self):
        # Colores
        WHITE = (255, 255, 255)
        BLACK = (0, 0, 0)
        LIGHT_GRAY = (200, 200, 200)

        # Configuración inicial
        x_start = self.ancho_ventana / 2 + 20  # Iniciar la tabla a la derecha del mapa
        y_start = 10
        cell_width = 125
        cell_height = 30

        # Dibujar el encabezado central
        header_text = "*** EASY CAB Release 1 ***"
        text_surface = self.font.render(header_text, True, BLACK)
        self.screen.blit(text_surface, (x_start + cell_width, y_start))
        y_start += cell_height  # Mover hacia abajo para iniciar las tablas

        # Encabezados de la tabla de taxis
        headers = ["ID Taxi", "Destino", "Estado"]
        for i, header in enumerate(headers):
            pygame.draw.rect(self.screen, LIGHT_GRAY, (x_start + i * cell_width, y_start, cell_width, cell_height))
            text_surface = self.font.render(header, True, BLACK)
            self.screen.blit(text_surface, (x_start + i * cell_width + 10, y_start + 5))

        # Dibujar los datos de la tabla de taxis
        for j, (taxi_id, info) in enumerate(self.taxis_disponibles.items()):
            row_y = y_start + (j + 1) * cell_height
            destino = info.get('destino', "No asignado")
            taxi_data = [taxi_id, destino, info['estado']]
            for i, data in enumerate(taxi_data):
                pygame.draw.rect(self.screen, WHITE, (x_start + i * cell_width, row_y, cell_width, cell_height), 1)
                text_surface = self.font.render(str(data), True, BLACK)
                self.screen.blit(text_surface, (x_start + i * cell_width + 10, row_y + 5))

        # Separación vertical entre la tabla de taxis y la de clientes
        separator_x = x_start + 3 * cell_width + 10  # Espacio para tres columnas de taxis
        pygame.draw.line(self.screen, BLACK, (separator_x, y_start), (separator_x, y_start + (len(self.taxis_disponibles) + 1) * cell_height), 2)

        # Encabezados de la tabla de clientes
        client_headers = ["ID Cliente", "Destino", "Estado"]
        for i, header in enumerate(client_headers):
            pygame.draw.rect(self.screen, LIGHT_GRAY, (separator_x + 10 + i * cell_width, y_start, cell_width, cell_height))
            text_surface = self.font.render(header, True, BLACK)
            self.screen.blit(text_surface, (separator_x + 10 + i * cell_width + 10, y_start + 5))

        # Dibujar los datos de la tabla de clientes
        for j, (cliente_id, info) in enumerate(self.clientes_activos.items()):
            row_y = y_start + (j + 1) * cell_height
            client_data = [cliente_id, info.get('destino', 'N/A'), info.get('estado', 'N/A')]  # Definir client_data aquí
            for i, data in enumerate(client_data):
                pygame.draw.rect(self.screen, WHITE, (separator_x + 10 + i * cell_width, row_y, cell_width, cell_height), 1)
                text_surface = self.font.render(str(data), True, BLACK)
                self.screen.blit(text_surface, (separator_x + 10 + i * cell_width + 10, row_y + 5))

        pygame.display.flip()  # Actualizar pantalla




    def finalizar_servicio_cliente(self, cliente_id):
        # Llamada al finalizar el servicio, elimina al cliente del mapa
        if cliente_id in self.localizaciones_clientes:
            del self.localizaciones_clientes[cliente_id]
            del self.clientes_activos[cliente_id]
        self.dibujar_mapa()  # Actualiza el mapa después de eliminar el cliente
        self.dibujar_tabla_estados()


    # def actualizar_pygame(self):
    #     while True:
            
    #         #time.sleep(0.1)  # Pequeña pausa para no saturar la CPU
            
            
    def cargar_localizaciones(self):
        # Lee el archivo EC_locations.json y carga las localizaciones en el mapa
        try:
            with open(self.map_path, 'r') as archivo_localizaciones:
                configuracion_localizaciones = json.load(archivo_localizaciones)
                self.localizaciones = {}
                for localizacion in configuracion_localizaciones['locations']:
                    id_localizacion = localizacion['Id']
                    x, y = map(int, localizacion['POS'].split(','))
                    self.mapa[x][y] = id_localizacion
                    self.localizaciones[id_localizacion] = (x, y)
                print("Localizaciones cargadas correctamente.")
        except Exception as e:
            print(f"Error cargando las localizaciones: {e}")
                
    def cargar_taxis_desde_bd(self):
        try:
            with open(self.db_path, 'r') as archivo_taxis:
                taxis_data = json.load(archivo_taxis)
                for taxi in taxis_data:
                    taxi_id = taxi['id']
                    estado = taxi.get('estado', 'FREE')
                    posicion = taxi.get('posicion', [1, 1])

                    # Validar estado
                    if estado not in ['FREE', 'BUSY', 'STOPPED', 'END',]:
                        print(f"Estado '{estado}' no reconocido para el taxi {taxi_id}. Taxi omitido.")
                        continue

                    # Validar posición
                    x, y = posicion
                    if not (0 <= x < 20 and 0 <= y < 20):
                        print(f"Posición {posicion} fuera del mapa para el taxi {taxi_id}. Taxi omitido.")
                        continue

                    # Verificar ID duplicado
                    if taxi_id in self.taxis_disponibles:
                        print(f"Taxi con id {taxi_id} ya cargado. Esperando su autenticación.")
                        continue

                    self.taxis_disponibles[taxi_id] = {
                        'estado': estado,
                        'posicion': posicion,
                        'destino': None  
                    }
                print(f"Taxi {taxi_id} cargado con posición {posicion} y estado {estado}.")    
            print("Taxis cargados correctamente.")
        except Exception as e:
            print(f"Error cargando los taxis: {e}")

        

    def inicializar_kafka(self):
        # Inicializa los productores y consumidores de Kafka para la comunicación con los taxis y clientes
        self.producer = KafkaProducer(bootstrap_servers=[self.broker_ip])
        self.consumer = KafkaConsumer('solicitudes', bootstrap_servers=[self.broker_ip], group_id='central')
        self.producer_respuestas = KafkaProducer(bootstrap_servers=[self.broker_ip])
        
        self.producer_mapa = KafkaProducer(bootstrap_servers=[self.broker_ip])

            
    
    def cargar_solicitudes(self):
        # Lee el archivo EC_Requests.json y carga las solicitudes
        try:
            with open('EC_Requests.json', 'r') as archivo_requests:
                requests_data = json.load(archivo_requests)
                self.solicitudes = []
                for request in requests_data['Requests']:
                    destino_id = request['Id']
                    self.solicitudes.append(destino_id)
                print("Solicitudes de clientes cargadas correctamente.")
        except Exception as e:
            print(f"Error cargando las solicitudes de clientes: {e}")
            self.solicitudes = []


    def cargar_mapa(self):
        # Lee el archivo JSON con la configuración del mapa y actualiza el mapa vacío
        try:
            with open(self.map_path, 'r') as archivo_mapa:
                configuracion_mapa = json.load(archivo_mapa)
                for localizacion in configuracion_mapa['localizaciones']:
                    x, y = localizacion['x'], localizacion['y']
                    self.mapa[x][y] = localizacion['id']

                for taxi in configuracion_mapa['taxis']:
                    x, y = taxi['posicion']['x'], taxi['posicion']['y']
                    self.mapa[x][y] = f'T{taxi["id"]}'
                    self.taxis_disponibles.append(taxi['id'])

            print("Mapa cargado correctamente.")
        except Exception as e:
            print(f"Error cargando el mapa: {e}")

    def autentifica(self, datos_taxi):
        # Autenticar taxi usando su ID
        try:
            with open(self.db_path, 'r') as archivo_taxis:
                taxis_data = json.load(archivo_taxis)
        except Exception as e:
            print(f"Error cargando los taxis: {e}")
            taxis_data = []

        taxi_encontrado = None
        for taxi in taxis_data:
            if taxi['id'] == datos_taxi['id']:
                taxi_encontrado = taxi
                break

        if datos_taxi['id'] in self.taxis_autenticados:
            print(f"Taxi con id {datos_taxi['id']} ya está autenticado y conectado.")
            return False

        if taxi_encontrado:
            print(f"El taxi con id {datos_taxi['id']} ha accedido al sistema.")
        else:
            print(f"El taxi con id {datos_taxi['id']} no está registrado en la base de datos.")
            return False

        estado_valido = taxi_encontrado['estado'] in ['FREE', 'BUSY', 'STOPPED', 'END']
        if not estado_valido:
            print(f"Estado '{taxi_encontrado['estado']}' no reconocido para el taxi {datos_taxi['id']}.")
            return False

        x, y = taxi_encontrado['posicion']
        if not (0 <= x < 20 and 0 <= y < 20):
            print(f"Posición {taxi_encontrado['posicion']} fuera del mapa para el taxi {datos_taxi['id']}.")
            return False

        taxi_autenticado = {
            "id": datos_taxi['id'],
            "posicion": taxi_encontrado['posicion'],
            "estado": taxi_encontrado['estado']
        }
        self.taxis_autenticados[datos_taxi['id']] = taxi_autenticado
        self.taxis_disponibles[datos_taxi['id']] = taxi_autenticado

        # Llamar a dibujar_mapa() para mostrar el taxi recién autenticado
        self.dibujar_mapa()
        self.actualizar_tabla = True
        self.dibujar_tabla_estados()
        # Enviar el mapa actualizado a todos los taxis
        self.enviar_mapa_actualizado()

        if datos_taxi['id'] in self.taxi_cliente:
            print(f"[CENTRAL] Taxi {datos_taxi['id']} reconectado y restaurando servicio al cliente.")
        
        # Actualizar el mapa y enviar a todos los taxis y clientes conectados
        self.enviar_mapa_actualizado()  # << Esta es la línea añadida para actualizar el mapa

        return True




    # def procesar_autenticacion_taxi(self, autenticacion):
    #     # Procesa la autenticación de taxis
    #     if self.autentifica(autenticacion):
    #         print(f"Taxi {autenticacion['id']} autenticado con éxito.")
    #         self.enviar_mensaje_autenticacion(autenticacion['id'], 'OK')
    #     else:
    #         print(f"Taxi {autenticacion['id']} falló en la autenticación.")
    #         self.enviar_mensaje_autenticacion(autenticacion['id'], 'KO')

    # def enviar_mensaje_autenticacion(self, taxi_id, estado):
    #     # Envía el estado de autenticación al taxi
    #     mensaje = {
    #         'taxi_id': taxi_id,
    #         'estado': estado
    #     }
    #     self.producer.send('respuesta_auth_taxi', json.dumps(mensaje).encode())

    def agregar_cliente(self, cliente_id, destino):
        # Agregar cliente al diccionario
        self.clientes_activos[cliente_id] = {
            'destino': self.localizaciones.get(destino),
            'estado': 'activo'
        }
        print(f"[CENTRAL] Cliente {cliente_id} agregado.")

    def procesar_peticion_cliente(self, peticion):
        cliente_id = peticion.get('cliente_id')
        destino = peticion.get('destino')
        destino_coord = self.localizaciones.get(destino)

        if not destino_coord:
            print(f"[CENTRAL] Destino {destino} no encontrado.")
            self.enviar_mensaje_cliente(cliente_id, 'KO')
            return

        print(f"[CENTRAL] Recibida petición de cliente {cliente_id} para destino {destino_coord}")

        # Convertir el origen del cliente a formato (x, y)
        origen_cliente = peticion.get('origen')
        if isinstance(origen_cliente, str):
            try:
                x, y = map(int, origen_cliente.split(','))  # Divide y convierte a enteros
                origen_cliente = (x, y)
                self.localizaciones_clientes[cliente_id] = {
                    'origen': origen_cliente,  # Origen en formato (x, y)
                    'destino': destino_coord   # Destino del cliente
                }
                self.agregar_cliente(cliente_id, destino)
            except ValueError:
                print(f"Formato incorrecto para el origen del cliente {cliente_id}: {origen_cliente}")
                return
        else:
            print(f"Formato no válido para el origen del cliente {cliente_id}: {origen_cliente}")
            return

        # Dibuja el mapa con el cliente registrado
        self.dibujar_mapa()
        self.dibujar_tabla_estados()

        # Asigna un taxi al cliente
        taxi_asignado = self.asignar_taxi(cliente_id, destino_coord)
        if taxi_asignado:
            print(f"[CENTRAL] Servicio aceptado para el cliente {cliente_id}. Enviando taxi {taxi_asignado}.")
            self.taxi_cliente[taxi_asignado] = cliente_id
            self.enviar_mensaje_cliente(cliente_id, 'OK')
            self.enviar_taxi(taxi_asignado, cliente_id, destino_coord)
        else:
            print(f"No se ha podido asignar taxi a cliente {cliente_id}")
            self.enviar_mensaje_cliente(cliente_id, 'KO')


    def procesar_comandos_arbitrarios(self):
        while True:
            comando_input = input("Ingrese un comando (formato: TAXI_ID COMANDO [DESTINO]): ")
            if comando_input:
                partes = comando_input.strip().split()
                if len(partes) >= 2:
                    taxi_id = partes[0]
                    comando = partes[1].upper()
                    if taxi_id in self.taxis_autenticados:
                        if comando == 'PARAR':
                            self.enviar_comando_taxi(taxi_id, 'STOP')
                        elif comando == 'REANUDAR':
                            self.enviar_comando_taxi(taxi_id, 'RESUME')
                        elif comando == 'IR_A_DESTINO' and len(partes) == 3:
                            destino_id = partes[2]
                            destino_coord = self.localizaciones.get(destino_id)
                            if destino_coord:
                                self.enviar_instrucciones_taxi(taxi_id, destino_coord)
                            else:
                                print(f"[CENTRAL] Destino {destino_id} no encontrado.")
                        elif comando == 'VOLVER_BASE':
                            self.enviar_instrucciones_taxi(taxi_id, (1, 1))
                        else:
                            print("[CENTRAL] Comando no reconocido o parámetros insuficientes.")
                    else:
                        print(f"[CENTRAL] Taxi {taxi_id} no está autenticado.")
                else:
                    print("[CENTRAL] Formato incorrecto. Use: TAXI_ID COMANDO [DESTINO]")
    
    def enviar_instrucciones_taxi(self, taxi_id, destino_coord):
        cliente_socket = self.sockets_taxis.get(taxi_id)
        if cliente_socket:
            data = f'GO#{destino_coord[0]}#{destino_coord[1]}'
            lrc = self.calcular_lrc(data)
            mensaje = f'<STX>{data}<ETX><LRC>{lrc}' 
            cliente_socket.send(mensaje.encode())
            print(f"[CENTRAL] Instrucciones enviadas al taxi {taxi_id}")
        else:
            print(f"[CENTRAL] No se pudo enviar instrucciones al taxi {taxi_id}. Socket no encontrado.")



    def enviar_mensaje_cliente(self, cliente_id, estado):
        # Envía un mensaje al cliente a través de Kafka
        mensaje = {
            'cliente_id': cliente_id,
            'estado': estado
        }
        self.producer_respuestas.send('respuestas_clientes', json.dumps(mensaje).encode())
        print(f"[CENTRAL] Enviada respuesta al cliente {cliente_id}: {estado}")


    def asignar_taxi(self, cliente_id, destino_coord):
        # Lógica para asignar un taxi disponible a una solicitud
        for taxi_id, taxi_info in self.taxis_autenticados.items():
            if taxi_info.get('estado', 'FREE') == 'FREE':
                taxi_info['estado'] = 'BUSY'
                taxi_info['destino'] = destino_coord 
                self.actualizar_mapa = True
                print(f"Taxi {taxi_id} asignado al cliente {cliente_id} para el destino {destino_coord}")
                return taxi_id
        print("No hay taxis disponibles.")
        return None

    
    def enviar_taxi(self, taxi_id, cliente_id, destino):
        taxi_info = self.taxis_autenticados.get(taxi_id)
        cliente_origen = self.localizaciones_clientes[cliente_id]['origen']  # Origen del cliente

        if taxi_info:
            print(f"Taxi {taxi_id} se dirige a recoger al cliente {cliente_id} en {cliente_origen}.")
            taxi_info['estado'] = 'RUN'
            self.actualizar_mapa = True
            self.enviar_instrucciones_taxi(taxi_id, cliente_origen)

            # Una vez en el origen, cambia el estado del cliente a "RECOGIDO"
            print(f"Taxi {taxi_id} ha llegado a {cliente_origen} para recoger al cliente.")
            self.localizaciones_clientes[cliente_id]['estado'] = 'RECOGIDO'
            
            # Instrucciones para llevar al cliente al destino
            print(f"Taxi {taxi_id} llevando al cliente {cliente_id} a {destino}.")
            self.enviar_instrucciones_taxi(taxi_id, destino)
        else:
            print(f"[CENTRAL] No se pudo enviar taxi {taxi_id} al cliente {cliente_id}.")



    def calcular_siguiente_paso(self, posicion_actual, destino):
        """Calcula el siguiente paso en el trayecto del taxi hacia su destino"""
        x_actual, y_actual = posicion_actual
        x_dest, y_dest = destino

        if x_actual < x_dest:
            x_actual += 1
        elif x_actual > x_dest:
            x_actual -= 1

        if y_actual < y_dest:
            y_actual += 1
        elif y_actual > y_dest:
            y_actual -= 1

        return x_actual, y_actual

    # def procesar_mensajes_sensores(self):
    #     # Procesar mensajes recibidos de los sensores de los taxis
    #     for mensaje in self.consumer_sensores:
    #         datos_sensor = json.loads(mensaje.value.decode())
    #         taxi_id = datos_sensor.get('taxi_id')
    #         estado = datos_sensor.get('estado')
            
    #         if taxi_id in self.taxis_autenticados:
    #             if estado == 'KO':
    #                 print(f"Taxi {taxi_id} ha detectado una incidencia y se detiene.")
    #                 self.taxis_autenticados[taxi_id]['estado'] = 'stopped'
    #                 self.enviar_mensaje_taxi(taxi_id, 'STOP')
    #             elif estado == 'OK':
    #                 # Si el estado vuelve a OK, reanudar el servicio del taxi
    #                 print(f"Taxi {taxi_id} ha resuelto la incidencia y continúa su viaje.")
    #                 self.taxis_autenticados[taxi_id]['estado'] = 'BUSY'
    #                 self.enviar_mensaje_taxi(taxi_id, 'RESUME')
    
    # def procesar_comandos(self):
    #     # Procesa comandos arbitrarios enviados a los taxis desde EC_Central
    #     for mensaje in self.consumer_comando:
    #         datos_comando = json.loads(mensaje.value.decode())
    #         taxi_id = datos_comando.get('taxi_id')
    #         comando = datos_comando.get('comando')
            
    #         if taxi_id in self.taxis_autenticados:
    #             print(f"Procesando comando '{comando}' para el taxi {taxi_id}")
    #             self.enviar_mensaje_taxi(taxi_id, comando)
    #             # Ejecutar acción local según el comando
    #             if comando == 'PARAR':
    #                 self.taxis_autenticados[taxi_id]['estado'] = 'stopped'
    #             elif comando == 'REANUDAR':
    #                 self.taxis_autenticados[taxi_id]['estado'] = 'BUSY'
    #             elif comando == 'VOLVER_BASE':
    #                 self.taxis_autenticados[taxi_id]['estado'] = 'returning'
    #                 self.enviar_taxi_a_base(taxi_id)
    
    # def enviar_taxi_a_base(self, taxi_id):
    #     # Envía un comando para que el taxi regrese a la base
    #     mensaje = {
    #         'taxi_id': taxi_id,
    #         'destino': '1,1'  # La posición de la base es [1,1]
    #     }
    #     self.producer.send('ordenes_taxi', json.dumps(mensaje).encode())
    #     print(f"Enviado taxi {taxi_id} a la base.")
        
        
    # def enviar_mensaje_taxi(self, taxi_id, comando):
    #     # Envía un comando específico a un taxi
    #     mensaje = {
    #         'taxi_id': taxi_id,
    #         'comando': comando
    #     }
    #     self.producer.send('comando_taxi', json.dumps(mensaje).encode())
    #     print(f"Enviado comando '{comando}' al taxi {taxi_id}")
    
    # def actualizar_mapa_taxi(self, taxi_id, destino):
    #     # Actualiza la posición del taxi en el mapa y envía el estado del mapa
    #     taxi_info = self.taxis_autenticados.get(taxi_id)
    #     if taxi_info:
    #         pos_actual = taxi_info['posicion']
    #         destino_coord = destino.split(',')
    #         taxi_info['posicion'] = destino_coord  # Solo como ejemplo, debería calcularse el movimiento
    #         print(f"Taxi {taxi_id} moviéndose de {pos_actual} a {destino_coord}")
    #         self.enviar_mapa_actualizado()
            
    def enviar_comando_taxi(self, taxi_id, comando):
        cliente_socket = self.sockets_taxis.get(taxi_id)
        if cliente_socket:
            data = f'CMD#{comando}'
            lrc = self.calcular_lrc(data)
            mensaje = f'<STX>{data}<ETX><LRC>{lrc}'
            cliente_socket.send(mensaje.encode())
            print(f"[CENTRAL] Comando '{comando}' enviado al taxi {taxi_id}")
        else:
            print(f"[CENTRAL] No se pudo enviar comando al taxi {taxi_id}. Socket no encontrado.")

    def actualizar_ruta_taxi(self, taxi_id, origen, destino):
        ruta = {
            "origen": origen,
            "destino": destino
        }
        archivo_ruta = f"EC_Route_Taxi_{taxi_id}.json"
        try:
            with open(archivo_ruta, 'a') as archivo:
                json.dump(ruta, archivo)
                archivo.write("\n")  # Nueva línea para cada ruta
            print(f"[CENTRAL] Ruta actualizada en {archivo_ruta} para taxi {taxi_id}.")
        except Exception as e:
            print(f"[CENTRAL] Error actualizando la ruta de taxi {taxi_id}: {e}")


        
    def enviar_mapa_a_clientes(self):
        # Envía el mapa actualizado a los clientes a través de Kafka
        with self.lock:
            taxis_estado = {}
            for taxi_id, taxi_info in self.taxis_autenticados.items():
                taxis_estado[taxi_id] = {
                    'posicion': taxi_info['posicion'],
                    'estado': taxi_info['estado']
                }
            mensaje_mapa = {
                'mapa': taxis_estado
            }
        self.producer_mapa.send('mapa_estado', json.dumps(mensaje_mapa).encode())
        print("[CENTRAL] Mapa actualizado enviado a los clientes.")

        
    def escuchar_peticiones(self):
        # Escucha peticiones tanto de clientes como de taxis (autenticación)
        print("[CENTRAL] Esperando peticiones...")
        
        while True:
             
            for mensaje in self.consumer:
                peticion = json.loads(mensaje.value.decode())
                self.procesar_peticion_cliente(peticion)

            for mensaje in self.consumer_taxi:
                autenticacion = json.loads(mensaje.value.decode())
                self.procesar_autenticacion_taxi(autenticacion)
                

    def procesar_peticiones_kafka(self):
        while not self.quit:
            # Procesar mensajes de solicitudes de clientes
            raw_msgs = self.consumer.poll(timeout_ms=1000)
            for tp, messages in raw_msgs.items():
                for message in messages:
                    peticion = json.loads(message.value.decode())
                    self.procesar_peticion_cliente(peticion)
                    
    def procesar_solicitudes(self):
        cliente_id = 1  # Podemos asignar IDs incrementales para los clientes
        for destino_id in self.solicitudes:
            # Convertir el ID del destino a coordenadas
            destino_coord = self.localizaciones.get(destino_id)
            if destino_coord:
                print(f"Procesando solicitud del cliente {cliente_id} para destino {destino_id} en coordenadas {destino_coord}")
                peticion = {
                    'cliente_id': cliente_id,
                    'destino': destino_coord
                }
                self.procesar_peticion_cliente(peticion)
                cliente_id += 1
                time.sleep(1)  # Simular tiempo entre solicitudes
            else:
                print(f"Destino {destino_id} no encontrado en las localizaciones.")

    
    def conectar_bd(self):
        if os.path.exists(self.db_path):
            print(f"Archivo de taxis '{self.db_path}' encontrado.")
            return True
        else:
            print(f"El archivo de taxis '{self.db_path}' no existe.")
            return False



    def imprimir_taxis(self):
        if not self.taxis_disponibles:
            print("No hay taxis disponibles.")
        else:
            for taxi_id, taxi_info in self.taxis_disponibles.items():
                print(f"Soy el taxi {taxi_id}, estoy disponible en la posición {taxi_info['posicion']}.")




if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ejecutar EC_Central con parámetros de conexión y autenticación.")

    parser.add_argument('puerto_escucha', type=int, help='Puerto de escucha de EC_Central')
    parser.add_argument('broker_ip', type=str, help='IP del Broker')
    parser.add_argument('db_path', type=str, help='BD de los taxis')

    args = parser.parse_args()

    puerto_escucha = args.puerto_escucha
    broker_ip = args.broker_ip
    db_path = args.db_path

    map_path = "EC_locations.json"  # Aquí habria que leerlo para evitar problemas

    # Instanciar la central
    ec_central = ECCentral(puerto_escucha, broker_ip, db_path, map_path)
    
    # Conectar a la base de datos y cargar taxis
    if not ec_central.conectar_bd():
        print("No se pudo conectar a la base de datos. Finalizando.")
        exit(1)

    # Cargar taxis y localizaciones desde archivos y base de datos
    ec_central.cargar_localizaciones()
    ec_central.cargar_taxis_desde_bd()
    ec_central.imprimir_taxis()

    # Ejecutar el bucle principal
    ec_central.run()