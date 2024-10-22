import json
import os
import socket
import time
import kafka
import sqlite3
from pygame import *
import pygame
import threading
from kafka import KafkaProducer, KafkaConsumer 
import argparse
 
class ECCentral:
    def __init__(self, puerto_escucha, broker_ip, db_path, map_path):
        self.puerto_escucha = puerto_escucha
        self.broker_ip = broker_ip
        self.db_path = db_path
        self.map_path = map_path
        self.mapa = [[' ' for _ in range(20)] for _ in range(20)]  # Mapa 20x20 vacío
        self.taxis_disponibles = {}
        self.clientes_activos = []
        self.taxi_cliente = {}  # Diccionario para asociar taxi_id con cliente_id
        self.taxis_autenticados = {}
        self.sockets_taxis = {}  # Almacenar los sockets de los taxis
        self.iniciar_servidor_sockets()
        self.cargar_localizaciones()  # Carga las localizaciones desde el archivo JSON
        self.inicializar_kafka()
        
        pygame.init()
        self.ancho_ventana = 400  # Ancho de la ventana
        self.alto_ventana = 400   # Alto de la ventana
        self.tamaño_celda = 20    # Tamaño de cada celda en píxeles
        self.ventana = pygame.display.set_mode((self.ancho_ventana, self.alto_ventana))
        pygame.display.set_caption("Mapa de Taxis")
        self.font = pygame.font.SysFont(None, 14)
 
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
            # Paso 1: Recibir ENQ
            mensaje = cliente_socket.recv(1024).decode()
            if mensaje == 'ENQ':
                # Enviar ACK
                cliente_socket.send('ACK'.encode())
            else:
                cliente_socket.send('NACK'.encode())
                cliente_socket.close()
                return

            # Paso 2: Recibir solicitud de autenticación
            mensaje = cliente_socket.recv(1024).decode()
            # Extraer <STX>, <DATA>, <ETX>, <LRC>
            stx_index = mensaje.find('<STX>')
            etx_index = mensaje.find('<ETX>')
            lrc_index = mensaje.find('<LRC>')

            if stx_index != -1 and etx_index != -1 and lrc_index != -1:
                data = mensaje[stx_index+5:etx_index]
                lrc = mensaje[lrc_index+5:]
                # Verificar LRC
                if self.verificar_lrc(data, lrc):
                    # Procesar datos de autenticación
                    campos = data.split('#')
                    if campos[0] == 'AUTH':
                        taxi_id = campos[1]
                        datos_taxi = {'id': taxi_id}
                        if self.autentifica(datos_taxi):
                            cliente_socket.send('ACK'.encode())
                            # Almacenar el socket del taxi
                            self.sockets_taxis[taxi_id] = cliente_socket
                            # Mantener comunicación con el taxi
                            threading.Thread(target=self.gestionar_taxi, args=(cliente_socket, taxi_id), daemon=True).start()
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
        return calculated_lrc == lrc    

    def calcular_lrc(self, data):
        # Calcular LRC como XOR de los bytes
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
        # Mantener comunicación con el taxi
        try:
            while True:
                mensaje = cliente_socket.recv(1024).decode()
                print(f"[CENTRAL] Gestionando taxi {taxi_id}. Mensaje recibido: '{mensaje}'")  # Depuración
                if mensaje:
                    # Procesar mensajes del taxi
                    print(f"[CENTRAL] Procesando mensaje de taxi {taxi_id}: {mensaje}")
                    stx_index = mensaje.find('<STX>')
                    etx_index = mensaje.find('<ETX>')
                    lrc_index = mensaje.find('<LRC>')

                    if stx_index != -1 and etx_index != -1 and lrc_index != -1:
                        data = mensaje[stx_index+5:etx_index]
                        lrc = mensaje[lrc_index+5:]
                        # Verificar LRC
                        if self.verificar_lrc(data, lrc):
                            campos = data.split('#')
                            if campos[0] == 'POS':
                                # Actualizar posición del taxi
                                x, y = int(campos[1]), int(campos[2])
                                self.taxis_autenticados[taxi_id]['posicion'] = (x, y)
                                self.dibujar_mapa()
                                # Enviar ACK
                                cliente_socket.send('ACK'.encode())
                                # Enviar mapa actualizado a todos los taxis
                                self.enviar_mapa_actualizado()
                            elif campos[0] == 'STATUS':
                                estado = campos[1]
                                print(f"[CENTRAL] Taxi {taxi_id} está en estado '{estado}'")
                                self.taxis_autenticados[taxi_id]['estado'] = estado
                                self.dibujar_mapa()
                                # Enviar ACK
                                cliente_socket.send('ACK'.encode())
                                # Enviar mapa actualizado a todos los taxis
                                self.enviar_mapa_actualizado()
                                # Si el estado es 'END', notificar al cliente
                                if estado == 'END':
                                    cliente_id = self.taxi_cliente.get(taxi_id)
                                    if cliente_id:
                                        self.enviar_mensaje_cliente(cliente_id, 'COMPLETED')
                                        del self.taxi_cliente[taxi_id]
                            else:
                                cliente_socket.send('NACK'.encode())
                        else:
                            cliente_socket.send('NACK'.encode())
                    else:
                        cliente_socket.send('NACK'.encode())
                else:
                    # Si no recibimos ningún mensaje, el cliente puede haber cerrado la conexión
                    print(f"[CENTRAL] Taxi {taxi_id} cerró la conexión (mensaje vacío).")
                    break
        except Exception as e:
            print(f"[CENTRAL] Error en la conexión con taxi {taxi_id}: {e}")
            cliente_socket.close()
            # Iniciar temporizador de 10 segundos antes de marcar incidencia
            threading.Thread(target=self.esperar_reconexion_taxi, args=(taxi_id,), daemon=True).start()
            del self.sockets_taxis[taxi_id]
            del self.taxis_autenticados[taxi_id]
            if taxi_id in self.taxis_disponibles:
                del self.taxis_disponibles[taxi_id]
            self.dibujar_mapa()
            self.enviar_mapa_actualizado()
    
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
        if taxi_id in self.taxis_autenticados:
            del self.taxis_autenticados[taxi_id]
        if taxi_id in self.sockets_taxis:
            del self.sockets_taxis[taxi_id]
        if taxi_id in self.taxis_disponibles:
            del self.taxis_disponibles[taxi_id]
        self.dibujar_mapa()
        self.enviar_mapa_actualizado()
        # Notificar al cliente si el taxi estaba asignado
        cliente_id = self.taxi_cliente.get(taxi_id)
        if cliente_id:
            self.enviar_mensaje_cliente(cliente_id, 'TAXI_DISCONNECTED')
            del self.taxi_cliente[taxi_id]
            
            
    def dibujar_mapa(self):
        for evento in pygame.event.get():
            if evento.type == pygame.QUIT:
                pygame.quit()
                exit()

        self.ventana.fill((255, 255, 255))  # Limpiar la pantalla con color blanco

        # Dibujar la cuadrícula
        for x in range(0, self.ancho_ventana, self.tamaño_celda):
            for y in range(0, self.alto_ventana, self.tamaño_celda):
                rect = pygame.Rect(x, y, self.tamaño_celda, self.tamaño_celda)
                pygame.draw.rect(self.ventana, (200, 200, 200), rect, 1)

        # Dibujar las localizaciones
        for id_localizacion, coord in self.localizaciones.items():
            x, y = coord
            rect = pygame.Rect(x * self.tamaño_celda, y * self.tamaño_celda, self.tamaño_celda, self.tamaño_celda)
            pygame.draw.rect(self.ventana, (0, 0, 255), rect)  # Color azul para las localizaciones
            # Opcional: dibujar el ID de la localización
            font = pygame.font.SysFont(None, 14)
            img = font.render(id_localizacion, True, (255, 255, 255))
            self.ventana.blit(img, (x * self.tamaño_celda + 2, y * self.tamaño_celda + 2))

        # Dibujar los taxis
        for taxi_id, taxi_info in self.taxis_autenticados.items():
            x, y = taxi_info['posicion']
            rect = pygame.Rect(x * self.tamaño_celda, y * self.tamaño_celda, self.tamaño_celda, self.tamaño_celda)
            if taxi_info['estado'] == 'RUN':
                color_taxi = (0, 255, 0)  # Verde para estado RUN
            elif taxi_info['estado'] == 'STOPPED':
                color_taxi = (255, 255, 0)  # Amarillo para estado STOPPED
            elif taxi_info['estado'] == 'END':
                color_taxi = (255, 0, 255)  # Magenta para estado BUSY
            else:
                color_taxi = (255, 0, 0)  # Morado para estado desconocido
            pygame.draw.rect(self.ventana, color_taxi, rect)
            
            img = font.render(str(taxi_id), True, (255, 255, 255))
            self.ventana.blit(img, (x * self.tamaño_celda + 2, y * self.tamaño_celda + 2))

        pygame.display.flip()  # Actualizar la pantalla

    def actualizar_pygame(self):
        while True:
            self.dibujar_mapa()
            #time.sleep(0.1)  # Pequeña pausa para no saturar la CPU
            
            
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
                    if estado not in ['FREE', 'BUSY', 'STOPPED', 'END']:
                        print(f"Estado '{estado}' no reconocido para el taxi {taxi_id}. Taxi omitido.")
                        continue

                    # Validar posición
                    x, y = posicion
                    if not (0 <= x < 20 and 0 <= y < 20):
                        print(f"Posición {posicion} fuera del mapa para el taxi {taxi_id}. Taxi omitido.")
                        continue

                    # Verificar ID duplicado
                    if taxi_id in self.taxis_disponibles:
                        print(f"Taxi con id {taxi_id} ya cargado. Taxi omitido.")
                        continue

                    self.taxis_disponibles[taxi_id] = {
                        'estado': estado,
                        'posicion': posicion
                    }
                print("Taxis cargados correctamente.")
        except Exception as e:
            print(f"Error cargando los taxis: {e}")

        

    def inicializar_kafka(self):
        # Inicializa los productores y consumidores de Kafka para la comunicación con los taxis y clientes
        self.producer = KafkaProducer(bootstrap_servers=[self.broker_ip])
        self.consumer = KafkaConsumer('solicitudes', bootstrap_servers=[self.broker_ip], group_id='central')
        self.consumer_taxi = KafkaConsumer('auth_taxi', bootstrap_servers=[self.broker_ip], group_id='central')
        self.consumer_comando = KafkaConsumer('comando_taxi', bootstrap_servers=[self.broker_ip], group_id='central')
        self.producer_respuestas = KafkaProducer(bootstrap_servers=[self.broker_ip])

            
    
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
            # Cargar taxis existentes desde el archivo JSON
            with open(self.db_path, 'r') as archivo_taxis:
                taxis_data = json.load(archivo_taxis)
        except Exception as e:
            print(f"Error cargando los taxis: {e}")
            taxis_data = []
        
        # Verificar si el taxi ya está registrado
        taxi_encontrado = None
        for taxi in taxis_data:
            if taxi['id'] == datos_taxi['id']:
                taxi_encontrado = taxi
                break
        
        # Validar si el taxi ya está autenticado y conectado
        if datos_taxi['id'] in self.taxis_autenticados:
            print(f"Taxi con id {datos_taxi['id']} ya está autenticado y conectado.")
            return False

        if taxi_encontrado:
            print(f"El taxi con id {datos_taxi['id']} ha accedido al sistema.")
        else:
            print(f"El taxi con id {datos_taxi['id']} no está registrado en la base de datos.")
            return False

        # Validar el estado del taxi
        estado_valido = taxi_encontrado['estado'] in ['FREE', 'BUSY', 'STOPPED', 'END']
        if not estado_valido:
            print(f"Estado '{taxi_encontrado['estado']}' no reconocido para el taxi {datos_taxi['id']}.")
            return False

        # Validar la posición del taxi
        x, y = taxi_encontrado['posicion']
        if not (0 <= x < 20 and 0 <= y < 20):
            print(f"Posición {taxi_encontrado['posicion']} fuera del mapa para el taxi {datos_taxi['id']}.")
            return False

        # Añadir el taxi a taxis_autenticados y taxis_disponibles
        taxi_autenticado = {
            "id": datos_taxi['id'],
            "posicion": taxi_encontrado['posicion'],
            "estado": taxi_encontrado['estado']
        }
        self.taxis_autenticados[datos_taxi['id']] = taxi_autenticado
        self.taxis_disponibles[datos_taxi['id']] = taxi_autenticado
         # Si el taxi ya estaba asignado a un cliente, restaurar la asociación
        if datos_taxi['id'] in self.taxi_cliente:
            print(f"[CENTRAL] Taxi {datos_taxi['id']} reconectado y restaurando servicio al cliente.")
        return True



    def procesar_autenticacion_taxi(self, autenticacion):
        # Procesa la autenticación de taxis
        if self.autentifica(autenticacion):
            print(f"Taxi {autenticacion['id']} autenticado con éxito.")
            self.enviar_mensaje_autenticacion(autenticacion['id'], 'OK')
        else:
            print(f"Taxi {autenticacion['id']} falló en la autenticación.")
            self.enviar_mensaje_autenticacion(autenticacion['id'], 'KO')

    def enviar_mensaje_autenticacion(self, taxi_id, estado):
        # Envía el estado de autenticación al taxi
        mensaje = {
            'taxi_id': taxi_id,
            'estado': estado
        }
        self.producer.send('respuesta_auth_taxi', json.dumps(mensaje).encode())

    def procesar_peticion_cliente(self, peticion):
        # Procesa cada petición de servicio de taxi de los clientes
        cliente_id = peticion.get('cliente_id')
        destino = peticion.get('destino')
        destino_coord = self.localizaciones.get(destino)
        if not destino_coord:
            print(f"[CENTRAL] Destino {destino} no encontrado.")
            self.enviar_mensaje_cliente(cliente_id, 'KO')
            return                                        
        print(f"[CENTRAL] Recibida petición de cliente {cliente_id} para destino {destino_coord}")
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
        # Enviar el destino al taxi a través del socket
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
                print(f"Taxi {taxi_id} asignado al cliente {cliente_id} para el destino {destino_coord}")
                return taxi_id
        print("No hay taxis disponibles.")
        return None

    
    # def enviar_taxi(self, taxi_id, cliente_id, destino):
    #     # Simula el movimiento progresivo del taxi hasta el destino
    #     taxi_info = self.taxis_autenticados.get(taxi_id)
    #     if taxi_info:
    #         pos_actual = taxi_info['posicion']
    #         destino_coord = destino  # Ya es una tupla de coordenadas
            
    #         print(f"Taxi {taxi_id} comenzando movimiento hacia {destino_coord}.")
    #         taxi_info['estado'] = 'RUN'
             
    #         # Movimiento paso a paso hacia el destino
    #         while pos_actual != destino_coord:
    #             pos_actual = self.calcular_siguiente_paso(pos_actual, destino_coord)
    #             taxi_info['posicion'] = pos_actual
    #             print(f"Taxi {taxi_id} moviéndose a {pos_actual}")
    #             self.dibujar_mapa()
    #             time.sleep(0.5)  # Simular movimiento en tiempo real
                
    #         print(f"Taxi {taxi_id} ha llegado a su destino.")
    #         taxi_info['estado'] = 'FREE'  # Liberar el taxi para que pueda ser asignado a otro cliente
    #         self.dibujar_mapa()
    
    def enviar_taxi(self, taxi_id, cliente_id, destino):
        # Enviar instrucciones al taxi
        taxi_info = self.taxis_autenticados.get(taxi_id)
        if taxi_info:
            print(f"Taxi {taxi_id} comenzando movimiento hacia {destino}.")
            taxi_info['estado'] = 'RUN'
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

    def procesar_mensajes_sensores(self):
        # Procesar mensajes recibidos de los sensores de los taxis
        for mensaje in self.consumer_sensores:
            datos_sensor = json.loads(mensaje.value.decode())
            taxi_id = datos_sensor.get('taxi_id')
            estado = datos_sensor.get('estado')
            
            if taxi_id in self.taxis_autenticados:
                if estado == 'KO':
                    print(f"Taxi {taxi_id} ha detectado una incidencia y se detiene.")
                    self.taxis_autenticados[taxi_id]['estado'] = 'stopped'
                    self.enviar_mensaje_taxi(taxi_id, 'STOP')
                elif estado == 'OK':
                    # Si el estado vuelve a OK, reanudar el servicio del taxi
                    print(f"Taxi {taxi_id} ha resuelto la incidencia y continúa su viaje.")
                    self.taxis_autenticados[taxi_id]['estado'] = 'BUSY'
                    self.enviar_mensaje_taxi(taxi_id, 'RESUME')
    
    def procesar_comandos(self):
        # Procesa comandos arbitrarios enviados a los taxis desde EC_Central
        for mensaje in self.consumer_comando:
            datos_comando = json.loads(mensaje.value.decode())
            taxi_id = datos_comando.get('taxi_id')
            comando = datos_comando.get('comando')
            
            if taxi_id in self.taxis_autenticados:
                print(f"Procesando comando '{comando}' para el taxi {taxi_id}")
                self.enviar_mensaje_taxi(taxi_id, comando)
                # Ejecutar acción local según el comando
                if comando == 'PARAR':
                    self.taxis_autenticados[taxi_id]['estado'] = 'stopped'
                elif comando == 'REANUDAR':
                    self.taxis_autenticados[taxi_id]['estado'] = 'BUSY'
                elif comando == 'VOLVER_BASE':
                    self.taxis_autenticados[taxi_id]['estado'] = 'returning'
                    self.enviar_taxi_a_base(taxi_id)
    
    def enviar_taxi_a_base(self, taxi_id):
        # Envía un comando para que el taxi regrese a la base
        mensaje = {
            'taxi_id': taxi_id,
            'destino': '1,1'  # La posición de la base es [1,1]
        }
        self.producer.send('ordenes_taxi', json.dumps(mensaje).encode())
        print(f"Enviado taxi {taxi_id} a la base.")
        
        
    def enviar_mensaje_taxi(self, taxi_id, comando):
        # Envía un comando específico a un taxi
        mensaje = {
            'taxi_id': taxi_id,
            'comando': comando
        }
        self.producer.send('comando_taxi', json.dumps(mensaje).encode())
        print(f"Enviado comando '{comando}' al taxi {taxi_id}")
    
    def actualizar_mapa_taxi(self, taxi_id, destino):
        # Actualiza la posición del taxi en el mapa y envía el estado del mapa
        taxi_info = self.taxis_autenticados.get(taxi_id)
        if taxi_info:
            pos_actual = taxi_info['posicion']
            destino_coord = destino.split(',')
            taxi_info['posicion'] = destino_coord  # Solo como ejemplo, debería calcularse el movimiento
            print(f"Taxi {taxi_id} moviéndose de {pos_actual} a {destino_coord}")
            self.enviar_mapa_actualizado()
            
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

        
    def enviar_mapa_actualizado(self):
        # Envía el estado completo del mapa a todos los taxis y clientes conectados
        mensaje_mapa = {
            'mapa': self.mapa
        }
        self.producer.send('mapa_estado', json.dumps(mensaje_mapa).encode())
        print("Mapa actualizado enviado a los taxis y clientes.")
        
    def escuchar_peticiones(self):
        # Escucha peticiones tanto de clientes como de taxis (autenticación)
        print("[CENTRAL] Esperando solicitudes de clientes...")
        
        while True:
            self.dibujar_mapa()  # Actualizar la representación gráfica
             
            for mensaje in self.consumer:
                peticion = json.loads(mensaje.value.decode())
                self.procesar_peticion_cliente(peticion)

            for mensaje in self.consumer_taxi:
                autenticacion = json.loads(mensaje.value.decode())
                self.procesar_autenticacion_taxi(autenticacion)
                


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

    parser.add_argument('puerto_escucha', type=int, default=2181, help='Puerto de escucha de EC_Central')
    parser.add_argument('broker_ip', type=str, default='localhost:9092', help='Ip del Broker')
    parser.add_argument('db_path', type=str, default='taxis_db.json', help='BD de los taxis')

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

    
    # Iniciar el hilo de actualización de pygame
    threading.Thread(target=ec_central.actualizar_pygame, daemon=True).start()
    threading.Thread(target=ec_central.procesar_comandos_arbitrarios, daemon=True).start()
    
    # Comenzar a escuchar peticiones de Kafka
    try:
        print("EC_Central está iniciando y escuchando peticiones...")
        ec_central.escuchar_peticiones()  # Este método nunca finaliza, ya que escucha constantemente
    except KeyboardInterrupt:
        print("EC_Central detenido manualmente.")
        pygame.quit()