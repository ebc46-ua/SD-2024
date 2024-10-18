import json
import os
import socket
import time
import kafka
import sqlite3
from kafka import KafkaProducer, KafkaConsumer

class ECCentral:
    def __init__(self, puerto_escucha, broker_ip, db_path, map_path):
        self.puerto_escucha = puerto_escucha
        self.broker_ip = broker_ip
        self.db_path = db_path
        self.map_path = map_path
        self.mapa = [[' ' for _ in range(20)] for _ in range(20)]  # Mapa 20x20 vacío
        self.taxis_disponibles = {}
        self.clientes_activos = []
        self.taxis_autenticados = {}
        self.cargar_localizaciones()  # Carga las localizaciones desde el archivo JSON
        self.inicializar_kafka()
 

    def cargar_localizaciones(self):
        # Lee el archivo EC_locations.json y carga las localizaciones en el mapa
        try:
            with open('EC_locations.json', 'r') as archivo_localizaciones:
                configuracion_localizaciones = json.load(archivo_localizaciones)
                for localizacion in configuracion_localizaciones['locations']:
                    id_localizacion = localizacion['Id']
                    x, y = map(int, localizacion['POS'].split(','))
                    self.mapa[x][y] = id_localizacion
                print("Localizaciones cargadas correctamente.")
        except Exception as e:
            print(f"Error cargando las localizaciones: {e}")

    def inicializar_kafka(self):
        # Inicializa los productores y consumidores de Kafka para la comunicación con los taxis y clientes
        self.producer = KafkaProducer(bootstrap_servers=[self.broker_ip])
        self.consumer = KafkaConsumer('solicitudes', bootstrap_servers=[self.broker_ip], group_id='central')
        self.consumer_taxi = KafkaConsumer('auth_taxi', bootstrap_servers=[self.broker_ip], group_id='central')
        self.consumer_comando = KafkaConsumer('comando_taxi', bootstrap_servers=[self.broker_ip], group_id='central')
        
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
        # Autenticar taxi a través de la base de datos usando su ID y token
        try:
            conexion = sqlite3.connect(self.db_path)
            cursor = conexion.cursor()
            cursor.execute('SELECT alias FROM taxis WHERE id = ? AND token = ?', (datos_taxi['id'], datos_taxi['token']))
            resultado = cursor.fetchone()
            conexion.close()

            if resultado is not None:
                print(f"El taxi con id {datos_taxi['id']} ha accedido al sistema.")
                taxi_autenticado = {"id": datos_taxi['id'], "posicion": datos_taxi['posicion']}
                self.taxis_autenticados[datos_taxi['id']] = taxi_autenticado
                return True
            else:
                print(f"Autenticación fallida para el taxi con id {datos_taxi['id']}.")
                return False
        except sqlite3.Error as e:
            print(f"Error en la autenticación: {e}")
            return False

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
        print(f"Recibida petición de cliente {cliente_id} para destino {destino}")
        taxi_asignado = self.asignar_taxi(cliente_id, destino)
        if taxi_asignado:
            self.enviar_taxi(taxi_asignado, cliente_id, destino)
        else:
            print(f"No se ha podido asignar taxi a cliente {cliente_id}")

    def asignar_taxi(self, cliente_id, destino):
        # Lógica para asignar un taxi disponible a una solicitud, solo taxis autenticados
        for taxi in self.taxis_disponibles:
            if self.taxis_autenticados.get(taxi, False):
                self.taxis_disponibles.remove(taxi)
                print(f"Taxi {taxi} asignado al cliente {cliente_id} para el destino {destino}")
                return taxi
        print("No hay taxis disponibles o no autenticados.")
        return None

    # def enviar_taxi(self, taxi_id, cliente_id, destino):
    #     # Envía instrucciones al taxi a través de Kafka
    #     mensaje = {
    #         'taxi_id': taxi_id,
    #         'cliente_id': cliente_id,
    #         'destino': destino
    #     }
    #     self.producer.send('ordenes_taxi', json.dumps(mensaje).encode())
    #     print(f"Enviado taxi {taxi_id} al cliente {cliente_id} para destino {destino}")
    #     self.actualizar_mapa_taxi(taxi_id, destino)
    
    def enviar_taxi(self, taxi_id, cliente_id, destino):
        # Simula el movimiento progresivo del taxi hasta el destino
        taxi_info = self.taxis_autenticados.get(taxi_id)
        if taxi_info:
            pos_actual = taxi_info['posicion']
            destino_coord = list(map(int, destino.split(',')))
            
            print(f"Taxi {taxi_id} comenzando movimiento hacia {destino_coord}.")

            # Movimiento paso a paso hacia el destino
            while pos_actual != destino_coord:
                pos_actual = self.calcular_siguiente_paso(pos_actual, destino_coord)
                taxi_info['posicion'] = pos_actual
                print(f"Taxi {taxi_id} moviéndose a {pos_actual}")
                self.actualizar_mapa_taxi(taxi_id, pos_actual)
                time.sleep(1)  # Simular movimiento en tiempo real
            print(f"Taxi {taxi_id} ha llegado a su destino.")
            self.taxis_autenticados[taxi_id]['estado'] = 'free'
            self.enviar_mapa_actualizado()

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
                    self.taxis_autenticados[taxi_id]['estado'] = 'busy'
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
                    self.taxis_autenticados[taxi_id]['estado'] = 'busy'
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
        # Envía un comando arbitrario a un taxi
        mensaje = {
            'taxi_id': taxi_id,
            'comando': comando
        }
        self.producer.send('comando_taxi', json.dumps(mensaje).encode())
        print(f"Enviado comando '{comando}' al taxi {taxi_id}")
        
    def enviar_mapa_actualizado(self):
        # Envía el estado completo del mapa a todos los taxis y clientes conectados
        mensaje_mapa = {
            'mapa': self.mapa
        }
        self.producer.send('mapa_estado', json.dumps(mensaje_mapa).encode())
        print("Mapa actualizado enviado a los taxis y clientes.")
        
    def escuchar_peticiones(self):
        # Escucha peticiones tanto de clientes como de taxis (autenticación)
        while True:
            for mensaje in self.consumer:
                peticion = json.loads(mensaje.value.decode())
                self.procesar_peticion_cliente(peticion)

            for mensaje in self.consumer_taxi:
                autenticacion = json.loads(mensaje.value.decode())
                self.procesar_autenticacion_taxi(autenticacion)
    
    
    def conectar_bd(self):
        if os.path.exists(self.db_path):
            print(f"Base de datos '{self.db_path}' encontrada. Abriendo la base de datos.")
            try:
                conexion = sqlite3.connect(self.db_path)
                conexion.close()
                print(f"Base de datos '{self.db_path}' abierta correctamente.")
                return True
            except sqlite3.Error as e:
                print(f"Error al abrir la base de datos: {e}")
                return False
        else:
            print(f"La base de datos '{self.db_path}' no existe.")
            return False


if __name__ == "__main__":
    puerto_escucha = 5000  # EJEMPLO
    broker_ip = "localhost:9092"  # HABRA QUE CAMBIARLO
    db_path = "taxis.db"  # Ruta a la base de datos SQLite
    map_path = "EC_locations.json"  # Aquí habria que leerlo para evitarp roblemas

    # Instanciar la central
    ec_central = ECCentral(puerto_escucha, broker_ip, db_path, map_path)
    
    # Conectar a la base de datos y cargar taxis
    if not ec_central.conectar_bd():
        print("No se pudo conectar a la base de datos. Finalizando.")
        exit(1)

    # Cargar taxis y localizaciones desde archivos y base de datos
    ec_central.cargar_localizaciones()
    ec_central.cargar_taxis_desde_bd()

    # Comenzar a escuchar peticiones de Kafka
    try:
        print("EC_Central está iniciando y escuchando peticiones...")
        ec_central.escuchar_peticiones()  # Este método nunca finaliza, ya que escucha constantemente
    except KeyboardInterrupt:
        print("EC_Central detenido manualmente.")
