import socket
import threading
import time
import json

class EC_DE:
    def __init__(self, central_ip, central_puerto, taxi_id, token):
        self.central_ip = central_ip
        self.central_puerto = central_puerto
        self.taxi_id = taxi_id
        self.token = token
        self.posicion = (1, 1)
        self.sensor_status = 'OK'  # Estado inicial de los sensores
        self.stopped = False  # Indica si el taxi está detenido por contingencia
        self.stopped_by_command = False  # Indica si el taxi está detenido por un comando
        self.destino_actual = None
        self.autenticar()

        threading.Thread(target=self.iniciar_servidor_sensores, daemon=True).start()
    
    def autenticar(self):
        self.socket_central = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.socket_central.connect((self.central_ip, self.central_puerto))

        # Paso 1: Enviar ENQ
        self.socket_central.send('ENQ'.encode())
        respuesta = self.socket_central.recv(1024).decode()
        if respuesta == 'ACK':
            # Paso 2: Enviar solicitud de autenticación
            data = f'AUTH#{self.taxi_id}#{self.token}'
            lrc = self.calcular_lrc(data)
            mensaje = f'<STX>{data}<ETX><LRC>{lrc}'
            self.socket_central.send(mensaje.encode())
            respuesta = self.socket_central.recv(1024).decode()
            if respuesta == 'ACK':
                print("[EC_DE] Autenticación exitosa.")
                # Iniciar hilo para escuchar instrucciones
                threading.Thread(target=self.escuchar_instrucciones, daemon=True).start()
            else:
                print("[EC_DE] Autenticación fallida.")
                self.socket_central.close()
        else:
            print("[EC_DE] Error en la comunicación con EC_Central.")
            self.socket_central.close()


    def escuchar_instrucciones(self):
        try:
            while True:
                mensaje = self.socket_central.recv(1024).decode()
                if mensaje:
                    print(f"[EC_DE] Mensaje recibido: {mensaje}")
                    # Procesar mensaje
                    stx_index = mensaje.find('<STX>')
                    etx_index = mensaje.find('<ETX>')
                    lrc_index = mensaje.find('<LRC>')

                    if stx_index != -1 and etx_index != -1 and lrc_index != -1:
                        data = mensaje[stx_index+5:etx_index]
                        lrc = mensaje[lrc_index+5:]
                        # Verificar LRC
                        if self.verificar_lrc(data, lrc):
                            campos = data.split('#', 1)
                            if campos[0] == 'GO':
                                destino = tuple(map(int, campos[1].split('#')))
                                print(f"[EC_DE] Recibido destino: {destino}")
                                # Comenzar movimiento hacia el destino
                                self.mover_hacia_destino(destino)
                            elif campos[0] == 'MAP':
                                # Procesar el estado del mapa
                                map_data = json.loads(campos[1])
                                print(f"[EC_DE] Recibido mapa actualizado: {map_data}")
                                # Aquí puedes actualizar tu representación interna del mapa si es necesario
                            elif campos[0] == 'CMD':
                                comando = campos[1]
                                self.procesar_comando(comando)
                            else:
                                print("[EC_DE] Comando no reconocido.")
                        else:
                            print("[EC_DE] LRC incorrecto.")
                    else:
                        print("[EC_DE] Formato de mensaje incorrecto.")
                else:
                    break
        except Exception as e:
            print(f"[EC_DE] Conexión cerrada: {e}")
            self.socket_central.close()


    def procesar_comando(self, comando):
            if comando == 'STOP':
                self.stopped_by_command = True
                self.enviar_estado('STOPPED')
                print("[EC_DE] Comando 'STOP' recibido. Taxi detenido.")
            elif comando == 'RESUME':
                self.stopped_by_command = False
                self.enviar_estado('RESUME')
                print("[EC_DE] Comando 'RESUME' recibido. Taxi reanudando movimiento.")
            else:
                print(f"[EC_DE] Comando desconocido: {comando}")
    
    def iniciar_servidor_sensores(self):
        self.servidor_sensores = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.servidor_sensores.bind(('localhost', 8888))  # Puerto para comunicación con EC_S
        self.servidor_sensores.listen(1)
        print("[EC_DE] Esperando conexión de EC_S...")
        sensor_socket, direccion = self.servidor_sensores.accept()
        print(f"[EC_DE] Conexión de EC_S desde {direccion}")
        threading.Thread(target=self.recibir_datos_sensor, args=(sensor_socket,), daemon=True).start()

    def recibir_datos_sensor(self, sensor_socket):
        while True:
            mensaje = sensor_socket.recv(1024).decode()
            if mensaje:
                print(f"[EC_DE] Mensaje recibido de EC_S: {mensaje}")
                stx_index = mensaje.find('<STX>')
                etx_index = mensaje.find('<ETX>')
                lrc_index = mensaje.find('<LRC>')

                if stx_index != -1 and etx_index != -1 and lrc_index != -1:
                    data = mensaje[stx_index+5:etx_index]
                    lrc = mensaje[lrc_index+5:]
                    if self.verificar_lrc(data, lrc):
                        campos = data.split('#')
                        if campos[0] == 'SENSOR':
                            self.sensor_status = campos[1]
                            # Enviar ACK a EC_S
                            sensor_socket.send('ACK'.encode())
                            print(f"[EC_DE] Estado del sensor actualizado: {self.sensor_status}")
                        else:
                            sensor_socket.send('NACK'.encode())
                    else:
                        sensor_socket.send('NACK'.encode())
                else:
                    sensor_socket.send('NACK'.encode())
            else:
                break
    
    def calcular_lrc(self, data):
        lrc = 0
        for byte in data.encode():
            lrc ^= byte
        return str(lrc)
    
    def verificar_lrc(self, data, lrc):
        calculated_lrc = self.calcular_lrc(data)
        return calculated_lrc == lrc

    def mover_hacia_destino(self, destino):
        while self.posicion != destino:
            if self.stopped_by_command:
                print("[EC_DE] Taxi detenido por comando.")
                time.sleep(1)
                continue
            if self.sensor_status == 'OK':
                if self.stopped:
                    # Enviar estado RESUMED a EC_Central
                    self.enviar_estado('RESUMED')
                    self.stopped = False
                # Mover hacia el destino
                self.posicion = self.calcular_siguiente_paso(self.posicion, destino)
                print(f"[EC_DE] Moviéndome a {self.posicion}")
                # Notificar a EC_Central
                data = f'POS#{self.posicion[0]}#{self.posicion[1]}'
                lrc = self.calcular_lrc(data)
                mensaje = f'<STX>{data}<ETX><LRC>{lrc}'
                self.socket_central.send(mensaje.encode())
                # Esperar ACK
                respuesta = self.socket_central.recv(1024).decode()
                if respuesta != 'ACK':
                    print("[EC_DE] Error al enviar posición.")
                time.sleep(0.5)  # Simular movimiento en tiempo real
            else:
                if not self.stopped:
                    # Enviar estado STOPPED a EC_Central
                    self.enviar_estado('STOPPED')
                    self.stopped = True
                print("[EC_DE] Taxi detenido debido a una contingencia.")
                time.sleep(1)
        print("[EC_DE] Llegué al destino.")
        # Cambiar estado a 'END' y notificar a EC_Central
        self.state = 'END'
        self.enviar_estado('END')
        # Esperar nuevas instrucciones


    def enviar_estado(self, estado):
        data = f'STATUS#{estado}'
        lrc = self.calcular_lrc(data)
        mensaje = f'<STX>{data}<ETX><LRC>{lrc}'
        self.socket_central.send(mensaje.encode())
        # Esperar ACK
        respuesta = self.socket_central.recv(1024).decode()
        if respuesta != 'ACK':
            print("[EC_DE] Error al enviar estado.")
        else:
            print(f"[EC_DE] Estado '{estado}' enviado a EC_Central.")

    
    def calcular_siguiente_paso(self, posicion_actual, destino):
        x_actual, y_actual = posicion_actual
        x_dest, y_dest = destino

        dx = x_dest - x_actual
        dy = y_dest - y_actual

        # Normalizar los desplazamientos para moverse solo una coordenada
        if dx != 0:
            dx = dx // abs(dx)
        if dy != 0:
            dy = dy // abs(dy)

        x_actual += dx
        y_actual += dy

        return x_actual, y_actual

    

if __name__ == "__main__":
    central_ip = 'localhost'
    central_puerto = 2181  # Puerto donde EC_Central está escuchando
    taxi_id = '1'
    token = 'token1'  # Reemplaza con el token correcto

    ec_de = EC_DE(central_ip, central_puerto, taxi_id, token)