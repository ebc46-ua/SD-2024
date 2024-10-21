import socket
import threading
import time
import random

class EC_S:
    def __init__(self, de_ip, de_port):
        self.de_ip = de_ip
        self.de_port = de_port
        self.connect_to_ec_de()

    def connect_to_ec_de(self):
        self.socket_de = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.socket_de.connect((self.de_ip, self.de_port))
            print("[EC_S] Conectado al EC_DE.")
            # Iniciar el envío de datos de sensores
            threading.Thread(target=self.send_sensor_data, daemon=True).start()
        except ConnectionRefusedError:
            print(f"[EC_S] No se pudo conectar a EC_DE en {self.de_ip}:{self.de_port}. Asegúrate de que EC_DE esté en ejecución.")
            exit(1)
        except Exception as e:
            print(f"[EC_S] Ocurrió un error al intentar conectarse: {e}")
            exit(1)

    def calcular_lrc(self, data):
        lrc = 0
        for byte in data.encode():
            lrc ^= byte
        return str(lrc)

    def send_sensor_data(self):
        while True:
            time.sleep(1)
            # Simular sensor con 20% de probabilidad de contingencia NO SIMULAR, TIENE QUE SER USANDO EL TECLADO LA CONTINGENCIA
            if random.randint(1, 5) == 1:
                sensor_status = 'CONTINGENCY'
            else:
                sensor_status = 'OK'

            data = f'SENSOR#{sensor_status}'
            lrc = self.calcular_lrc(data)
            mensaje = f'<STX>{data}<ETX><LRC>{lrc}'
            self.socket_de.send(mensaje.encode())
            print(f"[EC_S] Enviado estado del sensor: {sensor_status}")
            # Esperar ACK
            respuesta = self.socket_de.recv(1024).decode()
            if respuesta != 'ACK':
                print("[EC_S] Error al enviar estado del sensor.")
            else:
                print("[EC_S] ACK recibido del EC_DE.")

if __name__ == "__main__":
    ec_de_ip = 'localhost'  # IP donde est� ejecut�ndose EC_DE
    ec_de_port = 8888       # Puerto para la comunicaci�n entre EC_S y EC_DE

    ec_s = EC_S(ec_de_ip, ec_de_port)
    # Mantener el programa en ejecuci�n
    while True:
        time.sleep(1)
