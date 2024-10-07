import socket
import json
import threading
import time
HOST = '127.0.0.1'
PORT = 65432

class Central:
    def __init__(self):
        self.taxis = self.cargar_data_taxi('taxi_db.json')
        self.locations = self.load_map('mapa.txt')
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.bind((HOST, PORT))
        self.server_socket.listen(5)
        print(f'Central started at {HOST}:{PORT}')
        self.client_sockets = []

    def load_map(self, file_path):
        locations = {}
        with open(file_path, 'r') as f:
            for line in f:
                parts = line.strip().split()
                loc_id, x, y = parts[0], float(parts[1]), float(parts[2])
                locations[loc_id] = (x, y)
        return locations

    def cargar_data_taxi(self, file_path):
        with open(file_path, 'r') as f:
            return json.load(f)

    def handle_client(self, client_socket):
        self.client_sockets.append(client_socket)
        while True:
            request = client_socket.recv(1024).decode()
            if not request:
                break
            print(f'Peticion recibida: {request}')
            response = self.procesar_peticion(request)
            client_socket.send(response.encode())
            self.broadcast_status()
        client_socket.close()
        self.client_sockets.remove(client_socket)

    def procesar_peticion(self, destination):
        # Asignar taxi disponible
        available_taxi = next((taxi for taxi in self.taxis if taxi['status'] == 'available'), None)
        
        if available_taxi:
            available_taxi['status'] = 'busy'
            return f'OK - Taxi {available_taxi["id"]} en camino a {destination}'
        else:
            return 'KO - No hay taxis disponibles'

    def broadcast_status(self):
        status_update = json.dumps(self.taxis)
        for client in self.client_sockets:
            client.send(status_update.encode())

    def run(self):
        while True:
            client_socket, addr = self.server_socket.accept()
            print(f'Conectado a {addr}')
            threading.Thread(target=self.handle_client, args=(client_socket,)).start()

if __name__ == "__main__":
    central = Central()
    central.run()