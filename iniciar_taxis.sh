#!/bin/bash

# Número de taxis que deseas iniciar
NUM_TAXIS=5
# Puerto base
BASE_PORT=8888
# Servidor
SERVER='localhost'
# Puerto escucha Central
CENTRAL='2196'
# Ciclo para abrir nuevas pestañas y ejecutar el comando
for (( i=1; i<=NUM_TAXIS; i++ ))
do
    # Calcular el puerto para este taxi
    PORT=$((BASE_PORT + i - 1))
    TAXI=$((0 + i))

    # Abrir una nueva pestaña de terminal y ejecutar el comando
    gnome-terminal --tab -- bash -c "python3 EC_DE.py $SERVER $CENTRAL $SERVER $PORT $TAXI $SERVER 9092; exec bash"
    gnome-terminal --tab -- bash -c "python3 EC_S.py $SERVER $PORT; exec bash"
done