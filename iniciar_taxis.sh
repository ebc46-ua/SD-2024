#!/bin/bash

# Número de taxis que deseas iniciar
NUM_TAXIS=3
# Puerto base
BASE_PORT=8888
# Puerto escucha Central
PUERTO_CENTRAL='2195'
IP_CENTRAL='localhost'

IP_PROPIA='localhost'
# Ciclo para abrir nuevas pestañas y ejecutar el comando
for (( i=1; i<=NUM_TAXIS; i++ ))
do
    # Calcular el puerto para este taxi
    PORT_S=$((BASE_PORT + i - 1))
    TAXI=$((0 + i))

    # Abrir una nueva pestaña de terminal y ejecutar el comando
    gnome-terminal --tab -- bash -c "python3 EC_DE.py $IP_CENTRAL $PUERTO_CENTRAL $IP_PROPIA $PORT_S $TAXI $IP_CENTRAL 9092; exec bash"
    gnome-terminal --tab -- bash -c "python3 EC_S.py $IP_PROPIA $PORT_S; exec bash"
done
