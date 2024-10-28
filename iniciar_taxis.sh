#!/bin/bash

# Número de taxis que deseas iniciar
NUM_TAXIS=5
# Puerto base
BASE_PORT=8888

# Ciclo para abrir nuevas pestañas y ejecutar el comando
for (( i=1; i<=NUM_TAXIS; i++ ))
do
    # Calcular el puerto para este taxi
    PORT=$((BASE_PORT + i - 1))
    TAXI=$((0 + i))

    # Abrir una nueva pestaña de terminal y ejecutar el comando
    gnome-terminal --tab -- bash -c "python3 EC_DE.py localhost 2196 localhost $PORT $TAXI localhost 9092; exec bash"
done