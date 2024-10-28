#!/bin/bash

# Número de clientes que quieres iniciar
NUM_CLIENTES=4  # Puedes cambiar este número según tus necesidades

# Puerto del servidor
SERVER="localhost"

# Contador de caracteres, comenzando desde 'a'
ascii_code=97  # ASCII de 'a'

# Iniciar los clientes
for ((i=0; i<NUM_CLIENTES; i++)); do

    # Convertir el código ASCII a carácter y calcular el ID de cliente
    CARACTER=$(printf "\\$(printf '%03o' $ascii_code)")
    CLIENT_ID=$((ascii_code - 96))  # Convertir 'a' en 1, 'b' en 2, etc.

    # Nombre del archivo de solicitud basado en el ID de cliente
    REQUEST_FILE="EC_Requests_${CLIENT_ID}.json"

    # Iniciar el cliente en una nueva pestaña de terminal con el archivo correspondiente
    gnome-terminal --tab -- bash -c "python3 EC_Customer.py $SERVER:9092 $CARACTER $REQUEST_FILE; exec bash"

    # Incrementar el código ASCII para el siguiente cliente
    ascii_code=$((ascii_code + 1))
done

# Esperar a que todos los procesos terminen
wait
