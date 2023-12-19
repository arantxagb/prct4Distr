#!/bin/bash

# Definir la ruta del archivo de salida
output_file="resultados.txt"

# Bucle para ejecutar el comando 500 veces
for ((i=1; i<=500; i++)); do
    # Ejecutar el comando y agregar la salida al archivo
    ./subscriber --ip 192.168.1.105 --port 8441 --topic \len  & >> "$output_file"
done

echo "Se han ejecutado 500 veces. Resultados almacenados en $output_file"
