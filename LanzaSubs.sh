#!/bin/bash

for ((i=1; i<=900; i++))
do
    ./subscriber --ip 212.128.254.31 --port 8441 --topic \len &
    sleep 0.00001  # Espera 0.2 segundos entre cada lanzamiento
done

# Espera a que todos los procesos hijos terminen antes de salir
wait
