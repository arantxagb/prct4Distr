import csv
import re
import statistics

# ruta del archivo de entrada y salida
input_file_path = 'resJust900.txt'
output_file_path = 'resultadosFinalesJust900.csv'

# valores de latencia
latencies = []

# Abrir el archivo de salida en modo escritura
with open(output_file_path, 'w', newline='') as csvfile:
    # Crear un objeto escritor CSV
    csv_writer = csv.writer(csvfile)

    # Escribir la primera línea de encabezado
    csv_writer.writerow(['Modo', 'N', 'min', 'max', 'avg', 'std'])

    # Establecer el modo y el valor de N
    modo = "justo"
    N = 900

    # Abrir el archivo de entrada en modo lectura
    with open(input_file_path, 'r') as file:
        # Iterar sobre cada línea en el archivo de entrada
        for line in file:
            # Buscar la cadena que contiene la información de latencia
            match = re.search(r'Latencia: (\d+\.\d+)', line)
            if match:
                latency = float(match.group(1))  # Obtener el valor de latencia
                latencies.append(latency)  # Agregar la latencia a la lista

    # Calcular valores de min, max, avg, std
    if latencies:
        min_val = min(latencies)
        max_val = max(latencies)
        avg = statistics.mean(latencies)
        std = statistics.stdev(latencies)
    else:
        # Si no hay datos de latencia, asignar 0 a los valores
        min_val = max_val = avg = std = 0

    # Escribir la línea en el archivo CSV
    csv_writer.writerow([modo, N, min_val, max_val, avg, std])

# Imprimir mensaje de éxito
print(f'Se ha generado el archivo CSV en: {output_file_path}')
