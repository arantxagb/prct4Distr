import csv

archivos_csv = ["resultadosFinalesJust50.csv", "resultadosFinalesJust500.csv", "resultadosFinalesJust900.csv",
                "resultadosFinalesPar50.csv", "resultadosFinalesPar500.csv", "resultadosFinalesPar900.csv",
                "resultadosFinalesSec50.csv", "resultadosFinalesSec500.csv", "resultadosFinalesSec900.csv"]

# Lista para almacenar las líneas de la segunda línea de cada archivo
lineas = []

for archivo in archivos_csv:
    with open(archivo, 'r') as csv_file:
        reader = csv.reader(csv_file)
        next(reader)  # Saltar la primera línea
        segunda_linea = next(reader, None)  # Obtener la segunda línea o None si no hay más líneas
        if segunda_linea:
            lineas.append(segunda_linea)

# Crear un nuevo archivo CSV con las líneas combinadas
with open("results_latency.csv", 'w', newline='') as csv_file:
    writer = csv.writer(csv_file)

    # Escribir la cabecera
    writer.writerow(["Modo", "N", "min", "max", "avg", "std"])

    # Escribir los valores de la segunda línea de cada archivo en una fila separada
    for linea in lineas:
        writer.writerow(linea)
