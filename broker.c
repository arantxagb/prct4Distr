#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include "stub.h"

int main(int argc, char *argv[]) {
    int port = -1;  // Valor predeterminado para el puerto
    char *mode = NULL;  // Valor predeterminado para el modo

    // Definir las opciones de línea de comandos
    static struct option long_options[] = {
        {"port", required_argument, 0, 'p'},
        {"mode", optional_argument, 0, 'm'},
        {0, 0, 0, 0}
    };

    int option_index = 0;
    int c;

    while ((c = getopt_long(argc, argv, "p:m:", long_options, &option_index)) != -1) {
        switch (c) {
            case 'p':
                port = atoi(optarg);
                break;
            case 'm':
                mode = optarg;
                break;
            case '?':
                // Manejar opciones no reconocidas o argumentos incorrectos aquí
                break;
            default:
                abort();
        }
    }

    // Verificar si se proporcionó el puerto
    if (port == -1) {
        fprintf(stderr, "Error: Se debe proporcionar el puerto con --port.\n");
        return 1;
    }
    
    if (!mode){
        mode = "secuencial";
    }

    broker(port, mode);

    return 0;
}
