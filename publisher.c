#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include "stub.h"

int main(int argc, char *argv[]) {
    char *broker_ip = NULL;
    int broker_port = -1;
    char *topic = NULL;

    static struct option long_options[] = {
        {"ip", required_argument, 0, 'i'},
        {"port", required_argument, 0, 'p'},
        {"topic", required_argument, 0, 't'},
        {0, 0, 0, 0}
    };

    int option_index = 0;
    int c;

    while ((c = getopt_long(argc, argv, "i:p:t:", long_options, &option_index)) != -1) {
        switch (c) {
            case 'i':
                broker_ip = optarg;
                break;
            case 'p':
                broker_port = atoi(optarg);
                break;
            case 't':
                topic = optarg;
                break;
            case '?':
                // Manejar opciones no reconocidas o argumentos incorrectos aquí
                break;
            default:
                abort();
        }
    }

    // Verificar si se proporcionaron todos los parámetros requeridos
    if (broker_ip == NULL || broker_port == -1 || topic == NULL) {
        fprintf(stderr, "Error: Se deben proporcionar --ip, --port y --topic.\n");
        return 1;
    }

    publisher(broker_ip, broker_port, topic);

    return 0;
}
