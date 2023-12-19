#include <time.h>

enum operations {
    REGISTER_PUBLISHER = 0,
    UNREGISTER_PUBLISHER,
    REGISTER_SUBSCRIBER,
    UNREGISTER_SUBSCRIBER,
    PUBLISH_DATA
};

struct publish {
    struct timespec time_generated_data;
    char data[100];
};

struct message {
    enum operations action;
    char topic[100];
    // Solo utilizado en mensajes de UNREGISTER
    int id;
    // Solo utilizado en mensajes PUBLISH_DATA
    struct publish data;
};

enum status {
    ERROR = 0,
    LIMIT,
    OK
};

struct response {
    enum status response_status;
    int id;
};

struct broker_info {
    char *ip;
    int port;
    int fd;
    int position;
    char *mode;
};

struct topic_info {
    int id;
    char topic[100];
    // int *publishers;
    // int *subscribers;
    int publishers[100];
    int subscribers[900];
    int num_publishers;
    int num_subscribers;
};

struct send_info {
    int fd;
    struct message *msg;
};

void broker(int port, char* mode);
void publisher(char *broker_ip, int broker_port, char *topic);
void subscriber(char *broker_ip, int broker_port, char *topic);
