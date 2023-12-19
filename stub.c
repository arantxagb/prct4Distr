#include "stub.h"
#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include <stdbool.h>
#include <pthread.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <semaphore.h>
#include <time.h>
#include <arpa/inet.h>

#define MAX_CLIENTS 1000
#define MAX_PUBLISHERS 100
#define MAX_SUBSCRIBERS 900
#define MAX_TOPICS 10

bool end = false; // Variable to end the broker
int opt = 1; // Variable to set the socket options
int ocupation[MAX_CLIENTS] = {0};
int ocupation_topics[MAX_TOPICS] = {0};
int ocupation_subscribers[MAX_SUBSCRIBERS] = {0};
int ocupation_publishers[MAX_PUBLISHERS] = {0};
int publishers[MAX_PUBLISHERS] = {0};
int subscribers[MAX_SUBSCRIBERS] = {0};
char topics[MAX_TOPICS][50] = {0};
struct topic_info* topic_info;
int num_justo_subs = 0;

pthread_mutex_t ocupation_topics_mutex;
pthread_mutex_t ocupation_subscribers_mutex;
pthread_mutex_t ocupation_publishers_mutex;
pthread_mutex_t ocupation_mutex;
pthread_mutex_t register_topic_mutex;
pthread_mutex_t cond_justo_mutex;
pthread_mutex_t mutex_justo;

pthread_cond_t cond_justo;

void sigint_handler(int sig){ // Signal handler to end the broker
    end = true;
}

int position_free(int *array, int size){ // Returns the first free position in the array
    for(int i = 0; i < size; i++){
        if(array[i] == 0){
            return i;
        }
    }
    return -1;
}

void reset_list(char *flag, int index) {

    int j = 0; 

    if (strcmp(flag, "publisher") == 0){
        int temp[MAX_PUBLISHERS];

        for (int i = 0; i < MAX_PUBLISHERS; i++) {
            if (topic_info[index].subscribers[i] != 0) {
                temp[j] = topic_info[index].subscribers[i];
                j++;
            }
        }

        for (int i = 0; i < j; i++) {
            topic_info[index].subscribers[j]= temp[i];
        }

        for (; j < MAX_PUBLISHERS; j++) {
            topic_info[index].subscribers[j] = 0;
        }

    } else if(strcmp(flag, "subscriber") == 0){
        int temp[MAX_SUBSCRIBERS];

        for (int i = 0; i < MAX_SUBSCRIBERS; i++) {
            if (topic_info[index].subscribers[i] != 0) {
                temp[j] = topic_info[index].subscribers[i];
                j++;
            }
        }

        for (int i = 0; i < j; i++) {
            topic_info[index].subscribers[i] = temp[i];
        }

        for (; j < MAX_SUBSCRIBERS; j++) {
            topic_info[index].subscribers[j] = 0;
        }
    }
}

void error_message(int fd, int position, int error){ // Function to send an error message
    struct response response; // Create the struct to store the response
    response.response_status = 1; // Set the response status as LIMIT
    response.id = -1; // Set the id as -1
    response.response_status = error; // Set the response status as LIMIT
    if(send(fd, &response, sizeof(response), 0) < 0){ // Send the response
        fprintf(stderr, "Error sending the response \n");
        exit(EXIT_FAILURE);
    }
    close(fd); // Close the socket
    pthread_mutex_lock(&ocupation_mutex);
    ocupation[position] = 0; // Set the position as free
    pthread_mutex_unlock(&ocupation_mutex);
    pthread_exit(NULL); // Exit the thread
}

void register_topic(int position, int fd, struct message *msg, char *flag){ // Function to check if the topic is already registered
    pthread_mutex_lock(&register_topic_mutex);
    int registered = 1;
    
    for(int i = 0; i < MAX_TOPICS; i++){ // Check if the topic is already registered
        if(strcmp(topic_info[i].topic, msg->topic) == 0){
            registered = 0;

            if (strcmp(flag, "publisher") == 0){
                topic_info[i].num_publishers++;
                topic_info[i].publishers[topic_info[i].num_publishers - 1] = fd;
            } else if(strcmp(flag, "subscriber") == 0){
                topic_info[i].num_subscribers++;
                topic_info[i].subscribers[topic_info[i].num_subscribers - 1] = fd;
            } else{
                pthread_mutex_unlock(&register_topic_mutex);
                error_message(fd, 0, 0);
            }
        } 
    }
    if (registered == 1){
        pthread_mutex_lock(&ocupation_topics_mutex);
        int id_topic = position_free(ocupation_topics, MAX_TOPICS); // Get the first free position in the array
        pthread_mutex_unlock(&ocupation_topics_mutex);
        if (id_topic == -1) { // Check if there is space for the topic
            pthread_mutex_unlock(&register_topic_mutex);
            error_message(fd, position, 1);
        } else {
            pthread_mutex_lock(&ocupation_topics_mutex);
            ocupation_topics[id_topic] = 1; // Set the position as occupied
            pthread_mutex_unlock(&ocupation_topics_mutex);
           
            if (strcmp(flag, "publisher") == 0){
                topic_info[id_topic].num_publishers = 1;
                topic_info[id_topic].num_subscribers = 0;
                strcpy(topic_info[id_topic].topic, msg->topic); // Store the topic in the array                
                topic_info[id_topic].publishers[0] = fd;
            
            } else if(strcmp(flag, "subscriber") == 0){
                topic_info[id_topic].num_publishers = 0;
                topic_info[id_topic].num_subscribers++;
                strcpy(topic_info[id_topic].topic, msg->topic); // Store the topic in the array
                topic_info[id_topic].subscribers[0] = fd; 
            
            } else{
                pthread_mutex_unlock(&register_topic_mutex);
                error_message(fd, 0, 0);
            }
        }
    }

    pthread_mutex_unlock(&register_topic_mutex);
}

void existing_topic(int i){
    if(topic_info[i].num_publishers <= 0 && topic_info[i].num_subscribers <= 0){
        topic_info[i].id = -1;
        strcpy(topic_info[i].topic, "");
        for (int j = 0; j < MAX_PUBLISHERS; j++){
            topic_info[i].publishers[j] = 0;
        }
        for (int j = 0; j < MAX_SUBSCRIBERS; j++){
            topic_info[i].subscribers[j] = 0;
        }
        topic_info[i].num_publishers = 0;
        topic_info[i].num_subscribers = 0;
        pthread_mutex_lock(&ocupation_topics_mutex);
        ocupation_topics[i] = 0; // Set the position as free
        pthread_mutex_unlock(&ocupation_topics_mutex);
    } 
}

void desregister(int fd, struct message *msg, char *flag){ // Function to desregister a topic
    for(int i = 0; i < MAX_TOPICS; i++){
        if(strcmp(topic_info[i].topic, msg->topic) == 0){

            pthread_mutex_lock(&register_topic_mutex);
            if(strcmp(flag, "publisher") == 0){
                for (int j = 0; j < topic_info[i].num_publishers; j++){
                    if(topic_info[i].publishers[j] == fd){
                        topic_info[i].publishers[j] = 0;
                        topic_info[i].num_publishers--;
                        reset_list("publisher", i);
                        existing_topic(i);
                        pthread_mutex_lock(&ocupation_publishers_mutex);
                        ocupation_publishers[j] = 0; // Set the position as free
                        pthread_mutex_unlock(&ocupation_publishers_mutex);
                        break;
                    }
                }
            } else if(strcmp(flag, "subscriber") == 0){
                for (int j = 0; j < topic_info[i].num_subscribers; j++){
                    if(topic_info[i].subscribers[j] == fd){
                        topic_info[i].subscribers[j] = 0;
                        topic_info[i].num_subscribers--;
                        reset_list("subscriber", i);
                        existing_topic(i);
                        pthread_mutex_lock(&ocupation_publishers_mutex);
                        ocupation_subscribers[j] = 0; // Set the position as free
                        pthread_mutex_unlock(&ocupation_publishers_mutex);
                        break;
                    }
                }
            } else{
                pthread_mutex_unlock(&register_topic_mutex);
                error_message(fd, 0, 0);
            }
            pthread_mutex_unlock(&register_topic_mutex);
        }
    }

}

void secuencial(int fd, struct message *msg){ // Function to send the data to the subscribers in secuencial mode
    for(int i = 0; i < MAX_TOPICS; i++){
        if(strcmp(topic_info[i].topic, msg->topic) == 0){
            pthread_mutex_lock(&register_topic_mutex); // NO PUEDE SER QUE PARA CADA TOPIC SE BLOQUEE EL MISMO MUTEX
            for(int j = 0; j < topic_info[i].num_subscribers; j++){
                //printf("data: %s \n", msg->data.data);
                if (send(topic_info[i].subscribers[j], &msg->data, sizeof(msg->data), 0) < 0){ // Send the message
                    fprintf(stderr, "Error sending the message \n");
                    exit(EXIT_FAILURE);
                }
                usleep(10);
            }
            pthread_mutex_unlock(&register_topic_mutex);
        }
    }
}

void *thread_function_paralelo(void *arg){ // Function to handle the paralelo threads
    struct send_info *msg = (struct send_info *)arg;
    if(send(msg->fd, msg->msg, sizeof(msg), 0) < 0){ // Send the message
        fprintf(stderr, "Error sending the message \n");
        exit(EXIT_FAILURE);
    }
    pthread_exit(NULL); // Exit the thread
}

void paralelo(int fd, struct message *msg){ // Function to send the data to the subscribers in paralelo mode
    for(int i = 0; i < MAX_TOPICS; i++){
        if(strcmp(topic_info[i].topic, msg->topic) == 0){
            pthread_t threads[topic_info[i].num_subscribers];
            for (int j = 0; j < topic_info[i].num_subscribers; j++){
                struct send_info msg_2_send; // Create the struct to store the message
                msg_2_send.fd = topic_info[i].subscribers[j];
                msg_2_send.msg->data= msg->data;
                pthread_create(&threads[j], NULL, thread_function_paralelo, &msg_2_send); // Create the thread
            }
        }
    }
}

void *thread_function_justo(void *arg){ // Function to handle the justo threads
    
    struct send_info *msg = (struct send_info *)arg;

    pthread_mutex_lock(&cond_justo_mutex);
    num_justo_subs++;
    if (num_justo_subs == topic_info->num_subscribers) {
        pthread_cond_broadcast(&cond_justo);
    }
    pthread_mutex_unlock(&cond_justo_mutex);
    
    pthread_mutex_lock(&mutex_justo);
    while (num_justo_subs < topic_info->num_subscribers) {
        pthread_cond_wait(&cond_justo, &mutex_justo);
    }

    if(send(msg->fd, msg->msg, sizeof(msg), 0) < 0){ // Send the message
        fprintf(stderr, "Error sending the message \n");
        exit(EXIT_FAILURE);
    }
    pthread_mutex_unlock(&mutex_justo);
    pthread_exit(NULL); // Exit the thread
}

void justo(int fd, struct message *msg){ // Function to send the data to the subscribers in justo mode
    for(int i = 0; i < MAX_TOPICS; i++){
        if(strcmp(topic_info[i].topic, msg->topic) == 0){
            for (int j = 0; j < topic_info[i].num_subscribers; j++){
                pthread_t threads[topic_info[i].num_subscribers];
                struct send_info msg_2_send; // Create the struct to store the message
                msg_2_send.fd = topic_info[i].subscribers[j];
                msg_2_send.msg->data = msg->data;
                pthread_create(&threads[j], NULL, thread_function_justo, &msg_2_send); // Create the thread
            }
        }
    }
}

void resume(){
    int num_topics = 0;
    printf("Resumen: \n");
    for (int i = 0; i < MAX_TOPICS; i++){
        if (strcmp(topic_info[i].topic, "") != 0){
            printf("%s: %d Suscriptores - %d Publicadores \n", topic_info[i].topic, topic_info[i].num_subscribers, topic_info[i].num_publishers);
            num_topics++;
        }
    }
    if (num_topics == 0){
        printf("No hay topics registrados \n");
    }
}

void *thread_function_broker(void *arg){ // Function to handle the broker threads
    struct broker_info *info = (struct broker_info *)arg;
    int position = info->position;
    struct message msg; // Create the struct to store the message
    struct timespec time;
    
    if(recv(info->fd, &msg, sizeof(msg), 0) < 0){ // Receive the message
        fprintf(stderr, "Error receiving the message \n");
        exit(EXIT_FAILURE);
    }

    int id;
    if(msg.action == 0){ // REGISTER_PUBLISHER
        
        pthread_mutex_lock(&ocupation_publishers_mutex);
        id = position_free(ocupation_publishers, MAX_PUBLISHERS); // Get the first free position in the array
        pthread_mutex_unlock(&ocupation_publishers_mutex);
        if(id == -1){ // Check if there is space for the publisher
            error_message(info->fd, position, 1);
        }
        else{
            struct response response; // Create the struct to store the response
            response.response_status = 2; // Set the response status as OK
            response.id = id; // Set the id as the first free position
            if(send(info->fd, &response, sizeof(response), 0) < 0){ // Send the response
                fprintf(stderr, "Error sending the response \n");
                exit(EXIT_FAILURE);
            }
            register_topic(position, info->fd, &msg, "publisher");
            pthread_mutex_lock(&ocupation_publishers_mutex);
            ocupation_publishers[id] = 1; // Set the position as occupied
            pthread_mutex_unlock(&ocupation_publishers_mutex);
            publishers[id] = info->fd; // Store the socket in the array
        }
        clock_gettime(CLOCK_REALTIME, &time);
        printf("[%ld.%ld] Nuevo cliente (%d) Publicador conectado \n", time.tv_sec, time.tv_nsec, id);
        resume();
    
    } else if(msg.action == 2){
        pthread_mutex_lock(&ocupation_subscribers_mutex);
        id = position_free(ocupation_subscribers, MAX_SUBSCRIBERS); // Get the first free position in the array
        pthread_mutex_unlock(&ocupation_subscribers_mutex);

        if(id == -1){ // Check if there is space for the subscriber
            error_message(info->fd, position, 1);
        }
        else{
            register_topic(position, info->fd, &msg, "subscriber");
            struct response response; // Create the struct to store the response
            response.response_status = 2; // Set the response status as OK
            response.id = id + 1; // Set the id as the first free position
            if(send(info->fd, &response, sizeof(response), 0) < 0){ // Send the response
                fprintf(stderr, "Error sending the response \n");
                exit(EXIT_FAILURE);
            }
            pthread_mutex_lock(&ocupation_subscribers_mutex);
            ocupation_subscribers[id] = 1; // Set the position as occupied
            pthread_mutex_unlock(&ocupation_subscribers_mutex);
            subscribers[id] = info->fd; // Store the socket in the array
        }
        clock_gettime(CLOCK_REALTIME, &time);
        printf("[%ld.%ld] Nuevo cliente (%d) Suscriptor conectado \n", time.tv_sec, time.tv_nsec, id + 1);
        resume();
    
    } else{
        error_message(info->fd, position, 0);
    }

    int end_conection = 0;

    while(!end_conection){

        if(recv(info->fd, &msg, sizeof(msg), 0) < 0){ // Receive the message
            fprintf(stderr, "Error receiving the message \n");
            exit(EXIT_FAILURE);
        }

        if(msg.action == 4){ // PUBLISH_DATA
            clock_gettime(CLOCK_REALTIME, &time);
            printf("[%ld.%ld] Recibido mensaje topic: %s - mensaje: %s - Generó: [%ld.%ld]\n", time.tv_sec, time.tv_nsec, msg.topic, msg.data.data, msg.data.time_generated_data.tv_sec, msg.data.time_generated_data.tv_nsec);
            for (int i = 0; i < MAX_TOPICS; i++){
                if(strcmp(topic_info[i].topic, msg.topic) == 0){
                    struct timespec time;
                    printf("[%ld.%ld] Enviando mensaje topic: %s - mensaje: %s - Generó: [%ld.%ld]\n", time.tv_sec, time.tv_nsec, msg.topic, msg.data.data, msg.data.time_generated_data.tv_sec, msg.data.time_generated_data.tv_nsec);
                    if(strcmp(info->mode, "secuencial") == 0){
                        secuencial(info->fd, &msg);
                    } else if(strcmp(info->mode, "justo") == 0){
                        justo(info->fd, &msg);
                    } else if(strcmp(info->mode, "paralelo") == 0){
                        paralelo(info->fd, &msg);
                    } else{
                        error_message(info->fd, position, 0);
                    }
                }
            }
        } else if(msg.action == 1){
            desregister(info->fd, &msg, "publisher");
            clock_gettime(CLOCK_REALTIME, &time);
            printf("[%ld.%ld] Eliminado cliente (%d) Publicador \n", time.tv_sec, time.tv_nsec, msg.id);
            resume();
            end_conection = 1;

        } else if(msg.action == 3){
            desregister(info->fd, &msg, "subscriber");
            clock_gettime(CLOCK_REALTIME, &time);
            printf("[%ld.%ld] Eliminado cliente (%d) Suscriptor \n", time.tv_sec, time.tv_nsec, msg.id +1);
            resume();
            end_conection = 1;

        } else{
            error_message(info->fd, position, 0);
        }
    }
    close(info->fd); // Close the socket
    
}

void broker(int port, char *mode){  // Initializes the connection with the N clients
   
    signal(SIGINT, sigint_handler); // Signal to end the broker
    srand(time(NULL)); // Random seed
    setbuf(stdout, NULL); // Disable buffering
    
    // Initialize the broker
    pthread_t threads[MAX_CLIENTS];
    int broker_fd;
    struct sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(port);

    if ((broker_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) { // Create the socket
        fprintf(stderr, "Socket creation error \n");
        exit(EXIT_FAILURE);
    }

	if (setsockopt(broker_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt)) < 0) { // Set the socket options
		fprintf(stderr, "setsockopt failed\n");
		exit(EXIT_FAILURE);
	}

    if (bind(broker_fd, (const struct sockaddr *) &serv_addr, sizeof(serv_addr)) < 0) { // Bind the socket  
        fprintf(stderr, "Bind Failed \n");
        exit(EXIT_FAILURE);
    }

    if (listen(broker_fd, MAX_CLIENTS) < 0) { // Listen the socket
        fprintf(stderr, "Listen Failed \n");
        exit(EXIT_FAILURE);
    }
    
    struct sockaddr_in client_addr;
    socklen_t client_addr_size = sizeof(client_addr);

    topic_info = (struct topic_info*)malloc(sizeof(struct topic_info) * 10);

    while(!end){
        
        struct broker_info *info = (struct broker_info *)malloc(sizeof(struct broker_info)); // Create the struct to pass the info to the thread
        
        info->fd = 0; 
        info->mode = mode;
        
        // Accept the connection
        if ((info->fd = accept(broker_fd, (struct sockaddr*)&client_addr, &client_addr_size)) < 0) {
            fprintf(stderr, "Accept Failed \n");
            free(info); // Free the struct before exiting
            exit(EXIT_FAILURE);
        }

        info->position = position_free(ocupation, MAX_CLIENTS); // Get the first free position in the array
        ocupation[info->position] = 1; // Set the position as occupied
        threads[info->position] = 0; // Set the thread as not created

        pthread_create(&threads[info->position], NULL, thread_function_broker, info); // Create the thread
    }

    free(topic_info); // Free the struct
}

void publisher(char *broker_ip, int broker_port, char *topic){ // Initializes the connection with the broker
    struct sockaddr_in serv_addr;
    struct in_addr ipv4Addr;
    int status;
    int fd;
    struct timespec time;

    setbuf(stdout, NULL); // Disable buffering
    signal(SIGINT, sigint_handler); // Signal to end the publisher

    if(inet_pton(AF_INET, broker_ip, &ipv4Addr) < 0){ // Check the ip
        printf("Invalid address/ Address not supported\n");
        exit(EXIT_FAILURE);
    }

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = ipv4Addr.s_addr;
    serv_addr.sin_port = htons(broker_port);

    if ((fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) { // Create the socket
		fprintf(stderr, "Socket creation error \n");
		exit(EXIT_FAILURE);
	}

	if ((status = connect(fd, (const struct sockaddr*)&serv_addr, sizeof(serv_addr))) < 0) { // Connect the socket
		fprintf(stderr, "Connection Failed \n");
		exit(EXIT_FAILURE);
	}

    clock_gettime(CLOCK_REALTIME, &time);
    printf("[%ld.%ld] Conectado al broker correctamente.\n", time.tv_sec, time.tv_nsec);
    
    struct message msg;
    msg.action = 0; // Set the action as REGISTER_PUBLISHER
    strcpy(msg.topic, topic); // Copy the topic to the struct

    if(send(fd, &msg, sizeof(msg), 0) < 0){ // Send the message
        fprintf(stderr, "Error sending the message \n");
        exit(EXIT_FAILURE);
    }

    struct response response; // Create the struct to store the response
    if(recv(fd, &response, sizeof(response), 0) < 0){ // Receive the response
        fprintf(stderr, "Error receiving the response \n");
        exit(EXIT_FAILURE);
    }

    if(response.response_status == 1){ // Check if the response is LIMIT
        clock_gettime(CLOCK_REALTIME, &time);
        printf("[%ld.%ld] Error al hacer el registro: error=%d \n", time.tv_sec, time.tv_nsec, response.response_status);
        exit(EXIT_FAILURE);
    }

    if(response.response_status == 0){ // Check if the response is ERROR
        clock_gettime(CLOCK_REALTIME, &time);
        printf("[%ld.%ld] Error al hacer el registro: error=%d \n", time.tv_sec, time.tv_nsec, response.response_status);
        exit(EXIT_FAILURE);
    }
    int id = response.id; // Get the id from the response

    clock_gettime(CLOCK_REALTIME, &time);
    printf("[%ld.%ld] Registrado correctamente con ID: %d para topic %s \n", msg.data.time_generated_data.tv_sec, msg.data.time_generated_data.tv_nsec, id, topic);


    while(!end){
        struct publish publish; // Create the struct to store the publish
        clock_gettime(CLOCK_REALTIME, &time);
        sprintf(publish.data, "%d", rand()); // Generate the data
        publish.time_generated_data = time; // Store the time

        msg.action = 4; // Set the action as PUBLISH_DATA
        strcpy(msg.topic, topic); // Copy the topic to the struct
        msg.data = publish; // Copy the publish to the struct

        clock_gettime(CLOCK_REALTIME, &time);
        printf("[%ld.%ld] Enviando mensaje topic: %s - mensaje: %s - Generó: [%ld.%ld]\n", time.tv_sec, time.tv_nsec, msg.topic, msg.data.data, msg.data.time_generated_data.tv_sec, msg.data.time_generated_data.tv_nsec);

        if(send(fd, &msg, sizeof(msg), 0) < 0){ // Send the message
            fprintf(stderr, "Error sending the message \n");
            exit(EXIT_FAILURE);
        }
        sleep(4); // Sleep 4 seconds

    }

    msg.action = 1; // Set the action as UNREGISTER_PUBLISHER
    strcpy(msg.topic, topic); // Copy the topic to the struct
    msg.id = id; // Copy the id to the struct

    if(send(fd, &msg, sizeof(msg), 0) < 0){ // Send the message
        fprintf(stderr, "Error sending the message \n");
        exit(EXIT_FAILURE);
    }

    if(recv(fd, &response, sizeof(response), 0) < 0){ // Receive the message
        fprintf(stderr, "Error receiving the message \n");
        exit(EXIT_FAILURE);
    }

    clock_gettime(CLOCK_REALTIME, &time);
    printf("[%ld.%ld] De-Registrado (%d) correctamente del broker. \n", time.tv_sec, time.tv_nsec, id);

    close(fd); // Close the socket

}

void subscriber(char *broker_ip, int broker_port, char *topic){ // Initializes the connection with the broker
    struct sockaddr_in serv_addr;
    struct in_addr ipv4Addr;
    int status;
    int fd;
    struct timespec time;

    setbuf(stdout, NULL); // Disable buffering
    signal(SIGINT, sigint_handler); // Signal to end the subscriber

    if(inet_pton(AF_INET, broker_ip, &ipv4Addr) < 0){ // Check the ip
        printf("Invalid address/ Address not supported\n");
        exit(EXIT_FAILURE);
    }

    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = ipv4Addr.s_addr;
    serv_addr.sin_port = htons(broker_port);

    if ((fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) { // Create the socket
        fprintf(stderr, "Socket creation error \n");
        exit(EXIT_FAILURE);
    }

    if ((status = connect(fd, (const struct sockaddr*)&serv_addr, sizeof(serv_addr))) < 0) { // Connect the socket
        fprintf(stderr, "Connection Failed \n");
        exit(EXIT_FAILURE);
    }

    clock_gettime(CLOCK_REALTIME, &time);
    printf("[%ld.%ld] Subscriber conectado con broker\n", time.tv_sec, time.tv_nsec);

    struct message msg_2_recieve;

    msg_2_recieve.action = 2; // Set the action as REGISTER_SUBSCRIBER
    strcpy(msg_2_recieve.topic, topic); // Copy the topic to the struct

    if(send(fd, &msg_2_recieve, sizeof(msg_2_recieve), 0) < 0){ // Send the message
        fprintf(stderr, "Error sending the message \n");
        exit(EXIT_FAILURE);
    }

    struct response response; // Create the struct to store the response

    if(recv(fd, &response, sizeof(response), 0) < 0){ // Receive the response
        fprintf(stderr, "Error receiving the response \n");
        exit(EXIT_FAILURE);
    }

    if(response.response_status == 1){ // Check if the response is LIMIT
        clock_gettime(CLOCK_REALTIME, &time);
        printf("[%ld.%ld] Error al hacer el registro: %d \n", time.tv_sec, time.tv_nsec, response.response_status);
        exit(EXIT_FAILURE);
    }

    if(response.response_status == 0){ // Check if the response is ERROR
        clock_gettime(CLOCK_REALTIME, &time);
        printf("[%ld.%ld] Error al hacer el registro: %d \n", time.tv_sec, time.tv_nsec, response.response_status);
        exit(EXIT_FAILURE);
    }

    int id = response.id; // Get the id from the response

    clock_gettime(CLOCK_REALTIME, &time);
    printf("[%ld.%ld] Registrado correctamente con ID: %d para topic %s \n", time.tv_sec, time.tv_nsec, id, topic);

    while(!end){
        
        struct publish publish; // Create the struct to store the publish

        if(recv(fd, &publish, sizeof(publish), 0) < 0){ // Receive the message
            fprintf(stderr, "Error receiving the message \n");
            exit(EXIT_FAILURE);
        }

        clock_gettime(CLOCK_REALTIME, &time);

        long latency_sec = time.tv_sec - publish.time_generated_data.tv_sec;
        long latency_nsec = time.tv_nsec - publish.time_generated_data.tv_nsec;

        if (latency_nsec < 0) {
            latency_sec--;
            latency_nsec += 1000000000;
        }

        double latency_nsec_all = latency_sec * 1000000000 + latency_nsec;

        double latency = latency_nsec_all / 1000000000;

        printf("[%ld.%ld] Recibido mensaje topic: %s - mensaje: %s - Generó: [%ld.%ld] - Recibido: [%ld.%ld] - Latencia: %06f \n", time.tv_sec, time.tv_nsec, topic, publish.data, publish.time_generated_data.tv_sec, publish.time_generated_data.tv_nsec, time.tv_sec, time.tv_nsec, latency);
    }   

    msg_2_recieve.action = 3; // Set the action as UNREGISTER_SUBSCRIBER
    strcpy(msg_2_recieve.topic, topic); // Copy the topic to the struct
    msg_2_recieve.id = id - 1; // Copy the id to the struct

    if(send(fd, &msg_2_recieve, sizeof(msg_2_recieve), 0) < 0){ // Send the message
        fprintf(stderr, "Error sending the message \n");
        exit(EXIT_FAILURE);
    }

    if(recv(fd, &response, sizeof(response), 0) < 0){ // Receive the message
        fprintf(stderr, "Error receiving the message \n");
        exit(EXIT_FAILURE);
    }

    clock_gettime(CLOCK_REALTIME, &time);
    printf("[%ld.%ld] De-Registrado (%d) correctamente del broker. \n", time.tv_sec, time.tv_nsec, id); 

    close(fd); // Close the socket
}