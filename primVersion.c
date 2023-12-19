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
int ocupation[MAX_CLIENTS] = {};
int ocupation_topics[MAX_TOPICS] = {};
int ocupation_subscribers[MAX_SUBSCRIBERS] = {};
int ocupation_publishers[MAX_PUBLISHERS] = {};
int publishers[MAX_PUBLISHERS] = {};
int subscribers[MAX_SUBSCRIBERS] = {};
char topics[MAX_TOPICS][50] = {};
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

int position_free(int *array){ // Returns the first free position in the array
    for(int i = 0; i < MAX_CLIENTS; i++){
        if(array[i] == 0){
            return i;
        }
    }
    return -1;
}

int *reset_list(int *array){ // Resets the array
    int *new_array = (int *)malloc(sizeof(int) * 100);
    int j = 0;
    for(int i = 0; i < MAX_CLIENTS; i++){
        if (array[i] != 0){
            new_array[j] = array[i];
            j++;
        } 
    }

    return new_array;
}

void error_message(int fd, int position){ // Function to send an error message
    struct response response; // Create the struct to store the response
    response.response_status = 1; // Set the response status as LIMIT
    response.id = -1; // Set the id as -1
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
            //ocupation_topics[i] = 1; // Set the position as occupied
            registered = 0;
            if (strcmp(flag, "publisher") == 0){
                topic_info[i].num_publishers++;
                topic_info[i].publishers[topic_info[i].num_publishers - 1] = fd;
            } else if(strcmp(flag, "subscriber") == 0){
                topic_info[i].num_subscribers++;
                topic_info[i].subscribers[topic_info[i].num_subscribers - 1] = fd;
            } else{
                pthread_mutex_unlock(&register_topic_mutex);
                error_message(fd, 0);
            }
        } 
    }
    if (registered == 1){
        pthread_mutex_lock(&ocupation_topics_mutex);
        int id_topic = position_free(ocupation_topics); // Get the first free position in the array
        pthread_mutex_unlock(&ocupation_topics_mutex);
        if (id_topic == -1) {
            pthread_mutex_unlock(&register_topic_mutex);
            error_message(fd, position);
        } else {
            pthread_mutex_lock(&ocupation_topics_mutex);
            ocupation_topics[id_topic] = 1; // Set the position as occupied
            pthread_mutex_unlock(&ocupation_topics_mutex);
           
            topic_info[id_topic].subscribers = (int *)malloc(sizeof(int) * 900);
            topic_info[id_topic].publishers = (int *)malloc(sizeof(int) * 100);
           
            if (strcmp(flag, "publisher") == 0){
                topic_info[id_topic].num_publishers = 1;
                topic_info[id_topic].num_subscribers = 0;
                strcpy(topic_info[id_topic].topic, msg->topic); // Store the topic in the array                
                topic_info[id_topic].publishers[0] = fd;
            } else if(strcmp(flag, "subscriber") == 0){
                topic_info[id_topic].num_publishers = 0;
                topic_info[id_topic].num_subscribers = 1;
                strcpy(topic_info[id_topic].topic, msg->topic); // Store the topic in the array
                topic_info[id_topic].subscribers[0] = fd; 
            } else{
                pthread_mutex_unlock(&register_topic_mutex);
                error_message(fd, 0);
            }
        }
    }
    printf("num_publishers: %d \n", topic_info[0].num_publishers);
    printf("num_subscribers: %d \n", topic_info[0].num_subscribers);

    pthread_mutex_unlock(&register_topic_mutex);
}

void existing_topic(int i){
    if(topic_info[i].num_publishers <= 0 && topic_info[i].num_subscribers <= 0){
        topic_info[i].id = -1;
        strcpy(topic_info[i].topic, "");
        topic_info[i].publishers = NULL;
        topic_info[i].subscribers = NULL;
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
                        topic_info[i].publishers = reset_list(topic_info[i].publishers);
                        existing_topic(i);
                        break;
                    }
                }
            } else if(strcmp(flag, "subscriber") == 0){
                for (int j = 0; j < topic_info[i].num_subscribers; j++){
                    if(topic_info[i].subscribers[j] == fd){
                        topic_info[i].subscribers[j] = 0;
                        topic_info[i].num_subscribers--;
                        topic_info[i].subscribers = reset_list(topic_info[i].subscribers);
                        existing_topic(i);
                        break;
                    }
                }
            } else{
                pthread_mutex_unlock(&register_topic_mutex);
                error_message(fd, 0);
            }
            if (topic_info[i].num_publishers <= 0 && topic_info[i].num_subscribers <= 0){
                topic_info[i].id = -1;
                strcpy(topic_info[i].topic, "");
                topic_info[i].publishers = NULL;
                topic_info[i].subscribers = NULL;
                topic_info[i].num_publishers = 0;
                topic_info[i].num_subscribers = 0;
                pthread_mutex_lock(&ocupation_topics_mutex);
                ocupation_topics[i] = 0; // Set the position as free
                pthread_mutex_unlock(&ocupation_topics_mutex);
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
                printf("data: %s \n", msg->data.data);
                if (send(topic_info[i].subscribers[j], &msg->data, sizeof(msg->data), 0) < 0){ // Send the message
                    fprintf(stderr, "Error sending the message \n");
                    exit(EXIT_FAILURE);
                }
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
            for (int j = 0; j < topic_info[i].num_subscribers; j++){
                pthread_t threads[topic_info[i].num_subscribers];
                struct send_info *msg_2_send = (struct send_info *)malloc(sizeof(struct send_info)); // Create the struct to store the message
                msg_2_send->fd = topic_info[i].subscribers[j];
                msg_2_send->msg->data = msg->data;
                pthread_create(&threads[j], NULL, thread_function_paralelo, msg_2_send); // Create the thread
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
                struct send_info *msg_2_send = (struct send_info *)malloc(sizeof(struct send_info)); // Create the struct to store the message
                msg_2_send->fd = topic_info[i].subscribers[j];
                msg_2_send->msg->data = msg->data;
                pthread_create(&threads[j], NULL, thread_function_justo, msg_2_send); // Create the thread
            }
        }
    }
}

void *thread_function_broker(void *arg){ // Function to handle the broker threads
    struct broker_info *info = (struct broker_info *)arg;
    int position = info->position;
    struct message *msg = (struct message *)malloc(sizeof(struct message)); // Create the struct to store the message

    //free(info); // Free the struct

    printf("fd: %d \n", info->fd);
    
    if(recv(info->fd, msg, sizeof(msg), 0) < 0){ // Receive the message
        fprintf(stderr, "Error receiving the message \n");
        exit(EXIT_FAILURE);
    }

    fprintf(stdout, "Action: %d \n", msg->action); // Print the action
    printf("topic: %s \n", msg->topic);

    int id;
    if(msg->action == 0){ // REGISTER_PUBLISHER
        
        pthread_mutex_lock(&ocupation_publishers_mutex);
        id = position_free(ocupation_publishers); // Get the first free position in the array
        pthread_mutex_unlock(&ocupation_publishers_mutex);
        if(id == -1){ // Check if there is space for the publisher
            error_message(info->fd, position);
        }
        else{
            struct response response; // Create the struct to store the response
            response.response_status = 2; // Set the response status as OK
            response.id = id; // Set the id as the first free position
            if(send(info->fd, &response, sizeof(response), 0) < 0){ // Send the response
                fprintf(stderr, "Error sending the response \n");
                exit(EXIT_FAILURE);
            }
            register_topic(position, info->fd, msg, "publisher");
            pthread_mutex_lock(&ocupation_publishers_mutex);
            ocupation_publishers[id] = 1; // Set the position as occupied
            pthread_mutex_unlock(&ocupation_publishers_mutex);
            publishers[id] = info->fd; // Store the socket in the array
        }

    } else if(msg->action == 2){
        pthread_mutex_lock(&ocupation_subscribers_mutex);
        id = position_free(ocupation_subscribers); // Get the first free position in the array
        pthread_mutex_unlock(&ocupation_subscribers_mutex);

        if(id == -1){ // Check if there is space for the subscriber
            error_message(info->fd, position);
        }
        else{
            register_topic(position, info->fd, msg, "subscriber");
            struct response response; // Create the struct to store the response
            response.response_status = 2; // Set the response status as OK
            response.id = id; // Set the id as the first free position
            if(send(info->fd, &response, sizeof(response), 0) < 0){ // Send the response
                fprintf(stderr, "Error sending the response \n");
                exit(EXIT_FAILURE);
            }
            pthread_mutex_lock(&ocupation_subscribers_mutex);
            ocupation_subscribers[id] = 1; // Set the position as occupied
            pthread_mutex_unlock(&ocupation_subscribers_mutex);
            subscribers[id] = info->fd; // Store the socket in the array
        }
    } else{
        error_message(info->fd, position);
    }

    int end_conection = 0;

    while(!end_conection){

        //memset(&msg, 0, sizeof(msg)); // Reset the struct

        if(recv(info->fd, msg, sizeof(msg), 0) < 0){ // Receive the message
            fprintf(stderr, "Error receiving the message \n");
            exit(EXIT_FAILURE);
        }

        fprintf(stdout, "Action: %d \n", msg->action); // Print the action

        if(msg->action == 4){ // PUBLISH_DATA
            printf("topic: %s \n", msg->topic);
            printf("data: %s \n", msg->data.data);
            for (int i = 0; i < MAX_TOPICS; i++){
                if(strcmp(topic_info[i].topic, msg->topic) == 0){
                    if(strcmp(info->mode, "secuencial") == 0){
                        secuencial(info->fd, msg);
                    } else if(strcmp(info->mode, "justo") == 0){
                        justo(info->fd, msg);
                    } else if(strcmp(info->mode, "paralelo") == 0){
                        paralelo(info->fd, msg);
                    } else{
                        error_message(info->fd, position);
                    }
                }
            }
        } else if(msg->action == 1){
            desregister(info->fd, msg, "publisher");
            end_conection = 1;

        } else if(msg->action == 3){
            desregister(info->fd, msg, "subscriber");
            end_conection = 1;

        } else{
            error_message(info->fd, position);
        }
    }

    close(info->fd); // Close the socket
    //pthread_exit(NULL); // Exit the thread
    
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
    memset(topic_info, 0, sizeof(struct topic_info) * 10);

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

        info->position = position_free(ocupation); // Get the first free position in the array
        ocupation[info->position] = 1; // Set the position as occupied
        threads[info->position] = 0; // Set the thread as not created

        pthread_create(&threads[info->position], NULL, thread_function_broker, info); // Create the thread
    }
}

void publisher(char *broker_ip, int broker_port, char *topic){ // Initializes the connection with the broker
    struct sockaddr_in serv_addr;
    struct in_addr ipv4Addr;
    int status;
    int fd;

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

    printf("fd: %d \n", fd); // NO SE CONECTAN BIEN LOS FD

    struct message *msg = (struct message *)malloc(sizeof(struct message)); // Create the struct to store the message
    msg->action = 0; // Set the action as REGISTER_PUBLISHER
    strcpy(msg->topic, topic); // Copy the topic to the struct

    //fprintf(stdout, "action: %d \n", msg->action); // Print the action

    if(send(fd, msg, sizeof(msg), 0) < 0){ // Send the message
        fprintf(stderr, "Error sending the message \n");
        exit(EXIT_FAILURE);
    }

    struct response response; // Create the struct to store the response
    if(recv(fd, &response, sizeof(response), 0) < 0){ // Receive the response
        fprintf(stderr, "Error receiving the response \n");
        exit(EXIT_FAILURE);
    }

    if(response.response_status == 1){ // Check if the response is LIMIT
        fprintf(stderr, "Error: Limit of publishers reached \n");
        exit(EXIT_FAILURE);
    }

    if(response.response_status == 0){ // Check if the response is ERROR
        fprintf(stderr, "Error: Invalid response \n");
        exit(EXIT_FAILURE);
    }

    int id = response.id; // Get the id from the response


    while(!end){
        struct publish *publish = (struct publish *)malloc(sizeof(struct publish)); // Create the struct to store the publish
        if (timespec_get(&publish->time_generated_data, TIME_UTC) == 0) {
            perror("Error getting time");
            exit(EXIT_FAILURE);
        }        
        sprintf(publish->data, "%d", rand()); // Generate the data

        fprintf(stdout, "data: %s \n", publish->data); // Print the data

        msg->action = 4; // Set the action as PUBLISH_DATA
        strcpy(msg->topic, topic); // Copy the topic to the struct
        msg->data = *publish; // Copy the publish to the struct

        if(send(fd, msg, sizeof(msg), 0) < 0){ // Send the message
            fprintf(stderr, "Error sending the message \n");
            exit(EXIT_FAILURE);
        }

        free(publish); // Free the struct
        sleep(4); // Sleep 4 seconds

    }

    msg->action = 1; // Set the action as UNREGISTER_PUBLISHER
    strcpy(msg->topic, topic); // Copy the topic to the struct
    msg->id = id; // Copy the id to the struct

    if(send(fd, msg, sizeof(msg), 0) < 0){ // Send the message
        fprintf(stderr, "Error sending the message \n");
        exit(EXIT_FAILURE);
    }

    if(recv(fd, &response, sizeof(response), 0) < 0){ // Receive the message
        fprintf(stderr, "Error receiving the message \n");
        exit(EXIT_FAILURE);
    }

    free(msg); // Free the struct
}

void subscriber(char *broker_ip, int broker_port, char *topic){ // Initializes the connection with the broker
    struct sockaddr_in serv_addr;
    struct in_addr ipv4Addr;
    int status;
    int fd;

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

    struct message *msg_2_recieve = (struct message *)malloc(sizeof(struct message)); // Create the struct to store the message
    msg_2_recieve->action = 2; // Set the action as REGISTER_SUBSCRIBER
    strcpy(msg_2_recieve->topic, topic); // Copy the topic to the struct

    if(send(fd, msg_2_recieve, sizeof(msg_2_recieve), 0) < 0){ // Send the message
        fprintf(stderr, "Error sending the message \n");
        exit(EXIT_FAILURE);
    }

    struct response response; // Create the struct to store the response

    if(recv(fd, &response, sizeof(response), 0) < 0){ // Receive the response
        fprintf(stderr, "Error receiving the response \n");
        exit(EXIT_FAILURE);
    }

    if(response.response_status == 1){ // Check if the response is LIMIT
        fprintf(stderr, "Error: Limit of subscribers reached \n");
        exit(EXIT_FAILURE);
    }

    while(!end){
        
        struct publish *publish = (struct publish *)malloc(sizeof(struct publish)); // Create the struct to store the publish

        if(recv(fd, publish, sizeof(publish), 0) < 0){ // Receive the message
            fprintf(stderr, "Error receiving the message \n");
            exit(EXIT_FAILURE);
        }

        fprintf(stdout, "data: %s \n", publish->data); // Print the id


        free(publish); // Free the struct
    }   

    msg_2_recieve->action = 3; // Set the action as UNREGISTER_SUBSCRIBER
    strcpy(msg_2_recieve->topic, topic); // Copy the topic to the struct

    if(send(fd, msg_2_recieve, sizeof(msg_2_recieve), 0) < 0){ // Send the message
        fprintf(stderr, "Error sending the message \n");
        exit(EXIT_FAILURE);
    }

    if(recv(fd, &response, sizeof(response), 0) < 0){ // Receive the message
        fprintf(stderr, "Error receiving the message \n");
        exit(EXIT_FAILURE);
    }

    free(msg_2_recieve); // Free the struct

}