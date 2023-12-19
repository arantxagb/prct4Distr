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

pthread_mutex_t ocupation_topics_mutex;
pthread_mutex_t ocupation_subscribers_mutex;
pthread_mutex_t ocupation_publishers_mutex;
pthread_mutex_t ocupation_mutex;

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
    int *new_array;
    int j = 0;
    for(int i = 0; i < MAX_CLIENTS; i++){
        if (array[i] != 0){
            new_array[j] = array[i];
            j++;
        } 
    }
    return new_array;
}

void *thread_function_broker(void *arg){ // Function to handle the broker threads
    struct broker_info *info = (struct broker_info *)arg;
    int fd = info->fd;
    int position = info->position;
    struct message msg; // Create the struct to store the message

    free(info); // Free the struct
    
    if(recv(fd, &msg, sizeof(msg), 0) < 0){ // Receive the message
        fprintf(stderr, "Error receiving the message \n");
        exit(EXIT_FAILURE);
    }

    int id;
    if(msg.action == 0){ // REGISTER_PUBLISHER

        for(int i = 0; i < MAX_TOPICS; i++){ // Check if the topic is already registered
            if(strcmp(topic_info[i].topic, msg.topic) == 0){
                //ocupation_topics[i] = 1; // Set the position as occupied
                topic_info[i].num_publishers++;
                topic_info[i].publishers = reset_list(topic_info[i].publishers);
                topic_info[i].publishers[topic_info[i].num_publishers - 1] = fd;
            } else{
                pthread_mutex_lock(&ocupation_topics_mutex);
                int id_topic = position_free(ocupation_topics); // Get the first free position in the array
                pthread_mutex_unlock(&ocupation_topics_mutex);
                if (id_topic == -1) {
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
                } else {
                    pthread_mutex_lock(&ocupation_topics_mutex);
                    ocupation_topics[id_topic] = 1; // Set the position as occupied
                    pthread_mutex_unlock(&ocupation_topics_mutex);
                    
                    topic_info[id_topic].num_publishers = 1;
                    strcpy(topic_info[id_topic].topic, msg.topic); // Store the topic in the array
                    topic_info[id_topic].publishers[0] = fd;
                }
            }

            }
            pthread_mutex_lock(&ocupation_topics_mutex);
            int id_topic = position_free(ocupation_topics); // Get the first free position in the array
            pthread_mutex_unlock(&ocupation_topics_mutex);
            if (id_topic == -1) {
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
            } else {
                pthread_mutex_lock(&ocupation_topics_mutex);
                ocupation_topics[id_topic] = 1; // Set the position as occupied
                pthread_mutex_unlock(&ocupation_topics_mutex);
                strcpy(topics[id_topic], msg.topic); // Store the topic in the array
            }
        }

        pthread_mutex_lock(&ocupation_publishers_mutex);
        id = position_free(ocupation_publishers); // Get the first free position in the array
        pthread_mutex_unlock(&ocupation_publishers_mutex);
        if(id == -1){ // Check if there is space for the publisher
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
        else{
            struct response response; // Create the struct to store the response
            response.response_status = 2; // Set the response status as OK
            response.id = id; // Set the id as the first free position
            if(send(fd, &response, sizeof(response), 0) < 0){ // Send the response
                fprintf(stderr, "Error sending the response \n");
                exit(EXIT_FAILURE);
            }
            pthread_mutex_lock(&ocupation_publishers_mutex);
            ocupation_publishers[id] = 1; // Set the position as occupied
            pthread_mutex_unlock(&ocupation_publishers_mutex);
            publishers[id] = fd; // Store the socket in the array
        }

    } else if(msg.action == 2){
        pthread_mutex_lock(&ocupation_subscribers_mutex);
        id = position_free(ocupation_subscribers); // Get the first free position in the array
        pthread_mutex_unlock(&ocupation_subscribers_mutex);
        if(id == -1){ // Check if there is space for the subscriber
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
        else{
            struct response response; // Create the struct to store the response
            response.response_status = 2; // Set the response status as OK
            response.id = id; // Set the id as the first free position
            if(send(fd, &response, sizeof(response), 0) < 0){ // Send the response
                fprintf(stderr, "Error sending the response \n");
                exit(EXIT_FAILURE);
            }
            pthread_mutex_lock(&ocupation_subscribers_mutex);
            ocupation_subscribers[id] = 1; // Set the position as occupied
            pthread_mutex_unlock(&ocupation_subscribers_mutex);
            subscribers[id] = fd; // Store the socket in the array
        }
    } else{
        struct response response; // Create the struct to store the response
        response.response_status = 0; // Set the response status as ERROR
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

    if(recv(fd, &msg, sizeof(msg), 0) < 0){ // Receive the message
        fprintf(stderr, "Error receiving the message \n");
        exit(EXIT_FAILURE);
    }

    // if(msg.action == 4){ // PUBLISH_DATA
    //     struct publish publish = msg.data; // Get the publish struct from the message
        
    //     for(int i = 0; i < MAX_SUBSCRIBERS; i++){ // Send the message to all the subscribers
    //         if(ocupation_subscribers[i] == 1){
    //             if(send(subscribers[i], &publish, sizeof(publish), 0) < 0){ // Send the message
    //                 fprintf(stderr, "Error sending the message \n");
    //                 exit(EXIT_FAILURE);
    //             }
    //         }
    //     }
    // }
    
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
        pthread_detach(threads[info->position]); // Detach the thread
    }
}

void publisher(char *broker_ip, int broker_port, char *topic){ // Initializes the connection with the broker
    struct sockaddr_in serv_addr;
    struct in_addr ipv4Addr;
    int status;
    int fd;

    setbuf(stdout, NULL); // Disable buffering

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

    struct message *msg = (struct message *)malloc(sizeof(struct message)); // Create the struct to store the message
    msg->action = 0; // Set the action as REGISTER_PUBLISHER
    strcpy(msg->topic, topic); // Copy the topic to the struct

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

    // while(!end){
    //     struct publish publish; // Create the struct to store the publish
    //     publish.time_generated_data = timespec_get(&publish.time_generated_data, TIME_UTC); // Get the time
    //     sprintf(publish.data, "%d", rand()); // Generate the data

    //     struct message *msg = (struct message *)malloc(sizeof(struct message)); // Create the struct to store the message
    //     msg->action = 4; // Set the action as PUBLISH_DATA
    //     strcpy(msg->topic, topic); // Copy the topic to the struct
    //     msg->id = id; // Copy the id to the struct
    //     msg->data = publish; // Copy the publish to the struct

    //     if(send(fd, msg, sizeof(msg), 0) < 0){ // Send the message
    //         fprintf(stderr, "Error sending the message \n");
    //         exit(EXIT_FAILURE);
    //     }

    //     free(msg); // Free the struct
    //     sleep(0); // Sleep 1 second
    // }

    free(msg); // Free the struct
}

void subscriber(char *broker_ip, int broker_port, char *topic){ // Initializes the connection with the broker
    struct sockaddr_in serv_addr;
    struct in_addr ipv4Addr;
    int status;
    int fd;

    setbuf(stdout, NULL); // Disable buffering

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

    struct message *msg = (struct message *)malloc(sizeof(struct message)); // Create the struct to store the message
    msg->action = 2; // Set the action as REGISTER_SUBSCRIBER
    strcpy(msg->topic, topic); // Copy the topic to the struct

    if(send(fd, msg, sizeof(msg), 0) < 0){ // Send the message
        fprintf(stderr, "Error sending the message \n");
        exit(EXIT_FAILURE);
    }

    free(msg); // Free the struct
}