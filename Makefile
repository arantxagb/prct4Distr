CC=gcc
CFLAGS=-I.

all: subscriber publisher broker

subscriber: subscriber.c stub.o
	$(CC) -o subscriber subscriber.c stub.o $(CFLAGS)

publisher: publisher.c stub.o
	$(CC) -o publisher publisher.c stub.o $(CFLAGS)

broker: broker.c stub.o
	$(CC) -o broker broker.c stub.o $(CFLAGS)

stub.o: stub.c stub.h
	$(CC) -c -o stub.o stub.c $(CFLAGS)

clean:
	rm -f *.o subscriber publisher broker 