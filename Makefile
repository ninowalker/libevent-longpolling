CFLAGS=-g -O2 -Wall

all: main.o
	$(CC) -o libevent-longpolling main.o -levent
