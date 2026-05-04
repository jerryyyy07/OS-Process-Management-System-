CC = gcc
CFLAGS = -Wall -Wextra -pthread -O2

all: dispatcher ingester processor reporter

dispatcher: dispatcher.c
	$(CC) $(CFLAGS) -o $@ $^

ingester: ingester.c
	$(CC) $(CFLAGS) -o $@ $^

processor: processor.c
	$(CC) $(CFLAGS) -o $@ $^

reporter: reporter.c
	$(CC) $(CFLAGS) -o $@ $^

clean:
	rm -f dispatcher ingester processor reporter
