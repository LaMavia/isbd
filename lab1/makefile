CC=clang
CFLAGS=-std=c23 -g
DEPS = md5.h crc64.h
OBJ = md5.o crc64.o main.o

main: $(OBJ)
	$(CC) -o $@ $^ $(CFLAGS) 

gen_crc64table:
	$(CC) -o gen_crc64table gen_crc64table.c -std=c23

crc64table.h: gen_crc64table
	./gen_crc64table > crc64table.h 

%.o: %.c $(DEPS)
	$(CC) -c -o $@ $< $(CFLAGS)

crc64.o: crc64table.h crc64.c
	$(CC) -c -o crc64.o crc64.c $(CFLAGS)

all: main

clean:
	rm -f *.o main gen_crc64table

