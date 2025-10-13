CC=clang
CFLAGS=-std=c23
DEPS = crc64.h
OBJ = crc64.o main.o

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

zip: crc64.c crc64.h gen_crc64table.c main.c makefile README.md
	zip zs438730.zip $^

ssh: zip
	scp ./zs438730.zip zs438730@students.mimuw.edu.pl:~/isbd/lab1/

clean:
	rm -f *.o main gen_crc64table


