# sposób użycia

```sh
make
./main block-size file-path [-v]
```

gdzie

1. `block-size` - rozmiar bloku w bajtach, dodatni, niewiększe niż 2'147'479'552, gdyż tyle maksymalnie może odczytać `read`, więc zmieniłoby to bloki w podejściu `read` vs. `mmap`.
2. `file-path` - ścieżka do pliku.
3. `-v` - flaga verbose, wypisuje bajty wczytanych bloków.

# przykładowe wyjście

## zwykłe

```sh
9499028d4cbdfa6d  seq read()	took: 12us	blocks read: 4
364b15c66eeb496f  rand read()	took: 8us	blocks read: 4
9499028d4cbdfa6d  seq mmap()	took: 5us	blocks read: 4
364b15c66eeb496f  rand mmap()	took: 4us	blocks read: 4
```

## verbose

```sh
Block 1:	31 31 31 31 32
Block 2:	32 32 32 33 33
Block 3:	33 33 34 34 34
Block 4:	34 00 00 00 00
9499028d4cbdfa6d  seq read()	took: 793us	blocks read: 4
Block 1:	31 31 31 31 32
Block 2:	34 00 00 00 00
Block 3:	32 32 32 33 33
Block 4:	33 33 34 34 34
364b15c66eeb496f  rand read()	took: 612us	blocks read: 4
Block 1:	31 31 31 31 32
Block 2:	32 32 32 33 33
Block 3:	33 33 34 34 34
Block 4:	34 00 00 00 00
9499028d4cbdfa6d  seq mmap()	took: 587us	blocks read: 4
Block 1:	31 31 31 31 32
Block 2:	34 00 00 00 00
Block 3:	32 32 32 33 33
Block 4:	33 33 34 34 34
364b15c66eeb496f  rand mmap()	took: 589us	blocks read: 4
```

# notatki

Skrajnie prawy blok jest paddowany bajtem `0x00`.
