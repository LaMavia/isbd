# Config

## Zmienne środowiskowe

```
TABLE_DIRECTORY : path, required
Ścieżka do folderu z plikami tabel. Wszystkie ścieżki w metastore są relatywne od niej.

RESULT_DIRECTORY : path, required
Ścieżka do folderu, w którym zapisywane są tymczasowo wyniki kwerend.

METASTORE_PATH : path, required
Ścieżka do pliku metastore.

DATA_DIRECTORY : path, required
Ścieżka do folderu z plikami do wczytania przy użyciu kwerendy COPY.

BUFFER_SIZE : int, optional=32000000 (32MB)
Rozmiar bufforu kolumny w bajtach. Pliki tabel zapisane poprzednio nadal będą używały poprzedniego rozmiaru buffora.
Determinuje rozmiar fragmentu w pliku oraz rozmiar grup pośrednich przy sortowaniu.
Przyjmujemy, że każdy rekord jest w stanie zmieścić się w pamięci N_COLUMNS * BUFFER_SIZE.
Faktyczny rozmiar buffora jest ustalany przez LZ4.compress_bound BUFFER_SIZE.

MERGE_WORKERS : int, optional=2
Ilość wątków używana przy merge'owaniu po sortowaniu grup pośrednich.

MAX_K_MERGE : int, optional=10
Maksymalna ilość grup pośrednich, dla której wykonujemy k-way merge sort.
Dla większej ilości grup wykonywany jest merge sort o logarytmicznym zagnieżdżeniu.
```

# Docker

Przykładowe uruchomienie dockera

```sh
make docker
docker run -d -v ./data:/data -v ./mdb_data:/mdb_data -p 8080:80 zs438730:latest
```

# Lokalnie

Dodałam target `local`, który również opiera się na nixie, więc wymaga on zainstalowanego [nixa](https://nixos.org/download/) (niekoniecznie nixos, sam manager wystarczy).
