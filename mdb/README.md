# Docker

Przykładowe uruchomienie dockera

```sh
make docker
docker run -d --volume ./data:/data -p 8080:80 zs438730:latest
```

# Lokalnie

Dodałam target `local`, który również opiera się na nixie, więc wymaga on zainstalowanego [nixa](https://nixos.org/download/) (niekoniecznie nixos, sam manager wystarczy).
