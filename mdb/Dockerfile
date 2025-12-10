FROM nixos/nix:latest

ENV TABLE_DIRECTORY="/tables" 
ENV RESULT_DIRECTORY="/results" 
ENV METASTORE_PATH="/metastore.json"
ENV DATA_DIRECTORY="/data"

RUN mkdir /tables /results

COPY . /app
WORKDIR /app

RUN nix \
    --extra-experimental-features "nix-command flakes" \
    --option filter-syscalls false \
    build

ENTRYPOINT [ "/app/result/bin/mdb" ]
