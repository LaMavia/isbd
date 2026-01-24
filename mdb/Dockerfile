FROM nixos/nix:latest

ENV TABLE_DIRECTORY="/mdb_data" 
ENV RESULT_DIRECTORY="/results" 
ENV METASTORE_PATH="/mdb_data/metastore.json"
ENV DATA_DIRECTORY="/data"

RUN mkdir /results /mdb_data

COPY . /app
WORKDIR /app

RUN nix \
    --extra-experimental-features "nix-command flakes" \
    --option filter-syscalls false \
    build

ENTRYPOINT [ "/app/result/bin/mdb" ]
