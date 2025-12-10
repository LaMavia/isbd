FROM nixos/nix:latest

ENV TABLE_DIRECTORY="/persist/tables" 
ENV RESULT_DIRECTORY="/results" 
ENV METASTORE_PATH="/persist/metastore.json"
ENV DATA_DIRECTORY="/data"

RUN mkdir /results /persist/tables

COPY . /app
WORKDIR /app

RUN nix \
    --extra-experimental-features "nix-command flakes" \
    --option filter-syscalls false \
    build

ENTRYPOINT [ "/app/result/bin/mdb" ]
