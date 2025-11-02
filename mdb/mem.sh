#!/usr/bin/env bash

dune build 
./_build/default/bin/main.exe $@ 2>&1 1>/dev/null &
PID=$!

while [ -f /proc/$PID/status ]; do grep -oP '^VmRSS:\s+\K\d+' /proc/$PID/status \
    | numfmt --from-unit Ki --to-unit Mi; done | ttyplot -u Mi
