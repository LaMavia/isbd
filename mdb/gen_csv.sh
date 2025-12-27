#!/usr/bin/env bash

N_ROWS=$1
N_COLS=$2

for c in $(seq $N_COLS); do
  printf 'col_%d' $c

  if [[ $c -ne 1 ]]; then
    printf ','
  fi
done
printf '\n'

for r in $(seq $N_ROWS); do
  for c in $(seq $N_COLS); do

    if [[ $c -ne 1 ]]; then
      printf ','
    fi
  done
done
