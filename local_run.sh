#!/bin/bash
PROC_SCALE_FACTOR=$([[ $(nproc) -gt 16 ]] && echo "4" || echo "2")

MAX_NODES=$(($(nproc) / $PROC_SCALE_FACTOR)) \
MPI_THREADS=$PROC_SCALE_FACTOR \
NRUNS=2 \
MAX_THREADS=$(nproc) \
THREADS_STEP=$PROC_SCALE_FACTOR \
./benchmark.sh $1 $2 $3
