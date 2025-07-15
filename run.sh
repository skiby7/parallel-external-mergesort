#!/bin/bash

MAX_MEM=100000000
INPUT_FILE=$1
if [ -z "$INPUT_FILE" ]; then
    echo "Usage: $0 <input_file>"
    exit 1
fi
OUTPUT_FILE=$(dirname $INPUT_FILE)/output.dat
run_omp() {
    for i in $(seq 4 4 $(nproc))
    do
        echo "Running with $i threads"
        ./mergesort_omp -t $i -m $MAX_MEM $INPUT_FILE
        /bin/rm /tmp/output.dat
    done
}

run_ff() {
    for i in $(seq 4 4 $(nproc))
    do
        echo "Running with $i threads"
        ./mergesort_ff -t $i -m $MAX_MEM $INPUT_FILE
        /bin/rm /tmp/output.dat
    done
}

run_seq() {
    echo "Running sequential version"
    ./mergesort_seq -k -m $MAX_MEM $INPUT_FILE
    /bin/rm /tmp/output.dat
}

run_seq
run_omp
run_ff
