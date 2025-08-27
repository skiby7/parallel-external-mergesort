#!/bin/bash


FILES="/tmp/mergesort_ls/test.dat test/test.dat"

for file in $FILES
do
    SRUN=1 ./benchmark.sh $file 64 50000000
done
