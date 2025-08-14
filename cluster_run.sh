#!/bin/bash


LOCATIONS="/tmp/mergesort_ls test"

for location in $LOCATIONS
do
    SRUN=1 ./benchmark.sh $location 64 25000000
done

for location in $LOCATIONS
do
    SRUN=1 ./benchmark.sh $location 16 100000000
done
