#!/bin/bash

./local_run.sh "/tmp/mergesort/test.dat" 16 20000000
./local_run.sh "/tmp/mergesort/test.dat" 64 10000000
./local_run.sh "/tmp/mergesort/test.dat" 512 1250000
./local_run.sh "/tmp/mergesort/test.dat" 2048 312500
