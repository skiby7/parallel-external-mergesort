#!/bin/bash


# ./gen_file -r 16 -s 20000000  "/tmp/mergesort/test.dat"
# ls -l "/tmp/mergesort/test.dat"
# /bin/rm "/tmp/mergesort/test.dat"
# ./gen_file -r 64 -s 10000000  "/tmp/mergesort/test.dat"
# ls -l "/tmp/mergesort/test.dat"
# /bin/rm "/tmp/mergesort/test.dat"
# ./gen_file -r 512 -s 1250000  "/tmp/mergesort/test.dat"
# ls -l "/tmp/mergesort/test.dat"
# /bin/rm "/tmp/mergesort/test.dat"
# ./gen_file -r 2048 -s 312500  "/tmp/mergesort/test.dat"
# ls -l "/tmp/mergesort/test.dat"
# /bin/rm "/tmp/mergesort/test.dat"


./local_run.sh "/tmp/mergesort/test.dat" 16 20000000
./local_run.sh "/tmp/mergesort/test.dat" 64 10000000
./local_run.sh "/tmp/mergesort/test.dat" 512 1250000
./local_run.sh "/tmp/mergesort/test.dat" 2048 312500
