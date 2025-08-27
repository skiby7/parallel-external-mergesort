#!/bin/bash


srun -w node01 rm -rf /tmp/mergesort_ls
srun -w node01 mkdir -p /tmp/mergesort_ls
srun -w node01 ./gen_file -r 64 -s 25000000 /tmp/mergesort_ls/test.dat
srun -w node01 mpirun -np 2 mergesort_mpi -t 16 /tmp/mergesort_ls/test.dat
