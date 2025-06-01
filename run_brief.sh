#!/bin/bash

./mergesort_seq -s 100000000

for i in {2..12}
do
  ./mergesort_ff -t $i -s 100000000
done

