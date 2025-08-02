#!/bin/bash

INPUT_FILE=$1
if [ -z "$INPUT_FILE" ]; then
    echo "Usage: $0 <input_file>"
    echo "Specify SRUN=1 to run on a node"
    exit 1
fi

if [ -n "$SRUN" ]; then
    SRUN="srun -w node08"
else
    SRUN=""
fi

OUTPUT_FILE=$(dirname $INPUT_FILE)/output.dat
FILESIZE=$($SRUN stat -c%s $INPUT_FILE)
ONE_THIRD_FILESIZE=$((FILESIZE/3)) # Simulating memory constraints
AVAIL_MEM=$(free -b | awk '/Mem:/ { print $7 }')
MAX_MEMORY=$((32 * 1024 * 1024 * 1024)) # 32GB
USABLE_MEM=$(
  echo "$ONE_THIRD_FILESIZE $AVAIL_MEM $MAX_MEMORY" |
  awk '{ min = $1; for (i = 2; i <= NF; i++) if ($i < min) min = $i; print min }'
)
LOG_FILE=run_$(date +%s).log

NRUNS=3
THREAD_COUNTS=(
    2
    4
    6
    8
    14
    16
    20
    26
    32
)

mkdir -p results
echo "" > results/$LOG_FILE

$SRUN make -j
run_parallel() {
    for i in "${THREAD_COUNTS[@]}"
    do
        echo "#################################" | tee -a results/$LOG_FILE
        echo -e "(nthreads=$i, max_mem=$USABLE_MEM)" | tee -a results/$LOG_FILE
        for j in {1..$NRUNS}
        do
            $SRUN ./mergesort_omp -k -t $i -m $USABLE_MEM $INPUT_FILE | tee -a results/$LOG_FILE
            $SRUN /bin/rm $OUTPUT_FILE
            $SRUN ./mergesort_ff -k -t $i -m $USABLE_MEM $INPUT_FILE | tee -a results/$LOG_FILE
            $SRUN /bin/rm $OUTPUT_FILE
        done
    done
}

run_seq() {
    echo "#################################" | tee -a results/$LOG_FILE
    echo -e "(sequential binary merge, max_mem=$USABLE_MEM)" | tee -a results/$LOG_FILE
    for j in {1..$NRUNS}
    do
        $SRUN ./mergesort_seq -m $USABLE_MEM $INPUT_FILE | tee -a results/$LOG_FILE
        $SRUN /bin/rm $OUTPUT_FILE
    done

    echo "#################################" | tee -a results/$LOG_FILE
    echo -e "(sequential kway merge, max_mem=$USABLE_MEM)" | tee -a results/$LOG_FILE
    for j in {1..$NRUNS}
    do
        $SRUN ./mergesort_seq -k -m $USABLE_MEM $INPUT_FILE | tee -a results/$LOG_FILE
        $SRUN /bin/rm $OUTPUT_FILE
    done
}

run_mpi() {
    if [[ -z "$SRUN" ]]; then
        return 1
    fi

    echo "#################################" | tee -a results/$LOG_FILE

    WORKER_NODES=(node01 node02 node03 node04 node05 node06 node07)

    for i in 1 2 4 8; do
        # Build nodelist starting with node08
        NODELIST="node08"
        for ((n=0; n<i-1; n++)); do
            NODELIST+=",${WORKER_NODES[n]}"
        done

        SRUN="srun --nodelist=${NODELIST} --ntasks-per-node=1 --mpi=pmix"

        echo -e "(nnodes=$i, nthreads=$NPROCS, max_mem=$USABLE_MEM)" | tee -a results/$LOG_FILE

        for j in $(seq 1 $NRUNS); do
            $SRUN ./mergesort_mpi -t 16 -m $USABLE_MEM $INPUT_FILE | tee -a results/$LOG_FILE
            $SRUN /bin/rm -f $OUTPUT_FILE
        done
    done
}

run_seq
run_parallel
run_mpi
echo "#################################" | tee -a results/$LOG_FILE
