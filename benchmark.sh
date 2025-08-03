#!/bin/bash

INPUT_FILE=$1
PAYLOAD_SIZE=$2
ITEMS_COUNT=$3
if [ -z "$INPUT_FILE" ]; then
    echo "Usage: $0 <input_file> [payload_size] [items_count]"
    echo "Specify SRUN=1 to schedule using slurm"
    exit 1
fi

MAIN_NODE="node01"
# MAIN_NODE="node04"
WORKER_NODES=(node02 node03 node04 node05 node06 node07 node08)
# WORKER_NODES=(node05 node06 node07 node08)
NODE_COUNTS=(2 4 8)
# NODE_COUNTS=(2 4 8)

if [ -n "$SRUN" ]; then
    SRUN="srun -w $MAIN_NODE"
else
    SRUN=""
fi

$SRUN ls $INPUT_FILE
INPUT_EXISTS=$?
if [ $INPUT_EXISTS -eq 0 ]; then
    echo "Input file already exists, to generate a new one, delete it first"
else
    if [ -z "$PAYLOAD_SIZE" ]; then
        PAYLOAD_SIZE=64
    fi
    if [ -z "$ITEMS_COUNT" ]; then
        ITEMS_COUNT=20000000
    fi
    echo "Generating input file with at most $PAYLOAD_SIZE bytes per item and $ITEMS_COUNT items..."
    $SRUN make gen_file
    $SRUN ./gen_file -r $PAYLOAD_SIZE -s $ITEMS_COUNT $INPUT_FILE
fi

OUTPUT_FILE=$(dirname $INPUT_FILE)/output.dat
FILESIZE=$($SRUN stat -c%s $INPUT_FILE)
ONE_THIRD_FILESIZE=$((FILESIZE/3)) # Simulating memory constraints
AVAIL_MEM=$($SRUN free -b | awk '/Mem:/ { print $7 }')
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

$SRUN make clean
$SRUN make -j
run_parallel() {
    for i in "${THREAD_COUNTS[@]}"
    do
        echo "#################################" | tee -a results/$LOG_FILE
        echo -e "(nthreads=$i, max_mem=$USABLE_MEM)" | tee -a results/$LOG_FILE
        for j in $(seq 1 "$NRUNS")
        do
            $SRUN ./mergesort_omp -t $i -m $USABLE_MEM $INPUT_FILE | tee -a results/$LOG_FILE
            $SRUN /bin/rm $OUTPUT_FILE
            $SRUN ./mergesort_ff -t $i -m $USABLE_MEM $INPUT_FILE | tee -a results/$LOG_FILE
            $SRUN /bin/rm $OUTPUT_FILE
        done
    done
}

run_seq() {
    echo "#################################" | tee -a results/$LOG_FILE
    echo -e "(sequential binary merge, max_mem=$USABLE_MEM)" | tee -a results/$LOG_FILE
    for j in $(seq 1 "$NRUNS")
    do
        $SRUN ./mergesort_seq -m $USABLE_MEM $INPUT_FILE | tee -a results/$LOG_FILE
        $SRUN /bin/rm $OUTPUT_FILE
    done

    echo "#################################" | tee -a results/$LOG_FILE
    echo -e "(sequential kway merge, max_mem=$USABLE_MEM)" | tee -a results/$LOG_FILE
    for j in $(seq 1 "$NRUNS")
    do
        $SRUN ./mergesort_seq -k -m $USABLE_MEM $INPUT_FILE | tee -a results/$LOG_FILE
        $SRUN /bin/rm $OUTPUT_FILE
    done
}

run_mpi_strong() {
    if [[ -z "$SRUN" ]]; then
        return 1
    fi

    echo "#################################" | tee -a results/$LOG_FILE
    LOG_FILE=run_$(date +%s)_mpi_strong.log
    NTHREADS=16
    for i in "${NODE_COUNTS[@]}"; do
        NODELIST=$MAIN_NODE
        for ((n=0; n<i-1; n++)); do
            NODELIST+=",${WORKER_NODES[n]}"
        done
        for j in $(seq 1 $NRUNS); do
            SRUN_MPI="srun --nodelist=${NODELIST} --ntasks-per-node=1 --mpi=pmix"
            echo -e "(nnodes=$i, nthreads=$NTHREADS, max_mem=$USABLE_MEM)" | tee -a results/$LOG_FILE
            $SRUN_MPI ./mergesort_mpi -t $NTHREADS -m $USABLE_MEM $INPUT_FILE | tee -a results/$LOG_FILE
            $SRUN /bin/rm -f $OUTPUT_FILE
        done
    done
    echo "#################################" | tee -a results/$LOG_FILE
}

run_mpi_weak() {
    if [[ -z "$SRUN" ]]; then
        return 1
    fi
    LOG_FILE=run_$(date +%s)_mpi_weak.log
    echo "#################################" | tee -a results/$LOG_FILE
    NTHREADS=16
    for i in "${NODE_COUNTS[@]}"; do
        NODELIST=$MAIN_NODE
        for ((n=0; n<i-1; n++)); do
            NODELIST+=",${WORKER_NODES[n]}"
        done
        for j in $(seq 1 $NRUNS); do
            SRUN_MPI="srun --nodelist=${NODELIST} --ntasks-per-node=1 --mpi=pmix"
            echo -e "(nnodes=$i, filesize=$($SRUN stat -c%s $INPUT_FILE | tr -d '\n'), nthreads=$NTHREADS, max_mem=$USABLE_MEM)" | tee -a results/$LOG_FILE
            $SRUN_MPI ./mergesort_mpi -t $NTHREADS -m $USABLE_MEM $INPUT_FILE | tee -a results/$LOG_FILE
            $SRUN /bin/rm -f $OUTPUT_FILE
        done
        # Double the input size for each run
        $SRUN bash -c "cat $INPUT_FILE $INPUT_FILE >> $INPUT_FILE.tmp"
        $SRUN mv $INPUT_FILE.tmp $INPUT_FILE
    done
    echo "#################################" | tee -a results/$LOG_FILE
}

cleanup() {
    $SRUN rm -f $INPUT_FILE
    $SRUN rm -f $OUTPUT_FILE
}

trap cleanup EXIT
run_seq
run_parallel
echo "#################################" | tee -a results/$LOG_FILE
run_mpi_strong
run_mpi_weak
