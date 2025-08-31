#!/bin/bash

INPUT_FILE=$1
PAYLOAD_SIZE=$2
ITEMS_COUNT=$3
if [ -z "$INPUT_FILE" ] || [ -z "$PAYLOAD_SIZE" ] || [ -z "$ITEMS_COUNT" ]; then
    echo "Usage: $0 <input_file> <payload_size> <items_count>"
    echo "Specify SRUN=1 to schedule using slurm"
    exit 1
fi

NODE_NAME="node"
MAIN_NODE="${NODE_NAME}01"
MAX_NODES=${MAX_NODES:-8}


if [ -n "$SRUN" ]; then
    SRUN="srun -w $MAIN_NODE"
else
    SRUN=""
fi
OUTPUT_FILE=$(dirname $INPUT_FILE)/output.dat
# Using ls return code because when running on the cluster, it has to be executed remotely and not on the login node
$SRUN ls $INPUT_FILE &> /dev/null
INPUT_EXISTS=$?
if [ $INPUT_EXISTS -eq 0 ]; then
    echo "Input file already exists, deleting it..."
    $SRUN rm -f $INPUT_FILE
    # exit 1
fi
$SRUN rm -f $OUTPUT_FILE
$SRUN mkdir -p $(dirname $INPUT_FILE)

genFile() {
    echo "Generating input file with at most $PAYLOAD_SIZE bytes per item and $ITEMS_COUNT items..."
    $SRUN rm -f $INPUT_FILE
    $SRUN ./gen_file -r $PAYLOAD_SIZE -s $ITEMS_COUNT $INPUT_FILE
    FILESIZE=$($SRUN stat -c%s $INPUT_FILE)
    FILESIZE_FRAC=$((FILESIZE/10)) # Simulating memory constraints
    AVAIL_MEM=$($SRUN free -b | awk '/Mem:/ { print $7 }')
    MAX_MEMORY=$((32 * 1024 * 1024 * 1024)) # 32GB
    # Once set it is never updated to make sure that every test runs with the same memory constraints
    if [ -z "$USABLE_MEM" ]; then
      USABLE_MEM=$(
        echo "$FILESIZE_FRAC $AVAIL_MEM $MAX_MEMORY" |
        awk '{ min = $1; for (i = 2; i <= NF; i++) if ($i < min) min = $i; print min }'
      )
    fi
}


TIMESTAMP=$(date +%s)
LOG_FILE=run_${TIMESTAMP}.log

NRUNS=${NRUNS:-3}
MAX_THREADS=${MAX_THREADS:-32}
THREADS_STEP=${THREADS_STEP:-4}

mkdir -p results
echo "" > results/$LOG_FILE

$SRUN make clean
$SRUN make -j
$SRUN make gen_file


run_seq() {
    echo "#################################" | tee -a results/$LOG_FILE
    echo -e "(sequential binary merge, max_mem=$USABLE_MEM)" | tee -a results/$LOG_FILE
    for j in $(seq 1 "$NRUNS")
    do
        $SRUN ./mergesort_seq -m "$USABLE_MEM" "$INPUT_FILE" | tee -a results/$LOG_FILE
        $SRUN /bin/rm $OUTPUT_FILE
    done

    echo "#################################" | tee -a results/$LOG_FILE
    echo -e "(sequential kway merge, max_mem=$USABLE_MEM)" | tee -a results/$LOG_FILE
    for j in $(seq 1 "$NRUNS")
    do
        $SRUN ./mergesort_seq -k -m "$USABLE_MEM" "$INPUT_FILE" | tee -a results/$LOG_FILE
        $SRUN /bin/rm $OUTPUT_FILE
    done
}

run_parallel() {
    # Including also the 2 and always finishing with MAX_THREADS
    for i in 2 $(seq 4 $THREADS_STEP $(($MAX_THREADS - 1))) $MAX_THREADS
    do
        if [[ $i -eq 0 ]]; then
            continue
        fi
        echo "#################################" | tee -a results/$LOG_FILE
        echo -e "(nthreads=$i, max_mem=$USABLE_MEM)" | tee -a results/$LOG_FILE
        for j in $(seq 1 "$NRUNS")
        do
            $SRUN ./mergesort_omp -t "$(($i - ${REDUCE_GAP:-0}))" -m "$USABLE_MEM" "$INPUT_FILE" | tee -a results/$LOG_FILE
            $SRUN /bin/rm $OUTPUT_FILE
            $SRUN ./mergesort_ff -t "$i" -m "$USABLE_MEM" "$INPUT_FILE" | tee -a results/$LOG_FILE
            $SRUN /bin/rm $OUTPUT_FILE
            $SRUN ./mergesort_ff -x -t "$i" -m "$USABLE_MEM" "$INPUT_FILE" | tee -a results/$LOG_FILE
            $SRUN /bin/rm $OUTPUT_FILE
            $SRUN ./mergesort_ff -y -t "$i" -m "$USABLE_MEM" "$INPUT_FILE" | tee -a results/$LOG_FILE
            $SRUN /bin/rm $OUTPUT_FILE
        done
    done
}

run_mpi_strong() {
    LOG_FILE=run_${TIMESTAMP}_mpi_strong.log
    touch results/$LOG_FILE
    echo "#################################" | tee -a results/$LOG_FILE
    NTHREADS=${MPI_THREADS-32} # Using the best speedup from the single node version as default
    for i in $(seq 2 ${MAX_NODES}); do
        NODELIST=$MAIN_NODE
        for ((n=1; n<i+1; n++)); do
            WORKER="${NODE_NAME}0$n"
            if [[ $WORKER != $MAIN_NODE ]]; then
                NODELIST+=",$WORKER"
            fi
        done
        echo -e "(nnodes=$i, nthreads=$NTHREADS, max_mem=$USABLE_MEM)" | tee -a results/$LOG_FILE
        if [[ -z "$SRUN" ]]; then
            MPI_RUN="mpirun -np $i"
        fi
        SRUN_MPI=${MPI_RUN-"srun --nodelist=${NODELIST} --ntasks-per-node=1 --mpi=pmix"}
        for j in $(seq 1 $NRUNS); do
            $SRUN_MPI ./mergesort_mpi -t "$NTHREADS" -m "$USABLE_MEM" "$INPUT_FILE" | tee -a results/$LOG_FILE
            $SRUN /bin/rm -f $OUTPUT_FILE
        done
    done
    echo "#################################" | tee -a results/$LOG_FILE
}

run_mpi_weak() {
    LOG_FILE=run_${TIMESTAMP}_mpi_weak.log
    touch results/$LOG_FILE
    INITIAL_ITEMS_COUNT=$ITEMS_COUNT
    echo "#################################" | tee -a results/$LOG_FILE
    NTHREADS=${MPI_THREADS-32} # Using the best speedup from the single node version as default
    # Now the size is twice the single node version
    for i in $(seq 2 ${MAX_NODES}); do
        NODELIST=$MAIN_NODE
        for ((n=1; n<i+1; n++)); do
            WORKER="${NODE_NAME}0$n"
            if [[ $WORKER != $MAIN_NODE ]]; then
                NODELIST+=",$WORKER"
            fi
        done
        genFile
        echo -e "(nnodes=$i, filesize=$($SRUN stat -c%s $INPUT_FILE | tr -d '\n'), nthreads=$NTHREADS, max_mem=$USABLE_MEM)" | tee -a results/$LOG_FILE
        if [[ -z "$SRUN" ]]; then
            MPI_RUN="mpirun -np $i"
        fi
        SRUN_MPI=${MPI_RUN-"srun --nodelist=${NODELIST} --ntasks-per-node=1 --mpi=pmix"}
        for j in $(seq 1 $NRUNS); do
            $SRUN_MPI ./mergesort_mpi -t "$NTHREADS" -m "$USABLE_MEM" "$INPUT_FILE" | tee -a results/$LOG_FILE
            $SRUN /bin/rm -f $OUTPUT_FILE
        done
        # Increase the input size for each run
        ITEMS_COUNT=$((ITEMS_COUNT + INITIAL_ITEMS_COUNT))
    done
    echo "#################################" | tee -a results/$LOG_FILE
}

cleanup() {
    $SRUN rm -f $INPUT_FILE
    $SRUN rm -f $OUTPUT_FILE
}

trap cleanup EXIT
genFile
run_seq
run_parallel
echo "#################################" | tee -a results/$LOG_FILE
run_mpi_strong
run_mpi_weak
