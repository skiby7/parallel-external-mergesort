#!/bin/bash
#SBATCH --job-name=mergesort
#SBATCH --output=results/slurm_%j.out
#SBATCH --error=results/slurm_%j.err
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --nodelist=node08

INPUT_FILE=$1
if [ -z "$INPUT_FILE" ]; then
    echo "Usage: $0 <input_file>"
    exit 1
fi
OUTPUT_FILE=$(dirname $INPUT_FILE)/output.dat
FILESIZE=$(stat -c%s $INPUT_FILE)
HALF_FILESIZE=$((FILESIZE/2))
AVAIL_MEM=$(free -b | awk '/Mem:/ { print $7 }')
MAX_MEMORY=$((32 * 1024 * 1024 * 1024))
USABLE_MEM=$(
  echo "$HALF_FILESIZE $AVAIL_MEM $MAX_MEMORY" |
  awk '{ min = $1; for (i = 2; i <= NF; i++) if ($i < min) min = $i; print min }'
)
LOG_FILE=run_$(date +%s).log
THREAD_COUNTS=(
    4
    8
    12
    16
    20
    32
    48
    64
    80
)
mkdir -p results
make -j

echo "" > results/$LOG_FILE

run_parallel() {
    for i in "${THREAD_COUNTS[@]}"
    do
        echo "#################################" | tee -a results/$LOG_FILE
        echo -e "(nthreads=$i, max_mem=$USABLE_MEM)" | tee -a results/$LOG_FILE
        for j in {1..3}
        do
            ./mergesort_omp -t $i -m $USABLE_MEM $INPUT_FILE | tee -a results/$LOG_FILE
            /bin/rm /tmp/output.dat
            ./mergesort_ff -t $i -m $USABLE_MEM $INPUT_FILE | tee -a results/$LOG_FILE
            /bin/rm /tmp/output.dat
        done
    done
}

run_seq() {
    echo "#################################" | tee -a results/$LOG_FILE
    echo -e "(sequential binary merge, max_mem=$USABLE_MEM)" | tee -a results/$LOG_FILE
    for j in {1..3}
    do
        ./mergesort_seq -k -m $USABLE_MEM $INPUT_FILE | tee -a results/$LOG_FILE
        /bin/rm /tmp/output.dat
    done

    echo "#################################" | tee -a results/$LOG_FILE
    echo -e "(sequential kway merge, max_mem=$USABLE_MEM)" | tee -a results/$LOG_FILE
    for j in {1..3}
    do
        ./mergesort_seq -k -m $USABLE_MEM $INPUT_FILE | tee -a results/$LOG_FILE
        /bin/rm /tmp/output.dat
    done
}

run_seq
run_parallel
echo "#################################" | tee -a results/$LOG_FILE
