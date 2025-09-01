# Parallel External Mergesort

This project implements and evaluates the performance of a **parallelized external mergesort algorithm** using:

- **OpenMP** and **FastFlow** for the single-node version.
- **MPI + OpenMP** for the distributed hybrid version.

---

## Project Structure

```
.
├── src/             # Source code, FastFlow library, and headers (.hpp)
├── scripts/         # Python scripts for plotting results and computing speedup
├── results/         # Execution logs with timing results
├── benchmark.sh     # Benchmark runner (local or cluster, via srun)
├── cluster_run.sh   # Preconfigured benchmark script for cluster execution
├── local_run.sh     # Preconfigured benchmark script for local execution
├── Makefile
```

---

## Running the Project

### Local Execution
`local_run.sh` and `benchmark.sh` accept 3 parameters:

1. `input_file`: Path to generate the test file (intermediate files stored in the same directory).
2. `max_payload_size`: Maximum payload size in bytes per record.
3. `number_of_records`: Number of records to generate.

**Example:**

```bash
./local_run.sh /tmp/test.dat 16 10000000
```

### Cluster Execution
For quick testing on the cluster, parameters are hardcoded in `cluster_run.sh`:

```bash
./cluster_run.sh
```

---

## Manual Compilation & Execution

### Compile
```bash
# Load MPI if required
module load mpi/openmpi-x86_64

# Build
make clean
make -j $(nproc)
```

### Generate Input & Run
```bash
# Generate test file
./gen_file -r payload_size -s n_records /path/to/file

# Run one of the implementations (example with FastFlow)
./mergesort_ff -t nthreads -m max_usable_mem_bytes /path/to/file
```

> All executables share the same parameter format.

---

## Options

Each executable supports the following options:

```
Usage: ./executable [options]

Options:
 -t T        Number of threads (default = NTHREADS)
 -k          Use k-way merge in sequential version (default = true/false)
 -m M        Set maximum memory usage in bytes (default = MAX_MEMORY)
 -p string   Temporary location for MPI worker nodes (default = TMP_LOCATION)
 -x          Disable FastFlow thread pinning (default = true/false)
 -y          Enable FastFlow blocking mode (default = true/false)
```

---

## Log Parsing & Plotting

Execution logs can be parsed and visualized with the provided Python script.

### Install requirements:
```bash
pip install plotly numpy
```

### Plot results:
```bash
# Point to the log file (omit _mpi* suffix)
python3 scripts/plot.py /path/to/logfile
```

This will generate plots for runtime and speedup comparison.

---

## License
This project is for academic and research purposes.
