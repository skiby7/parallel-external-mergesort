#include "include/cmdline.hpp"
#include "include/config.hpp"
#include "include/hpc_helpers.hpp"
#include "include/mpi_master.hpp"
#include "include/mpi_worker.hpp"
#include <mpi.h>
#include <iostream>
#include <string>
#include <cstdio>
#include <omp.h>
#include <cassert>



int main(int argc, char *argv[]) {
    int start = 0;
    if ((start = parseCommandLine(argc, argv)) < 0)
        return -1;
    std::string filename = argv[start];


    int rank, size;
    int provided;

    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    if (size < 2) {
        if (rank == 0) std::fprintf(stderr, "Need at least 2 ranks (1 master + >=1 worker)\n");
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    if (provided < MPI_THREAD_MULTIPLE) {
        if (rank == 0) std::cerr << "MPI does not support multithreading.\n";
        MPI_Abort(MPI_COMM_WORLD, 1);
    }
    if (rank == 0) {
         size_t file_size = getFileSize(filename);
         MAX_MEMORY = std::min(MAX_MEMORY, file_size + (file_size / 10));
    }
    uint64_t max_mem_wire = (rank == 0) ? (uint64_t)MAX_MEMORY : 0;
    MPI_Bcast(&max_mem_wire, 1, MPI_UNSIGNED_LONG_LONG, 0, MPI_COMM_WORLD);
    MAX_MEMORY = (size_t)max_mem_wire;

    if (rank == 0) {
        TIMERSTART(mergesort_mpi)
        master(filename, size);
        TIMERSTOP(mergesort_mpi)
    }
    else worker(TMP_LOCATION, size);

    MPI_Finalize();

    return 0;
}
