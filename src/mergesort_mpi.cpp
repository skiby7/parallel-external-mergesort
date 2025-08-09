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

    size_t file_size = getFileSize(filename);
    MAX_MEMORY = std::min(MAX_MEMORY, file_size + (file_size/10));
    int rank, size;
    int provided;
    MPI_Init_thread(&argc, &argv, MPI_THREAD_MULTIPLE, &provided);
    if (provided < MPI_THREAD_MULTIPLE) {
        if (rank == 0) std::cerr << "MPI does not support multithreading.\n";
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    if (rank == 0) {
        TIMERSTART(mergesort_mpi)
        master(filename, size);
        TIMERSTOP(mergesort_mpi)
    }
    else worker(TMP_LOCATION);

    MPI_Finalize();

    return 0;
}
