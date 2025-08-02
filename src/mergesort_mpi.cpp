#include "include/cmdline.hpp"
#include "include/config.hpp"
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
    if (provided < MPI_THREAD_MULTIPLE) {
        if (rank == 0) std::cerr << "MPI does not support multithreading.\n";
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    std::cout << "Rank " << rank << std::endl;
    if (rank == 0) master(filename, size);
    else worker(TMP_LOCATION);

    MPI_Finalize();

    return 0;
}
