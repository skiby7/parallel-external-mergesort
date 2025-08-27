#include <cstddef>
#include <string>
#include <sys/select.h>
#include <vector>
#include <cstdio>
#include <omp.h>
#include <cassert>
#include <filesystem>
#include "include/cmdline.hpp"
#include "include/common.hpp"
#include "include/config.hpp"
#include "include/hpc_helpers.hpp"
#include "include/omp_sort.hpp"



int main(int argc, char *argv[]) {
    int start = 0;

    if((start = parseCommandLine(argc, argv)) < 0) return -1;
    omp_set_num_threads(NTHREADS);
    std::string filename = argv[start];
    size_t file_size = getFileSize(filename);
    MAX_MEMORY = std::min(MAX_MEMORY, file_size + (file_size/10));

    std::filesystem::path p(filename);
    std::string run_prefix = p.parent_path().string() + "/run#";
    std::string merge_prefix = p.parent_path().string() + "/merge#";
    std::string output_file = p.parent_path().string() + "/output.dat";

    TIMERSTART(mergesort_omp)
    std::vector<std::string> sequences = genRuns(filename, run_prefix);
    ompMerge(sequences, merge_prefix, output_file);
    TIMERSTOP(mergesort_omp)

    return 0;
}
