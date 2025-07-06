#include <vector>
#include <cstdio>
#include <omp.h>
#include <cassert>
#include "include/cmdline.hpp"
#include "include/common.hpp"
#include "include/config.hpp"
#include "include/hpc_helpers.hpp"
#include "include/sorting.hpp"



int main(int argc, char *argv[]) {
    int start = 0;
    if((start = parseCommandLine(argc, argv)) < 0) return -1;
    std::string filename = argv[start];

    assert(!checkSortedFile(filename));
    TIMERSTART(mergesort_seq)
    genSequenceFiles(filename, 0, getFileSize(filename), MAX_MEMORY, "/tmp/run");
    std::vector<std::string> sequences = findFiles("/tmp/run");
    std::vector<std::vector<std::string>> next_level;
    next_level.push_back({});
    if (sequences.size() % 2) {
        next_level[0].push_back(sequences.back());
        sequences.pop_back();
    }
    #pragma omp parallel for
    for (size_t i = 0; i < sequences.size() - 1; i+=2) {
        next_level[0].push_back(mergeFiles(sequences[i], sequences[i + 1], MAX_MEMORY/omp_get_max_threads()));
    }
    size_t current_level = 1;
    while (next_level.back().size() > 1) {
        next_level.push_back({});
        #pragma omp parallel for
        for (size_t i = 0; i < next_level[current_level - 1].size() - 1; i += 2) {
            next_level[current_level].push_back(mergeFiles(next_level[current_level - 1][i], next_level[current_level - 1][i + 1], MAX_MEMORY/omp_get_max_threads()));
        }
        if (next_level[current_level - 1].size() % 2) {
            next_level[current_level].push_back(next_level[current_level - 1].back());
        }
        current_level++;
    }

    rename(next_level.back().back().c_str(), "/tmp/output.dat");
    TIMERSTOP(mergesort_seq)
    assert(checkSortedFile("/tmp/output.dat"));
    // destroyArray(records);
    return 0;

}
