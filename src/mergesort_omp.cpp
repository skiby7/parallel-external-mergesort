#include <cstddef>
#include <cstdint>
#include <iostream>
#include <iterator>
#include <string>
#include <utility>
#include <vector>
#include <cstdio>
#include <omp.h>
#include <cassert>
#include "include/cmdline.hpp"
#include "include/common.hpp"
#include "include/config.hpp"
#include "include/filesystem.hpp"
#include "include/hpc_helpers.hpp"
#include "include/sorting.hpp"



int main(int argc, char *argv[]) {
    int start = 0;
    if((start = parseCommandLine(argc, argv)) < 0) return -1;
    std::string filename = argv[start];

    assert(!checkSortedFile(filename));
    TIMERSTART(mergesort_omp)
    std::vector<std::pair<size_t, size_t>> chunks = computeChunks(filename, omp_get_max_threads());
    for(auto& chunk : chunks)
        std::cout << chunk.first << " " << chunk.second << " size: " << chunk.second - chunk.first << std::endl;
    // size_t total_size = 0;
    #pragma omp parallel for
    for (size_t i = 0; i < chunks.size(); i++) {
        size_t size = chunks[i].second - chunks[i].first;
        genSequenceFiles(filename, chunks[i].first, size, MAX_MEMORY/omp_get_max_threads(), "/tmp/run#" + generateUUID());
    }


    std::vector<std::string> sequences = findFiles("/tmp/run");
    std::vector<std::vector<std::string>> levels;

    levels.push_back({});
    if (sequences.size() % 2) {
        levels[0].push_back(sequences.back());
        sequences.pop_back();
    }

    #pragma omp parallel for
    for (size_t i = 0; i < sequences.size() - 1; i+=2) {
        std::string filename = "/tmp/merge#" + generateUUID();
        // This code merges the two sequences and then stores the result in filename
        mergeFiles(sequences[i], sequences[i + 1], filename, MAX_MEMORY/omp_get_max_threads());
        #pragma omp critical
        levels[0].push_back(filename);
    }

    size_t current_level = 1;
    while (levels.back().size() > 1) {
        levels.push_back({});
        if (levels[current_level - 1].size() % 2) {
            levels[current_level].push_back(levels[current_level - 1].back());
            levels[current_level - 1].pop_back();
        }
        #pragma omp parallel for
        for (size_t i = 0; i < levels[current_level - 1].size() - 1; i += 2) {
            std::string filename = "/tmp/merge#" + generateUUID();;
            mergeFiles(levels[current_level - 1][i], levels[current_level - 1][i + 1], filename, MAX_MEMORY/omp_get_max_threads());
            #pragma omp critical
            levels[current_level].push_back(filename);
        }
        current_level++;
    }

    rename(levels.back().back().c_str(), "/tmp/output.dat");

    std::cout << "Output file size: " << getFileSize("/tmp/output.dat") << std::endl;
    TIMERSTOP(mergesort_omp)
    assert(checkSortedFile("/tmp/output.dat"));
    return 0;
}
