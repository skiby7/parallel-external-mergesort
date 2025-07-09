#include <cstddef>
#include <iostream>
#include <string>
#include <vector>
#include <cstdio>
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
    TIMERSTART(mergesort_seq)
    genSequenceFiles(filename, 0, getFileSize(filename), MAX_MEMORY, "/tmp/run");
    std::vector<std::string> sequences = findFiles("/tmp/run");

    std::vector<std::vector<std::string>> levels;
    levels.push_back({});
    if (sequences.size() % 2) {
        levels[0].push_back(sequences.back());
        sequences.pop_back();
    }
    size_t total_size = 0;
    for (auto& file : sequences) {
        total_size += getFileSize(file);
    }

    for (size_t i = 0; i < sequences.size() - 1; i+=2) {
        std::string filename = "/tmp/merge#" + generateUUID();
        mergeFiles(sequences[i], sequences[i + 1], filename, MAX_MEMORY);
        levels[0].push_back(filename);
    }

    size_t current_level = 1;
    while (levels.back().size() > 1) {
        levels.push_back({});
        if (levels[current_level - 1].size() % 2) {
            levels[current_level].push_back(levels[current_level - 1].back());
            levels[current_level - 1].pop_back();
        }
        for (size_t i = 0; i < levels[current_level - 1].size() - 1; i += 2) {
            std::string filename = "/tmp/merge#" + generateUUID();;
            mergeFiles(levels[current_level - 1][i], levels[current_level - 1][i + 1], filename, MAX_MEMORY);
            levels[current_level].push_back(filename);
        }
        current_level++;
    }

    rename(levels.back().back().c_str(), "/tmp/output.dat");

    TIMERSTOP(mergesort_seq)
    assert(checkSortedFile("/tmp/output.dat"));
    return 0;
}
