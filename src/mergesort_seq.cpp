#include "include/cmdline.hpp"
#include "include/common.hpp"
#include "include/config.hpp"
#include "include/filesystem.hpp"
#include "include/hpc_helpers.hpp"
#include "include/sorting.hpp"
#include <cassert>
#include <cstddef>
#include <cstdio>
#include <iostream>
#include <string>
#include <vector>

int main(int argc, char *argv[]) {
    int start = 0;
    if ((start = parseCommandLine(argc, argv)) < 0)
        return -1;
    std::string filename = argv[start];

    assert(!checkSortedFile(filename));
    TIMERSTART(mergesort_seq)
    genSequenceFiles(filename, 0, getFileSize(filename), MAX_MEMORY,
                     "/tmp/run");
    std::vector<std::string> sequences = findFiles("/tmp/run");

    std::vector<std::vector<std::string>> levels;
    levels.push_back({});
    if (sequences.size() % 2) {
        levels[0].push_back(sequences.back());
        sequences.pop_back();
    }

    // REPLACE THE MERGE WITH THE KWAYMERGESORT
    kWayMergeFiles(sequences, "/tmp/output.dat", MAX_MEMORY);
    TIMERSTOP(mergesort_seq)
    assert(checkSortedFile("/tmp/output.dat"));
    return 0;
}
