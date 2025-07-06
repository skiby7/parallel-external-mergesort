#include <vector>
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

    TIMERSTART(mergesort_seq)
    genSequenceFiles(filename, 0, getFileSize(filename), MAX_MEMORY, "/tmp/run");
    std::vector<std::string> sequences = findFiles("/tmp/run");
    std::vector<std::string> next_level;
    if (sequences.size() % 2) {
        next_level.push_back(sequences.back());
        sequences.pop_back();
    }
    for (int i = 0; i < sequences.size() - 1; i++) {
        next_level.push_back(mergeFiles(sequences[i], sequences[i + 1], MAX_MEMORY));
    }
    wh

    rename("", "/tmp/output.dat");
    TIMERSTOP(mergesort_seq)
    assert(checkSortedFile("/tmp/output.dat"));
    // destroyArray(records);
    return 0;

}
