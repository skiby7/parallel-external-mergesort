#include <vector>
#include <omp.h>
#include <cassert>
#include "include/cmdline.hpp"
#include "include/common.hpp"
#include "include/hpc_helpers.hpp"

int main(int argc, char *argv[]) {
    if(parseCommandLine(argc, argv) < 0) {
        return -1;
    }
    std::vector<Record> records;
    generateArray(records);
    TIMERSTART(mergesort_seq)
    std::sort(records.begin(), records.end(),
              [](const Record a, const Record b) {
                  return a.key < b.key;
              });
    TIMERSTOP(mergesort_seq)
    assert(checkSorted(records));
    destroyArray(records);
    return 0;
    
}
