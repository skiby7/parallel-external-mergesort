#include "include/cmdline.hpp"
#include "include/config.hpp"
#include "include/hpc_helpers.hpp"
#include "include/sorting.hpp"
#include <cassert>
#include <cstdio>
#include <filesystem>
#include <string>
#include <sys/types.h>
#include <vector>
#include <filesystem>


int main(int argc, char *argv[]) {
    int start = 0;
    if ((start = parseCommandLine(argc, argv)) < 0)
        return -1;
    std::string filename = argv[start];

    std::filesystem::path p(filename);
    std::string run_prefix = p.parent_path().string() + "/run#";
    std::vector<std::string> sequences;
    TIMERSTART(std_sort)
    sequences = genSequenceFilesSTL(filename, 0, getFileSize(filename), MAX_MEMORY, run_prefix);
    TIMERSTOP(std_sort)
    std::cout << "Number of sorted runs: " << sequences.size() << std::endl;
    for (const auto& seq : sequences) {
        deleteFile(seq.c_str());
    }

    TIMERSTART(snow_plow)
    sequences = genSequenceFiles(filename, 0, getFileSize(filename), MAX_MEMORY, run_prefix);
    TIMERSTOP(snow_plow)
    std::cout << "Number of sorted runs: " << sequences.size() << std::endl;
    for (const auto& seq : sequences) {
        deleteFile(seq.c_str());
    }

    return 0;
}
