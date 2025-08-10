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

void multiLevelMerge(std::vector<std::string> &sequences, const std::string &merge_prefix, const std::string &output_file, size_t max_memory) {
    std::vector<std::vector<std::string>> levels;
    levels.push_back({});
    if (sequences.size() % 2) {
        levels[0].push_back(sequences.back());
        sequences.pop_back();
    }

    for (size_t i = 0; i < sequences.size() - 1; i+=2) {
        std::string filename = merge_prefix + generateUUID();
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
               std::string filename = merge_prefix + generateUUID();
               mergeFiles(levels[current_level - 1][i], levels[current_level - 1][i + 1], filename, MAX_MEMORY);
               levels[current_level].push_back(filename);
           }
           current_level++;
       }

       std::filesystem::rename(levels.back().back(), output_file);
}

int main(int argc, char *argv[]) {
    int start = 0;
    if ((start = parseCommandLine(argc, argv)) < 0)
        return -1;
    std::string filename = argv[start];
    size_t file_size = getFileSize(filename);
    MAX_MEMORY = std::min(MAX_MEMORY, file_size + (file_size/10));
    std::filesystem::path p(filename);
    std::string run_prefix = p.parent_path().string() + "/run#";
    std::string merge_prefix = p.parent_path().string() + "/merge#";
    std::string output_file = p.parent_path().string() + "/output.dat";
    TIMERSTART(mergesort_seq)
    std::vector<std::string> sequences = genSortedRunsWithSort(filename, 0, getFileSize(filename), MAX_MEMORY, run_prefix);
    if (sequences.size() == 1)
        std::filesystem::rename(sequences[0], output_file);
    else {
        if (KWAY_MERGE)
            kWayMergeFiles(sequences, output_file, MAX_MEMORY);
        else
            multiLevelMerge(sequences, merge_prefix, output_file, MAX_MEMORY);
    }
    TIMERSTOP(mergesort_seq)
    return 0;
}
