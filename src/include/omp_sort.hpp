#ifndef _OMP_SORT_HPP
#define _OMP_SORT_HPP

#include "common.hpp"
#include "config.hpp"
#include "sorting.hpp"
#include <omp.h>
#include <string>
#include <vector>

static void ompMerge(const std::string& run_prefix, const std::string& merge_prefix, const std::string& output_file) {
    std::vector<std::string> sequences = findFiles(run_prefix);
    std::vector<std::vector<std::string>> levels;

    levels.push_back({});
    if (sequences.size() % 2) {
        levels[0].push_back(sequences.back());
        sequences.pop_back();
    }

    #pragma omp parallel for
    for (size_t i = 0; i < sequences.size() - 1; i+=2) {
        std::string filename = merge_prefix + generateUUID();
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
            std::string filename = merge_prefix + generateUUID();
            mergeFiles(levels[current_level - 1][i], levels[current_level - 1][i + 1], filename, MAX_MEMORY/omp_get_max_threads());
            #pragma omp critical
            levels[current_level].push_back(filename);
        }
        current_level++;
    }
    rename(levels.back().back().c_str(), output_file.c_str());
}
#endif  // _OMP_SORT_HPP
