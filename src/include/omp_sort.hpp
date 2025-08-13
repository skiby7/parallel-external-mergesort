#ifndef _OMP_SORT_HPP
#define _OMP_SORT_HPP

#include "common.hpp"
#include "config.hpp"
#include "sorting.hpp"
#include <cstddef>
#include <filesystem>
#include <omp.h>
#include <string>
#include <vector>


static void ompMerge(const std::vector<std::string>& sequences, const std::string& merge_prefix, const std::string& output_file) {
    const size_t num_threads = omp_get_max_threads();

    if (sequences.empty()) return;
    if (sequences.size() == 1) {
        std::filesystem::rename(sequences[0], output_file);
        return;
    }

    /**
     * If not all the threads can contribute to the merge
     * just merge them altogether
     */
    if (sequences.size() < 2 * num_threads) {
        kWayMergeFiles(sequences, output_file, MAX_MEMORY);
        return;
    }

    /* Ceil division */
    const size_t group_size = (sequences.size() + num_threads - 1) / num_threads;
    std::vector<std::string> intermediate_files(num_threads);

    /**
     * This all threads from 0 to n-1 take exactly group_size sequences
     * while the last thread takes the remaining sequences
     */
    #pragma omp parallel for
    for (size_t i = 0; i < num_threads; i++) {
        size_t start = i * group_size;
        size_t end = std::min(start + group_size, sequences.size());
        if (start >= end) continue;

        std::vector<std::string> group(sequences.begin() + start, sequences.begin() + end);
        std::string filename = merge_prefix + generateUUID();
        kWayMergeFiles(group, filename, MAX_MEMORY / num_threads);
        intermediate_files[i] = filename;
    }

    /* Final merge of intermediate files */
    std::string final_file = merge_prefix + generateUUID();
    kWayMergeFiles(intermediate_files, output_file, MAX_MEMORY);
}


#endif  // _OMP_SORT_HPP
