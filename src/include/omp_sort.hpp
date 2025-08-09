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


static void ompBinaryMerge(const std::vector<std::string>& sequences,
                         const std::string& merge_prefix,
                         const std::string& output_file) {
    std::vector<std::vector<std::string>> levels;
    levels.push_back({});

    std::vector<std::string> input = sequences;

    if (input.size() % 2) {
        levels[0].push_back(input.back());
        input.pop_back();
    }

    const int num_threads = omp_get_max_threads();

    #pragma omp parallel for
    for (size_t i = 0; i < input.size(); i += 2) {
        std::string filename = merge_prefix + generateUUID();
        std::cout << "Merging two files" << std::endl;
        mergeFiles(input[i], input[i + 1], filename, MAX_MEMORY / num_threads);
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
        for (size_t i = 0; i < levels[current_level - 1].size(); i += 2) {
            std::string filename = merge_prefix + generateUUID();
            mergeFiles(levels[current_level - 1][i], levels[current_level - 1][i + 1], filename, MAX_MEMORY / num_threads);
            #pragma omp critical
            levels[current_level].push_back(filename);
        }

        ++current_level;
    }

    std::filesystem::rename(levels.back().back(), output_file);
}

static void ompMerge(const std::string& run_prefix, const std::string& merge_prefix, const std::string& output_file) {
    std::vector<std::string> sequences = findFiles(run_prefix);
    const size_t num_threads = omp_get_max_threads();

    if (sequences.empty()) return;

    if (sequences.size() == 1) {
        std::filesystem::rename(sequences[0], output_file);
        return;
    }

    // If many sequences, parallel k-way merge in chunks
    if (sequences.size() <= 2 * num_threads) {
        ompBinaryMerge(sequences, merge_prefix, output_file);
        return;
    }
    const size_t group_count = num_threads;
    const size_t group_size = (sequences.size() + group_count - 1) / group_count;

    std::vector<std::string> intermediate_files(group_count);

    #pragma omp parallel for
    for (size_t i = 0; i < group_count; ++i) {
        size_t start = i * group_size;
        size_t end = std::min(start + group_size, sequences.size());
        if (start >= end) continue;

        std::vector<std::string> group(sequences.begin() + start, sequences.begin() + end);
        std::string filename = merge_prefix + generateUUID();
        kWayMergeFiles(group, filename, MAX_MEMORY / num_threads);
        intermediate_files[i] = filename;
    }

    // Final merge of intermediate files (can fall back to pairwise or a single k-way merge)
    if (intermediate_files.size() < 2 * num_threads) {
        std::string final_file = merge_prefix + generateUUID();
        kWayMergeFiles(intermediate_files, final_file, MAX_MEMORY);
        std::filesystem::rename(final_file, output_file);
    } else ompBinaryMerge(intermediate_files, merge_prefix, output_file);
}


#endif  // _OMP_SORT_HPP
