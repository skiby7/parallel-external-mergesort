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


static std::vector<std::string> genRuns(const std::string& filename, const std::string& run_prefix) {
    size_t num_threads = omp_get_max_threads();
    size_t file_size = getFileSize(filename);
    size_t max_mem_per_worker = MAX_MEMORY / num_threads;
    size_t chunk_size = std::min(file_size / 100, 3 * max_mem_per_worker); // 1% of the file or the memory per worker to keep them busy
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Error opening file: " << strerror(errno) << std::endl;
        exit(EXIT_FAILURE);
    }

    std::vector<char> buffer(max_mem_per_worker);
    size_t buffer_offset = 0;
    size_t file_offset = 0;

    size_t start_offset = 0;
    size_t end_offset = 0;

    std::vector<std::vector<std::string>> sequences(num_threads);

    #pragma omp parallel
    {
        #pragma omp single
        {
            size_t bytes_in_buffer = 0;

            while (true) {
                if (buffer_offset < bytes_in_buffer) {
                    memmove(buffer.data(), buffer.data() + buffer_offset, bytes_in_buffer - buffer_offset);
                    bytes_in_buffer -= buffer_offset;
                } else {
                    bytes_in_buffer = 0;
                }
                buffer_offset = 0;

                ssize_t bytes_read = read(fd, buffer.data() + bytes_in_buffer, buffer.size() - bytes_in_buffer);
                if (bytes_read < 0) {
                    std::cerr << "Read error: " << strerror(errno) << std::endl;
                    close(fd);
                    exit(EXIT_FAILURE);
                } else if (bytes_read == 0 && bytes_in_buffer == 0) {
                    // EOF
                    break;
                }
                bytes_in_buffer += bytes_read;

                while (buffer_offset + sizeof(uint64_t) + sizeof(uint32_t) <= bytes_in_buffer) {
                    size_t local_offset = buffer_offset;

                    buffer_offset += sizeof(uint64_t);

                    uint32_t len = *reinterpret_cast<uint32_t*>(&buffer[buffer_offset]);
                    buffer_offset += sizeof(uint32_t);

                    if (buffer_offset + len > bytes_in_buffer) {
                        buffer_offset = local_offset;
                        break;
                    }

                    buffer_offset += len;
                    end_offset = file_offset + buffer_offset;

                    if (end_offset - start_offset >= chunk_size) {
                        size_t size = end_offset - start_offset;
                        std::string uuid = generateUUID();
                        #pragma omp task firstprivate(start_offset, size, uuid)
                        {

                            std::vector<std::string> seq = genSortedRunsWithSort(filename, start_offset, size, max_mem_per_worker, run_prefix + uuid);
                            sequences[omp_get_thread_num()]
                                .insert(
                                    sequences[omp_get_thread_num()].end(),
                                    std::make_move_iterator(seq.begin()),
                                    std::make_move_iterator(seq.end())
                                );
                        }
                        start_offset = end_offset;
                    }
                }

                file_offset += buffer_offset;
            }


            if (end_offset > start_offset) {
                size_t size = end_offset - start_offset;
                std::string uuid = generateUUID();
                #pragma omp task firstprivate(start_offset, size, uuid)
                {
                    std::vector<std::string> seq = genSortedRunsWithSort(filename, start_offset, size, max_mem_per_worker, run_prefix + uuid);
                    sequences[omp_get_thread_num()]
                        .insert(
                            sequences[omp_get_thread_num()].end(),
                            std::make_move_iterator(seq.begin()),
                            std::make_move_iterator(seq.end())
                        );
                }
            }

            #pragma omp taskwait
            close(fd);
        }
    }
    std::vector<std::string> all_sequences;

    size_t total_size = 0;
    for (const auto& v : sequences) total_size += v.size();
    all_sequences.reserve(total_size);

    for (auto& inner_vec : sequences) {
        all_sequences.insert(all_sequences.end(),
            std::make_move_iterator(inner_vec.begin()),
            std::make_move_iterator(inner_vec.end()));
    }
    all_sequences.erase(
        std::remove_if(all_sequences.begin(), all_sequences.end(),
                       [](const std::string& f) { return f.empty(); }),
        all_sequences.end()
    );

    return all_sequences;
}

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

    intermediate_files.erase(
        std::remove_if(intermediate_files.begin(), intermediate_files.end(),
                       [](const std::string& f) { return f.empty(); }),
        intermediate_files.end()
    );

    /* Final merge of intermediate files */
    std::string final_file = merge_prefix + generateUUID();
    kWayMergeFiles(intermediate_files, output_file, MAX_MEMORY);
}


#endif  // _OMP_SORT_HPP
