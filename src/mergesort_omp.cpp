#include <cstddef>
#include <iostream>
#include <string>
#include <utility>
#include <vector>
#include <cstdio>
#include <omp.h>
#include <cassert>
#include "include/cmdline.hpp"
#include "include/common.hpp"
#include "include/config.hpp"
#include "include/filesystem.hpp"
#include "include/hpc_helpers.hpp"
#include "include/sorting.hpp"

void computeChunksAndProcess(const std::string& filename, size_t num_threads) {
    size_t file_size = getFileSize(filename);
    size_t chunk_size = file_size / num_threads;

    int fd = open(filename.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Error opening file: " << strerror(errno) << std::endl;
        exit(EXIT_FAILURE);
    }

    std::vector<char> buffer(MAX_MEMORY / num_threads);
    size_t buffer_offset = 0;
    size_t file_offset = 0;

    size_t logical_start = 0;
    size_t logical_end = 0;

    #pragma omp parallel
    {
        #pragma omp single
        {
            size_t bytes_in_buffer = 0;

            while (true) {
                // Shift leftover bytes to the start
                if (buffer_offset < bytes_in_buffer) {
                    memmove(buffer.data(), buffer.data() + buffer_offset, bytes_in_buffer - buffer_offset);
                    bytes_in_buffer -= buffer_offset;
                } else {
                    bytes_in_buffer = 0;
                }
                buffer_offset = 0;

                // Read next chunk into buffer
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

                // Parse complete records from the buffer
                while (buffer_offset + sizeof(uint64_t) + sizeof(uint32_t) <= bytes_in_buffer) {
                    size_t local_offset = buffer_offset;

                    // uint64_t key = *reinterpret_cast<uint64_t*>(&buffer[buffer_offset]);
                    buffer_offset += sizeof(uint64_t);

                    uint32_t len = *reinterpret_cast<uint32_t*>(&buffer[buffer_offset]);
                    buffer_offset += sizeof(uint32_t);

                    if (buffer_offset + len > bytes_in_buffer) {
                        // Not enough bytes for full payload, wait for next read
                        buffer_offset = local_offset;
                        break;
                    }

                    // Skip the payload
                    buffer_offset += len;

                    logical_end = file_offset + buffer_offset;

                    // Emit chunk if we've gathered enough
                    if (logical_end - logical_start >= chunk_size) {
                        size_t size = logical_end - logical_start;
                        std::string uuid = generateUUID();
                        #pragma omp task firstprivate(logical_start, size, uuid)
                        {
                            genSequenceFiles(filename, logical_start, size, MAX_MEMORY / num_threads, "/tmp/run#" + uuid);
                        }
                        logical_start = logical_end;
                    }
                }

                file_offset += buffer_offset;
            }

            // Emit any remaining data at the end
            if (logical_end > logical_start) {
                size_t size = logical_end - logical_start;
                std::string uuid = generateUUID();
                #pragma omp task firstprivate(logical_start, size, uuid)
                {
                    genSequenceFiles(filename, logical_start, size, MAX_MEMORY / num_threads, "/tmp/run#" + uuid);
                }
            }

            close(fd);
            #pragma omp taskwait
        }
    }
}

int main(int argc, char *argv[]) {
    int start = 0;
    if((start = parseCommandLine(argc, argv)) < 0) return -1;
    omp_set_num_threads(NTHREADS);
    std::string filename = argv[start];

    // assert(!checkSortedFile(filename));

    TIMERSTART(mergesort_omp)
    computeChunksAndProcess(filename, omp_get_max_threads());

    std::vector<std::string> sequences = findFiles("/tmp/run");
    std::vector<std::vector<std::string>> levels;

    levels.push_back({});
    if (sequences.size() % 2) {
        levels[0].push_back(sequences.back());
        sequences.pop_back();
    }

    // kWayMergeFiles(sequences, "/tmp/output.dat", MAX_MEMORY);
    #pragma omp parallel for
    for (size_t i = 0; i < sequences.size() - 1; i+=2) {
        std::string filename = "/tmp/merge#" + generateUUID();
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
            std::string filename = "/tmp/merge#" + generateUUID();
            mergeFiles(levels[current_level - 1][i], levels[current_level - 1][i + 1], filename, MAX_MEMORY/omp_get_max_threads());
            #pragma omp critical
            levels[current_level].push_back(filename);
        }
        current_level++;
    }
    rename(levels.back().back().c_str(), "/tmp/output.dat");
    TIMERSTOP(mergesort_omp)

    assert(checkSortedFile("/tmp/output.dat"));
    return 0;
}
