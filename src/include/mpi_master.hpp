#ifndef _MPI_MASTER_HPP
#define _MPI_MASTER_HPP

#include "common.hpp"
#include "config.hpp"
#include "omp_sort.hpp"
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <iostream>
#include <mpi.h>
#include <string>
#include <unistd.h>
#include <vector>

/**
 * TODO:
 *   - Check this function
 *   - Receive the sorted files from the workers flushing the buffer to a single file for each worker
 *   - Merge the sorted files into a single file using OMP
 */
static void master(const std::string& filename, int world_size) {
    std::filesystem::path p(filename);
    std::string run_prefix = p.parent_path().string() + "/run#";
    std::string merge_prefix = p.parent_path().string() + "/merge#";
    std::string output_file = p.parent_path().string() + "/output.dat";
    int fd = openFile(filename);

    std::atomic<unsigned int> workers_done{0};
    const unsigned int num_workers = world_size - 1;

    size_t worker_idx = 0;
    std::vector<char> buffer(MAX_MEMORY);
    size_t bytes_in_buffer = 0;
    size_t buffer_offset = 0;

    std::vector<std::vector<char>> node_chunks(num_workers);
    while (true) {
        if (buffer_offset < bytes_in_buffer) {
            memmove(buffer.data(), buffer.data() + buffer_offset, bytes_in_buffer - buffer_offset);
            bytes_in_buffer -= buffer_offset;
        } else {
            bytes_in_buffer = 0;
        }
        buffer_offset = 0;

        ssize_t bytes_read = read(fd, buffer.data() + bytes_in_buffer, buffer.size() - bytes_in_buffer);

        if (bytes_read <= 0 && bytes_in_buffer == 0) break;
        if (bytes_read > 0) bytes_in_buffer += bytes_read;

        while (buffer_offset + sizeof(uint64_t) + sizeof(uint32_t) <= bytes_in_buffer) {
            size_t rec_start = buffer_offset;
            buffer_offset += sizeof(uint64_t);
            uint32_t len = *reinterpret_cast<uint32_t*>(&buffer[buffer_offset]);
            buffer_offset += sizeof(uint32_t);

            if (buffer_offset + len > bytes_in_buffer) {
                buffer_offset = rec_start;
                break;
            }

            size_t rec_size = sizeof(uint64_t) + sizeof(uint32_t) + len;
            std::vector<char> rec(rec_size);
            std::memcpy(rec.data(), &buffer[rec_start], rec_size);
            node_chunks[worker_idx].insert(node_chunks[worker_idx].end(), rec.begin(), rec.end());
            worker_idx = (worker_idx + 1) % num_workers;
            buffer_offset += len;
        }

        /* Send to workers. Starting from 1 because rank 0 is the master */
        for (unsigned int node = 1; node <= num_workers; node++) {
            int chunk_size = (int)node_chunks[node-1].size();
            MPI_Send(&chunk_size, 1, MPI_INT, node, 0, MPI_COMM_WORLD);
            if (chunk_size > 0)
                MPI_Send(node_chunks[node-1].data(), node_chunks[node-1].size(), MPI_CHAR, node, 0, MPI_COMM_WORLD);

            node_chunks[node-1].clear();
        }
    }
    close(fd);
    // Notify EOS
    for (unsigned int node = 1; node <= num_workers; ++node) {
        int zero = 0;
        MPI_Send(&zero, 1, MPI_INT, node, 0, MPI_COMM_WORLD);
    }

    int n_threads = num_workers;
    std::vector<std::string> sequences;
    // Here I spawn one thread per worker, or the max I can spawn
    // and each thread will receive a chunk of data from the master node
    #pragma omp parallel num_threads(n_threads)
    {
        int src = omp_get_thread_num() + 1;
        std::string fname = run_prefix + std::to_string(src);
        #pragma omp critical
        {
            sequences.push_back(fname);
        }
        int fd = openFile(fname, true);
        while (true) {
            MPI_Status status;
            int result_size;
            // Each thread waits for a worker

            MPI_Recv(&result_size, 1, MPI_INT, src, 1, MPI_COMM_WORLD, &status);


            if (result_size == 0) {
                workers_done.fetch_add(1);
                close(fd);
                break;
            }

            std::vector<char> result(result_size);

            MPI_Recv(result.data(), result_size, MPI_CHAR, src, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            write(fd, result.data(), result_size);
        }
    }

    /**
     * Only the master node access the disk and merges the sorted chunks
     */
     omp_set_num_threads(NTHREADS);
     ompMerge(sequences, merge_prefix, output_file);
}

#endif // _MPI_MASTER_HPP
