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
#include <unistd.h>
#include <vector>

/**
 * TODO:
 *   - Check this function
 *   - Receive the sorted files from the workers flushing the buffer to a single file for each worker
 *   - Merge the sorted files into a single file using OMP
 */
// static void master(const std::string& filename, int world_size) {
//     std::filesystem::path p(filename);
//     std::string run_prefix = p.parent_path().string() + "/run#";
//     std::string merge_prefix = p.parent_path().string() + "/merge#";
//     std::string output_file = p.parent_path().string() + "/output.dat";
//     int fd = openFile(filename);
//     // std::cout << "[Master] File size " << filename << ": " << getFileSize(filename) << std::endl;

//     std::atomic<unsigned int> workers_done{0};
//     const unsigned int num_workers = world_size - 1;
//     // std::cout << "[Master] Number of workers: " << num_workers << std::endl;

//     size_t worker_idx = 0;
//     std::vector<char> buffer(MAX_MEMORY / 2);
//     size_t bytes_in_buffer = 0;
//     size_t buffer_offset = 0;

//     std::vector<std::vector<char>> node_chunks(num_workers);
//     // std::cout << "[Master] Starting main loop" << std::endl;
//     while (true) {
//         if (buffer_offset < bytes_in_buffer) {
//             memmove(buffer.data(), buffer.data() + buffer_offset, bytes_in_buffer - buffer_offset);
//             bytes_in_buffer -= buffer_offset;
//         } else {
//             bytes_in_buffer = 0;
//         }
//         buffer_offset = 0;

//         ssize_t bytes_read = read(fd, buffer.data() + bytes_in_buffer, buffer.size() - bytes_in_buffer);
//         // std::cout << "[Master] Read " << bytes_read << " bytes" << std::endl;
//         if (bytes_read <= 0 && bytes_in_buffer == 0) break;
//         if (bytes_read > 0) bytes_in_buffer += bytes_read;

//         // std::cout << "[Master] Buffer offset: " << buffer_offset << std::endl;
//         // std::cout << "[Master] Bytes in buffer: " << bytes_in_buffer << std::endl;
//         // std::cout << "[Master] Bytes read: " << bytes_read << std::endl;
//         while (buffer_offset + sizeof(uint64_t) + sizeof(uint32_t) <= bytes_in_buffer) {
//             size_t rec_start = buffer_offset;
//             buffer_offset += sizeof(uint64_t);
//             uint32_t len = *reinterpret_cast<uint32_t*>(&buffer[buffer_offset]);
//             buffer_offset += sizeof(uint32_t);

//             if (buffer_offset + len > bytes_in_buffer) {
//                 buffer_offset = rec_start;
//                 break;
//             }

//             size_t rec_size = sizeof(uint64_t) + sizeof(uint32_t) + len;
//             std::vector<char> rec(rec_size);
//             std::memcpy(rec.data(), &buffer[rec_start], rec_size);
//             node_chunks[worker_idx].insert(node_chunks[worker_idx].end(), rec.begin(), rec.end());
//             worker_idx = (worker_idx + 1) % num_workers;
//             buffer_offset += len;
//         }
//         // std::cout << "[Master] node_chunks: " << node_chunks[node].size() << std::endl;

//         // Send to workers. Starting from 1 because rank 0 is the master
//         // std::cout << "Sending data to workers..." << std::endl;
//         for (unsigned int node = 1; node <= num_workers; node++) {
//             std::vector<char> send_buf;

//             int chunk_size = node_chunks[node-1].size();
//             MPI_Send(&chunk_size, 1, MPI_INT, node, 0, MPI_COMM_WORLD);
//             if (node_chunks[node-1].size() > 0)
//                 MPI_Send(node_chunks[node-1].data(), node_chunks[node-1].size(), MPI_CHAR, node, 0, MPI_COMM_WORLD);

//             node_chunks[node-1].clear();
//         }
//     }
//     close(fd);
//     // Notify EOS
//     for (unsigned int node = 1; node <= num_workers; ++node) {
//         int zero = 0;
//         MPI_Send(&zero, 1, MPI_INT, node, 0, MPI_COMM_WORLD);
//     }

//     int n_threads = std::min(NTHREADS, num_workers);
//     // Here I spawn one thread per worker, or the max I can spawn
//     // and each thread will receive a chunk of data from the master node
//     std::cout << "Spawning " << n_threads << " threads" << std::endl;
//     std::cout << workers_done.load() << " < " << num_workers << std::endl;
//     #pragma omp parallel num_threads(n_threads)
//     {
//         while (workers_done.load() < num_workers) {
//             MPI_Status status;
//             int result_size;
//             int src = 0;

//             // Serialize MPI calls
//             #pragma omp critical(mpi_recv)
//             {
//                 if (workers_done.load() < num_workers) {
//                     MPI_Recv(&result_size, 1, MPI_INT, MPI_ANY_SOURCE, 1, MPI_COMM_WORLD, &status);
//                     src = status.MPI_SOURCE;
//                 } else {
//                     result_size = 0; // Signal to exit
//                 }
//             }

//             if (result_size == 0) {
//                 workers_done.fetch_add(1);
//                 continue;
//             }

//             std::vector<char> result(result_size);

//             #pragma omp critical(mpi_recv)
//             {
//                 MPI_Recv(result.data(), result_size, MPI_CHAR, src, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
//             }

//             // File I/O can remain parallel
//             std::string fname = run_prefix + std::to_string(src);
//             int fd = openFile(fname, true);
//             write(fd, result.data(), result_size);
//             close(fd);
//         }
//     }

//     /**
//      * Only the master node access to the disk and merge the sorted chunks
//      */

//     // std::cout << "[Master] Starting merge" << std::endl;
//     ompMerge(run_prefix, merge_prefix, output_file);
//     // std::cout << "[Master] Merge complete" << std::endl;
// }
static void master(const std::string& filename, int world_size) {
    std::filesystem::path p(filename);
    std::string run_prefix = p.parent_path().string() + "/run#";
    std::string merge_prefix = p.parent_path().string() + "/merge#";
    std::string output_file = p.parent_path().string() + "/output.dat";

    std::cout << "[Master] Starting with " << world_size << " total processes" << std::endl;
    std::cout << "[Master] run_prefix: " << run_prefix << std::endl;

    int fd = openFile(filename);
    const unsigned int num_workers = world_size - 1;
    std::cout << "[Master] Number of workers: " << num_workers << std::endl;

    std::atomic<unsigned int> workers_done{0};
    size_t worker_idx = 0;
    std::vector<char> buffer(MAX_MEMORY / 2);
    size_t bytes_in_buffer = 0;
    size_t buffer_offset = 0;
    std::vector<std::vector<char>> node_chunks(num_workers);

    std::cout << "[Master] Starting main distribution loop" << std::endl;
    size_t total_records = 0;

    while (true) {
        // Compact buffer
        if (buffer_offset < bytes_in_buffer) {
            memmove(buffer.data(), buffer.data() + buffer_offset, bytes_in_buffer - buffer_offset);
            bytes_in_buffer -= buffer_offset;
        } else {
            bytes_in_buffer = 0;
        }
        buffer_offset = 0;

        // Read more data
        ssize_t bytes_read = read(fd, buffer.data() + bytes_in_buffer, buffer.size() - bytes_in_buffer);
        if (bytes_read <= 0 && bytes_in_buffer == 0) {
            std::cout << "[Master] Finished reading file" << std::endl;
            break;
        }
        if (bytes_read > 0) bytes_in_buffer += bytes_read;

        // Parse records
        while (buffer_offset + sizeof(uint64_t) + sizeof(uint32_t) <= bytes_in_buffer) {
            size_t rec_start = buffer_offset;
            buffer_offset += sizeof(uint64_t);
            uint32_t len = *reinterpret_cast<uint32_t*>(&buffer[buffer_offset]);
            buffer_offset += sizeof(uint32_t);

            // Validate record length
            if (len > MAX_MEMORY || len == 0) {
                std::cerr << "[Master] ERROR: Invalid record length: " << len << std::endl;
                buffer_offset = rec_start;
                break;
            }

            if (buffer_offset + len > bytes_in_buffer) {
                buffer_offset = rec_start;
                break;
            }

            size_t rec_size = sizeof(uint64_t) + sizeof(uint32_t) + len;
            std::vector<char> rec(rec_size);
            std::memcpy(rec.data(), &buffer[rec_start], rec_size);

            // Validate worker_idx
            if (worker_idx >= num_workers) {
                std::cerr << "[Master] ERROR: worker_idx " << worker_idx << " >= " << num_workers << std::endl;
                worker_idx = 0;
            }

            node_chunks[worker_idx].insert(node_chunks[worker_idx].end(), rec.begin(), rec.end());
            worker_idx = (worker_idx + 1) % num_workers;
            buffer_offset += len;
            total_records++;
        }

        // Send to workers periodically or when chunks get large
        bool should_send = false;
        for (const auto& chunk : node_chunks) {
            if (chunk.size() > MAX_MEMORY / 4) {
                should_send = true;
                break;
            }
        }

        if (should_send) {
            std::cout << "[Master] Sending data to workers (total records so far: " << total_records << ")" << std::endl;
            for (unsigned int rank = 1; rank <= num_workers; rank++) {
                unsigned int chunk_idx = rank - 1;
                int chunk_size = static_cast<int>(node_chunks[chunk_idx].size());

                std::cout << "[Master] Sending " << chunk_size << " bytes to worker " << rank << std::endl;

                // Check for MPI errors
                int result = MPI_Send(&chunk_size, 1, MPI_INT, rank, 0, MPI_COMM_WORLD);
                if (result != MPI_SUCCESS) {
                    std::cerr << "[Master] ERROR: MPI_Send size failed for worker " << rank << ", error: " << result << std::endl;
                    continue;
                }

                if (chunk_size > 0) {
                    result = MPI_Send(node_chunks[chunk_idx].data(), chunk_size, MPI_CHAR, rank, 0, MPI_COMM_WORLD);
                    if (result != MPI_SUCCESS) {
                        std::cerr << "[Master] ERROR: MPI_Send data failed for worker " << rank << ", error: " << result << std::endl;
                    }
                }
                node_chunks[chunk_idx].clear();
            }
        }
    }
    close(fd);

    // Send final chunks
    std::cout << "[Master] Sending final chunks to workers" << std::endl;
    for (unsigned int rank = 1; rank <= num_workers; rank++) {
        unsigned int chunk_idx = rank - 1;
        int chunk_size = static_cast<int>(node_chunks[chunk_idx].size());

        std::cout << "[Master] Final chunk to worker " << rank << ": " << chunk_size << " bytes" << std::endl;
        MPI_Send(&chunk_size, 1, MPI_INT, rank, 0, MPI_COMM_WORLD);
        if (chunk_size > 0) {
            MPI_Send(node_chunks[chunk_idx].data(), chunk_size, MPI_CHAR, rank, 0, MPI_COMM_WORLD);
        }
        node_chunks[chunk_idx].clear();
    }

    // Send EOS signals
    std::cout << "[Master] Sending EOS signals to workers" << std::endl;
    for (unsigned int rank = 1; rank <= num_workers; ++rank) {
        int zero = 0;
        int result = MPI_Send(&zero, 1, MPI_INT, rank, 0, MPI_COMM_WORLD);
        if (result != MPI_SUCCESS) {
            std::cerr << "[Master] ERROR: Failed to send EOS to worker " << rank << std::endl;
        } else {
            std::cout << "[Master] Sent EOS to worker " << rank << std::endl;
        }
    }

    std::cout << "[Master] Starting to receive results from workers" << std::endl;
    std::cout << "[Master] Total records distributed: " << total_records << std::endl;

    // Receive results - SINGLE THREADED for now to debug
    while (workers_done.load() < num_workers) {
        MPI_Status status;
        int result_size;

        std::cout << "[Master] Waiting for result from any worker..." << std::endl;
        int result = MPI_Recv(&result_size, 1, MPI_INT, MPI_ANY_SOURCE, 1, MPI_COMM_WORLD, &status);
        if (result != MPI_SUCCESS) {
            std::cerr << "[Master] ERROR: MPI_Recv failed, error: " << result << std::endl;
            break;
        }

        int src = status.MPI_SOURCE;
        std::cout << "[Master] Received result from worker " << src << ", size: " << result_size << std::endl;

        if (result_size == 0) {
            std::cout << "[Master] Worker " << src << " finished" << std::endl;
            workers_done.fetch_add(1);
            continue;
        }

        if (result_size < 0 || result_size > MAX_MEMORY) {
            std::cerr << "[Master] ERROR: Invalid result_size " << result_size << " from worker " << src << std::endl;
            workers_done.fetch_add(1);
            continue;
        }

        std::vector<char> result(result_size);
        result = MPI_Recv(result.data(), result_size, MPI_CHAR, src, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        if (result != MPI_SUCCESS) {
            std::cerr << "[Master] ERROR: Failed to receive data from worker " << src << std::endl;
            continue;
        }

        std::string fname = run_prefix + std::to_string(src);
        std::cout << "[Master] Saving result to file: " << fname << std::endl;

        int fd = openFile(fname, true);
        if (fd < 0) {
            std::cerr << "[Master] ERROR: Failed to open file " << fname << std::endl;
            continue;
        }

        ssize_t written = write(fd, result.data(), result_size);
        if (written != result_size) {
            std::cerr << "[Master] ERROR: Write failed. Expected " << result_size << ", wrote " << written << std::endl;
        }
        close(fd);
    }

    std::cout << "[Master] All workers finished. Starting merge." << std::endl;
    ompMerge(run_prefix, merge_prefix, output_file);
    std::cout << "[Master] Merge complete" << std::endl;
}

#endif // _MPI_MASTER_HPP
