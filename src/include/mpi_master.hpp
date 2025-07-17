#ifndef _MPI_MASTER_HPP
#define _MPI_MASTER_HPP
#include <iostream>
#include "config.hpp"
#include "common.hpp"
#include "filesystem.hpp"
#include "omp_sort.hpp"
#include <fstream>
#include <vector>
#include <cstring>
#include <filesystem>
#include <cstdint>
#include <cstdlib>
#include <unistd.h>
#include <fcntl.h>
#include <mpi.h>


static void master(const std::string& filename, int world_size) {
    std::filesystem::path p(filename);
    std::string run_prefix = p.parent_path().string() + "/run#";
    std::string merge_prefix = p.parent_path().string() + "/merge#";
    std::string output_file = p.parent_path().string() + "/output.dat";
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Error opening file" << std::endl;
        MPI_Abort(MPI_COMM_WORLD, 1);
    }

    std::atomic<bool> sending_done{false};
    std::atomic<int> workers_done{0};
    const int num_workers = world_size - 1;

    std::vector<char> buffer(MAX_MEMORY / 2);
    size_t bytes_in_buffer = 0;
    size_t buffer_offset = 0;
    std::vector<std::vector<std::vector<char>>> node_chunks(num_workers);

    #pragma omp parallel sections
    {
        #pragma omp section
        {
            // Sender thread
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

                    size_t node = rand() % num_workers;
                    node_chunks[node].push_back(std::move(rec));
                    buffer_offset += len;
                }

                // Send to workers
                for (int node = 1; node <= num_workers; ++node) {
                    std::vector<char> send_buf;
                    for (auto& rec : node_chunks[node - 1]) {
                        send_buf.insert(send_buf.end(), rec.begin(), rec.end());
                    }

                    int size = send_buf.size();
                    MPI_Send(&size, 1, MPI_INT, node, 0, MPI_COMM_WORLD);
                    if (size > 0)
                        MPI_Send(send_buf.data(), size, MPI_CHAR, node, 0, MPI_COMM_WORLD);

                    node_chunks[node - 1].clear();
                }
            }

            close(fd);

            // Notify termination
            for (int node = 1; node <= num_workers; ++node) {
                int zero = 0;
                MPI_Send(&zero, 1, MPI_INT, node, 0, MPI_COMM_WORLD);
            }

            sending_done.store(true);
        }

        #pragma omp section
        {
            // Receiver thread

            while (workers_done.load() < num_workers) {
                MPI_Status status;
                int result_size;
                MPI_Recv(&result_size, 1, MPI_INT, MPI_ANY_SOURCE, 1, MPI_COMM_WORLD, &status);
                int src = status.MPI_SOURCE;

                if (result_size == 0) {
                    workers_done.fetch_add(1);
                    continue;
                }

                std::vector<char> result(result_size);
                MPI_Recv(result.data(), result_size, MPI_CHAR, src, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                std::cout << "[Master] Received sorted chunk of " << result_size << " bytes from worker " << src << std::endl;
                std::string fname = run_prefix + generateUUID();
                int fd = open(fname.c_str(), O_WRONLY  | O_APPEND | O_CREAT, 0666);
                if (fd < 0) {
                    if (errno == EEXIST)
                        fd = open(fname.c_str(), O_WRONLY  | O_APPEND);

                    else {
                        std::cerr << "Error opening file for writing: " << filename << " " << strerror(errno) << std::endl;
                        exit(-1);
                    }
                }
                write(fd, result.data(), result_size);
                close(fd);
            }
        }
    }
    /**
     * Only the master node access to the disk and merge the sorted chunks
     */
    ompMerge(run_prefix, merge_prefix, output_file);
}

#endif // _MPI_MASTER_HPP
