#ifndef _MPI_WORKER_HPP
#define _MPI_WORKER_HPP

#include "common.hpp"
#include "config.hpp"
#include "omp_sort.hpp"
#include "record.hpp"
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <iostream>
#include <mpi.h>
#include <queue>
#include <string>
#include <unistd.h>
#include <vector>

static void worker(std::string tmp_location) {
    int fd = 0, done = 0;
    size_t accumulated_size = 0, read_size = 0, offset = 0, size = 0;
    std::priority_queue<Record, std::vector<Record>, RecordComparator> records;
    std::vector<char> send_buf;
    std::string run_prefix = tmp_location + "/run#";
    std::string merge_prefix = tmp_location + "/merge#";
    std::string output_file = tmp_location + "/output.dat";

    std::cout << "[Worker] Starting main loop" << std::endl;
    while (true) {
        MPI_Recv(&size, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        if (size == 0){
            std::cout << "[Worker] Received termination signal" << std::endl;
            break;
        }
        std::cout << "[Worker] Received data of size " << size << std::endl;

        std::vector<char> buf(size);
        MPI_Recv(buf.data(), size, MPI_CHAR, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);

        offset = 0;
        while (offset + sizeof(uint64_t) + sizeof(uint32_t) <= buf.size()) {
            uint64_t key = *reinterpret_cast<uint64_t*>(&buf[offset]);
            offset += sizeof(uint64_t);
            uint32_t len = *reinterpret_cast<uint32_t*>(&buf[offset]);
            offset += sizeof(uint32_t);

            if (offset + len > buf.size()) break;
            Record rec;
            rec.key = key;
            rec.len = len;
            rec.rpayload = std::make_unique<char[]>(len);
            std::memcpy(rec.rpayload.get(), &buf[offset], len);
            offset += len;

            records.push(std::move(rec));
            accumulated_size += sizeof(uint64_t) + sizeof(uint32_t) + len;
        }
        std::cout << "[Worker] Received " << records.size() << " records" << std::endl;
        // Since we have an heap, the vector is already sorted
        // so we can wait another round to then flush the content to disk
        // as the master will only send MAX_MEMORY/2 bytes at a time
        if (accumulated_size >= MAX_MEMORY) {
            std::string file = run_prefix + generateUUID();
            std::cout << "[Worker] Writing to file " << file << std::endl;
            int fd = openFile(file);
            appendToFile(fd, std::move(records), accumulated_size); // This empties the heap
            close(fd);
            accumulated_size = 0;
        }
    }

    if (!records.empty()) {
        std::string file = run_prefix + generateUUID();
        std::cout << "[Worker] Writing to file " << file << std::endl;
        int fd = openFile(file);
        appendToFile(fd, std::move(records), accumulated_size);
        close(fd);
        accumulated_size = 0;
    }
    std::cout << "[Worker] Worker starting merge" << std::endl;
    ompMerge(run_prefix, merge_prefix, output_file);
    std::cout << "[Worker] Worker completed merge" << std::endl;
    fd = openFile(output_file);

    // The receiver can only read up to MAX_MEMORY/2 bytes at a time
    send_buf.reserve(MAX_MEMORY/2);
    while ((read_size = read(fd, send_buf.data(), MAX_MEMORY/2)) > 0) {
        int send_size = send_buf.size();
        MPI_Send(&send_size, 1, MPI_INT, 0, 1, MPI_COMM_WORLD);
        MPI_Send(send_buf.data(), send_size, MPI_CHAR, 0, 1, MPI_COMM_WORLD);
        send_buf.clear();
    }
    // Done sending the sorted file, bye bye
    MPI_Send(&done, 1, MPI_INT, 0, 1, MPI_COMM_WORLD);
    // It is now responsibility of the master to merge the remaining files
    deleteFile(output_file.c_str()); // Cleanup
}

#endif // _MPI_WORKER_HPP
