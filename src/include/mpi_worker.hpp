#ifndef _MPI_WORKER_HPP
#define _MPI_WORKER_HPP
#include <deque>
#include <iostream>
#include "config.hpp"
#include "common.hpp"
#include "filesystem.hpp"
#include "omp_sort.hpp"
#include "record.hpp"
#include "sorting.hpp"
#include <fstream>
#include <queue>
#include <string>
#include <vector>
#include <cstring>
#include <filesystem>
#include <cstdint>
#include <cstdlib>
#include <unistd.h>
#include <fcntl.h>
#include <mpi.h>

static void worker(std::string tmp_location) {
    int fd = 0, done = 0;
    size_t accumulated_size = 0, read_size = 0, offset = 0, size = 0;
    std::priority_queue<Record, std::vector<Record>, RecordComparator> records;
    std::vector<char> send_buf;
    std::filesystem::path p(tmp_location);
    std::string run_prefix = p.parent_path().string() + "/run#";
    std::string merge_prefix = p.parent_path().string() + "/merge#";
    std::string output_file = p.parent_path().string() + "/output.dat";
    while (true) {
        MPI_Recv(&size, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        if (size == 0) break;

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
        }
        // Since we have an heap, the vector is already sorted
        // so we can wait another round to then flush the content to disk
        // as the master will only send MAX_MEMORY/2 bytes at a time
        if (accumulated_size >= MAX_MEMORY) {
            std::string file = run_prefix + generateUUID();
            fd = openFile(file);
            appendToFile(fd, std::move(records)); // This empties the heap
            close(fd);
            accumulated_size = 0;
        }
    }

    if (!records.empty()) {
        std::string file = run_prefix + generateUUID();
        int fd = openFile(file);
        appendToFile(fd, std::move(records));
        close(fd);
    }
    ompMerge(run_prefix, merge_prefix, output_file);
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
