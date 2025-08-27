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

static void worker(std::string tmp_location, size_t world_size) {
    int fd = 0, done = 0;
    size_t accumulated_size = 0, read_size = 0, offset = 0;
    int size = 0;
    std::vector<Record> records;
    /**
     * The master receives the sequences concurrently from all the workers so to respect
     * memory constraints we can send up to The total memory divided by the number of workers minus the master
     */
    size_t send_buf_size = MAX_MEMORY/(world_size-1);
    std::vector<char> send_buf(send_buf_size);
    std::filesystem::path tmp_path = tmp_location + "/" + generateUUID();
    if (!std::filesystem::create_directories(tmp_path)) {
        std::cerr << "Canot create " << tmp_path << std::endl;
        exit(EXIT_FAILURE);
    }
    std::string run_prefix = tmp_path.string() + "/run#";
    std::string merge_prefix = tmp_path.string() + "/merge#";
    std::string output_file = tmp_path.string() + "/output.dat";
    std::vector<std::string> sequences;
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

            records.push_back(std::move(rec));
            accumulated_size += sizeof(uint64_t) + sizeof(uint32_t) + len;
        }

        // Since we have an heap, the vector is already sorted
        // so we can wait another round to then flush the content to disk
        // as the master will only send MAX_MEMORY/2 bytes at a time
        if (accumulated_size >= MAX_MEMORY) {
            std::string file = run_prefix + generateUUID();
            sequences.push_back(file);
            int fd = openFile(file);
            std::sort(records.begin(), records.end(), RecordComparator{});
            appendToFile(fd, std::move(records), accumulated_size); // This empties the heap
            close(fd);
            accumulated_size = 0;
        }
    }

    if (!records.empty()) {
        std::string file = run_prefix + generateUUID();
        sequences.push_back(file);
        int fd = openFile(file);
        std::sort(records.begin(), records.end(), RecordComparator{});
        appendToFile(fd, std::move(records), accumulated_size);
        close(fd);
        accumulated_size = 0;
    }

    /* Setting the number of threads for the merge phase */
    omp_set_num_threads(NTHREADS);
    ompMerge(sequences, merge_prefix, output_file);
    fd = openFile(output_file);

    // The receiver can only read up to send_buf_size bytes at a time
    while ((read_size = read(fd, send_buf.data(), send_buf_size)) > 0) {
        int send_size = read_size;
        MPI_Send(&send_size, 1, MPI_INT, 0, 1, MPI_COMM_WORLD);
        MPI_Send(send_buf.data(), send_size, MPI_CHAR, 0, 1, MPI_COMM_WORLD);
        send_buf.clear();
    }
    // Done sending the sorted file, bye bye
    MPI_Send(&done, 1, MPI_INT, 0, 1, MPI_COMM_WORLD);
    // It is now responsibility of the master to merge the remaining files
    std::filesystem::remove_all(tmp_path); // Cleanup
}

#endif // _MPI_WORKER_HPP
