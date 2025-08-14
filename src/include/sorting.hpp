#ifndef _SORTING_HPP
#define _SORTING_HPP

#include "common.hpp"
#include "hpc_helpers.hpp"
#include "record.hpp"
#include <algorithm>
#include <cassert>
#include <cstddef>
#include <cstring>
#include <deque>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <iostream>
#include <omp.h>
#include <queue>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <utility>
#include <vector>

static inline std::string removeSubstring(std::string str, const std::string& toRemove) {
    size_t pos = str.find(toRemove);
    if (pos != std::string::npos) {
        str.erase(pos, toRemove.length());
    }
    return str;
}

/**
 * Merge two sorted files into a single output file.
 * It is used mainly in the parallel version of the algorithm.
 *
 * @param file1 The first input file.
 * @param file2 The second input file.
 * @param output_filename The output file.
 * @param max_mem The maximum memory available to read records from files.
 */
static void mergeFiles(const std::string& file1, const std::string& file2,
                       const std::string& output_filename, const ssize_t max_mem) {
    std::deque<Record> buffer1;
    std::deque<Record> buffer2;
    std::deque<Record> output_buffer;

    size_t bytes_to_process1 = getFileSize(file1);
    size_t bytes_to_process2 = getFileSize(file2);
    size_t usable_mem = max_mem / 3;

    size_t bytes_read1 = 0;
    size_t bytes_read2 = 0;
    size_t out_buf_size = 0;
    size_t bytes_written = 0;

    bool use_b1 = false;

    int out_fd = openFile(output_filename);
    int fd1 = openFile(file1);
    int fd2 = openFile(file2);
    // Initial buffer fills
    bytes_read1 += readRecordsFromFile(fd1, buffer1, bytes_read1, usable_mem);
    bytes_read2 += readRecordsFromFile(fd2, buffer2, bytes_read2, usable_mem);

    while (!buffer1.empty() || !buffer2.empty() ||
               bytes_read1 < bytes_to_process1 || bytes_read2 < bytes_to_process2) {
        // Refill buffer1 if needed and possible
        if (buffer1.empty() && bytes_read1 < bytes_to_process1)
            bytes_read1 += readRecordsFromFile(fd1, buffer1, bytes_read1, usable_mem);



        if (buffer2.empty() && bytes_read2 < bytes_to_process2)
            bytes_read2 += readRecordsFromFile(fd2, buffer2, bytes_read2, usable_mem);

        if (buffer1.empty()) use_b1 = false;
        else if (buffer2.empty()) use_b1 = true;
        else use_b1 = (buffer1.front() <= buffer2.front());


        if (use_b1) {
            Record r = buffer1.front();
            out_buf_size += r.size();
            output_buffer.push_back(std::move(buffer1.front()));
            buffer1.pop_front();
        } else {
            Record r = buffer2.front();
            out_buf_size += r.size();
            output_buffer.push_back(std::move(buffer2.front()));
            buffer2.pop_front();
        }

        if (out_buf_size >= usable_mem) {
            bytes_written += appendToFile(out_fd, std::move(output_buffer), out_buf_size);
            out_buf_size = 0;
            output_buffer.clear();
        }
    }

    if (!output_buffer.empty())
        bytes_written += appendToFile(out_fd, std::move(output_buffer), out_buf_size);


    close(fd1);
    close(fd2);
    close(out_fd);
    deleteFile(file1.c_str());
    deleteFile(file2.c_str());
}

struct BufferState {
    int fd;
    std::deque<Record> buffer;
    size_t bytes_read = 0;
    size_t total_bytes = 0;
    size_t file_index = 0;
    size_t usable_mem = 0;
    size_t available_mem = 0;

    BufferState(int fd, std::string name, size_t index, size_t usable_mem)
        : fd(fd), file_index(index), usable_mem(usable_mem), available_mem(usable_mem) {
            total_bytes = getFileSize(name);
    }

    bool hasMoreData() const {
        return !buffer.empty() || bytes_read < total_bytes;
    }

    void refill() {
        size_t last_read = readRecordsFromFile(
            fd, buffer, bytes_read, std::min(available_mem, usable_mem));
        // std::cout << "Refilled buffer " << filename << " with " << last_read << " bytes" << std::endl;
        bytes_read += last_read;
        available_mem -= last_read;
    }

    bool empty() const {
        return buffer.empty();
    }

    bool finished() const {
        return bytes_read == total_bytes;
    }

    Record get_front() {
        Record record = std::move(buffer.front());
        buffer.pop_front();
        available_mem += record.size();
        return record;
    }

    void close_fd() {
        close(fd);
    }
};


/**
 * This function performs k-way merge of sorted files into a single output file.
 * It is used in the sequential version of the merge sort.
 *
 * @param input_files The input file names.
 * @param output_filename The output file name.
 * @param max_mem The maximum memory available for sorting.
 */

static void kWayMergeFiles(const std::vector<std::string>& input_files,
                           const std::string& output_filename,
                           const ssize_t max_mem) {
    size_t num_files = input_files.size();
    size_t out_buffer_memory = max_mem / 3;
    /* Considering that I'm testing with at max 64 bytes payload, 4k are enough */
    size_t usable_mem = std::max((max_mem - out_buffer_memory) / num_files, 4096UL);
    std::vector<BufferState> buffers;
    buffers.reserve(num_files);

    // Initialize all buffer states
    for (size_t i = 0; i < num_files; i++) {
        buffers.emplace_back(openFile(input_files[i]), input_files[i], i, usable_mem);
        buffers[i].refill();
    }

    std::priority_queue<
               std::pair<Record, size_t>,
               std::vector<std::pair<Record, size_t>>,
               HeapPairRecordComparator
           > min_heap;

    // Prime the heap
    for (size_t i = 0; i < num_files; i++) {
        if (!buffers[i].empty()) {
            min_heap.emplace(buffers[i].get_front(), i);
        }
    }

    std::deque<Record> output_buffer;
    size_t out_buf_size = 0;
    size_t bytes_written = 0;

    int out_fd = open(output_filename.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0666);
    if (out_fd < 0) {
        std::cerr << "Error opening output file: " << output_filename
                  << " " << strerror(errno) << std::endl;
        exit(EXIT_FAILURE);
    }

    while (!min_heap.empty()) {
        auto [record, idx] = min_heap.top();
        min_heap.pop();

        out_buf_size += record.size();
        output_buffer.push_back(std::move(record));

        // Refill the buffer from the corresponding file if needed
        if (buffers[idx].empty() && !buffers[idx].finished()) {
            buffers[idx].refill();
        }

        if (!buffers[idx].empty()) {
            min_heap.emplace(buffers[idx].get_front(), idx);
        }

        if (out_buf_size >= out_buffer_memory) {
            bytes_written += appendToFile(out_fd, std::move(output_buffer), out_buf_size);
            output_buffer.clear();
            out_buf_size = 0;
        }
    }

    if (!output_buffer.empty()) {
        bytes_written += appendToFile(out_fd, std::move(output_buffer), out_buf_size);
    }

    close(out_fd);
    for (BufferState& buffer : buffers) {
        buffer.close_fd();
    }

    // Delete all input files
    for (const auto& f : input_files)
        deleteFile(f.c_str());
}

/**
 * This is an implementation of the snow plow
 * technique to generate sequence files longer than the memory available.
 * It produces sequences 2M long on average, reducing the number of merge levels needed.
 *
 * @param input_filename The input file name.
 * @param offset The offset to start reading from.
 * @param bytes_to_process The number of bytes to process.
 * @param max_memory The maximum memory to use.
 * @param output_filename_prefix The prefix for the output file names.
 */
static std::vector<std::string> genSequenceFiles(
    const std::string& input_filename,
    size_t offset,
    size_t bytes_to_process,
    size_t max_memory,
    const std::string& output_filename_prefix
) {
    size_t usable_mem = (max_memory * 8) / 10; // Leaving a 20% of space to the output buffer
    std::vector<Record> unsorted;
    std::deque<Record> output_buffer;
    size_t output_buffer_size = 0;
    std::priority_queue<Record, std::vector<Record>, HeapRecordComparator> heap;
    size_t heap_batch_size = 0;
    std::vector<Record> buffer;
    int out_fd;
    size_t io_offset = 0;
    std::vector<std::string> output_files;
    // Skip the unsorted initialization and push the records directly to the heap
    int fd = openFile(input_filename);
    ssize_t bytes_read = readRecordsFromFile(fd, heap, offset, std::min(usable_mem, bytes_to_process));
    ssize_t run = 1, curr_offset = offset + bytes_read, free_bytes = usable_mem - bytes_read, last_read = bytes_read;
    ssize_t bytes_remaining = bytes_to_process - bytes_read;

    while (bytes_remaining > 0 || !heap.empty() || !unsorted.empty()) { // I have to process all the bytes in the file
        std::string output_filename = output_filename_prefix + std::to_string(run);
        output_files.push_back(output_filename);

        out_fd = openFile(output_filename);
        bytes_remaining = bytes_to_process - bytes_read;

        heap_batch_size = std::max(1UL, heap.size()/20);
        // std::cout << "Heap batch size: " << heap_batch_size << std::endl;
        while (!heap.empty()) { // A run is complete when the heap is empty
            Record record;
            size_t record_key = 0;
            for (size_t i = 0; i < heap_batch_size && !heap.empty(); i++) {
                record = heap.top();
                heap.pop();
                record_key = record.key;

                // Flush the buffer to the output file and store the bytes freed
                free_bytes += record.size();
                output_buffer_size += record.size();
                output_buffer.push_back(std::move(record));
            }
            // Now record key is the last read
            if (bytes_remaining > 0) {
                // If there are bytes remained to process, read them into the buffer or at least read some bytes
                last_read = readRecordsFromFile(fd, buffer, curr_offset, std::min(bytes_remaining, free_bytes));
                // If I manage to read something I have to update the free_bytes counter
                // And the bytes_read counter
                free_bytes -= last_read;
                bytes_read += last_read;
                curr_offset += last_read;
                bytes_remaining = bytes_to_process - bytes_read;
            }
            for (auto& r : buffer) {
                if (r.key < record_key) unsorted.push_back(std::move(r));
                else heap.push(std::move(r));
            }
            buffer.clear();

            if (output_buffer_size > max_memory - usable_mem) {
                io_offset += appendToFile(out_fd, std::move(output_buffer), output_buffer_size);
                output_buffer.clear();
                output_buffer_size = 0;
            }
        }
        // If I'm here it means that the heap is empty, so let's process the unsorted set
        for(auto& r : unsorted)
            heap.push(std::move(r));
        // Now the heap is full again and we can clear the unsorted set
        unsorted.clear();
        if (output_buffer_size) {
            io_offset += appendToFile(out_fd, std::move(output_buffer), output_buffer_size);
            output_buffer.clear();
            output_buffer_size = 0;
        }
        close(out_fd);
        run++;
    }
    close(fd);
    return output_files;
}

/**
 * This is a simple implementation that reads chunks of data fitting in max_memory,
 * sorts them using std::sort, and flushes them to disk. It is compatible with the
 * merge-based approach and produces sorted runs for external merge sort.
 *
 * @param input_filename The input file name.
 * @param offset The offset to start reading from.
 * @param bytes_to_process The number of bytes to process.
 * @param max_memory The maximum memory to use.
 * @param output_filename_prefix The prefix for the output file names.
 */
static std::vector<std::string> genSortedRunsWithSort(
    const std::string& input_filename,
    size_t offset,
    size_t bytes_to_process,
    size_t max_memory,
    const std::string& output_filename_prefix
) {
    size_t usable_mem = (max_memory * 9) / 10; // Leave 10% for buffers, pointers, etc.
    size_t bytes_read = 0;
    size_t curr_offset = offset;
    size_t run = 1;
    std::vector<std::string> output_files;
    int input_fd = openFile(input_filename);
    while (bytes_read < bytes_to_process) {
        std::vector<Record> buffer;
        size_t chunk_size = std::min(usable_mem, bytes_to_process - bytes_read);
        ssize_t actual_bytes_read = readRecordsFromFile(input_fd, buffer, curr_offset, chunk_size);
        if (actual_bytes_read <= 0) break;

        curr_offset += actual_bytes_read;
        bytes_read += actual_bytes_read;

        std::sort(buffer.begin(), buffer.end(), RecordComparator{});
        std::string output_filename = output_filename_prefix + std::to_string(run);
        output_files.push_back(output_filename);
        int fd = openFile(output_filename);

        // Reuse your existing appendToFile logic, or adapt to vector
        appendToFile(fd, std::move(buffer), actual_bytes_read);

        close(fd);

        run++;
    }
    close(input_fd);
    return output_files;
}

#endif // _SORTING_HPP
