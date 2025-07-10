#ifndef _SORTING_HPP
#define _SORTING_HPP

#include "common.hpp"
#include "config.hpp"
#include "filesystem.hpp"
#include <algorithm>
#include <cstring>
#include <deque>
#include <fcntl.h>
#include <iostream>
#include <string>
#include <cassert>
#include <cstddef>
#include <queue>
#include <unistd.h>
#include <vector>
#include <errno.h>
#include <dirent.h>
#include <sys/stat.h>
#include <omp.h>
#include <sys/mman.h>

static void printRunFiles(const std::string& prefix_path);

struct RecordComparator {
    bool operator()(Record a, Record b){
        return (a.key > b.key);
    }
};


struct CompareRecord {
    bool operator()(const std::pair<Record, size_t>& a, const std::pair<Record, size_t>& b) const {
        return a.first > b.first;  // Min-heap based on Record comparison
    }
};
static std::string removeSubstring(std::string str, const std::string& toRemove) {
    size_t pos = str.find(toRemove);
    if (pos != std::string::npos) {
        str.erase(pos, toRemove.length());
    }
    return str;
}


static void mergeFiles(const std::string& file1, const std::string& file2,
                       const std::string& output_filename, const ssize_t max_mem) {
    std::deque<Record> buffer1;
    std::deque<Record> buffer2;
    std::deque<Record> output_buffer;

    size_t bytes_to_process1 = getFileSize(file1);
    size_t bytes_to_process2 = getFileSize(file2);
    size_t usable_mem = max_mem / 4;

    size_t bytes_read1 = 0;
    size_t bytes_read2 = 0;
    size_t out_buf_size = 0;
    size_t bytes_written = 0;

    bool use_b1 = false;

    int fp = open(output_filename.c_str(), O_RDWR | O_CREAT, 0666);
    if (fp < 0) {
        if (errno == EEXIST)
            fp = open(output_filename.c_str(), O_RDWR);
        else {
            std::cerr << "Error opening file for writing: " << output_filename
                      << " " << strerror(errno) << std::endl;
            exit(-1);
        }
    }


    // Initial buffer fills
    bytes_read1 += readRecordsFromFile(file1, buffer1, bytes_read1, usable_mem);
    bytes_read2 += readRecordsFromFile(file2, buffer2, bytes_read2, usable_mem);

    while (!buffer1.empty() || !buffer2.empty() ||
               bytes_read1 < bytes_to_process1 || bytes_read2 < bytes_to_process2) {
        // Refill buffer1 if needed and possible
        if (buffer1.empty() && bytes_read1 < bytes_to_process1)
            bytes_read1 += readRecordsFromFile(file1, buffer1, bytes_read1, usable_mem);



        if (buffer2.empty() && bytes_read2 < bytes_to_process2)
            bytes_read2 += readRecordsFromFile(file2, buffer2, bytes_read2, usable_mem);



        if (buffer1.empty()) use_b1 = false;
        else if (buffer2.empty()) use_b1 = true;
        else use_b1 = (buffer1.front() <= buffer2.front());


        if (use_b1) {
            Record r = buffer1.front();
            out_buf_size += sizeof(r.key) + sizeof(r.len) + r.len;
            output_buffer.push_back(std::move(buffer1.front()));
            buffer1.pop_front();
        } else {
            Record r = buffer2.front();
            out_buf_size += sizeof(r.key) + sizeof(r.len) + r.len;
            output_buffer.push_back(std::move(buffer2.front()));
            buffer2.pop_front();
        }

        if (out_buf_size >= usable_mem) {
            bytes_written += writeBatchToFile(fp, output_buffer);
            out_buf_size = 0;
            output_buffer.clear();
        }
    }

    if (!output_buffer.empty())
        bytes_written += writeBatchToFile(fp, output_buffer);


    deleteFile(file1.c_str());
    deleteFile(file2.c_str());
    close(fp);
}

struct BufferState {
    std::string filename;
    std::deque<Record> buffer;
    size_t bytes_read = 0;
    size_t total_bytes = 0;
    size_t file_index = 0;

    BufferState(std::string name, size_t index)
        : filename(std::move(name)), file_index(index) {
        total_bytes = getFileSize(filename);
    }

    bool hasMoreData() const {
        return !buffer.empty() || bytes_read < total_bytes;
    }
};


static void kWayMergeFiles(const std::vector<std::string>& input_files,
                           const std::string& output_filename,
                           const ssize_t max_mem) {
    size_t usable_mem = max_mem / 4;
    size_t num_files = input_files.size();

    std::vector<BufferState> buffers;
    buffers.reserve(num_files);

    // Initialize all buffer states
    for (size_t i = 0; i < num_files; ++i) {
        buffers.emplace_back(input_files[i], i);
        buffers[i].bytes_read += readRecordsFromFile(
            buffers[i].filename, buffers[i].buffer, buffers[i].bytes_read, usable_mem);
    }

    // Priority queue to track smallest record among buffers

    // std::priority_queue<Record, std::vector<Record>, RecordComparator> min_heap;
    std::priority_queue<
               std::pair<Record, size_t>,
               std::vector<std::pair<Record, size_t>>,
               CompareRecord
           > min_heap;

    // Prime the heap
    for (size_t i = 0; i < num_files; ++i) {
        if (!buffers[i].buffer.empty()) {
            min_heap.emplace(buffers[i].buffer.front(), i);
            buffers[i].buffer.pop_front();
        }
    }

    std::deque<Record> output_buffer;
    size_t out_buf_size = 0;
    size_t bytes_written = 0;

    int fd = open(output_filename.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0666);
    if (fd < 0) {
        std::cerr << "Error opening output file: " << output_filename
                  << " " << strerror(errno) << std::endl;
        exit(EXIT_FAILURE);
    }

    while (!min_heap.empty()) {
        auto [record, idx] = min_heap.top();
        min_heap.pop();

        out_buf_size += sizeof(record.key) + sizeof(record.len) + record.len;
        output_buffer.push_back(std::move(record));

        // Refill the buffer from the corresponding file if needed
        if (buffers[idx].buffer.empty() && buffers[idx].bytes_read < buffers[idx].total_bytes) {
            buffers[idx].bytes_read += readRecordsFromFile(
                buffers[idx].filename, buffers[idx].buffer,
                buffers[idx].bytes_read, usable_mem);
        }

        if (!buffers[idx].buffer.empty()) {
            min_heap.emplace(buffers[idx].buffer.front(), idx);
            buffers[idx].buffer.pop_front();
        }

        if (out_buf_size >= usable_mem) {
            bytes_written += writeBatchToFile(fd, output_buffer);
            output_buffer.clear();
            out_buf_size = 0;
        }
    }

    if (!output_buffer.empty()) {
        bytes_written += writeBatchToFile(fd, output_buffer);
    }

    close(fd);

    // Delete all input files
    for (const auto& f : input_files)
        deleteFile(f.c_str());
}

/**
 * After some tests we know that this function generates the sequences
 * as expected, even though the sum of the run files sizes are not equal to
 * the original file. This is not a problem, we just have to keep it in mind.
 */
// static void genSequenceFiles(
//     const std::string& input_filename,
//     ssize_t offset,
//     ssize_t bytes_to_process,
//     ssize_t max_memory,
//     const std::string& output_filename_prefix
// ) {
//     ssize_t usable_mem = (max_memory * 9)/10; // Leaving a 10% of space to read and write records
//     std::vector<Record> unsorted;
//     std::priority_queue<Record, std::vector<Record>, RecordComparator> heap;
//     std::vector<Record> buffer;
//     size_t total_records_read = 0;
//     size_t total_records_written = 0;

//     // Skip the unsorted initialization and push the records directly to the heap
//     ssize_t bytes_read = readRecordsFromFile(input_filename, heap, offset, std::min(usable_mem, bytes_to_process));

//     total_records_read += heap.size();
//     ssize_t run = 1, curr_offset = offset + bytes_read, free_bytes = usable_mem - bytes_read, last_read = bytes_read, bytes_remaining = bytes_to_process - bytes_read;

//     while (bytes_remaining > 0 || !heap.empty() || !unsorted.empty()) { // I have to process all the bytes in the file
//         std::string output_filename = output_filename_prefix + std::to_string(run);
//         bytes_remaining = bytes_to_process - bytes_read;
//         while (!heap.empty()) { // A run is complete when the heap is empty
//             Record record = heap.top();
//             heap.pop();

//             // Flush the buffer to the output file and store the bytes freed
//             free_bytes += appendRecordToFile(output_filename, record);
//             total_records_written++;
//             if (bytes_remaining > 0) {
//                 // If there are bytes remained to process, read them into the buffer or at least read some bytes
//                 last_read = readRecordsFromFile(input_filename, buffer, curr_offset, std::min(bytes_remaining, free_bytes));
//                 total_records_read += buffer.size();
//                 // If I manage to read something I have to update the free_bytes counter
//                 // And the bytes_read counter
//                 free_bytes -= last_read;
//                 bytes_read += last_read;
//                 curr_offset += last_read;
//                 bytes_remaining = bytes_to_process - bytes_read;
//             }
//             for (auto& r : buffer) {
//                 if (r < record) unsorted.push_back(std::move(r));
//                 else heap.push(std::move(r));
//             }
//             buffer.clear();
//         }
//         // If I'm here it means that the heap is empty, so let's process the unsorted set
//         for(auto& r : unsorted)
//             heap.push(std::move(r));
//         // Now the heap is full again and we can clear the unsorted set
//         unsorted.clear();
//         run++;
//     }
// }

static void genSequenceFiles(
    const std::string& input_filename,
    size_t offset,
    size_t bytes_to_process,
    size_t max_memory,
    const std::string& output_filename_prefix
) {
    size_t usable_mem = (max_memory * 18) / 30; // Leaving a 10% of space to read and write records and using half for the output buffer
    std::vector<Record> unsorted;
    std::deque<Record> output_buffer;
    size_t output_buffer_size = 0;
    std::priority_queue<Record, std::vector<Record>, RecordComparator> heap;

    std::vector<Record> buffer;
    size_t total_records_read = 0;
    size_t total_records_written = 0;
    int fd;
    size_t io_offset = 0;

    // Skip the unsorted initialization and push the records directly to the heap
    ssize_t bytes_read = readRecordsFromFile(input_filename, heap, offset, std::min(usable_mem, bytes_to_process));

    total_records_read += heap.size();
    ssize_t run = 1, curr_offset = offset + bytes_read, free_bytes = usable_mem - bytes_read, last_read = bytes_read, bytes_remaining = bytes_to_process - bytes_read;

    while (bytes_remaining > 0 || !heap.empty() || !unsorted.empty()) { // I have to process all the bytes in the file
        std::string output_filename = output_filename_prefix + std::to_string(run);
        fd = open(output_filename.c_str(), O_RDWR | O_CREAT, 0666);
        if (fd < 0) {
            if (errno == EEXIST)
                fd = open(output_filename.c_str(), O_RDWR);
            else {
                std::cerr << "Error opening file for writing: " << output_filename << " " << strerror(errno) << std::endl;
                exit(-1);
            }
        }
        bytes_remaining = bytes_to_process - bytes_read;
        while (!heap.empty()) { // A run is complete when the heap is empty
            Record record = heap.top();
            heap.pop();
            size_t record_key = record.key;

            // Flush the buffer to the output file and store the bytes freed
            free_bytes += sizeof(record.key) + sizeof(record.len) + record.len;
            output_buffer_size += sizeof(record.key) + sizeof(record.len) + record.len;
            output_buffer.push_back(std::move(record));
            // free_bytes += appendRecordToFile(output_filename, record);
            total_records_written++;
            if (bytes_remaining > 0) {
                // If there are bytes remained to process, read them into the buffer or at least read some bytes
                last_read = readRecordsFromFile(input_filename, buffer, curr_offset, std::min(bytes_remaining, free_bytes));
                total_records_read += buffer.size();
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
            if (output_buffer_size > usable_mem / 2) {
                io_offset += writeBatchToFile(fd, output_buffer);
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
            io_offset += writeBatchToFile(fd, output_buffer);
            output_buffer.clear();
            output_buffer_size = 0;
        }
        close(fd);
        run++;
    }
}


static std::vector<std::string> findFiles(const std::string& prefix_path) {

    std::vector<std::string> matching_files;


    // Manual path parsing instead of std::filesystem
    std::string directory;
    std::string filename_prefix;

    size_t last_slash = prefix_path.find_last_of('/');
    if (last_slash != std::string::npos) {
        directory = prefix_path.substr(0, last_slash);
        filename_prefix = prefix_path.substr(last_slash + 1);
    } else {
        directory = ".";
        filename_prefix = prefix_path;
    }

    // If directory is empty, use current directory
    if (directory.empty()) {
        directory = ".";
    }
    // Check if directory exists and is accessible
    struct stat dir_stat;
    if (stat(directory.c_str(), &dir_stat) != 0) {
        std::cerr << "Cannot access directory: " << directory << std::endl;
        std::cerr << "errno: " << errno << " (" << strerror(errno) << ")" << std::endl;
        return matching_files;
    }

    if (!S_ISDIR(dir_stat.st_mode)) {
        std::cerr << "Path is not a directory: " << directory << std::endl;
        return matching_files;
    }

    // Open directory using syscalls
    DIR* dir = opendir(directory.c_str());
    if (dir == nullptr) {
        std::cerr << "Error opening directory: " << directory << std::endl;
        std::cerr << "errno: " << errno << " (" << strerror(errno) << ")" << std::endl;
        return matching_files;
    }

    struct dirent* entry;
    while ((entry = readdir(dir)) != nullptr) {
        // Skip . and .. entries
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }

        // Build full path for stat
        std::string full_entry_path = directory + "/" + entry->d_name;

        // Check if it's a regular file
        struct stat file_stat;
        if (stat(full_entry_path.c_str(), &file_stat) == 0) {
            if (S_ISREG(file_stat.st_mode)) {  // Regular file
                std::string filename = entry->d_name;

                // Check if filename starts with the prefix
                if (filename.length() >= filename_prefix.length() &&
                    filename.substr(0, filename_prefix.length()) == filename_prefix) {
                    matching_files.push_back(full_entry_path);
                }
            }
        }
    }

    closedir(dir);

    return matching_files;
}

static void printRunFiles(const std::string& prefix_path) {
    std::vector<std::string> matching_files = findFiles(prefix_path);
    std::vector<Record> content;
    for(const auto& file : matching_files) {
        content.clear();  // Clear before reading each file
        readRecordsFromFile(file, content, 0, MAX_MEMORY);
        std::cout << "Printing records from file: " << file << std::endl;
        for (const auto& r : content) {
            std::cout << r.key << std::endl;
        }
        std::cout << "###########################" << std::endl;
    }
}

#endif // _SORTING_HPP
