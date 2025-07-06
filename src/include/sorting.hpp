#ifndef _SORTING_HPP
#define _SORTING_HPP

#include "common.hpp"
#include "config.hpp"
#include <algorithm>
#include <cstring>
#include <iostream>
#include <string>
#include <cassert>
#include <cstddef>
#include <queue>
#include <vector>
#include <errno.h>
#include <dirent.h>
#include <sys/stat.h>

struct RecordComparator {
    bool operator()(Record a, Record b){
        return (a.key > b.key);
    }
};

static std::string removeSubstring(std::string str, const std::string& toRemove) {
    size_t pos = str.find(toRemove);
    if (pos != std::string::npos) {
        str.erase(pos, toRemove.length());
    }
    return str;
}


static ssize_t writeBatchToFile(int fp, std::vector<Record>& records) {
    std::vector<char> buffer;

    while (!records.empty()) {
        Record record = records.front();
        const char* key_ptr = reinterpret_cast<const char*>(&record.key);
        buffer.insert(buffer.end(), key_ptr, key_ptr + sizeof(record.key));

        const char* len_ptr = reinterpret_cast<const char*>(&record.len);
        buffer.insert(buffer.end(), len_ptr, len_ptr + sizeof(record.len));

        buffer.insert(buffer.end(), record.rpayload, record.rpayload + record.len);
        records.erase(records.begin());
    }

    // Single write call
    ssize_t result = write(fp, buffer.data(), buffer.size());
    if (result < 0) {
        std::cerr << "Error writing to file: " << strerror(errno) << std::endl;
        exit(-1);
    }

    return static_cast<ssize_t>(result);
}

static std::string mergeFiles(const std::string& file1, const std::string& file2, ssize_t max_mem) {
    std::vector<Record> buffer1;
    std::vector<Record> buffer2;
    std::vector<Record> output_buffer;
    std::string output_filename = "/tmp/run";
    output_filename += removeSubstring(basename(file1.c_str()), "run");
    output_filename += "_";
    output_filename += removeSubstring(basename(file2.c_str()), "run");
    ssize_t bytes_to_process1 = getFileSize(file1);
    ssize_t bytes_to_process2 = getFileSize(file2);
    ssize_t bytes_read1 = readRecordsFromFile(file1, buffer1, 0, max_mem/2);
    ssize_t bytes_read2 = readRecordsFromFile(file2, buffer2, 0, max_mem/2);
    // ssize_t bytes_freed = 0;
    // ssize_t last_read = 0;

    int fp = open(output_filename.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0666);
    if (fp < 0) {
        if (errno == EEXIST)
            fp = open(output_filename.c_str(), O_WRONLY | O_APPEND);
        else {
            std::cerr << "Error opening file for writing: " << output_filename << " " << strerror(errno) << std::endl;
            exit(-1);
        }
    }

    while (bytes_read1 < bytes_to_process1 || bytes_read2 < bytes_to_process2){
        while (!buffer1.empty() && !buffer2.empty()) {
            if (buffer1.empty()) {
                for (size_t i = 0; i < buffer2.size(); i++)
                    output_buffer.push_back(std::move(buffer2[i]));
                buffer2.clear();
                break;
            }
            if (buffer2.empty()) {
                for (size_t i = 0; i < buffer1.size(); i++)
                    output_buffer.push_back(std::move(buffer1[i]));
                buffer1.clear();
                break;
            }
            if (buffer1.front() < buffer2.front()) {
                output_buffer.push_back(std::move(buffer1.front()));
                buffer1.erase(buffer1.begin());
            } else {
                output_buffer.push_back(std::move(buffer2.front()));
                buffer2.erase(buffer2.begin());
            }
        }
        buffer1.clear();
        buffer2.clear();
        writeBatchToFile(fp,  output_buffer);
        output_buffer.clear();

        bytes_read1 += readRecordsFromFile(file1, buffer1, 0, max_mem/2);
        bytes_read2 += readRecordsFromFile(file2, buffer2, 0, max_mem/2);

    }
    deleteFile(file1.c_str());
    deleteFile(file2.c_str());
    close(fp);
    return output_filename;
}


static void genSequenceFiles(
    const std::string& input_filename,
    ssize_t offset,
    ssize_t bytes_to_process,
    ssize_t max_memory,
    const std::string& output_filename_prefix
) {
    std::cout << "Generating sequence files..." << std::endl;
    std::cout << "Input file: " << input_filename << std::endl;
    std::cout << "Offset: " << offset << std::endl;
    std::cout << "Bytes to process: " << bytes_to_process << std::endl;
    std::cout << "Max memory: " << max_memory << std::endl;
    std::cout << "Output file prefix: " << output_filename_prefix << std::endl;

    ssize_t usable_mem = (max_memory * 9)/10; // Leaving a 10% of space to read and write records
    std::vector<Record> unsorted;
    std::priority_queue<Record, std::vector<Record>, RecordComparator> heap;
    std::vector<Record> buffer;
    // Skip the unsorted initialization and push the records directly to the heap
    ssize_t bytes_read = readRecordsFromFile(input_filename, heap, offset, usable_mem);
    ssize_t run = 1, bytes_freed = 0, last_read = bytes_read;
    while (bytes_read < bytes_to_process || !heap.empty()) { // I have to process all the bytes in the file
        std::string output_filename = output_filename_prefix + std::to_string(run);
        while (!heap.empty()) { // A run is complete when the heap is empty
            Record record = heap.top();
            heap.pop();
            bytes_freed += appendRecordToFile(output_filename, record);
            last_read = readRecordsFromFile(input_filename, buffer, offset + bytes_read, bytes_freed);
            // If I manage to read something I have to reset the bytes_written counter
            if (last_read) bytes_freed -= last_read;

            bytes_read += last_read;
            for (auto& r : buffer) {
                if (r < record) unsorted.push_back(std::move(r));
                else heap.push(std::move(r));
            }
            buffer.clear();
        }
        for(auto& r : unsorted)
            heap.push(std::move(r));
        unsorted.clear();
        // if (heap.empty() && bytes_read < bytes_to_process && bytes_freed)
        //     bytes_read += readRecordsFromFile(input_filename, heap, offset + bytes_read, bytes_freed);
        std::cout << "Bytes read: " << bytes_read << std::endl;
        std::cout << "Bytes freed: " << bytes_freed << std::endl;
        std::cout << "Bytes remaining: " << bytes_to_process - bytes_read << std::endl;
        std::cout << "Heap: " << heap.size() << std::endl;
        std::cout << "Unsorted: " << unsorted.size() << std::endl;
        run++;
    }
    std::cout << "Files generated in " << run - 1 << " runs!" << std::endl;
}


// static void genSequenceFiles(
//     const std::string& input_filename,
//     ssize_t offset,
//     ssize_t bytes_to_process,
//     ssize_t max_size,
//     const std::string& output_filename
// ) {
//     std::cout << "Bytes to process: " << bytes_to_process << std::endl;
//     ssize_t running_size = (max_size * 4) / 5; // Memory for heap and buffers
//     std::priority_queue<Record, std::vector<Record>, RecordComparator> heap;
//     std::vector<Record> unsorted; // Records that belong to next run
//     std::vector<Record> buffer;   // Buffer for reading new records

//     // Initial load of records into heap
//     ssize_t bytes_read = readRecordsFromFile(input_filename, heap, offset, running_size);
//     ssize_t total_bytes_read = bytes_read;
//     ssize_t run = 1;

//     while (!heap.empty() || !unsorted.empty()) {
//         // Start a new run
//         std::string current_output = output_filename + std::to_string(run);
//         Record last_output_record; // Track last record written to maintain order
//         bool first_record = true;

//         // Process current run
//         while (!heap.empty()) {
//             Record record = heap.top();
//             heap.pop();

//             // Write record to current run file
//             ssize_t bytes_written = appendRecordToFile(current_output, record);
//             last_output_record = record;
//             first_record = false;

//             // Try to read more records if there's space and more data available
//             if (total_bytes_read < bytes_to_process) {
//                 ssize_t new_bytes = readRecordsFromFile(input_filename, buffer, offset + total_bytes_read,
//                                                      bytes_written);
//                 total_bytes_read += new_bytes;

//                 // Classify new records: can go in current run or must wait for next run
//                 for (auto& r : buffer) {
//                     if (first_record || r >= last_output_record) {
//                         heap.push(std::move(r)); // Can be in current run
//                     } else {
//                         unsorted.push_back(std::move(r)); // Must wait for next run
//                     }
//                 }
//                 buffer.clear();
//             }
//         }

//         // Move unsorted records to heap for next run
//         for (auto& r : unsorted) {
//             heap.push(std::move(r));
//         }
//         unsorted.clear();

//         run++;
//     }

//     std::cout << "Files generated in " << (run - 1) << " runs!" << std::endl;
// }

static std::vector<std::string> findFiles(const std::string& prefix_path) {

    std::vector<std::string> matching_files;

    std::cout << "Extracting directory and filename prefix..." << std::endl;

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

    std::cout << "Directory: " << directory << std::endl;
    std::cout << "Filename prefix: " << filename_prefix << std::endl;

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

    std::cout << "Found " << matching_files.size() << " matching files" << std::endl;
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
