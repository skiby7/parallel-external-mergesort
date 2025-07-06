#ifndef _SORTING_HPP
#define _SORTING_HPP

#include "common.hpp"
#include "config.hpp"
#include <cstring>
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



static std::string mergeFiles(const std::string& file1, const std::string& file2, size_t max_mem) {
    std::vector<Record> buffer1;
    std::vector<Record> buffer2;
    std::string output_filename = "/tmp/";
    output_filename += basename(file1.c_str());
    output_filename += basename(file2.c_str());
    size_t bytes_read1 = readRecordsFromFile(file1, buffer1, 0, max_mem/2);
    size_t bytes_read2 = readRecordsFromFile(file2, buffer2, 0, max_mem/2);
    size_t bytes_written = 0;

    while (!buffer1.empty() && !buffer2.empty()) {
        if (buffer1.front() < buffer2.front()) {
            bytes_written = appendRecordToFile(output_filename, buffer1.front());
            buffer1.erase(buffer1.begin());
            bytes_read1 += readRecordsFromFile(file1, buffer1, bytes_read1, bytes_written);
        } else {
            bytes_written = appendRecordToFile(output_filename, buffer2.front());
            buffer2.erase(buffer2.begin());
            bytes_read2 += readRecordsFromFile(file2, buffer2, bytes_read2, bytes_written);
        }
    }
    deleteFile(file1.c_str());
    deleteFile(file2.c_str());

    return output_filename;
}

static void genSequenceFiles(
    const std::string& input_filename,
    size_t offset,
    size_t bytes_to_process,
    size_t max_size,
    const std::string& output_filename
) {
    size_t running_size = (max_size * 4)/5; // Leaving a little bit of space to read and write records
    std::vector<Record> unsorted;
    std::priority_queue<Record, std::vector<Record>, RecordComparator> heap;
    std::vector<Record> buffer;
    // Skip the unsorted initialization and push the records directly to the heap
    size_t bytes_read = readRecordsFromFile(input_filename, heap, offset, running_size);
    size_t run = 1, bytes_written = 0, last_read = bytes_read;
    while (last_read && bytes_to_process > bytes_read) {
        while (!heap.empty()) {
            Record record = heap.top();
            heap.pop();
            bytes_written = appendRecordToFile(output_filename + std::to_string(run), record);
            last_read = readRecordsFromFile(input_filename, buffer, offset + bytes_read, bytes_written);
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
        run++;
    }
    std::cout << "Files generated in " << run << " runs!" << std::endl;
}


// static void genSequenceFiles(
//     const std::string& input_filename,
//     size_t offset,
//     size_t bytes_to_process,
//     size_t max_size,
//     const std::string& output_filename
// ) {
//     std::cout << "Bytes to process: " << bytes_to_process << std::endl;
//     size_t running_size = (max_size * 4) / 5; // Memory for heap and buffers
//     std::priority_queue<Record, std::vector<Record>, RecordComparator> heap;
//     std::vector<Record> unsorted; // Records that belong to next run
//     std::vector<Record> buffer;   // Buffer for reading new records

//     // Initial load of records into heap
//     size_t bytes_read = readRecordsFromFile(input_filename, heap, offset, running_size);
//     size_t total_bytes_read = bytes_read;
//     size_t run = 1;

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
//             appendRecordToFile(current_output, record);
//             last_output_record = record;
//             first_record = false;

//             // Try to read more records if there's space and more data available
//             if (heap.size() + unsorted.size() < running_size && total_bytes_read < bytes_to_process) {
//                 size_t new_bytes = readRecordsFromFile(input_filename, buffer, offset + total_bytes_read,
//                                                      running_size - heap.size() - unsorted.size());
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

static void printRunFiles(const std::string& file_name) {
    std::vector<std::string> matching_files = findFiles(file_name)
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
