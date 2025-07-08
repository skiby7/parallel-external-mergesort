#ifndef _SORTING_HPP
#define _SORTING_HPP

#include "common.hpp"
#include "config.hpp"
#include <algorithm>
#include <cstring>
#include <deque>
#include <iostream>
#include <string>
#include <cassert>
#include <cstddef>
#include <queue>
#include <vector>
#include <errno.h>
#include <dirent.h>
#include <sys/stat.h>
#include <omp.h>


static void printRunFiles(const std::string& prefix_path);

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


static ssize_t writeBatchToFile(int fp, std::deque<Record>& records) {
    std::vector<char> buffer;

    while (!records.empty()) {
        Record record = records.front();

        // Write key
        buffer.resize(buffer.size() + sizeof(record.key));
        memcpy(buffer.data() + buffer.size() - sizeof(record.key), &record.key, sizeof(record.key));

        // Write len (4 bytes)
        buffer.resize(buffer.size() + sizeof(record.len));
        memcpy(buffer.data() + buffer.size() - sizeof(record.len), &record.len, sizeof(record.len));

        // Write payload
        buffer.insert(buffer.end(), record.rpayload, record.rpayload + record.len);

        records.pop_front();
    }

    ssize_t result = write(fp, buffer.data(), buffer.size());
    if (result < 0) {
        std::cerr << "Error writing to file: " << strerror(errno) << std::endl;
        exit(-1);
    }

    return static_cast<ssize_t>(result);
}
static void mergeFiles(const std::string& file1, const std::string& file2,
                       const std::string& output_filename, const ssize_t max_mem) {
    std::deque<Record> buffer1;
    std::deque<Record> buffer2;
    std::deque<Record> output_buffer;

    ssize_t bytes_to_process1 = getFileSize(file1);
    ssize_t bytes_to_process2 = getFileSize(file2);
    size_t buffer_size = max_mem / 4;

    ssize_t bytes_read1 = 0;
    ssize_t bytes_read2 = 0;
    size_t bytes_written = 0;

    // Track current positions in buffers
    size_t pos1 = 0, pos2 = 0;

    // Flags to track if files are exhausted
    bool file1_exhausted = false;
    bool file2_exhausted = false;

    int fp = open(output_filename.c_str(), O_WRONLY | O_CREAT | O_DIRECT, 0666);
    if (fp < 0) {
        if (errno == EEXIST)
            fp = open(output_filename.c_str(), O_WRONLY | O_DIRECT);
        else {
            std::cerr << "Error opening file for writing: " << output_filename
                      << " " << strerror(errno) << std::endl;
            exit(-1);
        }
    }

    std::cout << "Merging " << file1 << " and " << file2 << " into " << output_filename << std::endl;

    // Initial buffer fills
    if (bytes_read1 < bytes_to_process1) {
        bytes_read1 += readRecordsFromFile(file1, buffer1, bytes_read1, buffer_size);
        if (bytes_read1 >= bytes_to_process1) file1_exhausted = true;
    } else {
        file1_exhausted = true;
    }

    if (bytes_read2 < bytes_to_process2) {
        bytes_read2 += readRecordsFromFile(file2, buffer2, bytes_read2, buffer_size);
        if (bytes_read2 >= bytes_to_process2) file2_exhausted = true;
    } else {
        file2_exhausted = true;
    }

    // Main merge loop
    while (pos1 < buffer1.size() || pos2 < buffer2.size()) {
        // Refill buffer1 if needed and possible
        if (pos1 >= buffer1.size() && !file1_exhausted) {
            buffer1.clear();
            pos1 = 0;
            if (bytes_read1 < bytes_to_process1) {
                bytes_read1 += readRecordsFromFile(file1, buffer1, bytes_read1, buffer_size);
                if (bytes_read1 >= bytes_to_process1) file1_exhausted = true;
            } else {
                file1_exhausted = true;
            }
        }

        // Refill buffer2 if needed and possible
        if (pos2 >= buffer2.size() && !file2_exhausted) {
            buffer2.clear();
            pos2 = 0;
            if (bytes_read2 < bytes_to_process2) {
                bytes_read2 += readRecordsFromFile(file2, buffer2, bytes_read2, buffer_size);
                if (bytes_read2 >= bytes_to_process2) file2_exhausted = true;
            } else {
                file2_exhausted = true;
            }
        }

        // Decide which element to take
        bool take_from_buffer1 = false;

        if (pos1 >= buffer1.size()) {
            // buffer1 is exhausted, take from buffer2
            take_from_buffer1 = false;
        } else if (pos2 >= buffer2.size()) {
            // buffer2 is exhausted, take from buffer1
            take_from_buffer1 = true;
        } else {
            // Both buffers have data, compare and take smaller
            take_from_buffer1 = (buffer1[pos1] <= buffer2[pos2]);
        }

        // Move the chosen element to output buffer
        if (take_from_buffer1) {
            output_buffer.push_back(std::move(buffer1[pos1]));
            pos1++;
        } else {
            output_buffer.push_back(std::move(buffer2[pos2]));
            pos2++;
        }

        // Write output buffer when it gets large enough
        if (output_buffer.size() * sizeof(Record) >= buffer_size) {
            bytes_written += appendRecordsToFile(output_filename, output_buffer);
            output_buffer.clear();


        }
    }

    // Write any remaining output
    if (!output_buffer.empty()) {
        bytes_written += appendRecordsToFile(output_filename, output_buffer);
    }

    deleteFile(file1.c_str());
    deleteFile(file2.c_str());
    std::cout << "\nWritten " << bytes_written << " bytes." << std::endl;
    close(fp);

}

// static void mergeFiles(const std::string& file1, const std::string& file2,  const std::string& output_filename, const ssize_t max_mem) {
//     // Using deque to be able to use pop_front()
//     std::deque<Record> buffer1;
//     std::deque<Record> buffer2;
//     std::deque<Record> output_buffer;

//     size_t bytes_to_process1 = getFileSize(file1);
//     size_t bytes_to_process2 = getFileSize(file2);
//     // Using a fourth of the memory for each buffer so that the output_buffer can hold the merged records
//     // Without having to allocate/deallocate memory in the loop
//     ssize_t bytes_read1 = 0;
//     ssize_t bytes_read2 = 0;
//     size_t bytes_written = 0;

//     int fp = open(output_filename.c_str(), O_WRONLY | O_CREAT | O_DIRECT, 0666);
//     if (fp < 0) {
//         if (errno == EEXIST)
//             fp = open(output_filename.c_str(), O_WRONLY | O_DIRECT);
//         else {
//             std::cerr << "Error opening file for writing: " << output_filename << " " << strerror(errno) << std::endl;
//             exit(-1);
//         }
//     }
//     std::cout << "Merging " << file1 << " and " << file2 << " into " << output_filename << std::endl;


//     // while (bytes_read1 < bytes_to_process1 || bytes_read2 < bytes_to_process2) {
//     while (bytes_written != bytes_to_process1 + bytes_to_process2) {
//         bytes_read1 += readRecordsFromFile(file1, buffer1, bytes_read1, max_mem/4);
//         bytes_read2 += readRecordsFromFile(file2, buffer2, bytes_read2, max_mem/4);
//         assert(std::is_sorted(buffer1.begin(), buffer1.end()));
//         assert(std::is_sorted(buffer2.begin(), buffer2.end()));
//         std::merge(std::make_move_iterator(buffer1.begin()),
//                    std::make_move_iterator(buffer1.end()),
//                    std::make_move_iterator(buffer2.begin()),
//                    std::make_move_iterator(buffer2.end()),
//                    std::back_inserter(output_buffer)
//         );
//         assert(checkSorted(output_buffer));
//         buffer1.clear();
//         buffer2.clear();
//         bytes_written += appendRecordsToFile(output_filename, output_buffer);

//         // bytes_written += writeBatchToFile(fp, output_buffer);
//         output_buffer.clear();
//     }
//     deleteFile(file1.c_str());
//     deleteFile(file2.c_str());
//     std::cout << "Written " << bytes_written << " bytes." << std::endl;
//     close(fp);
// }


static void genSequenceFiles(
    const std::string& input_filename,
    ssize_t offset,
    ssize_t bytes_to_process,
    ssize_t max_memory,
    const std::string& output_filename_prefix
) {
    ssize_t usable_mem = (max_memory * 9)/10; // Leaving a 10% of space to read and write records
    std::vector<Record> unsorted;
    std::priority_queue<Record, std::vector<Record>, RecordComparator> heap;
    std::vector<Record> buffer;

    // Skip the unsorted initialization and push the records directly to the heap
    ssize_t bytes_read = readRecordsFromFile(input_filename, heap, offset, std::min(usable_mem, bytes_to_process));
    ssize_t run = 1, free_bytes = usable_mem - bytes_read, last_read = bytes_read, bytes_remaining = bytes_to_process - bytes_read;

    while (bytes_remaining > 0 || !heap.empty()) { // I have to process all the bytes in the file
        std::string output_filename = output_filename_prefix + std::to_string(run);
        while (!heap.empty()) { // A run is complete when the heap is empty
            Record record = heap.top();
            heap.pop();

            // Flush the buffer to the output file and store the bytes freed
            free_bytes += appendRecordToFile(output_filename, record);
            if (bytes_remaining > 0) {
                // If there are bytes remaining to process, read them into the buffer or at least read some bytes
                last_read = readRecordsFromFile(input_filename, buffer, offset + bytes_read, std::min(bytes_remaining, free_bytes));
                // If I manage to read something I have to update the free_bytes counter
                // And the bytes_read counter
                free_bytes -= last_read;
                bytes_read += last_read;
            }
            for (auto& r : buffer) {
                if (r < record) unsorted.push_back(std::move(r));
                else heap.push(std::move(r));
            }
            buffer.clear();
        }
        // If I'm here it means that the heap is empty, so let's process the unsorted set
        for(auto& r : unsorted)
            heap.push(std::move(r));
        // Now the heap is full again and we can clear the unsorted set
        unsorted.clear();
        // If I didn't manage to read anything in the loop above, let's fill the heap again
        if (heap.empty() && bytes_remaining > 0) {
            last_read += readRecordsFromFile(input_filename, heap, offset + bytes_read, std::min(bytes_remaining, free_bytes));
            free_bytes -= last_read;
            bytes_read += last_read;
        }
        // Update the bytes_remaining counter
        bytes_remaining = bytes_to_process - bytes_read;
        #pragma omp critical
        {
            std::cout << "Thread: " << omp_get_thread_num() << std::endl;
            std::cout << "Offset: " << offset << std::endl;
            std::cout << "Bytes to process: " << bytes_to_process << std::endl;
            std::cout << "Bytes read: " << bytes_read << std::endl;
            std::cout << "Bytes freed: " << free_bytes << std::endl;
            std::cout << "Bytes remaining: " << bytes_remaining << std::endl;
            std::cout << "Heap: " << heap.size() << std::endl;
            std::cout << "Unsorted: " << unsorted.size() << std::endl;
        }
        run++;
    }
    std::cout << "Files generated in " << run - 1 << " runs!" << std::endl;
}

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
