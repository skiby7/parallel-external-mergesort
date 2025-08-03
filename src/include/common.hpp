/**
 * This file utilities functions shared between the various merge
 * implementations and the file generator.
 */

#ifndef _COMMON_HPP
#define _COMMON_HPP

#include "config.hpp"
#include "feistel.hpp"
#include "record.hpp"
#include <cerrno>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <iostream>
#include <queue>
#include <stdlib.h>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>
#include <random>
#include <sstream>
#include <iomanip>

template<typename Container>
static ssize_t appendToFile(int fd, Container&& records, ssize_t size = -1);
static int openFile(const std::string& filename, bool append = false);


static std::string generateUUID() {
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint32_t> dist32;
    std::uniform_int_distribution<uint16_t> dist16;

    uint32_t data1 = dist32(gen);
    uint16_t data2 = dist16(gen);
    uint16_t data3 = dist16(gen);
    uint16_t data4 = dist16(gen);
    uint64_t data5 = ((uint64_t)dist32(gen) << 32) | dist32(gen);

    data3 = (data3 & 0x0FFF) | 0x4000;

    data4 = (data4 & 0x3FFF) | 0x8000;

    std::ostringstream oss;
    oss << std::hex << std::setfill('0')
        << std::setw(8) << data1 << "-"
        << std::setw(4) << data2 << "-"
        << std::setw(4) << data3 << "-"
        << std::setw(4) << data4 << "-"
        << std::setw(12) << (data5 & 0xFFFFFFFFFFFFULL);

    return oss.str();
}

static void generateFile(std::string filename) {
    int fd = open(filename.c_str(), O_RDWR | O_CREAT | O_APPEND, 0666);
    if (fd < 0) {
        if (errno == EEXIST)
            fd = open(filename.c_str(), O_RDWR | O_APPEND);
        else {
            std::cerr << "Error opening file for writing: " << filename << " " << strerror(errno) << std::endl;
            exit(-1);
        }
    }

    Record record;
    std::deque<Record> records;
    size_t size = 0;

    for (size_t i = 0; i < ARRAY_SIZE; i++) {
        record.key = feistel_encrypt((uint32_t)i, 0xDEADBEEF, ROUNDS);
        record.len = rand() % (RECORD_SIZE - 8) + 8;
        record.rpayload = std::make_unique<char[]>(record.len);
        for (size_t j = 0; j < record.len; j++)
            record.rpayload[j] = feistel_encrypt(j + i, 0x01, 1) & 0xFF;

        size += sizeof(record) + record.len;
        records.push_back(record);

        if (size > MAX_MEMORY) {
            size = 0;
            appendToFile(fd, std::move(records));
        }

        if (i % (ARRAY_SIZE / 100) == 0 || i == ARRAY_SIZE - 1) {
            printf("\rProgress: %zu%%", (i * 100) / ARRAY_SIZE);
            fflush(stdout);
        }
    }

    if (!records.empty())
        appendToFile(fd, std::move(records));

    printf("\rProgress: 100%%\n");
    close(fd);
}



static inline bool checkSorted(std::vector<unsigned long>& array) {
    for (size_t i = 1; i < array.size(); i++)
        if (array[i] < array[i-1]) {
            // std::cout << "Array is not sorted at index " << i << ": " << array[i] << " < " << array[i0] << std::endl;
            return false;
        }
    return true;
}

static bool checkSortedFile(const std::string& filename) {
    int fd = openFile(filename);
    ssize_t read_size = 0;
    unsigned long key = 0;
    uint32_t len = 0;

    unsigned long prev_key = -1;

    while(true) {
        read_size = read(fd, &key, sizeof(key));
        if (read_size == 0) break; // EOF
        if (read_size < 0) {
            std::cerr << "Error reading key: " << strerror(errno) << std::endl;
            close(fd);
            exit(-1);
        }
        if (key < prev_key) {
            std::cerr << "Array is not sorted: " << key << " < " << prev_key << std::endl;
            close(fd);
            return false;
        }
        prev_key = key;

        // Read length
        read_size = read(fd, &len, sizeof(len));
        if (read_size < 0) {
            std::cerr << "Error reading length: " << strerror(errno) << std::endl;
            close(fd);
            exit(-1);
        }

        // Skip payload
        if (lseek(fd, len, SEEK_CUR) == -1) {
            std::cerr << "Error seeking past payload: " << strerror(errno) << std::endl;
            close(fd);
            exit(-1);
        }
    }

    close(fd);
    return true;
}



static size_t getFileSize(const std::string& filename) {
    struct stat stat_buf;
    if (stat(filename.c_str(), &stat_buf) != 0) {
        std::cerr << "Error getting file stats: " << filename << std::endl;
        return 0;
    }
    return stat_buf.st_size;
}

static bool deleteFile(const char* filename) {
    if (unlink(filename) == 0) {
        return true;
    } else {
        std::cerr << "Error deleting file '" << filename << "': "
                  << strerror(errno) << std::endl;
        return false;
    }
}

static int openFile(const std::string& filename, bool append) {
    int fd = open(filename.c_str(), O_RDWR | O_CREAT | (append ? O_APPEND : 0), 0666);
    if (fd < 0) {
        if (errno == EEXIST)
            fd = open(filename.c_str(), O_RDWR | (append ? O_APPEND : 0));
        else {
            std::cerr << "Error opening file for writing: " << filename
                      << " " << strerror(errno) << std::endl;
            exit(-1);
        }
    }
    return fd;
}


/**
 * This function appends a list of records to a file using mmap,
 * returning the bytes written. Also, it clears the records deque before returning.
 *
 * @param fd The file descriptor of the file to append to.
 * @param records The records to append.
 * @return The number of bytes written.
 */
template<typename Container>
static ssize_t appendToFile(int fd, Container&& records, ssize_t size) {
    size_t page_size = sysconf(_SC_PAGE_SIZE);

    off_t current_size = lseek(fd, 0, SEEK_END);
    if (current_size == -1) {
        std::cerr << "lseek failed: " << strerror(errno) << std::endl;
        exit(EXIT_FAILURE);
    }

    size_t batch_size = 0;
    if constexpr (!std::is_same_v<Container, std::priority_queue<Record, std::vector<Record>, RecordComparator>>) {
        for (const auto& record : records)
            batch_size += sizeof(record.key) + sizeof(record.len) + record.len;
    } else {
        if (size < 0) {
            std::cerr << "Invalid size" << std::endl;
            exit(EXIT_FAILURE);
        }
        batch_size = size;
    }
    std::cout << "Debug: fd=" << fd << ", current_size=" << current_size
                 << ", batch_size=" << batch_size << std::endl;
    size_t new_size = current_size + batch_size;
    if (ftruncate(fd, new_size) != 0) {
        std::cerr << "ftruncate failed: " << strerror(errno) << std::endl;
        exit(EXIT_FAILURE);
    }

    off_t aligned_offset = current_size & ~(page_size - 1);
    size_t delta = current_size - aligned_offset;
    size_t map_len = delta + batch_size;

    void* map_ptr = mmap(nullptr, map_len, PROT_WRITE, MAP_SHARED, fd, aligned_offset);
    if (map_ptr == MAP_FAILED) {
        std::cerr << "mmap failed: " << strerror(errno) << std::endl;
        exit(EXIT_FAILURE);
    }

    char* out = static_cast<char*>(map_ptr) + delta;
    size_t offset = 0;

    auto serialize_record = [&](const Record& record) {
        memcpy(out + offset, &record.key, sizeof(record.key));
        offset += sizeof(record.key);
        memcpy(out + offset, &record.len, sizeof(record.len));
        offset += sizeof(record.len);
        memcpy(out + offset, record.rpayload.get(), record.len);
        offset += record.len;
    };

    if constexpr (std::is_same_v<Container, std::priority_queue<Record, std::vector<Record>, RecordComparator>>) {
        while (!records.empty()) {
            serialize_record(records.top());
            records.pop();
        }
    } else {
        for (const auto& record : records)
            serialize_record(record);
    }

    if (msync(map_ptr, map_len, MS_SYNC) != 0)
        std::cerr << "msync failed: " << strerror(errno) << std::endl;


    if (munmap(map_ptr, map_len) != 0) {
        std::cerr << "munmap failed: " << strerror(errno) << std::endl;
        exit(EXIT_FAILURE);
    }

    if constexpr (!std::is_same_v<Container, std::priority_queue<Record, std::vector<Record>, RecordComparator>>)
        records.clear();

    return static_cast<ssize_t>(batch_size);
}

static ssize_t appendRecordToFile(const std::string& filename, Record record) {
    int fd = openFile(filename, true);
    ssize_t bytes_written = 0;
    if ((bytes_written = write(fd, &record.key, sizeof(record.key))) < 0) exit(-1);
    if ((bytes_written += write(fd, &record.len, sizeof(record.len))) < 0) exit(-1);
    if ((bytes_written += write(fd, record.rpayload.get(), record.len)) < 0) exit(-1);
    close(fd);
    return bytes_written;
}


/**
 * Read records from a file into a container that can be a vector, a deque or a priority queue.
 * It reads a chunk of data from the file and parses it into records to minimize the number of system calls.
 *
 * @param filename The name of the file to read from.
 * @param records The container to store the records in.
 * @param offset The offset in the file to start reading from.
 * @param max_mem The maximum amount of memory to use for reading.
 * @return The number of bytes read from the file.
 */
template<typename Container>
static size_t readRecordsFromFile(const std::string& filename, Container& records, size_t offset, size_t max_mem) {

    int fd = openFile(filename);

    if (lseek(fd, static_cast<off_t>(offset), SEEK_SET) < 0) {
        std::cerr << "lseek failed: " << strerror(errno) << std::endl;
        close(fd);
        exit(EXIT_FAILURE);
    }

    std::vector<char> read_buf(max_mem);
    size_t bytes_in_buffer = 0;
    size_t buffer_offset = 0;
    size_t total_bytes_read = 0;

    while (total_bytes_read < max_mem) {
        if (buffer_offset < bytes_in_buffer) {
            size_t remaining = bytes_in_buffer - buffer_offset;
            memmove(read_buf.data(), read_buf.data() + buffer_offset, remaining);
            bytes_in_buffer = remaining;
        } else
            bytes_in_buffer = 0;

        ssize_t read_bytes = read(fd, read_buf.data() + bytes_in_buffer, read_buf.size() - bytes_in_buffer);
        if (read_bytes < 0) {
            std::cerr << "Read error: " << strerror(errno) << std::endl;
            close(fd);
            exit(EXIT_FAILURE);
        } else if (read_bytes == 0) {
            break; // EOF
        }

        bytes_in_buffer += static_cast<size_t>(read_bytes);

        while (buffer_offset + sizeof(uint64_t) + sizeof(uint32_t) <= bytes_in_buffer) {
            const char* base = read_buf.data() + buffer_offset;
            uint64_t key = *reinterpret_cast<const uint64_t*>(base);
            uint32_t len = *reinterpret_cast<const uint32_t*>(base + sizeof(uint64_t));

            size_t record_size = sizeof(uint64_t) + sizeof(uint32_t) + static_cast<size_t>(len);

            if (buffer_offset + record_size > bytes_in_buffer || total_bytes_read + record_size > max_mem)
                goto finish;

            Record rec;
            rec.key = key;
            rec.len = len;
            rec.rpayload = std::make_unique<char[]>(len);
            std::memcpy(rec.rpayload.get(), base + sizeof(uint64_t) + sizeof(uint32_t), len);

            if constexpr (
                std::is_same_v<Container, std::vector<Record>> ||
                std::is_same_v<Container, std::deque<Record>>) {
                records.push_back(std::move(rec));
            } else {
                records.push(std::move(rec));
            }

            buffer_offset += record_size;
            total_bytes_read += record_size;
        }
    }

finish:
    close(fd);
    return total_bytes_read;
}


/**
 * Find files in a directory with a given prefix.
 *
 * @param prefix_path The path to the directory and the prefix of the files to find.
 * @return A vector of strings containing the paths of the matching files.
 */
static std::vector<std::string> findFiles(const std::string& prefix_path) {
    std::vector<std::string> matching_files;
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

    if (directory.empty()) {
        directory = ".";
    }
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

    DIR* dir = opendir(directory.c_str());
    if (dir == nullptr) {
        std::cerr << "Error opening directory: " << directory << std::endl;
        std::cerr << "errno: " << errno << " (" << strerror(errno) << ")" << std::endl;
        return matching_files;
    }

    struct dirent* entry;
    while ((entry = readdir(dir)) != nullptr) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }
        std::string full_entry_path = directory + "/" + entry->d_name;
        struct stat file_stat;
        if (stat(full_entry_path.c_str(), &file_stat) == 0) {
            if (S_ISREG(file_stat.st_mode)) {  // Regular file
                std::string filename = entry->d_name;
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


#endif // !_COMMON_HPP
