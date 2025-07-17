#ifndef _FILESYSTEM_HPP
#define _FILESYSTEM_HPP

#include "config.hpp"
#include "record.hpp"
#include "sorting.hpp"
#include <iostream>
#include <errno.h>
#include <dirent.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <cmath>
#include <ff/ff.hpp>
#include <ff/pipeline.hpp>
#include <sys/mman.h>
#include <unistd.h>  // for sysconf
#include <cstdint>

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

static int openFile(const std::string& filename, bool append = false) {
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
static ssize_t appendToFile(int fd, Container&& records) {
    size_t page_size = sysconf(_SC_PAGE_SIZE);

    off_t current_size = lseek(fd, 0, SEEK_END);
    if (current_size == -1) {
        std::cerr << "lseek failed: " << strerror(errno) << std::endl;
        exit(EXIT_FAILURE);
    }

    size_t batch_size = 0;
    for (const auto& record : records)
        batch_size += sizeof(record.key) + sizeof(record.len) + record.len;

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

    for (const auto& record : records) {
        memcpy(out + offset, &record.key, sizeof(record.key));
        offset += sizeof(record.key);

        memcpy(out + offset, &record.len, sizeof(record.len));
        offset += sizeof(record.len);

        memcpy(out + offset, record.rpayload.get(), record.len);
        offset += record.len;
    }

    if (msync(map_ptr, map_len, MS_SYNC) != 0)
        std::cerr << "msync failed: " << strerror(errno) << std::endl;


    if (munmap(map_ptr, map_len) != 0) {
        std::cerr << "munmap failed: " << strerror(errno) << std::endl;
        exit(EXIT_FAILURE);
    }

    if (!std::is_same_v<Container, std::priority_queue<Record, std::vector<Record>, RecordComparator>>)
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

template<typename Container>
static ssize_t appendRecordsToFile(const std::string& filename, Container records) {
    int fd = openFile(filename, true);
    ssize_t bytes_written = 0;
    for (auto record: records) {
        if ((bytes_written += write(fd, &record.key, sizeof(record.key))) < 0) exit(-1);
        if ((bytes_written += write(fd, &record.len, sizeof(record.len))) < 0) exit(-1);
        if ((bytes_written += write(fd, record.rpayload, record.len)) < 0) exit(-1);
    }
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
    constexpr size_t kInitialBufSize = 64 * 1024;

    int fd = openFile(filename);

    if (lseek(fd, static_cast<off_t>(offset), SEEK_SET) < 0) {
        std::cerr << "lseek failed: " << strerror(errno) << std::endl;
        close(fd);
        exit(EXIT_FAILURE);
    }

    std::vector<char> read_buf(std::min(kInitialBufSize, max_mem));
    size_t bytes_in_buffer = 0;
    size_t buffer_offset = 0;
    size_t total_bytes_read = 0;

    while (total_bytes_read < max_mem) {
        if (buffer_offset < bytes_in_buffer) {
            size_t remaining = bytes_in_buffer - buffer_offset;
            memmove(read_buf.data(), read_buf.data() + buffer_offset, remaining);
            bytes_in_buffer = remaining;
        } else {
            bytes_in_buffer = 0;
        }
        buffer_offset = 0;

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

#endif // _FILESYSTEM_HPP
