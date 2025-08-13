/**
 * This file utilities functions shared between the various merge
 * implementations and the file generator.
 */

#ifndef _COMMON_HPP
#define _COMMON_HPP

#include "config.hpp"
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
static ssize_t appendToFile(int fd, Container&& records, ssize_t size);
static int openFile(const std::string& filename, bool append = false);


static std::string generateUUID() {
    thread_local std::mt19937_64 gen{std::random_device{}()};
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
    std::random_device rd;
    std::mt19937_64 gen(rd());
    std::uniform_int_distribution<uint32_t> dist32;
    for (size_t i = 0; i < ARRAY_SIZE; i++) {
        record.key = dist32(gen);
        record.len = rand() % (RECORD_SIZE - 8) + 8;
        record.rpayload = std::make_unique<char[]>(record.len);
        for (size_t j = 0; j < record.len; j++)
            record.rpayload[j] = rand() & 0xFF;

        size += sizeof(record) + record.len;
        records.push_back(record);

        if (size > MAX_MEMORY) {
            appendToFile(fd, std::move(records), size);
            size = 0;
        }

        if (i % (ARRAY_SIZE / 100) == 0 || i == ARRAY_SIZE - 1) {
            printf("\rProgress: %zu%%", (i * 100) / ARRAY_SIZE);
            fflush(stdout);
        }
    }

    if (!records.empty())
        appendToFile(fd, std::move(records), size);

    printf("\rProgress: 100%%\n");
    close(fd);
}



static inline bool checkSorted(std::vector<Record>& array) {
    for (size_t i = 1; i < array.size(); i++)
        if (array[i].key < array[i-1].key) {
            return false;
        }
    return true;
}

static bool checkSortedFile(const std::string& filename) {
    int fd = openFile(filename);
    ssize_t read_size = 0;
    unsigned long key = 0;
    uint32_t len = 0;

    unsigned long prev_key = 0;

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

        /* Read length */
        read_size = read(fd, &len, sizeof(len));
        if (read_size < 0) {
            std::cerr << "Error reading length: " << strerror(errno) << std::endl;
            close(fd);
            exit(-1);
        }

        /* Skip payload */
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
    if (fd < 0) {
        std::cerr << "openFile failed for " << filename << ": " << strerror(errno) << std::endl;
        exit(EXIT_FAILURE);
    }

    /* Get file size */
    struct stat st;
    if (fstat(fd, &st) < 0) {
        std::cerr << "fstat failed: " << strerror(errno) << std::endl;
        close(fd);
        exit(EXIT_FAILURE);
    }
    size_t file_size = static_cast<size_t>(st.st_size);
    if (offset >= file_size) {
        close(fd);
        return 0; // nothing to read
    }

    /* === Page-align the mmap region === */
    const long page_size = sysconf(_SC_PAGESIZE);

    /**
     * Let's say page_size is 4096 bytes.
     * page_mask is 4095 -> 12 bits set to 1.
     * Being an unsigned long its likely 64 bits -> ~page_mask is 1...100000000000
     * Computing offset & ~page_mask clears off the lower 12 bits of the offset, giving us the highest multiple of page_size
     * lower than the actual offset.
     * So, to know the offset within the page, we simply compute offset - map_offset.
     *
     * Offset: 12345
     * Map Offset: 12288
     * Page Offset: 57
     * File:   |------ Page0 ------|------ Page1 ------|------ Page2 ------|------ Page3 ------|
     * Offset: 0                 4096                8192               12288   ^           16384
     *                                                                          |
     *                                                                  12288 + 57 = 12345
     */
    off_t map_offset = offset & ~(page_size - 1);  // align down
    size_t start_in_map = offset - map_offset;  // where parsing starts within map
    size_t max_map_len = std::min(max_mem + start_in_map, file_size - map_offset);
    if (max_map_len == 0) {
        close(fd);
        return 0;
    }

    void* mapped = mmap(nullptr, max_map_len, PROT_READ, MAP_PRIVATE, fd, map_offset);
    if (mapped == MAP_FAILED) {
        std::cerr << "mmap failed: " << strerror(errno) << std::endl;
        close(fd);
        exit(EXIT_FAILURE);
    }

    const char* base = static_cast<const char*>(mapped);
    size_t pos = start_in_map;
    size_t total_bytes_parsed = 0;

    while (true) {
        /* need at least key + len */
        if (pos + sizeof(uint64_t) + sizeof(uint32_t) > max_map_len) break;

        uint64_t key;
        uint32_t len;

        std::memcpy(&key, base + pos, sizeof(key));
        std::memcpy(&len, base + pos + sizeof(key), sizeof(len));

        size_t record_size = sizeof(uint64_t) + sizeof(uint32_t) + static_cast<size_t>(len);

        /* If record would cross mapped region or exceed allowed max_mem, stop */
        if (
            pos + record_size > max_map_len
            || total_bytes_parsed + record_size > max_mem
        ) break;

        Record rec;
        rec.key = key;
        rec.len = len;
        rec.rpayload = std::make_unique<char[]>(len);
        std::memcpy(rec.rpayload.get(), base + pos + sizeof(uint64_t) + sizeof(uint32_t), len);

        if constexpr (
            std::is_same_v<Container, std::vector<Record>> ||
            std::is_same_v<Container, std::deque<Record>>) {
            records.push_back(std::move(rec));
        } else {
            records.push(std::move(rec)); // for e.g. priority_queue
        }

        pos += record_size;
        total_bytes_parsed += record_size;
    }

    munmap(mapped, max_map_len);
    close(fd);
    return total_bytes_parsed;
}

/**
 * This function appends a list of records to a file using mmap,
 * returning the bytes written. Also, it clears the records Container before returning.
 * It expects the file to be already open.
 * @param fd The file descriptor of the file to append to.
 * @param records The records to append.
 * @return The number of bytes written.
 */
template<typename Container>
static ssize_t appendToFile(int fd, Container&& records, ssize_t size) {

    off_t current_size = lseek(fd, 0, SEEK_END);
    if (current_size == -1) {
        std::cerr << "lseek failed: " << strerror(errno) << std::endl;
        exit(EXIT_FAILURE);
    }

    size_t batch_size = size;
    if constexpr (!std::is_same_v<Container, std::priority_queue<Record, std::vector<Record>, RecordComparator>>) {
        if (batch_size <= 0)
            for (const auto& record : records)
                batch_size += sizeof(record.key) + sizeof(record.len) + record.len;
    } else {
        if (batch_size < 0) {
            std::cerr << "Invalid size" << std::endl;
            exit(EXIT_FAILURE);
        }
    }
    size_t new_size = current_size + batch_size;
    if (ftruncate(fd, new_size) != 0) {
        std::cerr << "ftruncate failed: " << strerror(errno) << std::endl;
        exit(EXIT_FAILURE);
    }

    size_t page_size = sysconf(_SC_PAGE_SIZE);

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


    munmap(map_ptr, map_len);

    if constexpr (!std::is_same_v<Container, std::priority_queue<Record, std::vector<Record>, RecordComparator>>)
        records.clear();

    return static_cast<ssize_t>(batch_size);
}
#endif // !_COMMON_HPP
