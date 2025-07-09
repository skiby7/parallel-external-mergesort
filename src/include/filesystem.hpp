#ifndef _FILESYSTEM_HPP
#define _FILESYSTEM_HPP

#include "config.hpp"
#include "record.hpp"
#include <iostream>

#include <fcntl.h>
#include <sys/stat.h>
#include <cmath>
#include <ff/ff.hpp>
#include <ff/pipeline.hpp>

static size_t getFileSize(const std::string& filename) {
    struct stat stat_buf;
    if (stat(filename.c_str(), &stat_buf) != 0) {
        std::cerr << "Error getting file stats: " << filename << std::endl;
        return 0;
    }
    return stat_buf.st_size;
}

static std::vector<std::pair<size_t, size_t>> computeChunks(const std::string& filename, size_t num_threads) {
    std::vector<std::pair<size_t, size_t>> chunks;
    // Each thread should have a chunk of data to sort
    size_t chunk_size = getFileSize(filename) / num_threads;
    int fp = open(filename.c_str(), O_RDONLY);
    if (fp < 0) {
        std::cerr << "Error opening file for reading: " << filename << " " << strerror(errno) << std::endl;
        exit(-1);
    }
    size_t start = 0, read_size = 0;
    size_t current_pos = 0;
    unsigned long key;
    uint32_t len;
    while(true) {
        read_size = read(fp, &key, sizeof(key));
        if (read_size == 0) {
            current_pos = lseek(fp, 0, SEEK_CUR);
            chunks.push_back({start, current_pos});
            break;
        } // EOF
        if (read_size < 0) {
            std::cerr << "Error reading key: " << strerror(errno) << std::endl;
            close(fp);
            exit(-1);
        }

        // Read length
        read_size = read(fp, &len, sizeof(len));
        if (read_size < 0) {
            std::cerr << "Error reading length: " << strerror(errno) << std::endl;
            close(fp);
            exit(-1);
        }

        // Skip payload
        if (lseek(fp, len, SEEK_CUR) == -1) {
            std::cerr << "Error seeking past payload: " << strerror(errno) << std::endl;
            close(fp);
            exit(-1);
        }

        // Get current position
        current_pos = lseek(fp, 0, SEEK_CUR);

        // Check if we've exceeded target chunk size
        if (current_pos - start >= chunk_size) {
            chunks.push_back({start, current_pos});
            start = current_pos;
        }
    }
    close(fp);
    return chunks;
}

static ssize_t writeRecordsToFile(const std::string& filename, std::vector<Record> records) {
    int fp = open(filename.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0666);
    if (fp < 0) {
        if (errno == EEXIST)
            fp = open(filename.c_str(), O_WRONLY);
        else {
            std::cerr << "Error opening file for writing: " << filename << " " << strerror(errno) << std::endl;
            exit(-1);
        }
    }
    ssize_t bytes_written = 0;
    for (auto record: records) {
        if ((bytes_written += write(fp, &record.key, sizeof(record.key))) < 0) exit(-1);
        if ((bytes_written += write(fp, &record.len, sizeof(record.len))) < 0) exit(-1);
        if ((bytes_written += write(fp, record.rpayload, record.len)) < 0) exit(-1);
    }
    close(fp);
    return bytes_written;
}


static ssize_t appendRecordToFile(const std::string& filename, Record record) {
    int fp = open(filename.c_str(), O_WRONLY | O_CREAT | O_APPEND | O_DIRECT, 0666);
    if (fp < 0) {
        if (errno == EEXIST)
            fp = open(filename.c_str(), O_WRONLY | O_APPEND | O_DIRECT);
        else {
            std::cerr << "Error opening file for writing: " << filename << " " << strerror(errno) << std::endl;
            exit(-1);
        }
    }
    ssize_t bytes_written = 0;
    if ((bytes_written = write(fp, &record.key, sizeof(record.key))) < 0) exit(-1);
    if ((bytes_written += write(fp, &record.len, sizeof(record.len))) < 0) exit(-1);
    if ((bytes_written += write(fp, record.rpayload, record.len)) < 0) exit(-1);
    close(fp);
    return bytes_written;
}

template<typename Container>
static ssize_t appendRecordsToFile(const std::string& filename, Container records) {
    int fp = open(filename.c_str(), O_WRONLY | O_APPEND | O_CREAT | O_DIRECT, 0666);
    if (fp < 0) {
        if (errno == EEXIST)
            fp = open(filename.c_str(), O_WRONLY | O_APPEND | O_DIRECT);
        else {
            std::cerr << "Error opening file for writing: " << filename << " " << strerror(errno) << std::endl;
            exit(-1);
        }
    }
    ssize_t bytes_written = 0;
    for (auto record: records) {
        if ((bytes_written += write(fp, &record.key, sizeof(record.key))) < 0) exit(-1);
        if ((bytes_written += write(fp, &record.len, sizeof(record.len))) < 0) exit(-1);
        if ((bytes_written += write(fp, record.rpayload, record.len)) < 0) exit(-1);
    }
    close(fp);
    return bytes_written;
}


template<typename Container>
static ssize_t readRecordsFromFile(const std::string& filename, Container& records, ssize_t offset, ssize_t max_mem) {
    int fp = open(filename.c_str(), O_RDONLY);
    if (fp < 0) {
        std::cerr << "Error opening file for reading: " << filename << " " << strerror(errno) << std::endl;
        exit(-1);
    }

    ssize_t bytes_read = 0;
    ssize_t read_size = 0;
    ssize_t loop_count = 0;
    unsigned long key = 0;
    uint32_t len = 0;
    ssize_t current_size = RECORD_SIZE;
    char* buffer = new char[current_size];
    // Setting the cursor to the offset and then
    // reading as much bytes as possible
    lseek(fp, offset, SEEK_SET);

    while (true) {
        read_size = read(fp, &key, sizeof(key));
        if (read_size == 0) break;
        else if (read_size < 0) exit(-1);
        else  loop_count += read_size;
        if (bytes_read + loop_count > max_mem) break;

        read_size = read(fp, &len, sizeof(len));
        if (read_size < 0) exit(-1);
        else loop_count += read_size;
        if (bytes_read + loop_count + len > max_mem) break;

        if (len+1 > current_size) {
            delete[] buffer;
            current_size = len+1;
            buffer = new char[current_size];
        }
        memset(buffer, 0, current_size);
        read_size = read(fp, buffer, len);
        if (read_size == 0) break;
        else if (read_size < 0) exit(-1);
        else loop_count += read_size;

        bytes_read += loop_count;

        if constexpr (std::is_same_v<Container, std::vector<Record>> || std::is_same_v<Container, std::deque<Record>>) {
            records.push_back(Record{key, len, buffer});
        } else {
            records.push(Record{key, len, buffer});
        }
        loop_count = 0;
    }

    delete[] buffer;
    close(fp);
    return bytes_read;
}

/**
 * This function reads the keys skipping the payload so that the checkSorted
 * function can be used to verify the order of the keys more efficiently.
 */
static void readKeysFromFile(const std::string& filename, std::vector<unsigned long>& keys) {
    int fp = open(filename.c_str(), O_RDONLY);
    if (fp < 0) {
        std::cerr << "Error opening file for reading: " << filename << " " << strerror(errno) << std::endl;
        exit(-1);
    }

    ssize_t read_size = 0;
    unsigned long key = 0;
    uint32_t len = 0;


    while(true) {
        read_size = read(fp, &key, sizeof(key));
        keys.push_back(key);
        if (read_size == 0) break; // EOF
        if (read_size < 0) {
            std::cerr << "Error reading key: " << strerror(errno) << std::endl;
            close(fp);
            exit(-1);
        }

        // Read length
        read_size = read(fp, &len, sizeof(len));
        if (read_size < 0) {
            std::cerr << "Error reading length: " << strerror(errno) << std::endl;
            close(fp);
            exit(-1);
        }

        // Skip payload
        if (lseek(fp, len, SEEK_CUR) == -1) {
            std::cerr << "Error seeking past payload: " << strerror(errno) << std::endl;
            close(fp);
            exit(-1);
        }
    }

    close(fp);
}

#endif // _FILESYSTEM_HPP
