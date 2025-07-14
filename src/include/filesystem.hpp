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


static ssize_t writeBatchToFile(int fd, std::deque<Record>& records) {
    // Get system page size
    size_t page_size = sysconf(_SC_PAGE_SIZE);

    // Get current file size
    off_t current_size = lseek(fd, 0, SEEK_END);
    if (current_size == -1) {
        std::cerr << "lseek failed: " << strerror(errno) << std::endl;
        exit(EXIT_FAILURE);
    }

    // Calculate total size of new records
    size_t batch_size = 0;
    for (const auto& record : records)
        batch_size += sizeof(record.key) + sizeof(record.len) + record.len;

    // Extend the file
    size_t new_size = current_size + batch_size;
    if (ftruncate(fd, new_size) != 0) {
        std::cerr << "ftruncate failed: " << strerror(errno) << std::endl;
        exit(EXIT_FAILURE);
    }

    // Align offset to page boundary
    off_t aligned_offset = current_size & ~(page_size - 1);
    size_t delta = current_size - aligned_offset;
    size_t map_len = delta + batch_size;

    void* map_ptr = mmap(nullptr, map_len, PROT_WRITE, MAP_SHARED, fd, aligned_offset);
    if (map_ptr == MAP_FAILED) {
        std::cerr << "mmap failed: " << strerror(errno) << std::endl;
        exit(EXIT_FAILURE);
    }

    // Offset into the mapped memory where writing should start
    char* out = static_cast<char*>(map_ptr) + delta;
    size_t offset = 0;

    for (const auto& record : records) {
        memcpy(out + offset, &record.key, sizeof(record.key));
        offset += sizeof(record.key);

        memcpy(out + offset, &record.len, sizeof(record.len));
        offset += sizeof(record.len);

        memcpy(out + offset, record.rpayload, record.len);
        offset += record.len;
    }

    records.clear();

    if (msync(map_ptr, map_len, MS_SYNC) != 0) {
        std::cerr << "msync failed: " << strerror(errno) << std::endl;
    }

    if (munmap(map_ptr, map_len) != 0) {
        std::cerr << "munmap failed: " << strerror(errno) << std::endl;
        exit(EXIT_FAILURE);
    }

    return static_cast<ssize_t>(batch_size);
}

// static ssize_t writeBatchToFile(int fd, std::deque<Record>& records) {
//     // First pass: calculate total size
//     size_t total_size = 0;
//     for (const auto& record : records)
//         total_size += sizeof(record.key) + sizeof(record.len) + record.len;

//     std::vector<char> buffer(total_size);
//     size_t offset = 0;

//     // Second pass: fill the buffer
//     for (const auto& record : records) {
//         memcpy(buffer.data() + offset, &record.key, sizeof(record.key));
//         offset += sizeof(record.key);

//         memcpy(buffer.data() + offset, &record.len, sizeof(record.len));
//         offset += sizeof(record.len);

//         memcpy(buffer.data() + offset, record.rpayload, record.len);
//         offset += record.len;
//     }

//     records.clear();  // Clear after writing

//     // Single write syscall
//     ssize_t result = write(fd, buffer.data(), buffer.size());
//     if (result < 0) {
//         std::cerr << "Error writing to file: " << strerror(errno) << std::endl;
//         exit(EXIT_FAILURE);
//     }

//     return result;
// }

// static ssize_t writeBatchToFile(int fd, std::deque<Record>& records) {
//     std::vector<char> buffer;

//     while (!records.empty()) {
//         Record record = records.front();

//         // Write key
//         buffer.resize(buffer.size() + sizeof(record.key));
//         memcpy(buffer.data() + buffer.size() - sizeof(record.key), &record.key, sizeof(record.key));

//         // Write len (4 bytes)
//         buffer.resize(buffer.size() + sizeof(record.len));
//         memcpy(buffer.data() + buffer.size() - sizeof(record.len), &record.len, sizeof(record.len));

//         // Write payload
//         buffer.insert(buffer.end(), record.rpayload, record.rpayload + record.len);

//         records.pop_front();
//     }

//     ssize_t result = write(fd, buffer.data(), buffer.size());
//     if (result < 0) {
//         std::cerr << "Error writing to file: " << strerror(errno) << std::endl;
//         exit(-1);
//     }

//     return static_cast<ssize_t>(result);
// }

static std::vector<std::pair<size_t, size_t>> computeChunks(const std::string& filename, size_t num_threads) {
    std::vector<std::pair<size_t, size_t>> chunks;
    // Each thread should have a chunk of data to sort
    size_t chunk_size = getFileSize(filename) / num_threads;
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Error opening file for reading: " << filename << " " << strerror(errno) << std::endl;
        exit(-1);
    }
    size_t start = 0, read_size = 0;
    size_t current_pos = 0;
    unsigned long key;
    uint32_t len;
    while(true) {
        read_size = read(fd, &key, sizeof(key));
        if (read_size == 0) {
            current_pos = lseek(fd, 0, SEEK_CUR);
            chunks.push_back({start, current_pos});
            break;
        } // EOF
        if (read_size < 0) {
            std::cerr << "Error reading key: " << strerror(errno) << std::endl;
            close(fd);
            exit(-1);
        }

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

        // Get current position
        current_pos = lseek(fd, 0, SEEK_CUR);

        // Check if we've exceeded target chunk size
        if (current_pos - start >= chunk_size) {
            chunks.push_back({start, current_pos});
            start = current_pos;
        }
    }
    close(fd);
    return chunks;
}

template<typename Container>
static ssize_t writeRecordsToFile(const std::string& filename, Container records) {
    int fd = open(filename.c_str(), O_WRONLY  | O_CREAT | O_EXCL, 0666);

    if (fd < 0) {
        if (errno == EEXIST)
            fd = open(filename.c_str(), O_WRONLY );

        else {
            std::cerr << "Error opening file for writing: " << filename << " " << strerror(errno) << std::endl;
            exit(-1);
        }
    }
    ssize_t bytes_written = 0;
    for (auto record: records) {
        if ((bytes_written += write(fd, &record.key, sizeof(record.key))) < 0) exit(-1);
        if ((bytes_written += write(fd, &record.len, sizeof(record.len))) < 0) exit(-1);
        if ((bytes_written += write(fd, record.rpayload, record.len)) < 0) exit(-1);
    }
    close(fd);
    return bytes_written;
}


static ssize_t appendRecordToFile(const std::string& filename, Record record) {
    int fd = open(filename.c_str(), O_WRONLY  | O_CREAT | O_APPEND, 0666);

    if (fd < 0) {
        if (errno == EEXIST)
            fd = open(filename.c_str(), O_WRONLY  | O_APPEND);

        else {
            std::cerr << "Error opening file for writing: " << filename << " " << strerror(errno) << std::endl;
            exit(-1);
        }
    }
    ssize_t bytes_written = 0;
    if ((bytes_written = write(fd, &record.key, sizeof(record.key))) < 0) exit(-1);
    if ((bytes_written += write(fd, &record.len, sizeof(record.len))) < 0) exit(-1);
    if ((bytes_written += write(fd, record.rpayload, record.len)) < 0) exit(-1);
    close(fd);
    return bytes_written;
}

template<typename Container>
static ssize_t appendRecordsToFile(const std::string& filename, Container records) {
    int fd = open(filename.c_str(), O_WRONLY  | O_APPEND | O_CREAT, 0666);

    if (fd < 0) {
        if (errno == EEXIST)
            fd = open(filename.c_str(), O_WRONLY  | O_APPEND);

        else {
            std::cerr << "Error opening file for writing: " << filename << " " << strerror(errno) << std::endl;
            exit(-1);
        }
    }
    ssize_t bytes_written = 0;
    for (auto record: records) {
        if ((bytes_written += write(fd, &record.key, sizeof(record.key))) < 0) exit(-1);
        if ((bytes_written += write(fd, &record.len, sizeof(record.len))) < 0) exit(-1);
        if ((bytes_written += write(fd, record.rpayload, record.len)) < 0) exit(-1);
    }
    close(fd);
    return bytes_written;
}


// template<typename Container>
// static ssize_t readRecordsFromFile(const std::string& filename, Container& records, ssize_t offset, ssize_t max_mem) {
//     int fp = open(filename.c_str(), O_RDONLY);

//     if (fp < 0) {
//         std::cerr << "Error opening file for reading: " << filename << " " << strerror(errno) << std::endl;
//         exit(-1);
//     }

//     ssize_t bytes_read = 0;
//     ssize_t read_size = 0;
//     ssize_t loop_count = 0;
//     unsigned long key = 0;
//     uint32_t len = 0;
//     ssize_t current_size = RECORD_SIZE;
//     char* buffer = new char[current_size];
//     // Setting the cursor to the offset and then
//     // reading as much bytes as possible
//     lseek(fp, offset, SEEK_SET);

//     while (true) {
//         read_size = read(fp, &key, sizeof(key));
//         if (read_size == 0) break;
//         else if (read_size < 0) exit(-1);
//         else  loop_count += read_size;
//         if (bytes_read + loop_count > max_mem) break;

//         read_size = read(fp, &len, sizeof(len));
//         if (read_size < 0) exit(-1);
//         else loop_count += read_size;
//         if (bytes_read + loop_count + len > max_mem) break;

//         if (len+1 > current_size) {
//             delete[] buffer;
//             current_size = len+1;
//             buffer = new char[current_size];
//         }
//         memset(buffer, 0, current_size);
//         read_size = read(fp, buffer, len);
//         if (read_size == 0) break;
//         else if (read_size < 0) exit(-1);
//         else loop_count += read_size;

//         bytes_read += loop_count;

//         if constexpr (std::is_same_v<Container, std::vector<Record>> || std::is_same_v<Container, std::deque<Record>>) {
//             records.push_back(Record{key, len, buffer});
//         } else {
//             records.push(Record{key, len, buffer});
//         }
//         loop_count = 0;
//     }

//     delete[] buffer;
//     close(fp);
//     return bytes_read;
// }


template<typename Container>
static size_t readRecordsFromFile(const std::string& filename, Container& records, size_t offset, size_t max_mem) {
    constexpr size_t kInitialBufSize = 64 * 1024;

    int fp = open(filename.c_str(), O_RDONLY);
    if (fp < 0) {
        std::cerr << "Error opening file for reading: " << filename << " " << strerror(errno) << std::endl;
        exit(EXIT_FAILURE);
    }

    if (lseek(fp, static_cast<off_t>(offset), SEEK_SET) < 0) {
        std::cerr << "lseek failed: " << strerror(errno) << std::endl;
        close(fp);
        exit(EXIT_FAILURE);
    }

    std::vector<char> read_buf(std::max(kInitialBufSize, max_mem));
    size_t bytes_in_buffer = 0;
    size_t buffer_offset = 0;
    size_t total_bytes_read = 0;

    while (total_bytes_read < max_mem) {
        // Shift remaining bytes to front
        if (buffer_offset < bytes_in_buffer) {
            size_t remaining = bytes_in_buffer - buffer_offset;
            memmove(read_buf.data(), read_buf.data() + buffer_offset, remaining);
            bytes_in_buffer = remaining;
        } else {
            bytes_in_buffer = 0;
        }
        buffer_offset = 0;

        ssize_t read_bytes = read(fp, read_buf.data() + bytes_in_buffer, read_buf.size() - bytes_in_buffer);
        if (read_bytes < 0) {
            std::cerr << "Read error: " << strerror(errno) << std::endl;
            close(fp);
            exit(EXIT_FAILURE);
        } else if (read_bytes == 0) {
            break; // EOF
        }

        bytes_in_buffer += static_cast<size_t>(read_bytes);

        // Try to parse records
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
            rec.rpayload = new char[len];
            memcpy(rec.rpayload, base + sizeof(uint64_t) + sizeof(uint32_t), len);

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
    close(fp);
    return total_bytes_read;
}

/**
 * This function reads the keys skipping the payload so that the checkSorted
 * function can be used to verify the order of the keys more efficiently.
 */
static void readKeysFromFile(const std::string& filename, std::vector<unsigned long>& keys) {
    int fd = open(filename.c_str(), O_RDONLY );

    if (fd < 0) {
        std::cerr << "Error opening file for reading: " << filename << " " << strerror(errno) << std::endl;
        exit(-1);
    }

    ssize_t read_size = 0;
    unsigned long key = 0;
    uint32_t len = 0;


    while(true) {
        read_size = read(fd, &key, sizeof(key));
        keys.push_back(key);
        if (read_size == 0) break; // EOF
        if (read_size < 0) {
            std::cerr << "Error reading key: " << strerror(errno) << std::endl;
            close(fd);
            exit(-1);
        }

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
}

#endif // _FILESYSTEM_HPP
