#include "include/common.hpp"
#include "include/hpc_helpers.hpp"
#include "include/record.hpp"
#include <iostream>
#include <unistd.h>
#include <vector>
#define FILE "/tmp/mergesort/test.dat"
#define FILE_OUT "/tmp/mergesort/test_out.dat"



template <typename T>
std::vector<std::vector<T>> split_into_chunks(const std::vector<T>& vec, size_t n) {
    std::vector<std::vector<T>> chunks;
    if (n == 0) return chunks; // avoid division by zero

    size_t total = vec.size();
    size_t base_size = total / n;
    size_t remainder = total % n; // first 'remainder' chunks get +1 element

    chunks.reserve(n);

    size_t start = 0;
    for (size_t i = 0; i < n; ++i) {
        size_t chunk_size = base_size + (i < remainder ? 1 : 0);
        size_t end = start + chunk_size;
        if (start < total)
            chunks.emplace_back(vec.begin() + start, vec.begin() + std::min(end, total));
        else
            chunks.emplace_back(); // empty chunk
        start = end;
    }
    return chunks;
}

int main () {
    // Your code here
    size_t filesize = getFileSize(FILE);
    int fd = openFile(FILE);

    size_t bytes_read = 0;
    size_t bytes_written = 0;
    std::vector<Record> buffer;
    TIMERSTART(MMAP_SMALL_READ)
    while (bytes_read < filesize) {
        bytes_read += readRecordsFromFileMmap(fd, buffer, bytes_read, std::min(filesize - bytes_read, 100000UL)); // 100k
    }
    TIMERSTOP(MMAP_SMALL_READ)
    std::vector<std::vector<Record>> chunks = split_into_chunks(buffer, 1000);
    std::cout << "Chunks size: " << chunks[0].size() << std::endl;
    TIMERSTART(MMAP_SMALL_WRITE)
    int fd_out = openFile(FILE_OUT);
    for(auto& chunk : chunks) {
        while (!chunk.empty()) {
            bytes_written += appendToFileMmap(fd_out, chunk, 0); // small chunks
        }
        chunk.clear();
    }
    TIMERSTOP(MMAP_SMALL_WRITE)
    close(fd_out);
    deleteFile(FILE_OUT);
    buffer.clear();
    buffer.shrink_to_fit();
    bytes_read = 0;
    bytes_written = 0;
    TIMERSTART(MMAP_BIG_READ)
    while (bytes_read < filesize) {
        bytes_read += readRecordsFromFileMmap(fd, buffer, bytes_read, std::min(filesize - bytes_read, 200000000UL)); // 200MB
    }
    TIMERSTOP(MMAP_BIG_READ)
    chunks = split_into_chunks(buffer, 50);
    std::cout << "Chunks size: " << chunks[0].size() << std::endl;
    TIMERSTART(MMAP_BIG_WRITE)

    fd_out = openFile(FILE_OUT);
    for(auto& chunk : chunks) {
        while (!chunk.empty()) {
            bytes_written += appendToFileMmap(fd_out, chunk, 0); // small chunks
        }
        chunk.clear();
    }
    TIMERSTOP(MMAP_BIG_WRITE)
    deleteFile(FILE_OUT);
    buffer.clear();
    buffer.shrink_to_fit();
    bytes_read = 0;
    bytes_written = 0;
    TIMERSTART(POSIX_SMALL_READ)
    while (bytes_read < filesize) {
        bytes_read += readRecordsFromFilePosix(fd, buffer, bytes_read, std::min(filesize - bytes_read, 100000UL)); // 100k
    }
    TIMERSTOP(POSIX_SMALL_READ)
    chunks = split_into_chunks(buffer, 1000);
    std::cout << "Chunks size: " << chunks[0].size() << std::endl;
    TIMERSTART(POSIX_SMALL_WRITE)

    fd_out = openFile(FILE_OUT);
    for(auto& chunk : chunks) {
        while (!chunk.empty()) {
            bytes_written += appendToFilePosix(fd_out, chunk, 0); // small chunks
        }
        chunk.clear();
    }
    TIMERSTOP(POSIX_SMALL_WRITE)
    deleteFile(FILE_OUT);
    buffer.clear();
    buffer.shrink_to_fit();
    bytes_read = 0;
    bytes_written = 0;
    TIMERSTART(POSIX_BIG_READ)
    while (bytes_read < filesize) {
        bytes_read += readRecordsFromFilePosix(fd, buffer, bytes_read, std::min(filesize - bytes_read, 200000000UL)); // 200MB
    }
    TIMERSTOP(POSIX_BIG_READ)
    chunks = split_into_chunks(buffer, 50);
    std::cout << "Chunks size: " << chunks[0].size() << std::endl;
    TIMERSTART(POSIX_BIG_WRITE)

    fd_out = openFile(FILE_OUT);
    for(auto& chunk : chunks) {
        while (!chunk.empty()) {
            bytes_written += appendToFilePosix(fd_out, chunk, 0); // small chunks
        }
        chunk.clear();
    }
    TIMERSTOP(POSIX_BIG_WRITE)
    close(fd_out);
    close(fd);
    return 0;
}
