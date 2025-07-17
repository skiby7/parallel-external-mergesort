/**
 * This file utilities functions shared between the various merge
 * implementations and the file generator.
 */

#ifndef _COMMON_HPP
#define _COMMON_HPP

#include <cerrno>
#include <cstddef>
#include <iostream>
#include <cstdint>
#include <cstdlib>
#include <sys/stat.h>
#include <sys/types.h>
#include <vector>
#include <string>
#include <cstring>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include "arena.hpp"
#include "config.hpp"
#include "feistel.hpp"
#include "record.hpp"
#include "filesystem.hpp"
#include <uuid/uuid.h>


static std::string generateUUID() {
    uuid_t uuid;
    uuid_generate(uuid);
    char uuid_str[37];
    uuid_unparse(uuid, uuid_str);
    return std::string(uuid_str);
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


    MemoryArena records_arena(MAX_MEMORY);
    ArenaDeque<Record*> records((ArenaAllocator<Record*>(&records_arena)));
    Record* rec = new (records_arena.allocate(sizeof(Record), alignof(Record))) Record();
    size_t size = 0;
    char* payload = new char[RECORD_SIZE];

    for (size_t i = 0; i < ARRAY_SIZE; i++) {
        rec->key = feistel_encrypt((uint32_t)i, 0xDEADBEEF, ROUNDS);
        rec->len = rand() % (RECORD_SIZE - 8) + 8;
        for (size_t j = 0; j < rec->len; j++)
            payload[j] = feistel_encrypt(j + i, 0x01, 1) & 0xFF;
        std::memcpy(rec->payload(), payload, rec->len);

        size += sizeof(*rec) + rec->len;
        records.push_back(rec);

        if (size > MAX_MEMORY) {
            size = 0;
            appendToFile(fd, std::move(records));
            records.clear();
            records_arena.reset();
        }

        if (i % (ARRAY_SIZE / 100) == 0 || i == ARRAY_SIZE - 1) {
            printf("\rProgress: %zu%%", (i * 100) / ARRAY_SIZE);
            fflush(stdout);
        }
    }

    if (!records.empty())
        appendToFile(fd, std::move(records));

    records.clear();
    records_arena.reset();
    delete[] payload;
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
    int fd = open(filename.c_str(), O_RDONLY);
    if (fd < 0) {
        std::cerr << "Error opening file for reading: " << filename << " " << strerror(errno) << std::endl;
        exit(-1);
    }
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



#endif // !_COMMON_HPP
