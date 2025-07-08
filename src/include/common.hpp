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
    Record record;
    std::vector<Record> records;
    for (size_t i = 0; i < ARRAY_SIZE; i++) {
        record.key = feistel_encrypt((uint32_t)i, 0xDEADBEEF, ROUNDS);
        record.len = rand() % (RECORD_SIZE - 8) + 8;
        record.rpayload = new char[record.len];
        for (size_t j = 0; j < record.len; j++) {
            record.rpayload[j] = feistel_encrypt(j+i, 0x01, 1) & 0xFF;
        }
        records.push_back(record);
    }
    writeRecordsToFile(filename, records);
    records.clear();
}

static std::vector<Record> generateArray(std::vector<Record>& records) {
    records.resize(ARRAY_SIZE);
    const size_t num_threads = std::thread::hardware_concurrency();
    std::vector<std::thread> threads;

    auto worker = [&](size_t start, size_t end) {
        for (size_t i = start; i < end; i++) {
            records[i].key = feistel_encrypt((uint32_t)i, 0xDEADBEEF, ROUNDS);
            records[i].len = rand() % (RECORD_SIZE - 8) + 8;
            records[i].rpayload = new char[records[i].len];
            for (size_t j = 0; j < records[i].len; j++) {
                records[i].rpayload[j] = feistel_encrypt(j+i, 0x01, 1) & 0xFF;
            }
        }
    };

    size_t chunk_size = ARRAY_SIZE / num_threads;
    size_t remaining = ARRAY_SIZE % num_threads;
    size_t start = 0;

    for (size_t i = 0; i < num_threads; i++) {
        size_t end = start + chunk_size + (i < remaining ? 1 : 0);
        threads.emplace_back(worker, start, end);
        start = end;
    }

    for (auto& t : threads) {
        t.join();
    }

    return records;
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


static inline bool checkSorted(std::vector<unsigned long>& array) {
    for (size_t i = 1; i < array.size(); i++)
        if (array[i] < array[i-1]) {
            std::cout << "Array is not sorted at index " << i << ": " << array[i] << " < " << array[i-1] << std::endl;
            return false;
        }
    return true;
}

static bool checkSortedFile(const std::string& filename) {
    std::vector<unsigned long> records;
    readKeysFromFile(filename, records);
    return checkSorted(records);
}



#endif // !_COMMON_HPP
