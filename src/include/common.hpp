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
#include <fstream>
#include <algorithm>
#include <string>
#include <cstring>
#include <fcntl.h>
#include <stdlib.h>
#include <unistd.h>
#include "config.hpp"
#include "feistel.hpp"

typedef struct _Record {
    unsigned long key;
    uint32_t len;
    char *rpayload;

    _Record() : key(0), len(0), rpayload(nullptr) {}

    _Record(unsigned long _key, uint32_t _len, char *payload) {
        key = _key;
        len = _len;
        rpayload = new char[len];
        memmove(rpayload, payload, len);
    }


    _Record(const _Record& other) {
        key = other.key;
        len = other.len;
        rpayload = new char[len];
        memmove(rpayload, other.rpayload, len);
    }


    _Record(_Record&& other) noexcept {
        key = other.key;
        len = other.len;
        rpayload = other.rpayload;


        other.rpayload = nullptr;
        other.len = 0;
        other.key = 0;
    }


    _Record& operator=(const _Record& other) {
        if (this != &other) {
            delete[] rpayload;
            key = other.key;
            len = other.len;
            rpayload = new char[len];
            memmove(rpayload, other.rpayload, len);
        }
        return *this;
    }


    _Record& operator=(_Record&& other) noexcept {
        if (this != &other) {
            delete[] rpayload;

            key = other.key;
            len = other.len;
            rpayload = other.rpayload;


            other.rpayload = nullptr;
            other.len = 0;
            other.key = 0;
        }
        return *this;
    }

    ~_Record() {
        delete[] rpayload;
    }

    bool operator < (const _Record &a) const {
        return key < a.key;
    }

    bool operator <= (const _Record &a) const {
        return key <= a.key;
    }

    bool operator == (const _Record &a) const {
        return key == a.key;
    }

    bool operator >= (const _Record &a) const {
        return key >= a.key;
    }

    bool operator > (const _Record &a) const {
        return key > a.key;
    }
} Record;



static size_t writeRecordsToFile(const std::string& filename, std::vector<Record> records) {
    int fp = open(filename.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0666);
    if (fp < 0) {
        if (errno == EEXIST)
            fp = open(filename.c_str(), O_WRONLY);
        else {
            std::cerr << "Error opening file for writing: " << filename << " " << strerror(errno) << std::endl;
            exit(-1);
        }
    }
    size_t bytes_written = 0;
    for (auto record: records) {
        if ((bytes_written += write(fp, &record.key, sizeof(record.key))) < 0) exit(-1);
        if ((bytes_written += write(fp, &record.len, sizeof(record.len))) < 0) exit(-1);
        if ((bytes_written += write(fp, record.rpayload, record.len)) < 0) exit(-1);
    }
    close(fp);
    return bytes_written;
}


static size_t appendRecordToFile(const std::string& filename, Record record) {
    int fp = open(filename.c_str(), O_WRONLY | O_CREAT | O_APPEND, 0666);
    if (fp < 0) {
        if (errno == EEXIST)
            fp = open(filename.c_str(), O_WRONLY | O_APPEND);
        else {
            std::cerr << "Error opening file for writing: " << filename << " " << strerror(errno) << std::endl;
            exit(-1);
        }
    }
    size_t bytes_written = 0;
    if ((bytes_written = write(fp, &record.key, sizeof(record.key))) < 0) exit(-1);
    if ((bytes_written += write(fp, &record.len, sizeof(record.len))) < 0) exit(-1);
    if ((bytes_written += write(fp, record.rpayload, record.len)) < 0) exit(-1);
    close(fp);
    return bytes_written;
}

static size_t appendRecordsToFile(const std::string& filename, std::vector<Record> records) {
    int fp = open(filename.c_str(), O_WRONLY | O_APPEND | O_CREAT, 0666);
    if (fp < 0) {
        if (errno == EEXIST)
            fp = open(filename.c_str(), O_WRONLY | O_APPEND);
        else {
            std::cerr << "Error opening file for writing: " << filename << " " << strerror(errno) << std::endl;
            exit(-1);
        }
    }
    std::size_t bytes_written = 0;
    for (auto record: records) {
        if ((bytes_written += write(fp, &record.key, sizeof(record.key))) < 0) exit(-1);
        if ((bytes_written += write(fp, &record.len, sizeof(record.len))) < 0) exit(-1);
        if ((bytes_written += write(fp, record.rpayload, record.len)) < 0) exit(-1);
    }
    close(fp);
    return bytes_written;
}


template<typename Container>
static size_t readRecordsFromFile(const std::string& filename, Container& records, size_t offset, size_t max_mem) {
    int fp = open(filename.c_str(), O_RDONLY);
    if (fp < 0) {
        std::cerr << "Error opening file for reading: " << filename << " " << strerror(errno) << std::endl;
        exit(-1);
    }

    // Setting the cursor to the offset
    lseek(fp, offset, SEEK_SET);
    size_t bytes_read = 0;
    size_t read_size = 0;
    size_t loop_count = 0;
    unsigned long key = 0;
    uint32_t len = 0;
    size_t current_size = RECORD_SIZE;
    char* buffer = new char[current_size];
    // Reading as much bytes as possible

    while (true) {
        read_size = read(fp, &key, sizeof(key));
        if (read_size == 0) break;
        else if (read_size < 0) exit(-1);
        else  loop_count += read_size;
        if (bytes_read + loop_count > max_mem) break;

        read_size = read(fp, &len, sizeof(len));
        if (read_size == 0) break;
        else if (read_size < 0) exit(-1);
        else loop_count += read_size;
        if (bytes_read + loop_count > max_mem) break;

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
        if (bytes_read + loop_count > max_mem) break;

        bytes_read += loop_count;

        if constexpr (std::is_same_v<Container, std::vector<Record>>) {
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

static size_t getFileSize(const std::string& filename) {
    struct stat stat_buf;
    if (stat(filename.c_str(), &stat_buf) != 0) {
        std::cerr << "Error getting file stats: " << filename << std::endl;
        return 0;
    }
    return stat_buf.st_size;
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

static inline void destroyArray(std::vector<Record>& array) {
    const size_t num_threads = std::thread::hardware_concurrency();
    std::vector<std::thread> threads;

    auto worker = [&](size_t start, size_t end) {
        for (size_t i = start; i < end; ++i) {
            delete[] array[i].rpayload;
            array[i].rpayload = nullptr; // Optional: prevent dangling pointer
        }
    };

    size_t total = array.size();
    size_t chunk_size = total / num_threads;
    size_t remainder = total % num_threads;
    size_t start = 0;

    for (size_t i = 0; i < num_threads; ++i) {
        size_t end = start + chunk_size + (i < remainder ? 1 : 0);
        threads.emplace_back(worker, start, end);
        start = end;
    }

    for (auto& t : threads) {
        t.join();
    }
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

static inline bool checkSorted(std::vector<Record> array) {
    for (size_t i = 1; i < array.size(); i++)
        if (array[i].key < array[i-1].key)
            return false;

    return true;
}

static bool checkSortedFile(const std::string& filename) {
    std::vector<Record> records;
    size_t bytes_read = readRecordsFromFile(filename, records, 0, MAX_MEMORY);
    size_t last_read = bytes_read;
    while (last_read > 0) {
        if (!checkSorted(records)) {
            return false;
        }
        last_read = readRecordsFromFile(filename, records, bytes_read, MAX_MEMORY);
        bytes_read += last_read;
    }
    return true;
}

static inline void moveSorted(Record* records, size_t total_records, std::vector<Record>& sorted) {
    sorted.resize(total_records);
    const size_t BATCH_SIZE = 64;
    const size_t UNROLL = 8;
    size_t i = 0;

    for (; i + BATCH_SIZE <= total_records; i += BATCH_SIZE) {
        for (size_t j = 0; j < BATCH_SIZE; j += UNROLL) {
            sorted[i + j + 0] = records[i + j + 0];
            sorted[i + j + 1] = records[i + j + 1];
            sorted[i + j + 2] = records[i + j + 2];
            sorted[i + j + 3] = records[i + j + 3];
            sorted[i + j + 4] = records[i + j + 4];
            sorted[i + j + 5] = records[i + j + 5];
            sorted[i + j + 6] = records[i + j + 6];
            sorted[i + j + 7] = records[i + j + 7];
        }
    }

    for (; i < total_records; ++i)
        sorted[i] = records[i];
}

static inline void boundedMergeSort(std::vector<Record*> &arr, int left, int right) {
    size_t size = right - left + 1;

    if (size < SORT_THRESHOLD) {
       std::sort(arr.begin() + left, arr.begin() + right + 1,
                  [](const Record* a, const Record* b) {
                      return a->key < b->key;
                  });
        return;
    }

    int mid = (left + right) / 2;
    boundedMergeSort(arr, left, mid);
    boundedMergeSort(arr, mid + 1, right);

    std::vector<Record*> temp(size);
    int i = left, j = mid + 1, k = 0;
    while (i <= mid && j <= right) {
        temp[k++] = (arr[i]->key <= arr[j]->key) ? arr[i++] : arr[j++];
    }
    while (i <= mid) temp[k++] = arr[i++];
    while (j <= right) temp[k++] = arr[j++];

    for (int l = 0; l < k; ++l) {
        arr[left + l] = temp[l];
    }
}



#endif // !_COMMON_HPP
