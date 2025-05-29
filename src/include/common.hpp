#ifndef _COMMON_HPP
#define _COMMON_HPP

#include <vector>
#include <algorithm>
#include <cstring>
#include "config.hpp"
#include "feistel.hpp"

typedef struct {
    unsigned long key; // The items are sorted using this key
    char *rpayload;
} Record;


static std::vector<Record> generateArray(std::vector<Record>& records) {
    records.resize(ARRAY_SIZE);
    const size_t num_threads = std::thread::hardware_concurrency();
    std::vector<std::thread> threads;
    
    for (auto& record : records) {
        record.rpayload = new char[RECORD_SIZE];
    }


    auto worker = [&](size_t start, size_t end) {
        for (size_t i = start; i < end; i++) {
            records[i].key = feistel_encrypt((uint32_t)i, 0xDEADBEEF, ROUNDS);
            for (size_t j = 0; j < RECORD_SIZE; j++) {
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
    for (auto record : array)
        delete[] record.rpayload;
}

static inline bool checkSorted(std::vector<Record> array) {
    for (size_t i = 1; i < array.size(); i++)
        if (array[i].key < array[i-1].key)
            return false;

    return true;
}

static inline void moveSorted(std::vector<Record*> &arr, std::vector<Record>& sorted) {
    sorted.resize(arr.size());
    const size_t BATCH_SIZE = 64;
    size_t i = 0;
    for (; i + BATCH_SIZE <= arr.size(); i += BATCH_SIZE) 
        for (size_t j = 0; j < BATCH_SIZE; ++j) 
            sorted[i + j] = *arr[i + j];
    
    for (; i < arr.size(); ++i) 
        sorted[i] = *arr[i];
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
