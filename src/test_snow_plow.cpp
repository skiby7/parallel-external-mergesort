#include "include/common.hpp"
#include "include/config.hpp"
#include "include/sorting.hpp"
#include <cstddef>
#include <iostream>
#include <vector>

int main() {
    // generateArray(records);
    // Record* data = records.data();
    // Plower plower(100, data, 100);
    // for (size_t i = 100; i < ARRAY_SIZE; i++) {
    //     std::cout << plower.step(&data[i])->key << std::endl;
    // }
    // std::cout << "############" << std::endl;
    // Record* elem;
    // while ((elem = plower.step(nullptr))) {
    //     std::cout << elem->key << std::endl;
    // }

    // genSequenceFiles("/tmp/file.dat", 0, getFileSize("/tmp/file.dat"), 1 << 7, "/tmp/run");
    // printRunFiles("/tmp/run13");
    // std::vector<Record> r1;

    std::priority_queue<Record, std::vector<Record>, RecordComparator> r1;
    // std::vector<Record> r2;
    size_t counter = 0;
    size_t bytes_read = readRecordsFromFile("/tmp/file1.dat", r1, 0, 10000);
    counter += bytes_read;
    while (bytes_read > 0) {
        bytes_read = readRecordsFromFile("/tmp/file1.dat", r1, counter, 10000);
        counter += bytes_read;
    }
    // readRecordsFromFile("/tmp/output.dat", r2, 0, MAX_MEMORY);
    // std::cout << r1.size() << " " << r2.size() << std::endl;
    printRunFiles("/tmp/file1.dat");
    return 0;
    while (!r1.empty()) {
        appendRecordToFile("/tmp/test", r1.top());
        r1.pop();
    }

    // std::vector<std::string> sequences = findFiles("/tmp/run");

    // for (int i = sequences.size() - 1; i >= 0; i--) {
    //     readRecordsFromFile(sequences[i], r1, 0, MAX_MEMORY);
    // }
    // while (!r1.empty()) {
    //     appendRecordToFile("/tmp/test1", r1.top());
    //     r1.pop();
    // }
    // checkSortedFile("/tmp/test");
    // checkSortedFile("/tmp/test1");
    printRunFiles("/tmp/test");
    // printRunFiles("/tmp/test1");
    return 0;
}
