#include "include/common.hpp"
#include "include/config.hpp"
#include "include/plower.hpp"
#include <cstddef>
#include <iostream>
#include <vector>

int main() {
    std::vector<Record> records;
    ARRAY_SIZE = 200;
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
    generateFile("/tmp/file.dat");
    std::cout << "Files written" << std::endl;
    std::vector<Record> recs;
    size_t bytes_read = readRecordsFromFile("/tmp/file.dat", recs, 0, 100);
    std::cout << "Bytes read: " << bytes_read << std::endl;
    for (const auto& rec : recs) {
        std::cout << rec.key << std::endl;
    }
    return 0;
}
