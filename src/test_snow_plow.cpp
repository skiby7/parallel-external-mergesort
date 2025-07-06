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

    genSequenceFiles("/tmp/file.dat", 0, getFileSize("/tmp/file.dat"), 1 << 7, "/tmp/run");
    printRunFiles("/tmp/run");
    return 0;
}
