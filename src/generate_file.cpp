
#include "include/config.hpp"
#include "include/plower.hpp"
#include <cstddef>
#include <iostream>

int main() {
    std::vector<Record> records;
    generateArray(records);
    Record* data = records.data();
    Plower plower(100, data, 100);
    for (size_t i = 100; i < ARRAY_SIZE; i++) {
        std::cout << plower.step(&data[i])->key << std::endl;
    }
    std::cout << "############" << std::endl;
    Record* elem;
    while ((elem = plower.step(nullptr))) {
        std::cout << elem->key << std::endl;
    }
    return 0;
}
