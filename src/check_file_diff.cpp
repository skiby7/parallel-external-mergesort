
#include "include/common.hpp"
#include "include/cmdline.hpp"
#include "include/record.hpp"
#include <cassert>
#include <string>

int main(int argc, char* argv[]) {
    int start = 0;
    if((start = parseCommandLine(argc, argv)) < 0) return -1;
    std::string filename1 = argv[start];
    std::string filename2 = argv[start + 1];
    std::vector<Record> records1;
    std::vector<Record> records2;
    readRecordsFromFile(filename1, records1, 0, getFileSize(filename1));
    readRecordsFromFile(filename2, records2, 0, getFileSize(filename2));
    assert(records1.size() == records2.size());
    for (size_t i = 0; i < records1.size(); ++i) {
        if (
            records1[i].key != records2[i].key
            && records1[i].len != records2[i].len
            && records1[i].rpayload != records2[i].rpayload
        ) {
            std::cout << "Difference found at index " << i << std::endl;
            std::cout << "Record 1: " << records1[i] << std::endl;
            std::cout << "Record 2: " << records2[i] << std::endl;
        }
    }
    return 0;
}
