#include "include/common.hpp"
#include "include/cmdline.hpp"
#include <string>

int main(int argc, char* argv[]) {
    int start = 0;
    if((start = parseCommandLine(argc, argv)) < 0) return -1;
    std::string filename = argv[start];
    generateFile(filename);
    std::cout << "File generated successfully!" << std::endl;
    return 0;
}
