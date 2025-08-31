#include <string>
#include <vector>
#include "include/cmdline.hpp"
#include "include/config.hpp"
#include "include/ff_sort.hpp"
#include <ff/ff.hpp>
#include <filesystem>
#include <chrono>

std::chrono::time_point<std::chrono::system_clock> start, end;
inline void timer_start() {
    start = std::chrono::system_clock::now();
}

inline void timer_stop(std::string label) {
    end = std::chrono::system_clock::now();
    std::chrono::duration<double> delta = end-start;
    std::cout << "# elapsed time ("<< label <<"): " << delta.count() << "s" << std::endl;
}

int main(int argc, char *argv[]) {
    int start = 0;
    if((start = parseCommandLine(argc, argv)) < 0) return -1;
    std::string filename = argv[start];
    size_t file_size = getFileSize(filename);
    MAX_MEMORY = std::min(MAX_MEMORY, file_size + (file_size/10));
    std::filesystem::path p(filename);
    ff::ff_farm farm;
    Master m(filename, p.parent_path().string());
    farm.add_emitter(&m);

    std::vector<ff::ff_node*> W;
    for (size_t i = 0; i < NTHREADS-1; i++)
        W.push_back(new WorkerNode(p.parent_path().string()));

    farm.add_workers(W);
    farm.wrap_around();
    farm.cleanup_workers();

    if (FF_NO_MAPPING) // Def value is true
        farm.no_mapping();

    farm.blocking_mode(FF_BLOCKING_MODE);

    std::string label = FF_NO_MAPPING ? "mergesort_ff_no_mapping" : "mergesort_ff";
    if (FF_BLOCKING_MODE)
        label += "_blocking";
    timer_start();
    if (farm.run_and_wait_end() < 0) {
        std::cout << "Error running the farm" << std::endl;
    }
    timer_stop(label);

    return 0;
}
