#include <vector>
#include "include/cmdline.hpp"
#include "include/common.hpp"
#include "include/hpc_helpers.hpp"
#include "include/ff_sort.hpp"
#include <ff/ff.hpp>

int main(int argc, char *argv[]) {
    int start = 0;
    if((start = parseCommandLine(argc, argv)) < 0) return -1;
    std::string filename = argv[start];

    ff::ff_farm farm;
    Master m(filename);
    farm.add_emitter(&m);

    std::vector<ff::ff_node*> W;
    for (size_t i = 0; i < NTHREADS; i++)
        W.push_back(new WorkerNode());

    farm.add_workers(W);
    farm.wrap_around();
    farm.cleanup_workers();

    TIMERSTART(mergesort_ff);
    if (farm.run_and_wait_end() < 0) {
        std::cout << "Error running the farm" << std::endl;
    }
    TIMERSTOP(mergesort_ff);

    assert(checkSortedFile("/tmp/output.dat"));
    return 0;
}
