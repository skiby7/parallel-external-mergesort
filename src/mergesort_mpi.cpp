#include <vector>
#include <sstream>
#include "include/cmdline.hpp"
#include "include/common.hpp"
#include "include/hpc_helpers.hpp"
#include "include/ff_sort.hpp"
#include <ff/ff.hpp>
#include <mpi.h>

int main(int argc, char *argv[]) {
    if(parseCommandLine(argc, argv) < 0) {
        return -1;
    }
    
    std::vector<Record> records;
    generateArray(records);
    
    ff::ff_farm farm;
    Master m(records);  
    farm.add_emitter(&m); 
    
    std::vector<ff::ff_node*> W;
    for (size_t i = 0; i < NTHREADS; i++)
        W.push_back(new WorkerNode(records));  

    farm.add_workers(W);
    farm.wrap_around();
    farm.cleanup_workers();
    
    TIMERSTART(mergesort_ff);    
    if (farm.run_and_wait_end() < 0) {
        std::cout << "Error running the farm" << std::endl;
    }
    TIMERSTOP(mergesort_ff);    
    
    assert(checkSorted(records));
    destroyArray(records);
    return 0;
}

