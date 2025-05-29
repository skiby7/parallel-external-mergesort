#include <vector>
#include <omp.h>
#include <sstream>
#include "include/cmdline.hpp"
#include "include/common.hpp"
#include "include/hpc_helpers.hpp"
#include "include/ff_sort.hpp"
#include <ff/ff.hpp>

int main(int argc, char *argv[]) {
    if(parseCommandLine(argc, argv) < 0) {
        return -1;
    }
    //TIMERSTART(gen_array);
    std::vector<Record> records = generateArray();
    //TIMERSTOP(gen_array);
    std::vector<Record> sorted_records;
    std::vector<Record*> record_refs;
    for (auto& record: records) 
        record_refs.push_back(&record);
    ff::ff_farm farm;
    Master m(std::ref(record_refs), sorted_records);
    farm.add_emitter(&m); 
    std::vector<ff::ff_node*> W;
    for (size_t i = 0; i < NTHREADS; i++)
            W.push_back(new WorkerNode(std::ref(record_refs)));
    farm.add_workers(W);
    farm.wrap_around();
    farm.cleanup_workers();
    
    TIMERSTART(mergesort_ff);    
	if (farm.run_and_wait_end()<0){
	    std::cout << "Error running the farm" << std::endl;
    }
    TIMERSTOP(mergesort_ff);    

    assert(checkSorted(sorted_records));


    destroyArray(sorted_records);
    return 0;
    // 5.86s for 100000000 elements vs 82.31s with std::cout and std::endl
    /**
    
    std::ostringstream oss;
    for (int i = 0; i < ARRAY_SIZE; i++)
        oss << records[i].key << '\n';

    std::cout << oss.str();
    */
}
