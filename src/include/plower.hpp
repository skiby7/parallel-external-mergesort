#ifndef _PLOWER_HPP
#define _PLOWER_HPP
#include "common.hpp"
#include <cassert>
#include <cstddef>
#include <queue>
#include <vector>

struct RecordComparator {
    bool operator()(Record* a, Record* b){
        return (a->key > b->key);
    }
};

class Plower {
    size_t memory;
    std::vector<Record*> unsorted;
    std::priority_queue<Record*, std::vector<Record*>, RecordComparator> heap;
    Record* last;
public:
    Plower(size_t mem, Record* values, size_t nvalues): memory(mem) {
        for (size_t i = 0; i < nvalues; i++)
            heap.push(&values[i]);

        last = nullptr;
    }

    Plower() {
        last = nullptr;
    }

    /**
     * This is one step of the snow plow algorithm.
     * If the input record `r` is a null pointer, it means that the input sequence has terminated
     * and the step return one heap element at a time.
     */
    Record* step(Record* r) {
        if (heap.empty() && unsorted.empty()) return nullptr;
        if (heap.empty() && !unsorted.empty()) {
            while (!unsorted.empty()) {
                heap.push(unsorted.back());
                unsorted.pop_back();
            }
            assert(unsorted.empty());
        }

        Record* output = nullptr;

        if (r == nullptr) {
            output = heap.top();
            heap.pop();
            return output;
        }

        if (heap.top() < r) {
            output = heap.top();
            heap.pop();
            if (last != nullptr && last < r)
                unsorted.push_back(r);
            else if (last != nullptr && last > r)
                heap.push(r);
        } else {
            output = r;
        }
        if (last == nullptr) {
            last = output;
        }
        return output;
    }

};

#endif // _PLOWER_HPP
