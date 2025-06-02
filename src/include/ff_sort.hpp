#ifndef _FF_SORT_HPP
#define _FF_SORT_HPP
#include "common.hpp"
#include "config.hpp"
#include <cmath>
#include <ff/ff.hpp>
#include <ff/pipeline.hpp>
#include <chrono>

struct sort_task_t {
    size_t start;
    size_t end;
    size_t w_id;
};

struct merge_task_t {
    size_t start_a;  
    size_t middle;
    size_t end_b;    
};

struct work_t {
    struct sort_task_t *sort_task;
    struct merge_task_t *merge_task;
};


struct Master : ff::ff_monode_t<work_t> {
    std::vector<Record*>& record_refs; 
    std::vector<Record>& sorted;
    const size_t total_records; 
    size_t remainder; 
    size_t chunk_size; 
    size_t current_level;
    size_t submitted_chunks_count;
    size_t merge_count;
    size_t n_chunks;
    std::chrono::high_resolution_clock::time_point start;
    std::chrono::high_resolution_clock::time_point end;
    size_t active_threads;
    std::vector<std::vector<std::pair<size_t, size_t>>> merge_levels;
    Master(std::vector<Record*>& record_refs, std::vector<Record>& sorted) : 
        record_refs(record_refs), sorted(sorted), total_records(record_refs.size()),
        current_level(0), submitted_chunks_count(0), merge_count(0),
        n_chunks(NTHREADS), active_threads(NTHREADS) {
        chunk_size = record_refs.size()/n_chunks;
        remainder = record_refs.size()%n_chunks;
    } 
    
    void kill_threads(size_t expected_merges) {
        while (active_threads > expected_merges) 
            ff_send_out_to(EOS, --active_threads);
    }

    void send_out_sort_tasks () {
        size_t start_idx = 0;
        for (size_t i = 0; i < n_chunks; i++) {
            size_t current_chunk_size = chunk_size + (i < remainder ? 1 : 0);
            size_t end_idx = std::min(start_idx + current_chunk_size, record_refs.size());
            
            if (start_idx < total_records) {
                ff_send_out(new work_t{new sort_task_t{start_idx, end_idx, i}, nullptr});
                submitted_chunks_count++;
            }
            start_idx = end_idx;  
        }
    }

    work_t* svc(work_t* task) {
        if (!task) {
            start = std::chrono::high_resolution_clock::now();
            send_out_sort_tasks();
            return GO_ON;
        } else if (task->sort_task) {
            submitted_chunks_count--;

            if (merge_levels.size() == 0)
                merge_levels.push_back(std::vector<std::pair<size_t, size_t>>());
            merge_levels[current_level].push_back({task->sort_task->start, task->sort_task->end});

            // If all sorted chunks are collected, start merging
            if (submitted_chunks_count == 0) {

                end = std::chrono::high_resolution_clock::now();
                auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
                std::cout << "Sort duration: " << duration.count() << " ms" << std::endl;
                // Sort the tasks to use inplece_merge to merge adjacent chunks and
                // avoid allocating more memory
                std::sort(merge_levels[current_level].begin(), merge_levels[current_level].end(),
                      [](const auto& a, const auto& b) {
                          return a.first < b.first;              
                       });
                current_level++;
                merge_levels.push_back(std::vector<std::pair<size_t, size_t>>());
                
                for (size_t i = 0; i + 1 < merge_levels[current_level - 1].size(); i += 2) {
                    auto& range_a = merge_levels[current_level - 1][i];
                    auto& range_b = merge_levels[current_level - 1][i + 1];
                                  
                    ff_send_out_to(new work_t{nullptr, new merge_task_t{
                        range_a.first, range_a.second, range_b.second}}, i);
                }
                
                // If odd, pass the last range to next level directly
                if (merge_levels[current_level - 1].size() % 2) 
                    merge_levels[current_level].push_back(merge_levels[current_level - 1].back());
                 
            }            
            delete task->sort_task;
            delete task;
            return GO_ON;

        } else if (task->merge_task) {
            merge_count++;
            merge_levels[current_level].push_back({task->merge_task->start_a, task->merge_task->end_b});
            size_t expected_merges = merge_levels[current_level - 1].size() / 2;
            // If the expected merges are greater than the active threads
            // we can free some resources
            kill_threads(expected_merges);
            if (merge_count == expected_merges) {
                if (merge_levels[current_level].size() == 1) {
                    // Final merge complete - data is already in record_refs
                    moveSorted(record_refs, sorted); 
                    return EOS;
                }

                // Start next merge level
                // Sort the chunks to merge in place
                std::sort(merge_levels[current_level].begin(), merge_levels[current_level].end(),
                      [](const auto& a, const auto& b) {
                          return a.first < b.first;              
                       });

                current_level++;
                merge_levels.push_back(std::vector<std::pair<size_t, size_t>>());
                merge_count = 0;
                
                for (size_t i = 0; i + 1 < merge_levels[current_level - 1].size(); i += 2) {
                    auto& range_a = merge_levels[current_level - 1][i];
                    auto& range_b = merge_levels[current_level - 1][i + 1];
                    
                    ff_send_out_to(new work_t{nullptr, new merge_task_t{
                        range_a.first, range_a.second, 
                        range_b.second}}, i);
                }

                if (merge_levels[current_level - 1].size() % 2) 
                    merge_levels[current_level].push_back(merge_levels[current_level - 1].back());
                
            }           

            delete task->merge_task;
            delete task;
            return GO_ON;
        }
        return EOS;
    }
};

/**
 * [CHUNK_1 | CHUNK_2 | CHUNK_3 | CHUNK_4]
 * [CHUNK_1 | CHUNK_2 | CHUNK_3 | CHUNK_4]
 * [CHUNK_1 | CHUNK_2 | CHUNK_3 | CHUNK_4]
 * [CHUNK_1 | CHUNK_2 | CHUNK_3 | CHUNK_4]
 */



struct WorkerNode : ff::ff_node_t<work_t> {
    std::vector<Record*>& record_refs; 
    WorkerNode(std::vector<Record*>& record_refs) : record_refs(record_refs) {}
    work_t* svc(work_t* work) {
        if (work->sort_task) {
            std::sort(record_refs.begin() + work->sort_task->start,
                      record_refs.begin() + work->sort_task->end,
                [](const Record* a, const Record* b) {
                  return a->key < b->key;
            });
            ff_send_out(work);
        } else if (work->merge_task) {
            auto start_a = record_refs.begin() + work->merge_task->start_a;
            auto middle = record_refs.begin() + work->merge_task->middle;
            auto end_b = record_refs.begin() + work->merge_task->end_b;
            std::inplace_merge(start_a, middle, end_b,
                [](const Record* x, const Record* y) { return x->key <= y->key; });        
            ff_send_out(work);
        }
        return GO_ON;
    }
};



#endif // !_FF_SORT_HPP
