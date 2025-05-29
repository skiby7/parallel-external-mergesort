#ifndef _FF_SORT_HPP
#define _FF_SORT_HPP
#include "common.hpp"
#include "config.hpp"
#include <cmath>
#include <ff/ff.hpp>
#include <ff/pipeline.hpp>

struct sort_task_t {
    size_t start;
    size_t end;
    size_t w_id;
};

struct merge_task_t {
    std::vector<Record*> chunk_a;
    std::vector<Record*> chunk_b;
    std::vector<Record*> merged_chunk;
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
    std::vector<std::vector<std::vector<Record*>>> merge_levels;
    Master(std::vector<Record*>& record_refs, std::vector<Record>& sorted) : 
        record_refs(record_refs), sorted(sorted), total_records(record_refs.size()),
        current_level(0), submitted_chunks_count(0), merge_count(0), n_chunks(NTHREADS*CHUNKS_PER_THREAD) {
        chunk_size = record_refs.size()/n_chunks;
        remainder = record_refs.size()%n_chunks;
    } 
     
    work_t* svc(work_t* task) {
        if (!task) {
            // Task initialization and send out to workers
            size_t start_idx = 0;
            for (size_t i = 0; i < n_chunks; i++) {
                size_t current_chunk_size = chunk_size + (i < remainder ? 1 : 0);
                size_t end_idx = std::min(start_idx + current_chunk_size, record_refs.size());
                
                if (start_idx < total_records) {
                    ff_send_out(new work_t{new sort_task_t{start_idx, end_idx, i}, nullptr});
                    submitted_chunks_count++;
                }
                start_idx = end_idx;  // No +1 needed for exclusive
            }
            return GO_ON;

        } else if (task->sort_task) {
            submitted_chunks_count--;


            // Create chunk of Record* from start to end
            std::vector<Record*> chunk;
            chunk.reserve(task->sort_task->end - task->sort_task->start);
            for (size_t i = task->sort_task->start; i < task->sort_task->end; ++i)
                chunk.push_back(record_refs[i]);
            if (merge_levels.size() == 0)
                merge_levels.push_back(std::vector<std::vector<Record*>>());
            merge_levels[current_level].push_back(std::move(chunk));

            // If all sorted chunks are collected, start merging
            if (submitted_chunks_count == 0) {


                current_level++;
                merge_levels.push_back(std::vector<std::vector<Record*>>());
                for (size_t i = 0; i + 1 < merge_levels[current_level - 1].size(); i += 2) {
                    auto& a = merge_levels[current_level - 1][i];
                    auto& b = merge_levels[current_level - 1][i + 1];
                    ff_send_out(new work_t{nullptr, new merge_task_t{a, b, {}}});
                }
                // If odd, pass the last chunk to next level directly
                if (merge_levels[current_level - 1].size() % 2 == 1) {
                    merge_levels[current_level].push_back(
                        std::move(merge_levels[current_level - 1].back()));
                }
            }
            delete task->sort_task;
            delete task;
            return GO_ON;
        } else if (task->merge_task) {

            merge_count++;
            merge_levels[current_level].push_back(std::move(task->merge_task->merged_chunk));

            size_t expected_merges = merge_levels[current_level - 1].size() / 2;

            if (merge_count == expected_merges) {
                if (merge_levels[current_level].size() == 1) {
                    // Final merge complete
                    moveSorted(merge_levels[current_level][0], sorted); 
                    return EOS;
                }

                // Start next merge level
                current_level++;
                merge_levels.push_back(std::vector<std::vector<Record*>>());
                merge_count = 0;
                for (size_t i = 0; i + 1 < merge_levels[current_level - 1].size(); i += 2) {
                    auto& a = merge_levels[current_level - 1][i];
                    auto& b = merge_levels[current_level - 1][i + 1];
                    ff_send_out(new work_t{nullptr, new merge_task_t{a, b, {}}});
                }

                if (merge_levels[current_level - 1].size() % 2 == 1) {
                    merge_levels[current_level].push_back(
                        std::move(merge_levels[current_level - 1].back()));
                }
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
            boundedMergeSort(record_refs, work->sort_task->start, work->sort_task->end-1);
            ff_send_out(work);
        } else if (work->merge_task) {
            auto& a = work->merge_task->chunk_a;
            auto& b = work->merge_task->chunk_b;
            auto& merged = work->merge_task->merged_chunk;

            merged.resize(a.size() + b.size()); 
            size_t i = 0, j = 0, k = 0;

            while (i < a.size() && j < b.size()) {
                merged[k++] = a[i]->key <= b[j]->key ? a[i++] : b[j++];
            }

            std::copy(a.begin() + i, a.end(), merged.begin() + k);
            std::copy(b.begin() + j, b.end(), merged.begin() + k + a.size() - i);

            ff_send_out(work);
        }
        return GO_ON;
    }
};



#endif // !_FF_SORT_HPP
