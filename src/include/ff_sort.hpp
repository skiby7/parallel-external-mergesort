#ifndef _FF_SORT_HPP
#define _FF_SORT_HPP
#include "common.hpp"
#include "config.hpp"
#include "filesystem.hpp"
#include "sorting.hpp"
#include <cmath>
#include <cstddef>
#include <ff/ff.hpp>
#include <ff/pipeline.hpp>
#include <unistd.h>
#include <vector>

struct sort_task_t {
    std::string filename;
    size_t start;
    size_t size;
    size_t memory;
    size_t w_id;
};

struct merge_task_t {
    std::string file1;
    std::string file2;
    std::string output;
    size_t memory;
};

struct work_t {
    struct sort_task_t *sort_task;
    struct merge_task_t *merge_task;
};


struct Master : ff::ff_monode_t<work_t> {
    std::vector<std::vector<std::string>> levels;
    size_t current_level;
    size_t submitted_sort_tasks;
    size_t merge_count;
    size_t expected_merges;
    size_t nthreads;
    std::string filename;
    Master(std::string filename) :
        current_level(0), submitted_sort_tasks(0), merge_count(0),
        nthreads(NTHREADS), filename(filename) {}

    void kill_threads(size_t expected_merges) {
        while (nthreads > expected_merges)
            ff_send_out_to(EOS, --nthreads);
    }
    void send_out_sort_tasks () {
        std::vector<std::pair<size_t, size_t>> chunks = computeChunks(filename, nthreads);
        for (size_t i = 0; i < chunks.size(); i++) {
            size_t size = chunks[i].second - chunks[i].first;
            ff_send_out(new work_t{new sort_task_t{
                filename,
                chunks[i].first,
                size,
                MAX_MEMORY/nthreads,
                i
            }, nullptr});
            submitted_sort_tasks++;
        }
    }

    work_t* svc(work_t* task) {
        if (!task) {
            send_out_sort_tasks();
            return GO_ON;
        } else if (task->sort_task) {
            submitted_sort_tasks--;

            // If all sorted chunks are collected, start merging
            if (submitted_sort_tasks == 0) {
                std::vector<std::string> sequences = findFiles("/tmp/run");

                levels.push_back({});
                if (sequences.size() % 2) {
                    levels[0].push_back(sequences.back());
                    sequences.pop_back();
                }

                expected_merges = sequences.size() / 2;
                // kill_threads(expected_merges);
                for (size_t i = 0; i < sequences.size() - 1; i+=2) {
                    std::string filename = "/tmp/merge#" + generateUUID();
                    ff_send_out(new work_t{nullptr, new merge_task_t{
                        sequences[i],
                        sequences[i + 1],
                        filename,
                        MAX_MEMORY/nthreads}});
                    levels[0].push_back(filename);
                }
            }
            delete task->sort_task;
            delete task;
            return GO_ON;

        } else if (task->merge_task) {
            merge_count++;
            if (merge_count == expected_merges) {
                if (levels[current_level].size() == 1) {
                    rename(levels.back().back().c_str(), "/tmp/output.dat");
                    return EOS;
                }

                current_level++;
                levels.push_back({});
                if (levels[current_level - 1].size() % 2) {
                    levels[current_level].push_back(levels[current_level - 1].back());
                    levels[current_level - 1].pop_back();
                }

                expected_merges = levels[current_level - 1].size() / 2;
                // kill_threads(expected_merges);
                for (size_t i = 0; i < levels[current_level - 1].size() - 1; i += 2) {
                    std::string filename = "/tmp/merge#" + generateUUID();;
                    ff_send_out(new work_t{nullptr, new merge_task_t{
                        levels[current_level - 1][i],
                        levels[current_level - 1][i + 1],
                        filename,
                        MAX_MEMORY/nthreads}});
                    levels[current_level].push_back(filename);
                }
                merge_count = 0;
            }

            delete task->merge_task;
            delete task;
            return GO_ON;
        }
        return EOS;
    }
};


struct WorkerNode : ff::ff_node_t<work_t> {
    WorkerNode() {}
    work_t* svc(work_t* work) {
        if (work->sort_task) {
            genSequenceFiles(
                work->sort_task->filename,
                work->sort_task->start,
                work->sort_task->size,
                work->sort_task->memory,
                "/tmp/run#" + generateUUID());

            ff_send_out(work);
        } else if (work->merge_task) {
            mergeFiles(
                work->merge_task->file1,
                work->merge_task->file2,
                work->merge_task->output,
                work->merge_task->memory);
            ff_send_out(work);
        }
        return GO_ON;
    }
};



#endif // !_FF_SORT_HPP
