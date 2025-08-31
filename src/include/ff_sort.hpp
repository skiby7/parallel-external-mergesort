#ifndef _FF_SORT_HPP
#define _FF_SORT_HPP

#include "common.hpp"
#include "config.hpp"
#include "hpc_helpers.hpp"
#include "sorting.hpp"
#include <algorithm>
#include <cmath>
#include <cstddef>
#include <filesystem>
#include <ff/ff.hpp>
#include <ff/pipeline.hpp>
#include <iostream>
#include <iterator>
#include <string>
#include <unistd.h>
#include <vector>

struct sort_task_t {
    std::string filename;
    size_t start;
    size_t size;
    size_t memory;
    size_t w_id;
    std::vector<std::string> run_files;
};

struct merge_task_t {
    std::vector<std::string> files;
    std::string output;
    size_t memory;
};

struct work_t {
    struct sort_task_t *sort_task;
    struct merge_task_t *merge_task;
};


struct Master : ff::ff_monode_t<work_t> {
    size_t current_level;
    std::vector<size_t> submitted_sort_tasks;
    std::vector<std::vector<std::string>> run_files;
    std::vector<std::string> merge_files;
    size_t merge_count;
    size_t pending_merges;
    size_t expected_merges;
    size_t nworkers;
    std::string filename;
    std::string run_prefix;
    std::string merge_prefix;
    std::string output_file;
    Master(std::string filename, std::string base_path) :
        current_level(0), submitted_sort_tasks(0), merge_count(0), pending_merges(0),
        expected_merges(0), nworkers(NTHREADS-1), filename(filename), run_prefix(base_path+"/run#"),
        merge_prefix(base_path+"/merge#"), output_file(base_path+"/output.dat") {}

    void kill_threads(size_t expected_merges) {
        while (nworkers > expected_merges)
            ff_send_out_to(EOS, --nworkers);
    }

    void send_out_sort_tasks() {
        size_t file_size = getFileSize(filename);
        size_t max_mem_per_worker = MAX_MEMORY / NTHREADS;
        size_t chunk_size = std::min(file_size / 100, 3 * max_mem_per_worker); // 1% of the file or the memory per worker to keep them busy
        int fd = openFile(filename);

        /**
         * While the master thread is skipping through the file,
         * the memory usage is a little higher than the allowed memory.
         * But it is the same in the omp implementation, so the comparison is fair.
        */
        std::vector<char> buffer(max_mem_per_worker);
        size_t buffer_offset = 0, file_offset = 0, start_offset = 0, end_offset = 0;

        size_t bytes_in_buffer = 0;
        size_t worker_id = 0;
        submitted_sort_tasks.resize(nworkers);

        while (true) {
            if (buffer_offset < bytes_in_buffer) {
                memmove(buffer.data(), buffer.data() + buffer_offset, bytes_in_buffer - buffer_offset);
                bytes_in_buffer -= buffer_offset;
            } else {
                bytes_in_buffer = 0;
            }
            buffer_offset = 0;

            /* One single syscall is faster than reading key+len and lseek to the next key */
            ssize_t bytes_read = read(fd, buffer.data() + bytes_in_buffer, buffer.size() - bytes_in_buffer);
            if (bytes_read < 0) {
                std::cerr << "Read error: " << strerror(errno) << std::endl;
                close(fd);
                return;
            } else if (bytes_read == 0 && bytes_in_buffer == 0) {
                break; // EOF
            }
            bytes_in_buffer += bytes_read;

            while (buffer_offset + sizeof(uint64_t) + sizeof(uint32_t) <= bytes_in_buffer) {
                size_t local_offset = buffer_offset;
                buffer_offset += sizeof(uint64_t);
                uint32_t len = *reinterpret_cast<uint32_t*>(&buffer[buffer_offset]);
                buffer_offset += sizeof(uint32_t);

                if (buffer_offset + len > bytes_in_buffer) {
                    buffer_offset = local_offset;
                    break;
                }

                buffer_offset += len;
                end_offset = file_offset + buffer_offset;

                if (end_offset - start_offset >= chunk_size) {
                    size_t size = end_offset - start_offset;
                    ff_send_out_to(new work_t{new sort_task_t{
                        filename,
                        start_offset,
                        size,
                        max_mem_per_worker,
                        worker_id
                    }, nullptr}, worker_id);
                    submitted_sort_tasks[worker_id]++;
                    worker_id = (worker_id + 1) % nworkers;
                    start_offset = end_offset;
                }
            }

            file_offset += buffer_offset;
        }

        if (end_offset > start_offset) {
            size_t size = end_offset - start_offset;
            ff_send_out_to(new work_t{new sort_task_t{
                filename,
                start_offset,
                size,
                max_mem_per_worker,
                worker_id
            }, nullptr}, worker_id);
            submitted_sort_tasks[worker_id]++;
        }
        close(fd);
    }

    work_t* svc(work_t* task) {
        if (!task) {
            run_files.resize(nworkers);
            send_out_sort_tasks();
            return GO_ON;
        } else if (task->sort_task) {
            run_files[task->sort_task->w_id].insert(
                run_files[task->sort_task->w_id].end(),
               std::make_move_iterator(task->sort_task->run_files.begin()),
               std::make_move_iterator(task->sort_task->run_files.end()));
            if (--submitted_sort_tasks[task->sort_task->w_id] == 0) {
                if (run_files[task->sort_task->w_id].size() == 1) {
                    /* If it produced a single run file, it can be passed to the final merge */
                    merge_files.push_back(run_files[task->sort_task->w_id][0]);
                } else {
                    std::string filename = merge_prefix + generateUUID();
                    ff_send_out_to(new work_t{nullptr, new merge_task_t{
                        std::move(run_files[task->sort_task->w_id]),
                        filename,
                        MAX_MEMORY/nworkers}}, task->sort_task->w_id);
                    expected_merges++;
                }
            }
            delete task->sort_task;
            delete task;
            return GO_ON;

        } else if (task->merge_task) {
            merge_count++;
            merge_files.push_back(std::move(task->merge_task->output));
            auto all_finished = std::all_of(
                submitted_sort_tasks.begin(), submitted_sort_tasks.end(),
                [](const auto& count) { return count == 0; });
            if (merge_count == expected_merges && all_finished) {
                kWayMergeFiles(merge_files, output_file, MAX_MEMORY);
                delete task->merge_task;
                delete task;
                return EOS;
            }

            delete task->merge_task;
            delete task;
            return GO_ON;
        }

        return EOS;
    }
};


struct WorkerNode : ff::ff_node_t<work_t> {
    std::string run_prefix;
    WorkerNode(std::string base_path) : run_prefix(base_path+"/run#") {}
    work_t* svc(work_t* work) {
        if (work->sort_task)
            work->sort_task->run_files = genSequenceFilesSTL(
                work->sort_task->filename,
                work->sort_task->start,
                work->sort_task->size,
                work->sort_task->memory,
                run_prefix + generateUUID());
        else if (work->merge_task)
            kWayMergeFiles(work->merge_task->files, work->merge_task->output, work->merge_task->memory);

        ff_send_out(work);
        return GO_ON;
    }
};



#endif // _FF_SORT_HPP
