#ifndef _FF_SORT_HPP
#define _FF_SORT_HPP

#include "common.hpp"
#include "config.hpp"
#include "sorting.hpp"
#include <cmath>
#include <cstddef>
#include <ff/ff.hpp>
#include <ff/pipeline.hpp>
#include <string>
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
    std::vector<std::string> files;
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
    size_t pending_merges;
    size_t expected_merges;
    size_t nthreads;
    std::string filename;
    std::string run_prefix;
    std::string merge_prefix;
    std::string output_file;
    Master(std::string filename, std::string base_path) :
        current_level(0), submitted_sort_tasks(0), merge_count(0), pending_merges(0),
        expected_merges(0), nthreads(NTHREADS), filename(filename), run_prefix(base_path+"/run#"),
        merge_prefix(base_path+"/merge#"), output_file(base_path+"/output.dat") {}

    void kill_threads(size_t expected_merges) {
        while (nthreads > expected_merges)
            ff_send_out_to(EOS, --nthreads);
    }

    void send_out_sort_tasks() {
        size_t file_size = getFileSize(filename);
        size_t chunk_size = file_size / (nthreads * nthreads);

        int fd = openFile(filename);

        std::vector<char> buffer(MAX_MEMORY / nthreads);
        size_t buffer_offset = 0;
        size_t file_offset = 0;

        size_t logical_start = 0;
        size_t logical_end = 0;

        size_t bytes_in_buffer = 0;
        size_t task_id = 0;

        while (true) {
            // Shift leftover bytes
            if (buffer_offset < bytes_in_buffer) {
                memmove(buffer.data(), buffer.data() + buffer_offset, bytes_in_buffer - buffer_offset);
                bytes_in_buffer -= buffer_offset;
            } else {
                bytes_in_buffer = 0;
            }
            buffer_offset = 0;

            ssize_t bytes_read = read(fd, buffer.data() + bytes_in_buffer, buffer.size() - bytes_in_buffer);
            if (bytes_read < 0) {
                std::cerr << "Read error: " << strerror(errno) << std::endl;
                close(fd);
                return;
            } else if (bytes_read == 0 && bytes_in_buffer == 0) {
                break; // EOF
            }
            bytes_in_buffer += bytes_read;

            // Parse complete records
            while (buffer_offset + sizeof(uint64_t) + sizeof(uint32_t) <= bytes_in_buffer) {
                size_t local_offset = buffer_offset;

                buffer_offset += sizeof(uint64_t); // skip key

                uint32_t len = *reinterpret_cast<uint32_t*>(&buffer[buffer_offset]);
                buffer_offset += sizeof(uint32_t);

                if (buffer_offset + len > bytes_in_buffer) {
                    buffer_offset = local_offset;
                    break;
                }

                buffer_offset += len;

                logical_end = file_offset + buffer_offset;

                if (logical_end - logical_start >= chunk_size) {
                    size_t size = logical_end - logical_start;
                    ff_send_out(new work_t{new sort_task_t{
                        filename,
                        logical_start,
                        size,
                        MAX_MEMORY / nthreads,
                        task_id++
                    }, nullptr});
                    submitted_sort_tasks++;
                    logical_start = logical_end;
                }
            }

            file_offset += buffer_offset;
        }

        // Emit remaining bytes
        if (logical_end > logical_start) {
            size_t size = logical_end - logical_start;
            ff_send_out(new work_t{new sort_task_t{
                filename,
                logical_start,
                size,
                MAX_MEMORY / nthreads,
                task_id++
            }, nullptr});
            submitted_sort_tasks++;
        }

        close(fd);
    }

    work_t* svc(work_t* task) {
        if (!task) {
            send_out_sort_tasks();
            return GO_ON;
        } else if (task->sort_task) {
            submitted_sort_tasks--;

            // If all sorted chunks are collected, start merging
            if (submitted_sort_tasks == 0) {
                std::vector<std::string> sequences = findFiles(run_prefix);

                const size_t group_size = (sequences.size() + nthreads - 1) / nthreads;
                std::vector<std::string> level_0;

                for (size_t i = 0; i < nthreads; ++i) {
                    size_t start = i * group_size;
                    if (start >= sequences.size()) break;
                    size_t end = std::min(start + group_size, sequences.size());

                    std::vector<std::string> group(sequences.begin() + start, sequences.begin() + end);
                    if (group.empty()) continue;

                    std::string filename = merge_prefix + generateUUID();
                    ff_send_out(new work_t{nullptr, new merge_task_t{
                        group,
                        filename,
                        MAX_MEMORY / nthreads
                    }});
                    level_0.push_back(filename);
                }
                expected_merges = level_0.size(); // Every thread merges a group
                levels.push_back(std::move(level_0));
            }
            delete task->sort_task;
            delete task;
            return GO_ON;

        } else if (task->merge_task) {
            merge_count++;
            if (merge_count == expected_merges) {
                if (levels[current_level].size() == 1) {
                    rename(levels.back().back().c_str(), output_file.c_str());
                    return EOS;
                }

                current_level++;
                levels.push_back({});
                if (levels[current_level - 1].size() % 2) {
                    levels[current_level].push_back(levels[current_level - 1].back());
                    levels[current_level - 1].pop_back();
                }

                expected_merges = levels[current_level - 1].size() / 2;
                std::vector<std::string> inputs;
                for (size_t i = 0; i < levels[current_level - 1].size() - 1; i += 2) {
                    std::string filename = merge_prefix + generateUUID();
                    inputs.push_back(levels[current_level - 1][i]);
                    inputs.push_back(levels[current_level - 1][i + 1]);
                    ff_send_out(new work_t{nullptr, new merge_task_t{
                        std::move(inputs),
                        filename,
                        MAX_MEMORY/nthreads}});
                    inputs.clear();
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
    std::string run_prefix;
    WorkerNode(std::string base_path) : run_prefix(base_path+"/run#") {}
    work_t* svc(work_t* work) {
        if (work->sort_task) {
            genSequenceFiles(
                work->sort_task->filename,
                work->sort_task->start,
                work->sort_task->size,
                work->sort_task->memory,
                run_prefix + generateUUID());

            ff_send_out(work);
        } else if (work->merge_task) {
            if (work->merge_task->files.size() == 2)
                mergeFiles(
                    work->merge_task->files[0],
                    work->merge_task->files[1],
                    work->merge_task->output,
                    work->merge_task->memory);
            else
                kWayMergeFiles(work->merge_task->files, work->merge_task->output, work->merge_task->memory);
            ff_send_out(work);
        }
        return GO_ON;
    }
};



#endif // !_FF_SORT_HPP
