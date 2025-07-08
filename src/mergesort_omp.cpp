#include <cstddef>
#include <cstdint>
#include <iostream>
#include <iterator>
#include <string>
#include <utility>
#include <vector>
#include <cstdio>
#include <omp.h>
#include <cassert>
#include "include/cmdline.hpp"
#include "include/common.hpp"
#include "include/config.hpp"
#include "include/feistel.hpp"
#include "include/hpc_helpers.hpp"
#include "include/sorting.hpp"
#include <uuid/uuid.h>

std::string generateUUID() {
    uuid_t uuid;
    uuid_generate(uuid);
    char uuid_str[37];
    uuid_unparse(uuid, uuid_str);
    return std::string(uuid_str);
}

std::vector<std::pair<size_t, size_t>> computeChunks(const std::string& filename) {
    std::vector<std::pair<size_t, size_t>> chunks;
    // Each thread should have a chunk of data to sort
    size_t num_threads = omp_get_max_threads();
    size_t chunk_size = getFileSize(filename) / num_threads;
    int fp = open(filename.c_str(), O_RDONLY);
    if (fp < 0) {
        std::cerr << "Error opening file for reading: " << filename << " " << strerror(errno) << std::endl;
        exit(-1);
    }
    size_t start = 0, read_size = 0;
    size_t current_pos = 0;
    unsigned long key;
    uint32_t len;
    while(true) {
        read_size = read(fp, &key, sizeof(key));
        if (read_size == 0) {
            current_pos = lseek(fp, 0, SEEK_CUR);
            chunks.push_back({start, current_pos});
            break;
        } // EOF
        if (read_size < 0) {
            std::cerr << "Error reading key: " << strerror(errno) << std::endl;
            close(fp);
            exit(-1);
        }

        // Read length
        read_size = read(fp, &len, sizeof(len));
        if (read_size < 0) {
            std::cerr << "Error reading length: " << strerror(errno) << std::endl;
            close(fp);
            exit(-1);
        }

        // Skip payload
        if (lseek(fp, len, SEEK_CUR) == -1) {
            std::cerr << "Error seeking past payload: " << strerror(errno) << std::endl;
            close(fp);
            exit(-1);
        }

        // Get current position
        current_pos = lseek(fp, 0, SEEK_CUR);

        // Check if we've exceeded target chunk size
        if (current_pos - start >= chunk_size) {
            chunks.push_back({start, current_pos});
            start = current_pos;
        }
    }
    std::cout << "Divided file in " << chunks.size() << " chunks" << std::endl;
    close(fp);
    return chunks;
}

int main(int argc, char *argv[]) {
    int start = 0;
    if((start = parseCommandLine(argc, argv)) < 0) return -1;
    std::string filename = argv[start];

    assert(!checkSortedFile(filename));
    TIMERSTART(mergesort_seq)
    std::vector<std::pair<size_t, size_t>> chunks = computeChunks(filename);
    for(auto& chunk : chunks)
        std::cout << chunk.first << " " << chunk.second << " size: " << chunk.second - chunk.first << std::endl;
    // size_t total_size = 0;
    // #pragma omp parallel for
    // for (size_t i = 0; i < chunks.size(); i++) {
    //     size_t size = chunks[i].second - chunks[i].first;
    //     genSequenceFiles(filename, chunks[i].first, size, MAX_MEMORY/omp_get_max_threads(), "/tmp/run#" + std::to_string(omp_get_thread_num()));
    //     #pragma omp critical
    //     total_size += size;
    // }
    // std::cout << "Processed " << total_size << " bytes" << std::endl;
    genSequenceFiles(filename, 0, getFileSize(filename), MAX_MEMORY, "/tmp/run");
    // return 0;
    std::vector<std::string> sequences = findFiles("/tmp/run");

    /**
     * Reminder to the future me:
     *   - The sequential genSequenceFiles works and sum of the size of the chunks created corresponds to the original file size
     *   - The files once merged are sorted as expected
     * Now the problem is that the output file is two order of magnitude smaller than expected
     */
    std::vector<std::vector<std::string>> next_level;
    next_level.push_back({});
    if (sequences.size() % 2) {
        next_level[0].push_back(sequences.back());
        sequences.pop_back();
    }
    size_t total_size = 0;
    for (auto& file : sequences) {
        total_size += getFileSize(file);
    }
    std::cout << "Total size: " << total_size << std::endl;
    std::cout << "File size: " << getFileSize(filename) << std::endl;
    assert(total_size == getFileSize(filename));
    // #pragma omp parallel for
    for (size_t i = 0; i < sequences.size() - 1; i+=2) {
        std::string filename = "/tmp/merge#" + generateUUID();
        // This code merges the two sequences and then stores the result in filename
        mergeFiles(sequences[i], sequences[i + 1], filename, MAX_MEMORY/omp_get_max_threads());
        #pragma omp critical
        next_level[0].push_back(filename);
    }
    // for (auto& filename : next_level[0]) {
    //         std::cout << filename << std::endl;
    //         assert(checkSortedFile(filename));
    //     }

    // for (auto& filename : next_level[0])
    //     std::cout << filename << std::endl;
    size_t current_level = 1;
    while (next_level.back().size() > 1) {
        next_level.push_back({});
        if (next_level[current_level - 1].size() % 2) {
            next_level[current_level].push_back(next_level[current_level - 1].back());
            next_level[current_level - 1].pop_back();
        }
        // #pragma omp parallel for
        for (size_t i = 0; i < next_level[current_level - 1].size() - 1; i += 2) {
            std::string filename = "/tmp/merge#" + generateUUID();;
            mergeFiles(next_level[current_level - 1][i], next_level[current_level - 1][i + 1], filename, MAX_MEMORY/omp_get_max_threads());
            #pragma omp critical
            next_level[current_level].push_back(filename);
        }
        current_level++;
    }

    rename(next_level.back().back().c_str(), "/tmp/output.dat");

    std::cout << "Output file size: " << getFileSize("/tmp/output.dat") << std::endl;
    TIMERSTOP(mergesort_seq)
    assert(checkSortedFile("/tmp/output.dat"));
    // destroyArray(records);
    return 0;

}
