#include <cstddef>
#include <cstdint>
#include <iterator>
#include <utility>
#include <vector>
#include <cstdio>
#include <omp.h>
#include <cassert>
#include "include/cmdline.hpp"
#include "include/common.hpp"
#include "include/config.hpp"
#include "include/hpc_helpers.hpp"
#include "include/sorting.hpp"




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
        if (read_size == 0) break; // EOF
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
        if (current_pos - start >= chunk_size && chunks.size() < num_threads - 1) {
            chunks.push_back({start, current_pos-1});
            start = current_pos;
        }
    }
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
    // return 0;
    #pragma omp parallel for
    for (size_t i = 0; i < chunks.size(); i++)
        genSequenceFiles(filename, chunks[i].first, chunks[i].second-chunks[i].first, MAX_MEMORY/omp_get_max_threads(), "/tmp/run");
    std::vector<std::string> sequences = findFiles("/tmp/run");
    std::vector<std::vector<std::string>> next_level;
    next_level.push_back({});
    if (sequences.size() % 2) {
        next_level[0].push_back(sequences.back());
        sequences.pop_back();
    }
    #pragma omp parallel for
    for (size_t i = 0; i < sequences.size() - 1; i+=2) {
        next_level[0].push_back(mergeFiles(sequences[i], sequences[i + 1], MAX_MEMORY/omp_get_max_threads()));
    }
    size_t current_level = 1;
    while (next_level.back().size() > 1) {
        next_level.push_back({});
        #pragma omp parallel for
        for (size_t i = 0; i < next_level[current_level - 1].size() - 1; i += 2) {
            next_level[current_level].push_back(mergeFiles(next_level[current_level - 1][i], next_level[current_level - 1][i + 1], MAX_MEMORY/omp_get_max_threads()));
        }
        if (next_level[current_level - 1].size() % 2) {
            next_level[current_level].push_back(next_level[current_level - 1].back());
        }
        current_level++;
    }

    rename(next_level.back().back().c_str(), "/tmp/output.dat");
    TIMERSTOP(mergesort_seq)
    assert(checkSortedFile("/tmp/output.dat"));
    // destroyArray(records);
    return 0;

}
