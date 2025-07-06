#ifndef _CONFIG_HPP
#define _CONFIG_HPP
#include <cstdint>
#include <thread>

static unsigned int NTHREADS = std::thread::hardware_concurrency();
static unsigned int RECORD_SIZE = 64;
static unsigned int ARRAY_SIZE = 10000;
static unsigned int ROUNDS = 4;
static unsigned int SORT_THRESHOLD = 64;
static uint64_t MAX_MEMORY = 1ULL << 33; // 8 GB

#endif // !_CONFIG_HPP
