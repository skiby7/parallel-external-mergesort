#ifndef _CONFIG_HPP
#define _CONFIG_HPP
#include <thread>

static unsigned int NTHREADS = std::thread::hardware_concurrency();
static unsigned int RECORD_SIZE = 64;
static unsigned int ARRAY_SIZE = 10000;
static unsigned int ROUNDS = 4;
static unsigned int SORT_THRESHOLD = 64;

#endif // !_CONFIG_HPP
