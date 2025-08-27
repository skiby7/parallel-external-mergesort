#ifndef _CONFIG_HPP
#define _CONFIG_HPP
#include <cstdint>
#include <linux/limits.h>
#include <sys/stat.h>
#include <thread>

static unsigned int NTHREADS = std::thread::hardware_concurrency();
static unsigned int RECORD_SIZE = 64;
static unsigned int ARRAY_SIZE = 10000;
static unsigned int ROUNDS = 4;
static uint64_t MAX_MEMORY = 1ULL << 33; // 8 GB
static bool KWAY_MERGE = false;
static char TMP_LOCATION[PATH_MAX+1] = "/tmp";
static bool FF_NO_MAPPING = true;

#endif // _CONFIG_HPP
