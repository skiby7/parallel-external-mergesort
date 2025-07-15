#ifndef _CMDLINE_HPP
#define _CMDLINE_HPP

#include <cstdio>
#include <string>
#include <cstring>
#include <cstdlib>
#include <unistd.h>
#include <iostream>

#include "config.hpp"

static inline void usage(const char *argv0) {
    std::printf("--------------------\n");
    std::printf("Usage: %s [options]\n", argv0);
    std::printf("\nOptions:\n");
    std::printf(" -t T: number of threads (default=%d)\n", NTHREADS);
    std::printf(" -k: use k-way merge in the sequential version (default=%d)\n", KWAY_MERGE ? 1 :0);
    std::printf(" -m M: set the max memory usage (default=%ld)\n", MAX_MEMORY);
    std::printf("--------------------\n");
    /**
     * These options are still relevant for the generation of the file,
     * so I just keep them here as a reminder
     */
    // std::printf(" -d D: change feistel rounds to change the Record->key value (default=%d)\n", ROUNDS);
    // std::printf(" -s N: number array size (default s=%d)\n", ARRAY_SIZE);
    // std::printf(" -r R: record payload size in bytes (default r=%d)\n", RECORD_SIZE);
}


static bool isNumber(const char* s, long &n) {
    try {
		size_t e;
		n=std::stol(s, &e, 10);
		return e == strlen(s);
    } catch (const std::invalid_argument&) {
		return false;
    } catch (const std::out_of_range&) {
		return false;
    }
}

static inline int parseCommandLine(int argc, char *argv[]) {
    extern char *optarg;
    const std::string optstr = "r:s:t:d:m:k";
    long opt, start = 1;

    while ((opt = getopt(argc, argv, optstr.c_str())) != -1) {
        switch (opt) {
            case 'k': {
                KWAY_MERGE = true;
                start += 1;
            } break;
            case 's': {
                long s = 0;
                if (!isNumber(optarg, s)) {
                    std::fprintf(stderr, "Error: wrong '-s' option\n");
                    usage(argv[0]);
                    return -1;
                }
                ARRAY_SIZE = s;
                start += 2;
            } break;
            case 'm': {
                long m = 0;
                if (!isNumber(optarg, m)) {
                    std::fprintf(stderr, "Error: wrong '-m' option\n");
                    usage(argv[0]);
                    return -1;
                }
                MAX_MEMORY = m;
                start += 2;
            } break;
            case 'r': {
                long r = 0;
                if (!isNumber(optarg, r)) {
                    std::fprintf(stderr, "Error: wrong '-r' option\n");
                    usage(argv[0]);
                    return -1;
                }
                RECORD_SIZE = r;
                start += 2;
            } break;
            case 't': {
                long t = 0;
                if (!isNumber(optarg, t)) {
                    std::fprintf(stderr, "Error: wrong '-t' option\n");
                    usage(argv[0]);
                    return -1;
                }
                NTHREADS = t;
                start += 2;
            } break;
            case 'd': {
                long d = 0;
                if (!isNumber(optarg, d) || d < 1) {
                    std::fprintf(stderr, "Error: wrong '-d' option\n");
                    usage(argv[0]);
                    return -1;
                }
                ROUNDS = d;
                start += 2;
            } break;

            case 'x': {
                long x = 0;
                if (!isNumber(optarg, x) || x < 1) {
                    std::fprintf(stderr, "Error: wrong '-x' option\n");
                    usage(argv[0]);
                    return -1;
                }
                SORT_THRESHOLD = x;
                start += 2;
            } break;
            default:
                usage(argv[0]);
                return -1;
        }
    }
    return start;
}
#endif // !_CMDLINE_HPP
