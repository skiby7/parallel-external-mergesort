#ifndef _FEISTEL_HPP
#define _FEISTEL_HPP

#include <cstdint>


/**
 * Trying to implement the pseudorandom generator using a feistel network
 * This is a simple implementation to generate a pseudorandom sequence of unique integers
*/

static inline uint32_t mix32(uint32_t z) {
    z += 0x9e3779b9;                         // golden ratio
    z = (z ^ (z >> 16)) * 0x85ebca6b;
    z = (z ^ (z >> 13)) * 0xc2b2ae35;
    return  z ^ (z >> 16);
}

// --------------------------------------
// F-function: takes a 32-bit half + 32-bit subkey → 32-bit
static inline uint32_t F(uint32_t half, uint32_t subkey) {
    return mix32(half ^ subkey);
}

// --------------------------------------
// 64-bit Feistel (two 32-bit halves), 16 rounds
static inline uint32_t feistel_encrypt(uint32_t x,
                                 uint32_t key    = 0xDEADBEEF,
                                 uint32_t rounds = 16) {
    uint32_t L = x;
    uint32_t R = ~x;    // you can also start R=0 or R=key

    for(uint32_t i = 0; i < rounds; ++i) {
        uint32_t subkey = mix32(key ^ i);
        uint32_t newL   = R;
        uint32_t newR   = L ^ F(R, subkey);
        L = newL;
        R = newR;
    }
    // final “output diffusion”: xor the halves together
    return L ^ R;
}
#endif // !_FEISTEL_HPP
