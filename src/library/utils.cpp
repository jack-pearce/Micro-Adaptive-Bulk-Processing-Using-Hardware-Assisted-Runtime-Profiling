#include <immintrin.h>
#include <iostream>
#include <unistd.h>

#include "utils.h"

bool arrayIsSimd128Aligned(const int* array) {
    const size_t simdAlignment = sizeof(__m128i);
    return reinterpret_cast<uintptr_t>(array) % simdAlignment == 0;
}

bool arrayIsSimd256Aligned(const int* array) {
    const size_t simdAlignment = sizeof(__m256i);
    return reinterpret_cast<uintptr_t>(array) % simdAlignment == 0;
}

long getL3cacheSize() {
    return sysconf(_SC_LEVEL3_CACHE_SIZE);
}

long getBytesPerCacheLine() {
    return sysconf(_SC_LEVEL1_DCACHE_LINESIZE);
}
