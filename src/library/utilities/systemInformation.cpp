#include <immintrin.h>
#include <iostream>
#include <unistd.h>
#include <thread>

#include "systemInformation.h"

namespace MABPL {

bool arrayIsSimd128Aligned(const int *array) {
    const size_t simdAlignment = sizeof(__m128i);
    return reinterpret_cast<uintptr_t>(array) % simdAlignment == 0;
}

bool arrayIsSimd256Aligned(const int *array) {
    const size_t simdAlignment = sizeof(__m256i);
    return reinterpret_cast<uintptr_t>(array) % simdAlignment == 0;
}

long l3cacheSize() {
    return sysconf(_SC_LEVEL3_CACHE_SIZE);
}

long bytesPerCacheLine() {
    return sysconf(_SC_LEVEL1_DCACHE_LINESIZE);
}

int maxDop() {
    return static_cast<int>(std::thread::hardware_concurrency());
}

}
