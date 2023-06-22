#include <immintrin.h>
#include <cstdint>

#include "utils.h"

bool arrayIsSimd128Aligned(const int* array) {
    const size_t simdAlignment = sizeof(__m128i);
    return reinterpret_cast<uintptr_t>(array) % simdAlignment == 0;
}

bool arrayIsSimd256Aligned(const int* array) {
    const size_t simdAlignment = sizeof(__m256i);
    return reinterpret_cast<uintptr_t>(array) % simdAlignment == 0;
}
