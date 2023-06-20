#include <immintrin.h>
#include <cstdint>

#include "utils.h"

bool arrayIsSimd256Aligned(const int* array) {
    const size_t simdAlignment = sizeof(__m256i);
    return reinterpret_cast<uintptr_t>(array) % simdAlignment == 0;
}
