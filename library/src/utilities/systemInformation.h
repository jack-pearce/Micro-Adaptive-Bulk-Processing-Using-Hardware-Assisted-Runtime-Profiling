#ifndef HAQP_SYSTEMINFORMATION_H
#define HAQP_SYSTEMINFORMATION_H

#include <immintrin.h>


namespace HAQP {

template <typename T>
bool arrayIsSimd128Aligned(const T *array) {
    const size_t simdAlignment = sizeof(__m128i);
    return reinterpret_cast<uintptr_t>(array) % simdAlignment == 0;
}

template <typename T>
bool arrayIsSimd256Aligned(const T *array) {
    const size_t simdAlignment = sizeof(__m256i);
    return reinterpret_cast<uintptr_t>(array) % simdAlignment == 0;
}

long l3cacheSize();
long bytesPerCacheLine();
int logicalCoresCount();
std::string getCurrentWorkingDirectory();
void printIntelTlbSpecifications();
int l2TlbEntriesFor4KbytePages();

}


#endif //HAQP_SYSTEMINFORMATION_H
