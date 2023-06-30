#ifndef MABPL_UTILS_H
#define MABPL_UTILS_H

bool arrayIsSimd128Aligned(const int* array);
bool arrayIsSimd256Aligned(const int* array);

long getL3cacheSize();

#endif //MABPL_UTILS_H