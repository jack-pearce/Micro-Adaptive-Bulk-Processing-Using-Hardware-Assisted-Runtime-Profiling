#ifndef MABPL_SYSTEMINFORMATION_H
#define MABPL_SYSTEMINFORMATION_H


namespace MABPL {

bool arrayIsSimd128Aligned(const int *array);
bool arrayIsSimd256Aligned(const int *array);

long l3cacheSize();
long bytesPerCacheLine();

int maxDop();

}

#endif //MABPL_SYSTEMINFORMATION_H
