#ifndef MABPL_RADIXPARTITIONIMPLEMENTATION_H
#define MABPL_RADIXPARTITIONIMPLEMENTATION_H


#include <vector>
#include <cmath>
#include <iostream>

#include "../utilities/systemInformation.h"


namespace MABPL {

template<typename T>
inline void radixPartitionAux(int start, int end, T *keys, T *buffer, std::vector<int> &buckets, int msbPosition,
                              int radixBits, int maxElementsPerPartition, bool copyRequired) {
    radixBits = std::min(msbPosition, radixBits);
    int shifts = msbPosition - radixBits;
    int numBuckets = 1 << radixBits;
    unsigned int mask = numBuckets - 1;

    int i;
    for (i = start; i < end; i++) {
        buckets[1 + ((keys[i] >> shifts) & mask)]++;
    }

    for (i = 2; i <= numBuckets; i++) {
        buckets[i] += buckets[i - 1];
    }

    std::vector<int> partitions;
    partitions.reserve(numBuckets);
    for (i = 0; i < numBuckets; i++) {
        partitions[i] = start + buckets[1 + i];
    }


    //////////////////////
// ADAPTIVE CHECKS GO IN THIS FOR LOOP - CALL ANOTHER FUNCTION TO PERFORM CHECK AND ANOTHER TO PERFORM MERGE IF REQUIRED
// UPDATE PARTITIONS IN HERE TOO
    for (i = start; i < end; i++) {
        buffer[start + buckets[(keys[i] >> shifts) & mask]++] = keys[i];
    }
    //////////////////////


    std::fill(buckets.begin(), buckets.begin() + numBuckets + 1, 0);
    msbPosition -= radixBits;

    if (msbPosition == 0) {   // No ability to partition further, so return early
        if (copyRequired) {
            memcpy(keys + start, buffer + start, (end - start) * sizeof(T));
        }
        return;
    }

    int previous = start;
    for (i = 0; i < numBuckets; i++) {
        if ((partitions[i] - previous) > maxElementsPerPartition) {
            radixPartitionAux(previous, partitions[i], buffer, keys, buckets, msbPosition,
                              radixBits, maxElementsPerPartition, !copyRequired);
        } else if (copyRequired) {
            memcpy(keys + previous, buffer + previous, (partitions[i] - previous) * sizeof(T));
        }
        previous = partitions[i];
    }
}

template<typename T>
void radixPartition(int n, T *keys, int radixBits) {
    static_assert(std::is_integral<T>::value, "Partition column must be an integer type");

    int numBuckets = 1 << radixBits;
    T largest = 0;

    for (int i = 0; i < n; i++) {
        if (keys[i] > largest) {
            largest = keys[i];
        }
    }

    int msbPosition = 0;
    while (largest != 0) {
        largest >>= 1;
        msbPosition++;
    }

    int maxElementsPerPartition = l3cacheSize() / (sizeof(T) * 2 * 2.5);    // 2 for payload, 2.5 for load factor

    std::vector<int> buckets(1 + numBuckets, 0);
    T *buffer = new T[n];

    radixPartitionAux(0, n, keys, buffer, buckets, msbPosition, radixBits, maxElementsPerPartition,
                      true);

    delete[]buffer;
}


}

#endif //MABPL_RADIXPARTITIONIMPLEMENTATION_H
