#ifndef MABPL_RADIXPARTITIONIMPLEMENTATION_H
#define MABPL_RADIXPARTITIONIMPLEMENTATION_H


#include <vector>
#include <cmath>
#include <iostream>

#include "../utilities/systemInformation.h"
#include "../utilities/papi.h"
#include "../machine_constants/machineConstants.h"


namespace MABPL {

template<typename T>
inline void radixPartitionFixedAux(int start, int end, T *keys, T *buffer, std::vector<int> &buckets, int msbPosition,
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

    for (i = start; i < end; i++) {
        buffer[start + buckets[(keys[i] >> shifts) & mask]++] = keys[i];
    }

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
            radixPartitionFixedAux(previous, partitions[i], buffer, keys, buckets, msbPosition,
                                   radixBits, maxElementsPerPartition, !copyRequired);
        } else if (copyRequired) {
            memcpy(keys + previous, buffer + previous, (partitions[i] - previous) * sizeof(T));
        }
        previous = partitions[i];
    }
}

template<typename T>
void radixPartitionFixed(int n, T *keys, int radixBits) {
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

    // 2 for payload, 2.5 for load factor
    int maxElementsPerPartition = static_cast<double>(l3cacheSize()) / (sizeof(T) * 2 * 2.5);

    std::vector<int> buckets(1 + numBuckets, 0);
    T *buffer = new T[n];

    radixPartitionFixedAux(0, n, keys, buffer, buckets, msbPosition, radixBits, maxElementsPerPartition,
                           true);

    delete[]buffer;
}

template<typename T>
inline void radixPartitionAdaptiveAux(int start, int end, T *keys, T *buffer, std::vector<int> &buckets, int msbPosition,
                                   int radixBits, int maxElementsPerPartition, bool copyRequired) {
    radixBits = std::min(msbPosition, radixBits);
    int shifts = msbPosition - radixBits;
    int numBuckets = 1 << radixBits;
    unsigned int mask = numBuckets - 1;

    constexpr int tuplesPerChunk = 50 * 1000;
    float tuplesPerTlbStoreMiss = 75.0;
    std::vector<std::string> counters = {"DTLB-STORE-MISSES"};
    long_long *counterValues = Counters::getInstance().getSharedEventSetEvents(counters);

    int i, chunkStart, tuplesToProcess;
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

    i = start;
    while (i < end) {
        tuplesToProcess = std::min(tuplesPerChunk, end - i);
        chunkStart = i;

        Counters::getInstance().readSharedEventSet();

        for (; i < chunkStart + tuplesToProcess; i++) {
            buffer[start + buckets[(keys[i] >> shifts) & mask]++] = keys[i];
        }

        Counters::getInstance().readSharedEventSet();

/*        if ((static_cast<float>(tuplesToProcess) / static_cast<float>(counterValues[0])) < tuplesPerTlbStoreMiss) {
            // Reduce radix bits by one
            // Merge histogram and reduce size
            // Merge partitions and reduce size

            // if radix bits hits minimum then run another loop to completion and break
        }*/

        if (i >= 10 * 1000 * 1000) {
            for (; i < end; i++) {
                buffer[start + buckets[(keys[i] >> shifts) & mask]++] = keys[i];
            }
            return;
            // break;
        }

    }

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
            radixPartitionAdaptiveAux(previous, partitions[i], buffer, keys, buckets, msbPosition,
                                   radixBits, maxElementsPerPartition, !copyRequired);
        } else if (copyRequired) {
            memcpy(keys + previous, buffer + previous, (partitions[i] - previous) * sizeof(T));
        }
        previous = partitions[i];
    }
}

template<typename T>
void radixPartitionAdaptive(int n, T *keys) {
    static_assert(std::is_integral<T>::value, "Partition column must be an integer type");

    int startingRadixBits = 16; // Negligible gain for higher radix bits than 16

    int numBuckets = 1 << startingRadixBits;
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

    // 2 for payload, 2.5 for load factor
    int maxElementsPerPartition = static_cast<double>(l3cacheSize()) / (sizeof(T) * 2 * 2.5);

    std::vector<int> buckets(1 + numBuckets, 0);
    T *buffer = new T[n];

    radixPartitionAdaptiveAux(0, n, keys, buffer, buckets, msbPosition, startingRadixBits,
                              maxElementsPerPartition, true);

    delete[]buffer;
}

template<typename T>
void runRadixPartitionFunction(RadixPartition radixPartitionImplementation, int n, T *keys, int radixBits) {
    switch (radixPartitionImplementation) {
        case RadixPartition::RadixBitsFixed:
            return radixPartitionFixed(n, keys, radixBits);
        case RadixPartition::RadixBitsAdaptive:
            return radixPartitionAdaptive(n, keys);
        default:
            std::cout << "Invalid selection of 'Radix Partition' implementation!" << std::endl;
            exit(1);
    }
}


}

#endif //MABPL_RADIXPARTITIONIMPLEMENTATION_H
