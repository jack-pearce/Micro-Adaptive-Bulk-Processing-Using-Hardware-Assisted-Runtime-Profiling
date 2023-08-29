#ifndef MABPL_RADIXPARTITIONIMPLEMENTATION_H
#define MABPL_RADIXPARTITIONIMPLEMENTATION_H


#include <vector>
#include <cmath>
#include <iostream>
#include <limits>

#include "../utilities/systemInformation.h"
#include "../utilities/papi.h"
#include "../machine_constants/machineConstants.h"


namespace MABPL {

template<typename T>
inline void radixPartitionFixedAux(int n, T *keys, T *buffer, std::vector<int> &buckets, int msbToPartition,
                                   int radixBits, int maxElementsPerPartition, std::vector<int> &outputPartitions,
                                   int offset, bool copyRequired) {
    radixBits = std::min(msbToPartition, radixBits);
    int shifts = msbToPartition - radixBits;
    int numBuckets = 1 << radixBits;
    unsigned int mask = numBuckets - 1;

    int i;
    for (i = 0; i < n; i++) {
        buckets[1 + ((keys[i] >> shifts) & mask)]++;
    }

    for (i = 2; i <= numBuckets; i++) {
        buckets[i] += buckets[i - 1];
    }

    std::vector<int> partitions(buckets.data() + 1, buckets.data() + numBuckets + 1);

    for (i = 0; i < n; i++) {
        buffer[buckets[(keys[i] >> shifts) & mask]++] = keys[i];
    }

    std::fill(buckets.begin(), buckets.end(), 0);
    msbToPartition -= radixBits;

    if (msbToPartition == 0) {   // No ability to partition further, so return early
        if (copyRequired) {
            memcpy(keys, buffer, n * sizeof(T));
        }
        outputPartitions.push_back(offset + n);
        return;
    }

    int prevPartitionEnd = 0;
    for (int partitionEnd : partitions) {
        if ((partitionEnd - prevPartitionEnd) > maxElementsPerPartition) {
            radixPartitionFixedAux(partitionEnd - prevPartitionEnd, buffer + prevPartitionEnd,
                                   keys + prevPartitionEnd, buckets, msbToPartition, radixBits,
                                   maxElementsPerPartition, outputPartitions, offset + prevPartitionEnd,
                                   !copyRequired);
        } else {
            if (copyRequired) {
                memcpy(keys + prevPartitionEnd, buffer + prevPartitionEnd,
                       (partitionEnd- prevPartitionEnd) * sizeof(T));
            }
            outputPartitions.push_back(offset + partitionEnd);
        }
        prevPartitionEnd = partitionEnd;
    }
}

template<typename T>
std::vector<int> radixPartitionFixed(int n, T *keys, int radixBits) {
    static_assert(std::is_integral<T>::value, "Partition column must be an integer type");

    int numBuckets = 1 << radixBits;
    T largest = std::numeric_limits<T>::min();;

    for (int i = 0; i < n; i++) {
        largest = std::max(largest, keys[i]);
    }

    int msbToPartition = 0;
    while (largest != 0) {
        largest >>= 1;
        msbToPartition++;
    }

    // 2 for payload, 2.5 for load factor
    int maxElementsPerPartition = static_cast<double>(l3cacheSize()) / (sizeof(T) * 2 * 2.5);

    std::vector<int> outputPartitions;
    std::vector<int> buckets(1 + numBuckets, 0);
    T *buffer = new T[n];

    radixPartitionFixedAux(n, keys, buffer, buckets, msbToPartition, radixBits, maxElementsPerPartition,
                           outputPartitions, 0, true);

    delete[]buffer;
    return outputPartitions;
}

template<typename T>
inline void radixPartitionAdaptiveMergePartitions(T *buffer, std::vector<int> &buckets, std::vector<int> &partitions,
                                                  int numBuckets) {
    for (int j = 0; j < numBuckets; ++j) {      // Move values in buffer
        memcpy(&buffer[buckets[j << 1]],
               &buffer[partitions[j << 1]],
               (buckets[(j << 1) + 1] - partitions[j << 1]) * sizeof(T));
    }

    for (int j = 0; j < numBuckets; ++j) {      // Merge histogram values and reduce size
        buckets[j] = buckets[j << 1] + (buckets[(j << 1) + 1] - partitions[j << 1]);
    }
    buckets.resize(1 + numBuckets);

    for (int j = 1; j <= numBuckets; ++j) {     // Merge partitions and reduce size
        partitions[j - 1] = partitions[(j << 1) - 1];
    }
    partitions.resize(numBuckets);
}

template<typename T>
inline void radixPartitionAdaptiveAux(int n, T *keys, T *buffer, std::vector<int> &buckets,
                                      int msbToPartition, int &radixBits, int minimumRadixBits,
                                      int maxElementsPerPartition, std::vector<int> &outputPartitions, int offset,
                                      bool copyRequired) {
    radixBits = std::min(msbToPartition, radixBits);
    int shifts = msbToPartition - radixBits;
    int numBuckets = 1 << radixBits;
    unsigned int mask = numBuckets - 1;

    constexpr int tuplesPerChunk = 50 * 1000;
    float tuplesPerTlbStoreMiss = 100.0; //////////////////////// NEED TO AUTOMATE //////////////////////// - SHOULD BE 100?
    std::vector<std::string> counters = {"DTLB-STORE-MISSES"};
    long_long *counterValues = Counters::getInstance().getSharedEventSetEvents(counters);

    int i, chunkStart, tuplesToProcess;
    for (i = 0; i < n; i++) {
        buckets[1 + ((keys[i] >> shifts) & mask)]++;
    }

    for (i = 2; i <= numBuckets; i++) {
        buckets[i] += buckets[i - 1];
    }

    std::vector<int> partitions(buckets.data() + 1, buckets.data() + numBuckets + 1);

    i = 0;
    if (radixBits > minimumRadixBits) {
        while (i < n) {
            tuplesToProcess = std::min(tuplesPerChunk, n - i);
            chunkStart = i;

            Counters::getInstance().readSharedEventSet();

            for (; i < chunkStart + tuplesToProcess; i++) {     // Run chunk
                buffer[buckets[(keys[i] >> shifts) & mask]++] = keys[i];
            }

            Counters::getInstance().readSharedEventSet();

            if ((static_cast<float>(tuplesToProcess) / static_cast<float>(counterValues[0])) < tuplesPerTlbStoreMiss) {
                --radixBits;
                ++shifts;
                numBuckets >>= 1;
                mask = numBuckets - 1;

                /////////////////////////////////////// ADAPTIVITY OUTPUT ///////////////////////////////////////////
//                std::cout << "RadixBits reduced to " << radixBits << " after tuple " << i << " due to reading of ";
//                std::cout << (static_cast<float>(tuplesToProcess) / static_cast<float>(counterValues[0]));
//                std::cout << " tuples per TLB store miss" << std::endl;
                /////////////////////////////////////////////////////////////////////////////////////////////////////

                radixPartitionAdaptiveMergePartitions(buffer, buckets, partitions, numBuckets);

                if (radixBits == minimumRadixBits) {        // Complete partitioning to avoid unnecessary checks
                    for (; i < n; i++) {
                        buffer[buckets[(keys[i] >> shifts) & mask]++] = keys[i];
                    }
                    break;
                }
            }
        }
    } else {
        for (; i < n; i++) {
            buffer[buckets[(keys[i] >> shifts) & mask]++] = keys[i];
        }
    }

    std::fill(buckets.begin(), buckets.end(), 0);
    msbToPartition -= radixBits;

    if (msbToPartition == 0) {                                 // No ability to partition further, so return early
        if (copyRequired) {
            memcpy(keys, buffer, n * sizeof(T));
        }
        outputPartitions.push_back(offset + n);
        return;
    }

    int prevPartitionEnd = 0;
    for (int partitionEnd : partitions) {
        if ((partitionEnd - prevPartitionEnd) > maxElementsPerPartition) {
            radixPartitionAdaptiveAux(partitionEnd - prevPartitionEnd, buffer + prevPartitionEnd,
                                      keys + prevPartitionEnd, buckets, msbToPartition, radixBits,
                                      minimumRadixBits, maxElementsPerPartition, outputPartitions,
                                      offset + prevPartitionEnd, !copyRequired);
        } else {
            if (copyRequired) {
                memcpy(keys + prevPartitionEnd, buffer + prevPartitionEnd,
                       (partitionEnd - prevPartitionEnd) * sizeof(T));
            }
            outputPartitions.push_back(offset + partitionEnd);
        }
        prevPartitionEnd = partitionEnd;
    }
}

template<typename T>
std::vector<int> radixPartitionAdaptive(int n, T *keys) {
    static_assert(std::is_integral<T>::value, "Partition column must be an integer type");

    int radixBits = 16;         // Negligible gain for higher radix bits than 16
    int minimumRadixBits = 7;   // L2 TLB entries divided by 4 //////////////////////// NEED TO AUTOMATE ////////////////////////

    int numBuckets = 1 << radixBits;
    T largest = std::numeric_limits<T>::min();;

    for (int i = 0; i < n; i++) {
        largest = std::max(largest, keys[i]);
    }

    int msbToPartition = 0;
    while (largest != 0) {
        largest >>= 1;
        ++msbToPartition;
    }

    // 2 for payload, 2.5 for load factor
    int maxElementsPerPartition = static_cast<double>(l3cacheSize()) / (sizeof(T) * 2 * 2.5);

    std::vector<int> outputPartitions;
    std::vector<int> buckets(1 + numBuckets, 0);
    T *buffer = new T[n];

    radixPartitionAdaptiveAux(n, keys, buffer, buckets, msbToPartition, radixBits, minimumRadixBits,
                              maxElementsPerPartition, outputPartitions, 0, true);

    delete[]buffer;
    return outputPartitions;
}

template<typename T>
std::vector<int> runRadixPartitionFunction(RadixPartition radixPartitionImplementation, int n, T *keys, int radixBits) {
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
