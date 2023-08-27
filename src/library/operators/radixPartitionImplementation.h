#ifndef MABPL_RADIXPARTITIONIMPLEMENTATION_H
#define MABPL_RADIXPARTITIONIMPLEMENTATION_H


#include <vector>
#include <cmath>
#include <iostream>

namespace MABPL {

template<typename T1, typename T2>
inline void radixPartitionAux(int start, int end, T1 *keys, T2 *payloads, T1 *bufferKeys, T2 *bufferPayloads,
                       int mask, int numBuckets, std::vector<int> &buckets, int msbPosition, int &radixBits,
                       bool copyRequired) {
    int i;
    int shifts = msbPosition - radixBits;

    for (i = start; i < end; i++) {
        buckets[1 + ((keys[i] >> shifts) & mask)]++;
    }

    for (i = 2; i <= numBuckets; i++) {
        buckets[i] += buckets[i - 1];
    }

//    std::vector<int> partitions(buckets.data() + 1, buckets.data() + numBuckets + 1);
//    for (i = 0; i < numBuckets; i++) {
//        partitions[i] += start;
//    }

    // ADAPTIVE CHECKS GO IN THIS FOR LOOP - CALL ANOTHER FUNCTION TO PERFORM CHECK AND ANOTHER TO PERFORM MERGE IF REQUIRED
    for (i = start; i < end; i++) {
        bufferKeys[start + buckets[(keys[i] >> shifts) & mask]++] = keys[i];
//        bufferPayloads[start + buckets[(keys[i] >> shifts) & mask]++] = payloads[i];
    }

//    std::fill(buckets.begin(), buckets.end(), 0);
//    std::swap(keys, bufferKeys);
//    std::swap(payloads, bufferPayloads);
//    msbPosition -= radixBits;
//
//    Checks on partition size below
//
//    if (partitions[0] > start) {
//        radixPartitionAux(start, partitions[0], keys, payloads, bufferKeys, bufferPayloads, mask, numBuckets,
//                          buckets, pass, radixBits);
//    }
//    for (i = 1; i < numBuckets; i++) {
//        if (partitions[i] > partitions[i - 1]) {
//            radixPartitionAux(partitions[i - 1], partitions[i], keys, payloads, bufferKeys, bufferPayloads,
//                              mask, numBuckets, buckets, pass, radixBits);
//        }
//    }

    // UNCOMMENT AT THE END
/*    if (copyRequired) {
        memcpy(keys + start, bufferKeys + start, (end - start) * sizeof(T1));
        memcpy(payloads + start, bufferPayloads + start, (end - start) * sizeof(T2));
    }*/
}

template<typename T1, typename T2>
void radixPartition(int n, T1 *keys, T2 *payloads, int radixBits) {
    static_assert(std::is_integral<T1>::value, "Partition column must be an integer type");

    int i;
    int numBuckets = 1 << radixBits;
    int mask = numBuckets - 1;
    int largest = 0;

    for (i = 0; i < n; i++) {
        if (keys[i] > largest) {
            largest = keys[i];
        }
    }

    int msbPosition = 0;
    while (largest != 0) {
        largest >>= 1;
        msbPosition++;
    }

    std::vector<int> buckets(1 + numBuckets, 0);
    T1 *bufferKeys = new T1[n];
//    T2 *bufferPayloads = new T2[n];

    radixPartitionAux(0, n, keys, payloads, bufferKeys, bufferKeys, mask, numBuckets, buckets,
                      msbPosition, radixBits, true);

    delete[]bufferKeys;
//    delete[]bufferPayloads;
}


/*
template<typename T1, typename T2>
void radixPartitionAux(int start, int end, T1 *keys, T2 *payloads, T1 *bufferKeys, T2 *bufferPayloads,
                       int mask, int numBuckets, std::vector<int> &buckets, int pass, int &radixBits) {
    int i;

    for (i = start; i < end; i++) {
        buckets[1 + ((keys[i] >> (pass * radixBits)) & mask)]++;
    }

    for (i = 2; i <= numBuckets; i++) {
        buckets[i] += buckets[i - 1];
    }

    std::vector<int> partitions(buckets.data() + 1, buckets.data() + numBuckets + 1);
    for (i = 0; i < numBuckets; i++) {
        partitions[i] += start;
    }

    for (i = start; i < end; i++) {
        bufferKeys[start + buckets[(keys[i] >> (pass * radixBits)) & mask]] = keys[i];
        bufferPayloads[start + buckets[(keys[i] >> (pass * radixBits)) & mask]++] = payloads[i];
    }

    std::fill(buckets.begin(), buckets.end(), 0);
    std::swap(keys, bufferKeys);
    std::swap(payloads, bufferPayloads);
    --pass;

    if (pass < 0) {
        return;
    }

    if (partitions[0] > start) {
        radixPartitionAux(start, partitions[0], keys, payloads, bufferKeys, bufferPayloads, mask, numBuckets,
                          buckets, pass, radixBits);
    }
    for (i = 1; i < numBuckets; i++) {
        if (partitions[i] > partitions[i - 1]) {
            radixPartitionAux(partitions[i - 1], partitions[i], keys, payloads, bufferKeys, bufferPayloads,
                              mask, numBuckets, buckets, pass, radixBits);
        }
    }
}

template<typename T1, typename T2>
void radixPartition(int n, T1 *keys, T2 *payloads, int radixBits) {
    static_assert(std::is_integral<T1>::value, "Partition column must be an integer type");

    int i;
    int numBuckets = 1 << radixBits;
    int mask = numBuckets - 1;
    int largest = 0;

    for (i = 0; i < n; i++) {
        if (keys[i] > largest) {
            largest = keys[i];
        }
    }

    int msbPosition = 0;
    while (largest != 0) {
        largest >>= 1;
        msbPosition++;
    }

    int pass = static_cast<int>(std::ceil(static_cast<double>(msbPosition) / radixBits)) - 1;
//    int pass = 0;

    std::vector<int> buckets(1 + numBuckets, 0);
    T1 *bufferKeys = new T1[n];
    T2 *bufferPayloads = new T2[n];

    radixPartitionAux(0, n, keys, payloads, bufferKeys, bufferPayloads, mask, numBuckets, buckets,
                      pass, radixBits);

    if ((pass % 2) == 0) {
        memcpy(keys, bufferKeys, n * sizeof(T1));
        memcpy(payloads, bufferPayloads, n * sizeof(T2));
    }


    delete[]bufferKeys;
    delete[]bufferPayloads;
}
*/


}

#endif //MABPL_RADIXPARTITIONIMPLEMENTATION_H
