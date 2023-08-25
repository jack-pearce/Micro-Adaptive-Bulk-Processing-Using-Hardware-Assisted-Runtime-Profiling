#ifndef MABPL_RADIXPARTITIONIMPLEMENTATION_H
#define MABPL_RADIXPARTITIONIMPLEMENTATION_H


#include <vector>
#include <cmath>
#include <iostream>

namespace MABPL {

template<typename T1, typename T2>
inline void radixPartitionAux(int start, int end, T1 *keys, T2 *payloads, T1 *bufferKeys, T2 *bufferPayloads,
                       int mask, int numBuckets, std::vector<int> &buckets, int pass, int &radixBits) {
    int i;

    for (i = start; i < end; i++) {
        buckets[(keys[i] >> (pass * radixBits)) & mask]++;
    }

    for (i = 1; i < numBuckets; i++) {
        buckets[i] += buckets[i - 1];
    }

//    std::vector<int> partitions(buckets.data(), buckets.data() + numBuckets);
//    for (i = 0; i < numBuckets; i++) {
//        partitions[i] += start;
//    }

    for (i = end - 1; i >= start; i--) {
        bufferKeys[start + --buckets[(keys[i] >> (pass * radixBits)) & mask]] = keys[i];
//        bufferPayloads[start + buckets[(keys[i] >> (pass * radixBits)) & mask]] = payloads[i];
    }

//    std::fill(buckets.begin(), buckets.end(), 0);
//    std::swap(keys, bufferKeys);
//    std::swap(payloads, bufferPayloads);
//    --pass;
//
//    if (pass < 0) {
//        return;
//    }
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
}

template<typename T1, typename T2>
void radixPartition(int n, T1 *keys, T2 *payloads, int radixBits) {
    static_assert(std::is_integral<T1>::value, "Partition column must be an integer type");

//    int i;
    int numBuckets = 1 << radixBits;
    int mask = numBuckets - 1;
//    int largest = 0;

/*    for (i = 0; i < n; i++) {
        if (keys[i] > largest) {
            largest = keys[i];
        }
    }*/

/*    int msbPosition = 0;
    while (largest != 0) {
        largest >>= 1;
        msbPosition++;
    }*/

//    int pass = static_cast<int>(std::ceil(static_cast<double>(msbPosition) / radixBits)) - 1;
    int pass = 0;

    std::vector<int> buckets(1 << radixBits, 0);
    T1 *bufferKeys = new T1[n];
//    T2 *bufferPayloads = new T2[n];

    radixPartitionAux(0, n, keys, payloads, bufferKeys, bufferKeys, mask, numBuckets, buckets,
                      pass, radixBits);

/*    if ((pass % 2) == 0) {
    memcpy(keys, bufferKeys, n * sizeof(T1));
    memcpy(payloads, bufferPayloads, n * sizeof(T2));
}*/

    delete[]bufferKeys;
//    delete[]bufferPayloads;
}


/*
template<typename T1, typename T2>
void radixPartitionAux(int start, int end, T1 *keys, T2 *payloads, T1 *bufferKeys, T2 *bufferPayloads,
                       int mask, int numBuckets, std::vector<int> &buckets, int pass, int &radixBits) {
    int i;

    for (i = start; i < end; i++) {
        buckets[(keys[i] >> (pass * radixBits)) & mask]++;
    }

    for (i = 1; i < numBuckets; i++) {
        buckets[i] += buckets[i - 1];
    }

    std::vector<int> partitions(buckets.data(), buckets.data() + numBuckets);
    for (i = 0; i < numBuckets; i++) {
        partitions[i] += start;
    }

    for (i = end - 1; i >= start; i--) {
        bufferKeys[start + --buckets[(keys[i] >> (pass * radixBits)) & mask]] = keys[i];
        bufferPayloads[start + buckets[(keys[i] >> (pass * radixBits)) & mask]] = payloads[i];
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

//    int pass = static_cast<int>(std::ceil(static_cast<double>(msbPosition) / radixBits)) - 1;
    int pass = 0;

    std::vector<int> buckets(1 << radixBits, 0);
    T1 *bufferKeys = new T1[n];
    T2 *bufferPayloads = new T2[n];

    radixPartitionAux(0, n, keys, payloads, bufferKeys, bufferPayloads, mask, numBuckets, buckets,
                      pass, radixBits);

*/
/*    if ((pass % 2) == 0) {
        memcpy(keys, bufferKeys, n * sizeof(T1));
        memcpy(payloads, bufferPayloads, n * sizeof(T2));
    }*//*


    delete[]bufferKeys;
    delete[]bufferPayloads;
}
*/


}

#endif //MABPL_RADIXPARTITIONIMPLEMENTATION_H
