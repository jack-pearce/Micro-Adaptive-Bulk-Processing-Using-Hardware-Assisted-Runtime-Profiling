#ifndef MABPL_DATASTRUCTURESIMPLEMENTATION_H
#define MABPL_DATASTRUCTURESIMPLEMENTATION_H

#include <cmath>

namespace MABPL {

constexpr int BITS_PER_SORT_RADIX_PASS = 10;

template <typename T1, typename T2>
void sortVectorOfPairsAux(int start, int end, vectorOfPairs<T1, T2> *inputVectorOfPairs, vectorOfPairs<T1, T2> *buffer,
                          int mask, int numBuckets, std::vector<int> &buckets, int pass) {
    int i;

    for (i = start; i < end; i++) {
        buckets[((*inputVectorOfPairs)[i].first >> (pass * BITS_PER_SORT_RADIX_PASS)) & mask]++;
    }

    for (i = 1; i < numBuckets; i++) {
        buckets[i] += buckets[i - 1];
    }

    std::vector<int> partitions(buckets.data(), buckets.data() + numBuckets);
    for (i = 0; i < numBuckets; i++) {
        partitions[i] += start;
    }

    for (i = end - 1; i >= start; i--) {
        (*buffer)[start + --buckets[((*inputVectorOfPairs)[i].first >> (pass * BITS_PER_SORT_RADIX_PASS)) & mask]] =
                std::move((*inputVectorOfPairs)[i]);

    }

    std::fill(buckets.begin(), buckets.end(), 0);
    std::swap(inputVectorOfPairs, buffer);
    --pass;

    if (pass >= 0) {
        if (partitions[0] > start) {
            sortVectorOfPairsAux(start, partitions[0], inputVectorOfPairs, buffer, mask, numBuckets,
                                 buckets, pass);
        }
        for (i = 1; i < numBuckets; i++) {
            if (partitions[i] > partitions[i - 1]) {
                sortVectorOfPairsAux(partitions[i - 1], partitions[i], inputVectorOfPairs, buffer,
                                     mask, numBuckets, buckets, pass);
            }
        }
    }
}

template <typename T1, typename T2>
void sortVectorOfPairs(vectorOfPairs<T1, T2> *&inputVectorOfPairs) {
    size_t i;
    int numBuckets = 1 << BITS_PER_SORT_RADIX_PASS;
    int mask = numBuckets - 1;
    int largest = 0;

    for (i = 0; i < inputVectorOfPairs->size(); i++) {
        if ((*inputVectorOfPairs)[i].first > largest) {
            largest = (*inputVectorOfPairs)[i].first;
        }
    }
    int msbPosition = 0;
    while (largest != 0) {
        largest >>= 1;
        msbPosition++;
    }

    int pass = static_cast<int>(std::ceil(static_cast<double>(msbPosition) / BITS_PER_SORT_RADIX_PASS)) - 1;

    std::vector<int> buckets(1 << BITS_PER_SORT_RADIX_PASS, 0);
    auto *buffer = new vectorOfPairs<T1, T2>(inputVectorOfPairs->size(), std::pair<T1, T2>(0, 0));

    sortVectorOfPairsAux(0, inputVectorOfPairs->size(), inputVectorOfPairs, buffer,
                         mask, numBuckets, buckets, pass);

    if (pass % 2 == 0) {
        std::swap(inputVectorOfPairs, buffer);
    }

    delete buffer;
}

}

#endif //MABPL_DATASTRUCTURESIMPLEMENTATION_H
