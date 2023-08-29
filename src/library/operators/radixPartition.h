#ifndef MABPL_RADIXPARTITION_H
#define MABPL_RADIXPARTITION_H

#include <string>
#include <vector>


namespace MABPL {

enum RadixPartition {
    RadixBitsFixed,
    RadixBitsAdaptive
};

std::string getRadixPartitionName(RadixPartition radixPartitionImplementation);

template<typename T>
std::vector<int> radixPartitionFixed(int n, T *keys, int radixBits = 10);

template<typename T>
std::vector<int> radixPartitionAdaptive(int n, T *keys);

template<typename T>
std::vector<int> runRadixPartitionFunction(RadixPartition radixPartitionImplementation, int n, T *keys, int radixBits = 10);

}

#include "radixPartitionImplementation.h"

#endif //MABPL_RADIXPARTITION_H
