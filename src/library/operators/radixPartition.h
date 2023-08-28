#ifndef MABPL_RADIXPARTITION_H
#define MABPL_RADIXPARTITION_H

#include <string>


namespace MABPL {

enum RadixPartition {
    RadixBitsFixed,
    RadixBitsAdaptive
};

std::string getGroupByName(RadixPartition radixPartitionImplementation);

template<typename T>
void radixPartitionFixed(int n, T *keys, int radixBits = 10);

template<typename T>
void radixPartitionAdaptive(int n, T *keys);

template<typename T>
void runGroupByFunction(RadixPartition radixPartitionImplementation, int n, T *keys, int radixBits = 10);

}

#include "radixPartitionImplementation.h"

#endif //MABPL_RADIXPARTITION_H
