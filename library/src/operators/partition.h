#ifndef HAQP_PARTITION_H
#define HAQP_PARTITION_H

#include <string>
#include <vector>


namespace HAQP {

enum Partition {
    RadixBitsFixed,
    RadixBitsAdaptive
};

std::string getPartitionName(Partition partitionImplementation);

template<typename T>
std::vector<int> partitionFixed(int n, T *keys, int radixBits = 10);

template<typename T>
std::vector<int> partitionAdaptive(int n, T *keys);

template<typename T>
std::vector<int> runPartitionFunction(Partition partitionImplementation, int n, T *keys, int radixBits = 10);

}

#include "partitionImplementation.h"

#endif //HAQP_PARTITION_H
