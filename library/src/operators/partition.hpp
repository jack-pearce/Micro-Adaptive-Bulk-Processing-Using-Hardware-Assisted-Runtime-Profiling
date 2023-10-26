#ifndef HAQP_PARTITION_HPP
#define HAQP_PARTITION_HPP

#include <string>
#include <vector>


namespace HAQP {

enum PartitionOperators {
    RadixBitsFixed,
    RadixBitsAdaptive
};

std::string getPartitionName(PartitionOperators partitionImplementation);

template<typename T>
std::vector<int> runPartitionFunction(PartitionOperators partitionImplementation, int n, T *keys, int radixBits = 10);

}

#include "partitionImplementation.hpp"

#endif //HAQP_PARTITION_HPP
