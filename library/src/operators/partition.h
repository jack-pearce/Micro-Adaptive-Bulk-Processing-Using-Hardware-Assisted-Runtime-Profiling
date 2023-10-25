#ifndef HAQP_PARTITION_H
#define HAQP_PARTITION_H

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

#include "partitionImplementation.h"

#endif //HAQP_PARTITION_H
