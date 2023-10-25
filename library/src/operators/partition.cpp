#include <iostream>

#include "partition.h"

namespace HAQP {


std::string getPartitionName(PartitionOperators partitionImplementation) {
    switch (partitionImplementation) {
        case PartitionOperators::RadixBitsFixed:
            return "RadixPartition_Static";
        case PartitionOperators::RadixBitsAdaptive:
            return "RadixPartition_Adaptive";
        default:
            std::cout << "Invalid selection of 'Partition' implementation!" << std::endl;
            exit(1);
    }
}

}
