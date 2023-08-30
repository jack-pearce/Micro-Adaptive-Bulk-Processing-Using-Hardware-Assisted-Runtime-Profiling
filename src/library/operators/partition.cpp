#include <iostream>

#include "partition.h"

namespace MABPL {


std::string getPartitionName(Partition partitionImplementation) {
    switch (partitionImplementation) {
        case Partition::RadixBitsFixed:
            return "RadixPartition_Static";
        case Partition::RadixBitsAdaptive:
            return "RadixPartition_Adaptive";
        default:
            std::cout << "Invalid selection of 'Partition' implementation!" << std::endl;
            exit(1);
    }
}

}
