#include <iostream>

#include "radixPartition.h"

namespace MABPL {


std::string getGroupByName(RadixPartition radixPartitionImplementation) {
    switch (radixPartitionImplementation) {
        case RadixPartition::RadixBitsFixed:
            return "RadixPartition_Static";
        case RadixPartition::RadixBitsAdaptive:
            return "RadixPartition_Adaptive";
        default:
            std::cout << "Invalid selection of 'Radix Partition' implementation!" << std::endl;
            exit(1);
    }
}

}
