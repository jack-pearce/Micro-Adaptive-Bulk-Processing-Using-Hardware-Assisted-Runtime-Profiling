#include <iostream>

#include "groupBy.h"


namespace MABPL {

std::string getGroupByName(GroupBy groupByImplementation) {
    switch (groupByImplementation) {
        case GroupBy::Hash:
            return "GroupBy_Hash";
        case GroupBy::SortRadixOpt:
            return "GroupBy_SortRadixOptimal";
        case GroupBy::Hash_Count:
            return "GroupBy_Hash_Count";
        case GroupBy::SortRadixOpt_Count:
            return "GroupBy_SortRadixOptimal_Count";
        case GroupBy::Adaptive:
            return "GroupBy_Adaptive";
        case GroupBy::Adaptive2:
            return "GroupBy_Adaptive_NEW";
        default:
            std::cout << "Invalid selection of 'GroupBy' implementation!" << std::endl;
            exit(1);
    }
}

}