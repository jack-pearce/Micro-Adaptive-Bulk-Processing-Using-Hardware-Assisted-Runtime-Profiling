#include <iostream>

#include "groupBy.h"


namespace MABPL {

std::string getGroupByName(GroupBy groupByImplementation) {
    switch (groupByImplementation) {
        case GroupBy::Hash:
            return "GroupBy_Hash";
        case GroupBy::SortRadixOpt:
            return "GroupBy_SortRadixOptimal";
        case GroupBy::Adaptive:
            return "GroupBy_Adaptive";
        case GroupBy::AdaptiveSwitchToSortOnly:
            return "GroupBy_Adaptive_SwitchToSortOnly";
        case GroupBy::HashOLD:
            return "GroupBy_HashOLD";
        default:
            std::cout << "Invalid selection of 'GroupBy' implementation!" << std::endl;
            exit(1);
    }
}

}