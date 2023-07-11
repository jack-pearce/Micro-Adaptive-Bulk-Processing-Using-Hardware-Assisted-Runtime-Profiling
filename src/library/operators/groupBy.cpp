#include <iostream>

#include "groupBy.h"


namespace MABPL {

std::string getGroupByName(GroupBy groupByImplementation) {
    switch (groupByImplementation) {
        case GroupBy::Hash:
            return "GroupBy_Hash";
        case GroupBy::Sort:
            return "GroupBy_Sort";
        case GroupBy::Adaptive:
            return "GroupBy_Adaptive";
        default:
            std::cout << "Invalid selection of 'GroupBy' implementation!" << std::endl;
            exit(1);
    }
}

}