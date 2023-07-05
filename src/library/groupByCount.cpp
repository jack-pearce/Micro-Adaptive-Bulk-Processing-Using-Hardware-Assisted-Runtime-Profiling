#include <iostream>

#include "groupByCount.h"


std::string getGroupByCountName(GroupByCount groupByImplementation) {
    switch(groupByImplementation) {
        case GroupByCount::Count_Hash:
            return "GroupByCount_Hash";
        case GroupByCount::Count_HashGoogleDenseHashMap:
            return "GroupByCount_HashGoogleDenseHashMap";
        case GroupByCount::Count_HashFollyF14FastMap:
            return "GroupByCount_HashFollyF14FastMap";
        case GroupByCount::Count_HashAbseilFlatHashMap:
            return "GroupByCount_HashAbseilFlatHashMap";
        case GroupByCount::Count_HashTessilRobinMap:
            return "GroupByCount_HashTessilRobinMap";
        case GroupByCount::Count_HashTessilHopscotchMap:
            return "GroupByCount_HashTessilHopscotchMap";
        case GroupByCount::Count_SortRadix:
            return "GroupByCount_SortRadix";
        case GroupByCount::Count_SortRadixOpt:
            return "GroupByCount_SortRadixOptimal";
        case GroupByCount::Count_SingleRadixPassThenHash:
            return "GroupByCount_SingleRadixPassThenHash";
        case GroupByCount::Count_DoubleRadixPassThenHash:
            return "GroupByCount_DoubleRadixPassThenHash";
        case GroupByCount::Count_Adaptive:
            return "GroupByCount_Adaptive";
        default:
            std::cout << "Invalid selection of 'GroupByCount' implementation!" << std::endl;
            exit(1);
    }
}