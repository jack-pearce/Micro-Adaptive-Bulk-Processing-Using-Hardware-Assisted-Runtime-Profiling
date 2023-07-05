#ifndef MABPL_GROUPBYCOUNT_H
#define MABPL_GROUPBYCOUNT_H

#include <string>
#include <vector>
#include <memory>

using vectorOfPairs = std::vector<std::pair<int, int>>;

enum GroupByCount {
    Count_Hash,
    Count_HashGoogleDenseHashMap,
    Count_HashFollyF14FastMap,
    Count_HashAbseilFlatHashMap,
    Count_HashTessilRobinMap,
    Count_HashTessilHopscotchMap,
    Count_SortRadix,
    Count_SortRadixOpt,
    Count_SingleRadixPassThenHash,
    Count_DoubleRadixPassThenHash,
    Count_Adaptive
};

std::string getGroupByCountName(GroupByCount groupByImplementation);

template<typename T>
vectorOfPairs groupByCountHash(int n, const T *inputData);

template<typename T>
vectorOfPairs groupByCountHashGoogleDenseHashMap(int n, const T *inputData);
template<typename T>
vectorOfPairs groupByCountHashFollyF14FastMap(int n, const T *inputData);
template<typename T>
vectorOfPairs groupByCountHashAbseilFlatHashMap(int n, const T *inputData);
template<typename T>
vectorOfPairs groupByCountHashTessilRobinMap(int n, const T *inputData);
template<typename T>
vectorOfPairs groupByCountHashTessilHopscotchMap(int n, const T *inputData);

template<typename T>
vectorOfPairs groupByCountSortRadix(int n, T *inputData);
template<typename T>
vectorOfPairs groupByCountSortRadixOpt(int n, T *inputData);

template<typename T>
vectorOfPairs groupByCountSingleRadixPassThenHash(int n, T *inputData);
template<typename T>
vectorOfPairs groupByDoubleRadixPassThenHash(int n, T *inputData);

template<typename T>
vectorOfPairs groupByCountAdaptive(int n, T *inputData);

template<typename T>
vectorOfPairs runGroupByCountFunction(GroupByCount groupByImplementation, int n, T *inputData);


#include "groupByCountImplementation.h"

#endif //MABPL_GROUPBYCOUNT_H
