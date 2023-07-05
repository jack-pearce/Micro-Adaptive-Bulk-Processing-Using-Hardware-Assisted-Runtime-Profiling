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

vectorOfPairs groupByCountHash(int n, const int *inputData);

vectorOfPairs groupByCountHashGoogleDenseHashMap(int n, const int *inputData);
vectorOfPairs groupByCountHashFollyF14FastMap(int n, const int *inputData);
vectorOfPairs groupByCountHashAbseilFlatHashMap(int n, const int *inputData);
vectorOfPairs groupByCountHashTessilRobinMap(int n, const int *inputData);
vectorOfPairs groupByCountHashTessilHopscotchMap(int n, const int *inputData);

vectorOfPairs groupByCountSortRadix(int n, int *inputData);
vectorOfPairs groupByCountSortRadixOpt(int n, int *inputData);

vectorOfPairs groupByCountSingleRadixPassThenHash(int n, int *inputData);

vectorOfPairs groupByCountAdaptive(int n, int *inputData);

vectorOfPairs runGroupByCountFunction(GroupByCount groupByImplementation, int n, int *inputData);
std::string getGroupByCountName(GroupByCount groupByImplementation);


#endif //MABPL_GROUPBYCOUNT_H
