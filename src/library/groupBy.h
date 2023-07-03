#ifndef MABPL_GROUPBY_H
#define MABPL_GROUPBY_H

#include <string>
#include <vector>
#include <memory>

using vectorPair = std::pair<std::vector<int>, std::vector<int>>;

enum GroupBy {
    Hash,
    HashGoogleDenseHashMap,
    HashFollyF14FastMap,
    HashAbseilFlatHashMap,
    HashTessilRobinMap,
    HashTessilHopscotchMap,
    SortRadix,
    SortRadixOpt,
    SingleRadixPassThenHash,
    DoubleRadixPassThenHash,
    Adaptive
};

vectorPair groupByHash(int n, const int *inputData);

vectorPair groupByHashGoogleDenseHashMap(int n, const int *inputData);
vectorPair groupByHashFollyF14FastMap(int n, const int *inputData);
vectorPair groupByHashAbseilFlatHashMap(int n, const int *inputData);
vectorPair groupByHashTessilRobinMap(int n, const int *inputData);
vectorPair groupByHashTessilHopscotchMap(int n, const int *inputData);

vectorPair groupBySortRadix(int n, int *inputData);
vectorPair groupBySortRadixOpt(int n, int *inputData);

vectorPair groupBySingleRadixPassThenHash(int n, int *inputData);

vectorPair groupByAdaptive(int n, int *inputData);

vectorPair runGroupByFunction(GroupBy groupByImplementation, int n, int *inputData);
std::string getGroupByName(GroupBy groupByImplementation);


#endif //MABPL_GROUPBY_H
