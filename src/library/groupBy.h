#ifndef MABPL_GROUPBY_H
#define MABPL_GROUPBY_H

#include <string>
#include <vector>
#include <memory>

using vectorOfPairs = std::vector<std::pair<int, int>>;

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

vectorOfPairs groupByHash(int n, const int *inputData);

vectorOfPairs groupByHashGoogleDenseHashMap(int n, const int *inputData);
vectorOfPairs groupByHashFollyF14FastMap(int n, const int *inputData);
vectorOfPairs groupByHashAbseilFlatHashMap(int n, const int *inputData);
vectorOfPairs groupByHashTessilRobinMap(int n, const int *inputData);
vectorOfPairs groupByHashTessilHopscotchMap(int n, const int *inputData);

vectorOfPairs groupBySortRadix(int n, int *inputData);
vectorOfPairs groupBySortRadixOpt(int n, int *inputData);

vectorOfPairs groupBySingleRadixPassThenHash(int n, int *inputData);

vectorOfPairs groupByAdaptive(int n, int *inputData);

vectorOfPairs runGroupByFunction(GroupBy groupByImplementation, int n, int *inputData);
std::string getGroupByName(GroupBy groupByImplementation);


#endif //MABPL_GROUPBY_H
