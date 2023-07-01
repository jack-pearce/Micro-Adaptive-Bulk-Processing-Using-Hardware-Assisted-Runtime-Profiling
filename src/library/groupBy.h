#ifndef MABPL_GROUPBY_H
#define MABPL_GROUPBY_H

#include <string>
#include <vector>
#include <memory>

using Run = std::vector<std::pair<int, int>>;
using Runs = std::vector<std::unique_ptr<Run>>;

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

std::vector<std::pair<int, int>> groupByHash(int n, const int *inputData);

Run groupByHashGoogleDenseHashMap(int n, const int *inputData);
Run groupByHashFollyF14FastMap(int n, const int *inputData);
Run groupByHashAbseilFlatHashMap(int n, const int *inputData);
Run groupByHashTessilRobinMap(int n, const int *inputData);
Run groupByHashTessilHopscotchMap(int n, const int *inputData);

Run groupBySortRadix(int n, int *inputData);
Run groupBySortRadixOpt(int n, int *inputData);

Run groupBySingleRadixPassThenHash(int n, int *inputData);

Run groupByAdaptive(int n, int *inputData);

Run runGroupByFunction(GroupBy groupByImplementation, int n, int *inputData);
std::string getGroupByName(GroupBy groupByImplementation);


#endif //MABPL_GROUPBY_H
