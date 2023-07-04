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

using aggFuncPtr = int (*)(int, int, bool);

extern const aggFuncPtr minAggregation;
extern const aggFuncPtr maxAggregation;
extern const aggFuncPtr sumAggregation;
extern const aggFuncPtr countAggregation;

vectorOfPairs groupByHash(int n, int *inputGroupBy, int *inputAggregate, aggFuncPtr aggregator);

vectorOfPairs groupByHashGoogleDenseHashMap(int n, int *inputGroupBy, int *inputAggregate, aggFuncPtr aggregator);
vectorOfPairs groupByHashFollyF14FastMap(int n, int *inputGroupBy, int *inputAggregate, aggFuncPtr aggregator);
vectorOfPairs groupByHashAbseilFlatHashMap(int n, int *inputGroupBy, int *inputAggregate, aggFuncPtr aggregator);
vectorOfPairs groupByHashTessilRobinMap(int n, int *inputGroupBy, int *inputAggregate, aggFuncPtr aggregator);
vectorOfPairs groupByHashTessilHopscotchMap(int n, int *inputGroupBy, int *inputAggregate, aggFuncPtr aggregator);

vectorOfPairs groupBySortRadix(int n, int *inputGroupBy, int *inputAggregate, aggFuncPtr aggregator);
vectorOfPairs groupBySortRadixOpt(int n, int *inputGroupBy, int *inputAggregate, aggFuncPtr aggregator);

vectorOfPairs groupBySingleRadixPassThenHash(int n, int *inputGroupBy, int *inputAggregate, aggFuncPtr aggregator);
vectorOfPairs groupByDoubleRadixPassThenHash(int n, int *inputGroupBy, int *inputAggregate, aggFuncPtr aggregator);

vectorOfPairs groupByAdaptive(int n, int *inputGroupBy, int *inputAggregate, aggFuncPtr aggregator);

vectorOfPairs runGroupByFunction(GroupBy groupByImplementation, int n, int *inputGroupBy, int *inputAggregate, aggFuncPtr aggregator);
std::string getGroupByName(GroupBy groupByImplementation);


#endif //MABPL_GROUPBY_H
