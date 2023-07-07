#ifndef MABPL_GROUPBY_H
#define MABPL_GROUPBY_H

#include <string>
#include <vector>
#include <memory>

enum GroupBy {
    Hash,
    Hash_Count,
    SortRadixOpt,
    SortRadixOpt_Count,
    Adaptive
};

std::string getGroupByName(GroupBy groupByImplementation);

template <typename T1, typename T2>
using vectorOfPairs = std::vector<std::pair<T1, T2>>;

template <typename T>
struct MinAggregation {
    T operator()(T currentAggregate, T numberToInclude, bool firstAggregation) const;
};

template <typename T>
struct MaxAggregation {
    T operator()(T currentAggregate, T numberToInclude, bool firstAggregation) const;
};

template <typename T>
struct SumAggregation {
    T operator()(T currentAggregate, T numberToInclude, bool firstAggregation) const;
};

template <typename T>
struct CountAggregation {
    T operator()(T currentAggregate, T _, bool firstAggregation) const;
};


template <template<typename> class Aggregator, typename T1, typename T2>
vectorOfPairs<T1, T2> groupByHash(int n, T1 *inputGroupBy, T2 *inputAggregate);

template <template<typename> class Aggregator, typename T1, typename T2>
vectorOfPairs<T1, T2> groupBySortRadixOpt(int n, T1 *inputGroupBy, T2 *inputAggregate);

template<typename T>
vectorOfPairs<T, int> groupByHash_Count(int n, const T *inputGroupBy);

template<typename T>
vectorOfPairs<T, int> groupBySortRadixOpt_Count(int n, T *inputGroupBy);

template <template<typename> class Aggregator, typename T1, typename T2>
vectorOfPairs<T1, T2> groupByAdaptive(int n, T1 *inputGroupBy, T2 *inputAggregate);

template <template<typename> class Aggregator, typename T1, typename T2>
vectorOfPairs<T1, T2> runGroupByFunction(GroupBy groupByImplementation, int n, T1 *inputGroupBy, T2 *inputAggregate);


#include "groupByImplementation.h"

#endif //MABPL_GROUPBY_H
