#ifndef MABPL_GROUPBY_H
#define MABPL_GROUPBY_H

#include <string>
#include <vector>
#include <memory>

#include "../utilities/dataStructures.h"


namespace MABPL {

enum GroupBy {
    Hash,
    Sort,
    Adaptive,
    AdaptiveParallel
};

std::string getGroupByName(GroupBy groupByImplementation);


template<typename T>
struct MinAggregation {
    inline T operator()(T currentAggregate, T numberToInclude, bool firstAggregation) const;
};

template<typename T>
struct MaxAggregation {
    inline T operator()(T currentAggregate, T numberToInclude, bool firstAggregation) const;
};

template<typename T>
struct SumAggregation {
    inline T operator()(T currentAggregate, T numberToInclude, bool firstAggregation) const;
};

template<typename T>
struct CountAggregation {
    inline T operator()(T currentAggregate, T _, bool firstAggregation) const;
};


template<template<typename> class Aggregator, typename T1, typename T2>
vectorOfPairs<T1, T2>  groupByHash(int n, T1 *inputGroupBy, T2 *inputAggregate, int cardinality);

template<template<typename> class Aggregator, typename T1, typename T2>
vectorOfPairs<T1, T2> groupBySort(int n, T1 *inputGroupBy, T2 *inputAggregate);

template<template<typename> class Aggregator, typename T1, typename T2>
vectorOfPairs<T1, T2> groupByAdaptive(int n, T1 *inputGroupBy, T2 *inputAggregate, int cardinality);

template<template<typename> class Aggregator, typename T1, typename T2>
vectorOfPairs<T1, T2> groupByAdaptiveParallel(int n, T1 *inputGroupBy, T2 *inputAggregate, int cardinality, int dop);

template<template<typename> class Aggregator, typename T1, typename T2>
vectorOfPairs<T1, T2> runGroupByFunction(GroupBy groupByImplementation, int n, T1 *inputGroupBy, T2 *inputAggregate,
                                         int cardinality, int dop = 2);

}

#include "groupByImplementation.h"

#endif //MABPL_GROUPBY_H
