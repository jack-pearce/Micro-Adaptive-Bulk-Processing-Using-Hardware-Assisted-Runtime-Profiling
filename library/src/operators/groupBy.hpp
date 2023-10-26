#ifndef HAQP_GROUPBY_HPP
#define HAQP_GROUPBY_HPP

#include <string>
#include <vector>
#include <memory>

#include "utilities/dataStructures.hpp"


namespace HAQP {

enum GroupBy {
    Hash,
    Sort,
    Adaptive,
    AdaptiveParallel
};

std::string getGroupByName(GroupBy groupByImplementation);


template<typename T>
struct MinAggregation {
    T operator()(T currentAggregate, T numberToInclude, bool firstAggregation) const;
};

template<typename T>
struct MaxAggregation {
    T operator()(T currentAggregate, T numberToInclude, bool firstAggregation) const;
};

template<typename T>
struct SumAggregation {
    T operator()(T currentAggregate, T numberToInclude, bool firstAggregation) const;
};

template<typename T>
struct CountAggregation {
    T operator()(T currentAggregate, T _, bool firstAggregation) const;
};

template<template<typename> class Aggregator, typename T1, typename T2>
vectorOfPairs<T1, T2> runGroupByFunction(GroupBy groupByImplementation, int n, T1 *inputGroupBy, T2 *inputAggregate,
                                         int cardinality, int dop = 2);

}

#include "groupByImplementation.hpp"

#endif //HAQP_GROUPBY_HPP
