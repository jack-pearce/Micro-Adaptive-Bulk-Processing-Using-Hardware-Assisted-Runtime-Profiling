#ifndef MABPL_GROUPBYIMPLEMENTATION_H
#define MABPL_GROUPBYIMPLEMENTATION_H


#include <iostream>
#include <limits>
#include "tsl/robin_map.h"

#include "../utilities/systemInformation.h"
#include "../utilities/papi.h"


namespace MABPL {

constexpr int BITS_PER_RADIX_PASS = 10;
constexpr float GROUPBY_MACHINE_CONSTANT = 0.125;

template<typename T>
T MinAggregation<T>::operator()(T currentAggregate, T numberToInclude, bool firstAggregation) const {
    if (firstAggregation) {
        return numberToInclude;
    }
    return std::min(currentAggregate, numberToInclude);
}

template<typename T>
T MaxAggregation<T>::operator()(T currentAggregate, T numberToInclude, bool firstAggregation) const {
    if (firstAggregation) {
        return numberToInclude;
    }
    return std::max(currentAggregate, numberToInclude);
}

template<typename T>
T SumAggregation<T>::operator()(T currentAggregate, T numberToInclude, bool firstAggregation) const {
    if (firstAggregation) {
        return numberToInclude;
    }
    return currentAggregate + numberToInclude;
}

template<typename T>
T CountAggregation<T>::operator()(T currentAggregate, T _, bool firstAggregation) const {
    if (firstAggregation) {
        return 1;
    }
    return ++currentAggregate;
}

template<template<typename> class Aggregator, typename T1, typename T2>
vectorOfPairs<T1, T2> groupByHash(int n, T1 *inputGroupBy, T2 *inputAggregate, int cardinality) {
    static_assert(std::is_integral<T1>::value, "GroupBy column must be an integer type");
    static_assert(std::is_arithmetic<T2>::value, "Payload column must be an numeric type");

    tsl::robin_map<T1, T2> map(std::max(static_cast<int>(2.5 * cardinality), 400000));

    typename tsl::robin_map<T1, T2>::iterator it;
    for (auto i = 0; i < n; ++i) {
        it = map.find(inputGroupBy[i]);
        if (it != map.end()) {
            it.value() = Aggregator<T2>()(it->second, inputAggregate[i], false);
        } else {
            map.insert({inputGroupBy[i], Aggregator<T2>()(0, inputAggregate[i], true)});
        }
    }

    return {map.begin(), map.end()};
}

template<template<typename> class Aggregator, typename T1, typename T2>
void groupBySortRadixOptAuxAgg(int start, int end, const T1 *inputGroupBy, T2 *inputAggregate, int mask, int numBuckets,
                               std::vector<int> &buckets, vectorOfPairs <T1, T2> &result) {
    int i;
    bool bucketEntryPresent[1 << BITS_PER_RADIX_PASS] = {false};

    for (i = start; i < end; i++) {
        buckets[inputGroupBy[i] & mask] = Aggregator<T2>()(buckets[inputGroupBy[i] & mask], inputAggregate[i],
                                                           !bucketEntryPresent[inputGroupBy[i] & mask]);
        bucketEntryPresent[inputGroupBy[i] & mask] = true;
    }

    int valuePrefix = inputGroupBy[start] & ~mask;

    for (i = 0; i < numBuckets; i++) {
        if (bucketEntryPresent[i]) {
            result.emplace_back(valuePrefix | i, buckets[i]);
        }
    }

    std::fill(buckets.begin(), buckets.end(), 0);
}

template<template<typename> class Aggregator, typename T1, typename T2>
void groupBySortRadixOptAux(int start, int end, T1 *inputGroupBy, T2 *inputAggregate, T1 *bufferGroupBy, T2 *bufferAggregate,
                       int mask, int numBuckets, std::vector<int> &buckets, int pass, vectorOfPairs <T1, T2> &result) {
    int i;

    for (i = start; i < end; i++) {
        buckets[(inputGroupBy[i] >> (pass * BITS_PER_RADIX_PASS)) & mask]++;
    }

    for (i = 1; i < numBuckets; i++) {
        buckets[i] += buckets[i - 1];
    }

    std::vector<int> partitions(buckets.data(), buckets.data() + numBuckets);
    for (i = 0; i < numBuckets; i++) {
        partitions[i] += start;
    }

    for (i = end - 1; i >= start; i--) {
        bufferGroupBy[start + --buckets[(inputGroupBy[i] >> (pass * BITS_PER_RADIX_PASS)) & mask]] = inputGroupBy[i];
        bufferAggregate[start + buckets[(inputGroupBy[i] >> (pass * BITS_PER_RADIX_PASS)) & mask]] = inputAggregate[i];
    }

    std::fill(buckets.begin(), buckets.end(), 0);
    std::swap(inputGroupBy, bufferGroupBy);
    std::swap(inputAggregate, bufferAggregate);
    --pass;

    if (pass > 0) {
        if (partitions[0] > start) {
            groupBySortRadixOptAux<Aggregator>(start, partitions[0], inputGroupBy, inputAggregate,
                                               bufferGroupBy,
                                               bufferAggregate, mask, numBuckets, buckets, pass, result);
        }
        for (i = 1; i < numBuckets; i++) {
            if (partitions[i] > partitions[i - 1]) {
                groupBySortRadixOptAux<Aggregator>(partitions[i - 1], partitions[i], inputGroupBy,
                                                   inputAggregate,
                                                   bufferGroupBy, bufferAggregate, mask, numBuckets, buckets,
                                                   pass, result);
            }
        }
    } else {
        if (partitions[0] > start) {
            groupBySortRadixOptAuxAgg<Aggregator>(start, partitions[0], inputGroupBy, inputAggregate,
                                                  mask, numBuckets, buckets, result);
        }
        for (i = 1; i < numBuckets; i++) {
            if (partitions[i] > partitions[i - 1]) {
                groupBySortRadixOptAuxAgg<Aggregator>(partitions[i - 1], partitions[i], inputGroupBy,
                                                      inputAggregate,
                                                      mask, numBuckets, buckets, result);
            }
        }
    }
}

template<template<typename> class Aggregator, typename T1, typename T2>
vectorOfPairs<T1, T2> groupBySortRadixOpt(int n, T1 *inputGroupBy, T2 *inputAggregate) {
    static_assert(std::is_integral<T1>::value, "GroupBy column must be an integer type");
    static_assert(std::is_arithmetic<T2>::value, "Payload column must be an numeric type");

    int i;
    int numBuckets = 1 << BITS_PER_RADIX_PASS;
    int mask = numBuckets - 1;
    int largest = 0;

    for (i = 0; i < n; i++) {
        if (inputGroupBy[i] > largest) {
            largest = inputGroupBy[i];
        }
    }
    int msbPosition = 0;
    while (largest != 0) {
        largest >>= 1;
        msbPosition++;
    }

    int pass = static_cast<int>(std::ceil(static_cast<double>(msbPosition) / BITS_PER_RADIX_PASS)) - 1;
    vectorOfPairs<T1, T2> result;

    std::vector<int> buckets(1 << BITS_PER_RADIX_PASS, 0);
    T1 *bufferGroupBy = new T1[n];
    T2 *bufferAggregate = new T2[n];

    groupBySortRadixOptAux<Aggregator>(0, n, inputGroupBy, inputAggregate, bufferGroupBy,
                                       bufferAggregate, mask, numBuckets, buckets, pass, result);

    delete[]bufferGroupBy;
    delete[]bufferAggregate;

    return result;
}

template<template<typename> class Aggregator, typename T1, typename T2>
vectorOfPairs<T1, T2> groupByAdaptiveSortRadix(int n, T1 *inputGroupBy, T2 *inputAggregate,
                                               vectorOfPairs<int, int> &sectionsToBeSorted,
                                               tsl::robin_map<T1, T2> &map, T1 largest, vectorOfPairs<T1, T2> &result) {
    int i;

    for (const auto& section : sectionsToBeSorted) {
        for (i = section.first; i < section.second; i++) {
            largest = std::max(largest, inputGroupBy[i]);
        }
    }

    int msbPosition = 0;
    while (largest != 0) {
        largest >>= 1;
        msbPosition++;
    }

    int pass = static_cast<int>(std::ceil(static_cast<double>(msbPosition) / BITS_PER_RADIX_PASS)) - 1;

    int numBuckets = 1 << BITS_PER_RADIX_PASS;
    std::vector<int> buckets(1 << BITS_PER_RADIX_PASS, 0);

    int mask = numBuckets - 1;

    T1 *bufferGroupBy = new T1[n];
    T2 *bufferAggregate = new T2[n];


    for (const auto& section : sectionsToBeSorted) {
        for (i = section.first; i < section.second; i++) {
            buckets[(inputGroupBy[i] >> (pass * BITS_PER_RADIX_PASS)) & mask]++;
        }
    }
    for (auto it = map.begin(); it != map.end(); ++it) {
        buckets[(it->first >> (pass * BITS_PER_RADIX_PASS)) & mask]++;
    }

    for (i = 1; i < numBuckets; i++) {
        buckets[i] += buckets[i - 1];
    }

    std::vector<int> partitions(buckets.data(), buckets.data() + numBuckets);

    for (auto it = map.begin(); it != map.end(); it++) {
        bufferGroupBy[--buckets[(it->first >> (pass * BITS_PER_RADIX_PASS)) & mask]] = it->first;
        bufferAggregate[buckets[(it->first >> (pass * BITS_PER_RADIX_PASS)) & mask]] = it->second;
    }
    for (const auto& section : vectorOfPairs<int, int>(sectionsToBeSorted.rbegin(), sectionsToBeSorted.rend())) {
        for (i = section.first; i < section.second; i++) {
            bufferGroupBy[--buckets[(inputGroupBy[i] >> (pass * BITS_PER_RADIX_PASS)) & mask]] = inputGroupBy[i];
            bufferAggregate[buckets[(inputGroupBy[i] >> (pass * BITS_PER_RADIX_PASS)) & mask]] = inputAggregate[i];
        }
    }

    std::fill(buckets.begin(), buckets.end(), 0);
    std::swap(inputGroupBy, bufferGroupBy);
    std::swap(inputAggregate, bufferAggregate);
    --pass;

    if (pass > 0) {
        if (partitions[0] > 0) {
            groupBySortRadixOptAux<Aggregator>(0, partitions[0], inputGroupBy, inputAggregate,
                                               bufferGroupBy, bufferAggregate, mask, numBuckets, buckets,
                                               pass, result);
        }
        for (i = 1; i < numBuckets; i++) {
            if (partitions[i] > partitions[i - 1]) {
                groupBySortRadixOptAux<Aggregator>(partitions[i - 1], partitions[i], inputGroupBy,
                                                   inputAggregate, bufferGroupBy, bufferAggregate, mask,
                                                   numBuckets, buckets, pass, result);
            }
        }
    } else {
        if (partitions[0] > 0) {
            groupBySortRadixOptAuxAgg<Aggregator>(0, partitions[0], inputGroupBy, inputAggregate,
                                                  mask, numBuckets, buckets, result);
        }
        for (i = 1; i < numBuckets; i++) {
            if (partitions[i] > partitions[i - 1]) {
                groupBySortRadixOptAuxAgg<Aggregator>(partitions[i - 1], partitions[i], inputGroupBy,
                                                      inputAggregate, mask, numBuckets, buckets, result);
            }
        }
    }

    std::swap(inputGroupBy, bufferGroupBy);
    std::swap(inputAggregate, bufferAggregate);

    delete[]bufferGroupBy;
    delete[]bufferAggregate;

    return result;
}

template<template<typename> class Aggregator, typename T1, typename T2>
vectorOfPairs<T1, T2> groupByAdaptive(int n, T1 *inputGroupBy, T2 *inputAggregate, int cardinality) {
    static_assert(std::is_integral<T1>::value, "GroupBy column must be an integer type");
    static_assert(std::is_arithmetic<T2>::value, "Payload column must be an numeric type");

    constexpr int tuplesPerChunk = 75 * 1000;
    constexpr int tuplesBetweenHashing = 2*1000*1000;
    int initialSize = std::max(static_cast<int>(2.5 * cardinality), 400000);

    tsl::robin_map<T1, T2> map(initialSize);
    typename tsl::robin_map<T1, T2>::iterator it;

    std::vector<std::string> counters = {"PERF_COUNT_HW_CACHE_MISSES"};
    long_long *counterValues = Counters::getInstance().getEvents(counters);

    int hashTableEntryBytes = sizeof(T1) + sizeof(T2);
    float tuplesPerLastLevelCacheMissThreshold = (GROUPBY_MACHINE_CONSTANT * bytesPerCacheLine()) / hashTableEntryBytes;

    int index = 0;
    int tuplesToProcess;
    int chunkStartIndex;

    vectorOfPairs<int, int> sectionsToBeSorted;
    int elements = 0;

    vectorOfPairs<T1, T2> result;
    T1 mapLargest = std::numeric_limits<T1>::lowest();

    while (index < n) {

        while (index < n) {

            chunkStartIndex = index;
            tuplesToProcess = std::min(tuplesPerChunk, n - index);

            Counters::getInstance().readEventSet();

            for (; index < chunkStartIndex + tuplesToProcess; ++index) {
                it = map.find(inputGroupBy[index]);
                if (it != map.end()) {
                    it.value() = Aggregator<T2>()(it->second, inputAggregate[index], false);
                } else {
                    map.insert({inputGroupBy[index], Aggregator<T2>()(0, inputAggregate[index], true)});
                    mapLargest = std::max(mapLargest, inputGroupBy[index]);
                }
            }

            Counters::getInstance().readEventSet();

            if ((static_cast<float>(tuplesToProcess) / counterValues[0]) < tuplesPerLastLevelCacheMissThreshold) {
                tuplesToProcess = std::min(tuplesBetweenHashing, n - index);

                sectionsToBeSorted.emplace_back(index, index + tuplesToProcess);
                index += tuplesToProcess;
                elements += tuplesToProcess;

                break;
            }
        }
    }

    if (sectionsToBeSorted.empty()) {
        return {map.begin(), map.end()};
    }
    elements += map.size();
    return groupByAdaptiveSortRadix<Aggregator>(elements, inputGroupBy, inputAggregate, sectionsToBeSorted,
                                                map, mapLargest,result);
}

template<template<typename> class Aggregator, typename T1, typename T2>
vectorOfPairs<T1, T2> groupByAdaptiveSwitchToSortOnly(int n, T1 *inputGroupBy, T2 *inputAggregate, int cardinality) {
    static_assert(std::is_integral<T1>::value, "GroupBy column must be an integer type");

    int hashTableEntryBytes = sizeof(T1) + sizeof(T2);

    int tuplesPerCheck = 200000;
    int initialSize = std::max(static_cast<int>(2.5 * cardinality), 400000);

    std::vector<std::string> counters = {"PERF_COUNT_HW_CACHE_MISSES"};
    long_long *counterValues = Counters::getInstance().getEvents(counters);

    int index = 0;
    int tuplesToProcess;

    if (!std::is_same<Aggregator<T2>, CountAggregation<T2>>::value) {
        static_assert(std::is_arithmetic<T2>::value, "Payload column must be an numeric type");
    }

    tsl::robin_map<T1, T2> map(initialSize);
    typename tsl::robin_map<T1, T2>::iterator it;

    float tuplesPerLastLevelCacheMissThreshold = (GROUPBY_MACHINE_CONSTANT * bytesPerCacheLine()) / hashTableEntryBytes;

    unsigned long warmUpRows = std::min(static_cast<int>((l3cacheSize() / hashTableEntryBytes) /
                                                         (bytesPerCacheLine() / hashTableEntryBytes)), cardinality);

    for (; index < static_cast<int>(warmUpRows); ++index) {
        it = map.find(inputGroupBy[index]);
        if (it != map.end()) {
            it.value() = Aggregator<T2>()(it->second, inputAggregate[index], false);
        } else {
            map.insert({inputGroupBy[index], Aggregator<T2>()(0, inputAggregate[index], true)});
        }
    }

    while (index < n) {

        tuplesToProcess = std::min(tuplesPerCheck, n - index);

        Counters::getInstance().readEventSet();

        for (auto _ = 0; _ < tuplesToProcess; ++_) {
            it = map.find(inputGroupBy[index]);
            if (it != map.end()) {
                it.value() = Aggregator<T2>()(it->second, inputAggregate[index], false);
            } else {
                map.insert({inputGroupBy[index], Aggregator<T2>()(0, inputAggregate[index], true)});
            }
            ++index;
        }

        Counters::getInstance().readEventSet();

        if (__builtin_expect((static_cast<float>(tuplesToProcess) / counterValues[0])
                             < tuplesPerLastLevelCacheMissThreshold, false)) {

            int reducedNumElements = index - map.size();
            int i = reducedNumElements;

            for (it = map.begin(); it != map.end(); ++it) {
                inputGroupBy[i] = it->first;
                inputAggregate[i++] = it->second;
            }

            return groupBySortRadixOpt<Aggregator>(n - reducedNumElements,
                                                   inputGroupBy + reducedNumElements,
                                                   inputAggregate + reducedNumElements);
        }
    }

    return {map.begin(), map.end()};
}

template<template<typename> class Aggregator, typename T1, typename T2>
vectorOfPairs<T1, T2> runGroupByFunction(GroupBy groupByImplementation, int n, T1 *inputGroupBy, T2 *inputAggregate, int cardinality) {
    switch (groupByImplementation) {
        case GroupBy::Hash:
            return groupByHash<Aggregator>(n, inputGroupBy, inputAggregate, cardinality);
        case GroupBy::SortRadixOpt:
            return groupBySortRadixOpt<Aggregator>(n, inputGroupBy, inputAggregate);
        case GroupBy::Adaptive:
            return groupByAdaptive<Aggregator>(n, inputGroupBy, inputAggregate, cardinality);
        case GroupBy::AdaptiveSwitchToSortOnly:
            return groupByAdaptiveSwitchToSortOnly<Aggregator>(n, inputGroupBy, inputAggregate, cardinality);
        default:
            std::cout << "Invalid selection of 'GroupBy' implementation!" << std::endl;
            exit(1);
    }
}

}

#endif //MABPL_GROUPBYIMPLEMENTATION_H
