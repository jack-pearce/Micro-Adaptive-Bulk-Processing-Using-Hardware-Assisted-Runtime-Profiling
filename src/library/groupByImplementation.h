#ifndef MABPL_GROUPBYIMPLEMENTATION_H
#define MABPL_GROUPBYIMPLEMENTATION_H


#include <iostream>
#include <tsl/robin_map.h>

#include "groupBy.h"
#include "utilities/systemInformation.h"
#include "utilities/papi.h"


#define BITS_PER_PASS 10


template <typename T>
T MinAggregation<T>::operator()(T currentAggregate, T numberToInclude, bool firstAggregation) const {
    if (firstAggregation) {
        return numberToInclude;
    }
    return std::min(currentAggregate, numberToInclude);
}

template <typename T>
T MaxAggregation<T>::operator()(T currentAggregate, T numberToInclude, bool firstAggregation) const {
    if (firstAggregation) {
        return numberToInclude;
    }
    return std::max(currentAggregate, numberToInclude);
}

template <typename T>
T SumAggregation<T>::operator()(T currentAggregate, T numberToInclude, bool firstAggregation) const {
    if (firstAggregation) {
        return numberToInclude;
    }
    return currentAggregate + numberToInclude;
}

template <typename T>
T CountAggregation<T>::operator()(T currentAggregate, T _, bool firstAggregation) const {
    if (firstAggregation) {
        return 1;
    }
    return ++currentAggregate;
}

template <template<typename> class Aggregator, typename T1, typename T2>
vectorOfPairs<T1, T2> groupByHash(int n, T1 *inputGroupBy, T2 *inputAggregate) {
    static_assert(std::is_integral<T1>::value, "GroupBy column must be an integer type");
    static_assert(std::is_arithmetic<T2>::value, "Payload column must be an numeric type");

    int initialSize = round((l3cacheSize() / (3 * (sizeof(int) + sizeof(int)))) / 100000.0) * 100000;
    tsl::robin_map<T1, T2> map(initialSize);

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

template <template<typename> class Aggregator, typename T1, typename T2>
void groupBySortRadixOptAuxAgg(int start, int end, const T1 *inputGroupBy, T2 *inputAggregate,
                               int mask, int buckets, vectorOfPairs<T1, T2> &result) {
    int i;
    int bucket[1 << BITS_PER_PASS] = {0};
    bool bucketEntryPresent[1 << BITS_PER_PASS] = {false};

    for (i = start; i < end; i++) {
        bucket[inputGroupBy[i] & mask] = Aggregator<T2>()(bucket[inputGroupBy[i] & mask], inputAggregate[i],
                                                    !bucketEntryPresent[inputGroupBy[i] & mask]);
        bucketEntryPresent[inputGroupBy[i] & mask] = true;
    }

    int valuePrefix = inputGroupBy[start] & ~mask;

    for (i = 0; i < buckets; i++) {
        if (bucketEntryPresent[i]) {
            result.emplace_back(valuePrefix | i, bucket[i]);
        }
    }
}

template <template<typename> class Aggregator, typename T1, typename T2>
void groupBySortRadixOptAux(int start, int end, T1 *inputGroupBy, T2 *inputAggregate, T1 *bufferGroupBy,
                            T2 *bufferAggregate, int mask, int buckets, int pass, vectorOfPairs<T1, T2> &result) {
    int i;
    int bucket[1 << BITS_PER_PASS] = {0};

    for (i = start; i < end; i++) {
        bucket[(inputGroupBy[i] >> (pass * BITS_PER_PASS)) & mask]++;
    }

    for (i = 1; i < buckets; i++) {
        bucket[i] += bucket[i - 1];
    }

    std::vector<int> partitions(bucket, bucket + buckets);
    for (i = 0; i < buckets; i++) {
        partitions[i] += start;
    }

    for (i = end - 1; i >= start; i--) {
        bufferGroupBy[start + --bucket[(inputGroupBy[i] >> (pass * BITS_PER_PASS)) & mask]] = inputGroupBy[i];
        bufferAggregate[start + bucket[(inputGroupBy[i] >> (pass * BITS_PER_PASS)) & mask]] = inputAggregate[i];
    }

    std::swap(inputGroupBy, bufferGroupBy);
    std::swap(inputAggregate, bufferAggregate);
    --pass;

    if (pass > 0) {
        if (partitions[0] > start) {
            groupBySortRadixOptAux<Aggregator>(start, partitions[0], inputGroupBy, inputAggregate, bufferGroupBy,
                                   bufferAggregate, mask, buckets, pass, result);
        }
        for (i = 1; i < buckets; i++) {
            if (partitions[i] > partitions[i - 1]) {
                groupBySortRadixOptAux<Aggregator>(partitions[i - 1], partitions[i], inputGroupBy, inputAggregate,
                                       bufferGroupBy, bufferAggregate, mask, buckets, pass, result);
            }
        }
    } else {
        if (partitions[0] > start) {
            groupBySortRadixOptAuxAgg<Aggregator>(start, partitions[0], inputGroupBy, inputAggregate,
                                      mask, buckets, result);
        }
        for (i = 1; i < buckets; i++) {
            if (partitions[i] > partitions[i - 1]) {
                groupBySortRadixOptAuxAgg<Aggregator>(partitions[i - 1], partitions[i], inputGroupBy, inputAggregate,
                                          mask, buckets, result);
            }
        }
    }
}

template <template<typename> class Aggregator, typename T1, typename T2>
vectorOfPairs<T1, T2> groupBySortRadixOpt(int n, T1 *inputGroupBy, T2 *inputAggregate) {
    static_assert(std::is_integral<T1>::value, "GroupBy column must be an integer type");
    static_assert(std::is_arithmetic<T2>::value, "Payload column must be an numeric type");

    int i;
    int buckets = 1 << BITS_PER_PASS;
    int mask = buckets - 1;
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

    int passes = static_cast<int>(std::ceil(static_cast<double>(msbPosition) / BITS_PER_PASS)) - 1;
    vectorOfPairs<T1, T2> result;

    T1 *bufferGroupBy = new T1[n];
    T2 *bufferAggregate = new T2[n];

    groupBySortRadixOptAux<Aggregator>(0, n, inputGroupBy, inputAggregate, bufferGroupBy,
                           bufferAggregate, mask, buckets, passes, result);

    delete []bufferGroupBy;
    delete []bufferAggregate;

    return result;
}

template<typename T>
vectorOfPairs<T, int> groupByHash_Count(int n, const T *inputGroupBy) {
    static_assert(std::is_integral<T>::value, "GroupBy column must be an integer type");
    int initialSize = round((l3cacheSize() / (3 * (sizeof(int) + sizeof(int)))) / 100000.0) * 100000;
    tsl::robin_map<T, int> map(initialSize);

    typename tsl::robin_map<T, int>::iterator it;
    for (auto i = 0; i < n; ++i) {
        it = map.find(inputGroupBy[i]);
        if (it != map.end()) {
            it.value() = CountAggregation<T>()(it->second, inputGroupBy[i], false);
        } else {
            map.insert({inputGroupBy[i], CountAggregation<T>()(0, inputGroupBy[i], true)});
        }
    }

    return {map.begin(), map.end()};
}

template<typename T>
void groupBySortRadixOptAuxAgg_Count(int start, int end, const T *inputGroupBy, int mask, int buckets,
                                     vectorOfPairs<T, int> &result) {
    static_assert(std::is_integral<T>::value, "GroupBy column must be an integer type");
    int i;
    int bucket[1 << BITS_PER_PASS] = {0};

    for (i = start; i < end; i++) {
        bucket[inputGroupBy[i] & mask]++;
    }

    T valuePrefix = inputGroupBy[start] & ~mask;

    for (i = 0; i < buckets; i++) {
        if (bucket[i] > 0) {
            result.emplace_back(valuePrefix | i, bucket[i]);
        }
    }
}

template<typename T>
void groupBySortRadixOptAux_Count(int start, int end, T *inputGroupBy, T *buffer, int mask, int buckets, int pass,
                                  vectorOfPairs<T, int> &result) {
    static_assert(std::is_integral<T>::value, "GroupBy column must be an integer type");
    int i;
    int bucket[1 << BITS_PER_PASS] = {0};

    for (i = start; i < end; i++) {
        bucket[(inputGroupBy[i] >> (pass * BITS_PER_PASS)) & mask]++;
    }

    for (i = 1; i < buckets; i++) {
        bucket[i] += bucket[i - 1];
    }

    std::vector<int> partitions(bucket, bucket + buckets);
    for (i = 0; i < buckets; i++) {
        partitions[i] += start;
    }

    for (i = end - 1; i >= start; i--) {
        buffer[start + --bucket[(inputGroupBy[i] >> (pass * BITS_PER_PASS)) & mask]] = inputGroupBy[i];
    }

    std::swap(inputGroupBy, buffer);
    --pass;

    if (pass > 0) {
        if (partitions[0] > start) {
            groupBySortRadixOptAux_Count(start, partitions[0], inputGroupBy, buffer, mask, buckets, pass, result);
        }
        for (i = 1; i < buckets; i++) {
            if (partitions[i] > partitions[i - 1]) {
                groupBySortRadixOptAux_Count(partitions[i - 1], partitions[i], inputGroupBy, buffer, mask, buckets, pass,
                                             result);
            }
        }
    } else {
        if (partitions[0] > start) {
            groupBySortRadixOptAuxAgg_Count(start, partitions[0], inputGroupBy, mask, buckets, result);
        }
        for (i = 1; i < buckets; i++) {
            if (partitions[i] > partitions[i - 1]) {
                groupBySortRadixOptAuxAgg_Count(partitions[i - 1], partitions[i], inputGroupBy, mask, buckets, result);
            }
        }
    }
}

template<typename T>
vectorOfPairs<T, int> groupBySortRadixOpt_Count(int n, T *inputGroupBy) {
    static_assert(std::is_integral<T>::value, "GroupBy column must be an integer type");
    int i;
    int buckets = 1 << BITS_PER_PASS;
    int mask = buckets - 1;
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

    int passes = static_cast<int>(std::ceil(static_cast<double>(msbPosition) / BITS_PER_PASS)) - 1;
    vectorOfPairs<T, int> result;

    if (passes > 0) {
        T *buffer = new T[n];
        groupBySortRadixOptAux_Count(0, n, inputGroupBy, buffer, mask, buckets, passes, result);
        delete []buffer;
    } else {
        groupBySortRadixOptAuxAgg_Count(0, n, inputGroupBy, mask, buckets, result);
    }

    return result;
}

template <template<typename> class Aggregator, typename T1, typename T2>
vectorOfPairs<T1, T2> groupByAdaptive(int n, T1 *inputGroupBy, T2 *inputAggregate) {
    static_assert(std::is_integral<T1>::value, "GroupBy column must be an integer type");

    int tuplesPerCheck = 50000;
    int initialSize = round((l3cacheSize() / (3 * (sizeof(int) + sizeof(int)))) / 100000.0) * 100000;

    std::vector<std::string> counters = {"PERF_COUNT_HW_CACHE_MISSES"};
    long_long *counterValues = Counters::getInstance().getEvents(counters);
    long_long lastLevelCacheMisses = 0;
    long_long rowsProcessed = 0;

    int index = 0;
    int tuplesToProcess;

    if constexpr (std::is_same_v<Aggregator<T2>, CountAggregation<T2>>) {
        // Count Aggregation

        tsl::robin_map<T1, int> map(initialSize);
        typename tsl::robin_map<T1, int>::iterator it;

        float tuplesPerLastLevelCacheMissThreshold = (0.5 * bytesPerCacheLine()) / (sizeof(T1) + sizeof(int));

        unsigned long warmUpRows = std::min(l3cacheSize() / (sizeof(T1) + sizeof(int)), static_cast<unsigned long>(n));
        for (; index < static_cast<int>(warmUpRows); ++index) {
            it = map.find(inputGroupBy[index]);
            if (it != map.end()) {
                it.value() = CountAggregation<int>()(it->second, inputGroupBy[index], false);
            } else {
                map.insert({inputGroupBy[index], CountAggregation<int>()(0, inputGroupBy[index], true)});
            }
        }

        while (index < n) {

            tuplesToProcess = std::min(tuplesPerCheck, n - index);

            Counters::getInstance().readEventSet();

            for (auto _ = 0; _ < tuplesToProcess; ++_) {
                it = map.find(inputGroupBy[index]);
                if (it != map.end()) {
                    it.value() = CountAggregation<int>()(it->second, inputGroupBy[index], false);
                } else {
                    map.insert({inputGroupBy[index], CountAggregation<int>()(0, inputGroupBy[index], true)});
                }
                ++index;
            }


            Counters::getInstance().readEventSet();

            lastLevelCacheMisses += counterValues[0];
            rowsProcessed += tuplesToProcess;

            if (__builtin_expect((static_cast<float>(rowsProcessed) / lastLevelCacheMisses)
                                 < tuplesPerLastLevelCacheMissThreshold, false)) {
                return groupBySortRadixOpt_Count(n, inputGroupBy);
            }
        }

        return {map.begin(), map.end()};

    } else {
        // Non-Count Aggregation

        static_assert(std::is_arithmetic<T2>::value, "Payload column must be an numeric type");

        tsl::robin_map<T1, T2> map(initialSize);
        typename tsl::robin_map<T1, T2>::iterator it;

        float tuplesPerLastLevelCacheMissThreshold = (0.5 * bytesPerCacheLine()) / (sizeof(T1) + sizeof(T2));

        unsigned long warmUpRows = std::min(l3cacheSize() / (sizeof(T1) + sizeof(T2)),
                                            static_cast<unsigned long>(n));
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

            lastLevelCacheMisses += counterValues[0];
            rowsProcessed += tuplesToProcess;

            if (__builtin_expect((static_cast<float>(rowsProcessed) / lastLevelCacheMisses)
                                 < tuplesPerLastLevelCacheMissThreshold, false)) {
                return groupBySortRadixOpt<Aggregator>(n, inputGroupBy, inputAggregate);
            }
        }

        return {map.begin(), map.end()};
    }
}

template <template<typename> class Aggregator, typename T1, typename T2>
vectorOfPairs<T1, T2>  runGroupByFunction(GroupBy groupByImplementation, int n, T1 *inputGroupBy, T2 *inputAggregate) {
    switch(groupByImplementation) {
        case GroupBy::Hash:
            return groupByHash<Aggregator>(n, inputGroupBy, inputAggregate);
        case GroupBy::SortRadixOpt:
            return groupBySortRadixOpt<Aggregator>(n, inputGroupBy, inputAggregate);
        case GroupBy::Hash_Count:
            return groupByHash_Count(n, inputGroupBy);
        case GroupBy::SortRadixOpt_Count:
            return groupBySortRadixOpt_Count(n, inputGroupBy);
        case GroupBy::Adaptive:
            return groupByAdaptive<Aggregator>(n, inputGroupBy, inputAggregate);
        default:
            std::cout << "Invalid selection of 'GroupBy' implementation!" << std::endl;
            exit(1);
    }
}

#endif //MABPL_GROUPBYIMPLEMENTATION_H
