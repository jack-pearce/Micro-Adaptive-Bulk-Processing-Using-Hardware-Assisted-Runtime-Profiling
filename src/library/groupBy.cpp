#include <iostream>
#include <sparsehash/dense_hash_map>
#include <list>
#include <memory>
#include <algorithm>
#include <cmath>
//#include <folly/container/F14Map.h>
#include <absl/container/flat_hash_map.h>
#include <tsl/robin_map.h>
#include <tsl/hopscotch_map.h>
#include <random>
#include <set>

#include "groupBy.h"
#include "utils.h"
#include "../utils/papiHelpers.h"
#include "../library/papi.h"


#define BITS_PER_PASS 10


inline int minAggregationFunc(int currentAggregate, int numberToInclude, bool firstAggregation) {
    if (firstAggregation) {
        return numberToInclude;
    }
    return std::min(currentAggregate, numberToInclude);
}

inline int maxAggregationFunc(int currentAggregate, int numberToInclude, bool firstAggregation) {
    if (firstAggregation) {
        return numberToInclude;
    }
    return std::max(currentAggregate, numberToInclude);
}

inline int sumAggregationFunc(int currentAggregate, int numberToInclude, bool firstAggregation) {
    if (firstAggregation) {
        return numberToInclude;
    }
    return currentAggregate + numberToInclude;
}

inline int countAggregationFunc(int currentAggregate, int _, bool firstAggregation) {
    if (firstAggregation) {
        return 1;
    }
    return ++currentAggregate;
}

const aggFuncPtr minAggregation = &minAggregationFunc;
const aggFuncPtr maxAggregation = &maxAggregationFunc;
const aggFuncPtr sumAggregation = &sumAggregationFunc;
const aggFuncPtr countAggregation = &countAggregationFunc;


vectorOfPairs groupByHash(int n, int *inputGroupBy, int *inputAggregate, aggFuncPtr aggregator) {
    int initialSize = round((l3cacheSize() / (3 * (sizeof(int) + sizeof(int)))) / 100000.0) * 100000;
    tsl::robin_map<int, int> map(initialSize);

    tsl::robin_map<int, int>::iterator it;
    for (auto i = 0; i < n; ++i) {
        it = map.find(inputGroupBy[i]);
        if (it != map.end()) {
            it.value() = aggregator(it->second, inputAggregate[i], false);
        } else {
            map.insert({inputGroupBy[i], aggregator(0, inputAggregate[i], true)});
        }
    }

    return {map.begin(), map.end()};
}

vectorOfPairs groupByHashGoogleDenseHashMap(int n, int *inputGroupBy, int *inputAggregate, aggFuncPtr aggregator) {
    // Google SparseHash
    google::dense_hash_map<int, int> map;
    map.set_empty_key(-1);

    google::dense_hash_map<int, int>::iterator it;
    for (auto i = 0; i < n; ++i) {
        it = map.find(inputGroupBy[i]);
        if (it != map.end()) {
            it->second = aggregator(it->second, inputAggregate[i], false);
        } else {
            map.insert({inputGroupBy[i], aggregator(0, inputAggregate[i], true)});
        }
    }

    return {map.begin(), map.end()};
}

/*vectorOfPairs groupByHashFollyF14FastMap(int n, int *inputGroupBy, int *inputAggregate,
                                           aggFuncPtr aggregator) {
    // Facebook Folly
    folly::F14FastMap<int, int> map;

    folly::F14FastMap<int, int>::iterator it;
    for (auto i = 0; i < n; ++i) {
        it = map.find(inputGroupBy[i]);
        if (it != map.end()) {
            it->second = aggregator(it->second, inputAggregate[i], false);
        } else {
            map.insert({inputGroupBy[i], aggregator(0, inputAggregate[i], true)});
        }
    }

    return {map.begin(), map.end()};
}*/

vectorOfPairs groupByHashAbseilFlatHashMap(int n, int *inputGroupBy, int *inputAggregate, aggFuncPtr aggregator) {
    // Google Abseil
    absl::flat_hash_map<int, int> map;

    absl::flat_hash_map<int, int>::iterator it;
    for (auto i = 0; i < n; ++i) {
        it = map.find(inputGroupBy[i]);
        if (it != map.end()) {
            it->second = aggregator(it->second, inputAggregate[i], false);
        } else {
            map.insert({inputGroupBy[i], aggregator(0, inputAggregate[i], true)});
        }
    }

    return {map.begin(), map.end()};
}

vectorOfPairs groupByHashTessilRobinMap(int n, int *inputGroupBy, int *inputAggregate, aggFuncPtr aggregator) {
    // Tessil Robin Map
    tsl::robin_map<int, int> map;

    tsl::robin_map<int, int>::iterator it;
    for (auto i = 0; i < n; ++i) {
        it = map.find(inputGroupBy[i]);
        if (it != map.end()) {
            it.value() = aggregator(it->second, inputAggregate[i], false);
        } else {
            map.insert({inputGroupBy[i], aggregator(0, inputAggregate[i], true)});
        }
    }

    return {map.begin(), map.end()};
}

vectorOfPairs groupByHashTessilHopscotchMap(int n, int *inputGroupBy, int *inputAggregate, aggFuncPtr aggregator) {
    // Tessil Hopscotch Map
    tsl::hopscotch_map<int, int> map;

    tsl::hopscotch_map<int, int>::iterator it;
    for (auto i = 0; i < n; ++i) {
        it = map.find(inputGroupBy[i]);
        if (it != map.end()) {
            it.value() = aggregator(it->second, inputAggregate[i], false);
        } else {
            map.insert({inputGroupBy[i], aggregator(0, inputAggregate[i], true)});
        }
    }

    return {map.begin(), map.end()};
}

vectorOfPairs groupBySortRadix(int n, int *inputGroupBy, int *inputAggregate, aggFuncPtr aggregator) {
    int i;
    int *bufferGroupBy = new int[n];
    int *bufferAggregate = new int[n];
    int buckets = 1 << BITS_PER_PASS;
    int mask = buckets - 1;
    int* bucket = new int[buckets];

    int passes = static_cast<int>(std::ceil(static_cast<double>(32) / BITS_PER_PASS)) - 1;
    bool finalCopyRequired = (passes + 1) % 2;

    for (int pass = 0; pass <= passes; pass++) {
        for (i = 0; i < buckets; i++) {
            bucket[i] = 0;
        }

        for (i = 0; i < n; i++) {
            bucket[(inputGroupBy[i] >> (pass * BITS_PER_PASS)) & mask]++;
        }

        for (i = 1; i < buckets; i++) {
            bucket[i] += bucket[i - 1];
        }

        for (i = n - 1; i >= 0; i--) {
            bufferGroupBy[--bucket[(inputGroupBy[i] >> (pass * BITS_PER_PASS)) & mask]] = inputGroupBy[i];
            bufferAggregate[bucket[(inputGroupBy[i] >> (pass * BITS_PER_PASS)) & mask]] = inputAggregate[i];

        }

        std::swap(inputGroupBy, bufferGroupBy);
        std::swap(inputAggregate, bufferAggregate);
    }

    if (finalCopyRequired) {
        std::swap(inputGroupBy, bufferGroupBy);
        std::copy(bufferGroupBy, bufferGroupBy + n, inputGroupBy);
    }

    vectorOfPairs result;

    result.emplace_back(inputGroupBy[0], aggregator(0, inputAggregate[i], true));
    for (i = 1; i < n; i++) {
        if (inputGroupBy[i] == result.back().first) {
            result.back().second = aggregator(result.back().second, inputAggregate[i], false);
        } else {
            result.emplace_back(inputGroupBy[i], aggregator(0, inputAggregate[i], true));
        }
    }

    if (finalCopyRequired) {
        std::swap(inputAggregate, bufferAggregate);
    }

    delete []bufferGroupBy;
    delete []bucket;
    delete []bufferAggregate;

    return result;
}

void groupBySortRadixOptAuxAgg(int start, int end, const int *inputGroupBy, int *inputAggregate, aggFuncPtr aggregator,
                               int mask, int buckets, vectorOfPairs &result) {
    int i;
    int bucket[1 << BITS_PER_PASS] = {0};
    bool bucketEntryPresent[1 << BITS_PER_PASS] = {false};

    for (i = start; i < end; i++) {
        bucket[inputGroupBy[i] & mask] = aggregator(bucket[inputGroupBy[i] & mask], inputAggregate[i],
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

void groupBySortRadixOptAux(int start, int end, int *inputGroupBy, int *inputAggregate, aggFuncPtr aggregator,
                            int *bufferGroupBy, int *bufferAggregate, int mask, int buckets, int pass, vectorOfPairs &result) {
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
            groupBySortRadixOptAux(start, partitions[0], inputGroupBy, inputAggregate, aggregator, bufferGroupBy,
                                   bufferAggregate, mask, buckets, pass, result);
        }
        for (i = 1; i < buckets; i++) {
            if (partitions[i] > partitions[i - 1]) {
                groupBySortRadixOptAux(partitions[i - 1], partitions[i], inputGroupBy, inputAggregate, aggregator,
                                       bufferGroupBy, bufferAggregate, mask, buckets, pass, result);
            }
        }
    } else {
        if (partitions[0] > start) {
            groupBySortRadixOptAuxAgg(start, partitions[0], inputGroupBy, inputAggregate, aggregator,
                                      mask, buckets, result);
        }
        for (i = 1; i < buckets; i++) {
            if (partitions[i] > partitions[i - 1]) {
                groupBySortRadixOptAuxAgg(partitions[i - 1], partitions[i], inputGroupBy, inputAggregate, aggregator,
                                          mask, buckets, result);
            }
        }
    }
}

vectorOfPairs groupBySortRadixOpt(int n, int *inputGroupBy, int *inputAggregate, aggFuncPtr aggregator) {
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
    vectorOfPairs result;

    int *bufferGroupBy = new int[n];
    int *bufferAggregate = new int[n];

    groupBySortRadixOptAux(0, n, inputGroupBy, inputAggregate, aggregator, bufferGroupBy,
                           bufferAggregate, mask, buckets, passes, result);

    delete []bufferGroupBy;
    delete []bufferAggregate;

    return result;
}

vectorOfPairs groupBySingleRadixPassThenHash(int n, int *inputGroupBy, int *inputAggregate, aggFuncPtr aggregator) {
    int i;
    int *bufferGroupBy = new int[n];
    int *bufferAggregate = new int[n];
    int buckets = 1 << BITS_PER_PASS;
    int mask = buckets - 1;
    int bucket[buckets];

    for (i = 0; i < buckets; i++) {
        bucket[i] = 0;
    }

    for (i = 0; i < n; i++) {
        bucket[inputGroupBy[i] & mask]++;
    }

    for (i = 1; i < buckets; i++) {
        bucket[i] += bucket[i - 1];
    }

    std::vector<int> partitions(bucket, bucket + buckets);

    for (i = n - 1; i >= 0; i--) {
        bufferGroupBy[--bucket[inputGroupBy[i] & mask]] = inputGroupBy[i];
        bufferAggregate[bucket[inputGroupBy[i] & mask]] = inputAggregate[i];
    }

    std::swap(inputGroupBy, bufferGroupBy);
    std::swap(inputAggregate, bufferAggregate);

    vectorOfPairs result;
    if (partitions[0] > 0) {
        vectorOfPairs newResults = groupByHash(partitions[0], inputGroupBy, inputAggregate, aggregator);
        result.insert(result.end(), newResults.begin(), newResults.end());
    }
    for (i = 1; i < buckets; i++) {
        if (partitions[i] > partitions[i - 1]) {
            vectorOfPairs newResults = groupByHash(partitions[i] - partitions[i - 1], inputGroupBy + partitions[i - 1],
                                                   inputAggregate + partitions[i - 1], aggregator);
            result.insert(result.end(), newResults.begin(), newResults.end());
        }
    }

    std::swap(inputGroupBy, bufferGroupBy);
    std::swap(inputAggregate, bufferAggregate);
    delete []bufferGroupBy;
    delete []bufferAggregate;

    return result;
}

vectorOfPairs groupByDoubleRadixPassThenHash(int n, int *inputGroupBy, int *inputAggregate, aggFuncPtr aggregator) {
    int i;
    int *bufferGroupBy = new int[n];
    int *bufferAggregate = new int[n];
    int buckets = 1 << BITS_PER_PASS;
    int mask = buckets - 1;
    int bucket[buckets];
    int partitions[buckets];

    for (int pass = 0; pass < 2; pass++) {
        for (i = 0; i < buckets; i++) {
            bucket[i] = 0;
        }

        for (i = 0; i < n; i++) {
            bucket[(inputGroupBy[i] >> (pass * BITS_PER_PASS)) & mask]++;
        }

        for (i = 1; i < buckets; i++) {
            bucket[i] += bucket[i - 1];
        }

        if (pass == 1) {
            std::copy(bucket, bucket + buckets, partitions);
        }

        for (i = n - 1; i >= 0; i--) {
            bufferGroupBy[--bucket[(inputGroupBy[i] >> (pass * BITS_PER_PASS)) & mask]] = inputGroupBy[i];
            bufferAggregate[bucket[(inputGroupBy[i] >> (pass * BITS_PER_PASS)) & mask]] = inputAggregate[i];
        }

        std::swap(inputGroupBy, bufferGroupBy);
        std::swap(inputAggregate, bufferAggregate);

    }

    vectorOfPairs result;

    if (partitions[0] > 0) {
        vectorOfPairs newResults = groupByHash(partitions[0], inputGroupBy, inputAggregate, aggregator);
        result.insert(result.end(), newResults.begin(), newResults.end());
    }
    for (i = 1; i < buckets; i++) {
        if (partitions[i] > partitions[i - 1]) {
            vectorOfPairs newResults = groupByHash(partitions[i] - partitions[i - 1], inputGroupBy + partitions[i - 1],
                                                   inputAggregate + partitions[i - 1], aggregator);
            result.insert(result.end(), newResults.begin(), newResults.end());
        }
    }

    delete []bufferGroupBy;
    delete []bufferAggregate;

    return result;
}

vectorOfPairs groupByAdaptive(int n, int *inputGroupBy, int *inputAggregate, aggFuncPtr aggregator) {
    int tuplesPerCheck = 50000;
    int initialSize = round((l3cacheSize() / (3 * (sizeof(int) + sizeof(int)))) / 100000.0) * 100000;
    tsl::robin_map<int, int> map(initialSize);
    tsl::robin_map<int, int>::iterator it;

    float tuplesPerLastLevelCacheMissThreshold = 0.5 * (bytesPerCacheLine() / (sizeof(int) + sizeof(int)));

    std::vector<std::string> counters = {"PERF_COUNT_HW_CACHE_MISSES"};
    long_long *counterValues = Counters::getInstance().getEvents(counters);
    long_long lastLevelCacheMisses = 0;
    long_long rowsProcessed = 0;

    int index = 0;
    int tuplesToProcess;

    unsigned long warmUpRows = std::min(l3cacheSize() / (sizeof(int) + sizeof(int)),
                                        static_cast<unsigned long>(n));
    for (; index < static_cast<int>(warmUpRows); ++index) {
        it = map.find(inputGroupBy[index]);
        if (it != map.end()) {
            it.value() = aggregator(it->second, inputAggregate[index], false);
        } else {
            map.insert({inputGroupBy[index], aggregator(0, inputAggregate[index], true)});
        }
    }

    while (index < n) {

        tuplesToProcess = std::min(tuplesPerCheck, n - index);

        Counters::getInstance().readEventSet();

        for (auto _ = 0; _ < tuplesToProcess; ++_) {
            it = map.find(inputGroupBy[index]);
            if (it != map.end()) {
                it.value() = aggregator(it->second, inputAggregate[index], false);
            } else {
                map.insert({inputGroupBy[index], aggregator(0, inputAggregate[index], true)});
            }
            ++index;
        }

        Counters::getInstance().readEventSet();

        lastLevelCacheMisses += counterValues[0];
        rowsProcessed += tuplesToProcess;

        if (__builtin_expect((static_cast<float>(rowsProcessed) / lastLevelCacheMisses)
                                < tuplesPerLastLevelCacheMissThreshold, false)) {
            return groupBySortRadixOpt(n, inputGroupBy, inputAggregate, aggregator);
        }
    }

    return {map.begin(), map.end()};
}

vectorOfPairs runGroupByFunction(GroupBy groupByImplementation, int n, int *inputGroupBy, int *inputAggregate,
                                 aggFuncPtr aggregator) {
    switch(groupByImplementation) {
        case GroupBy::Hash:
            return groupByHash(n, inputGroupBy, inputAggregate, aggregator);
        case GroupBy::HashGoogleDenseHashMap:
            return groupByHashGoogleDenseHashMap(n, inputGroupBy, inputAggregate, aggregator);
        case GroupBy::HashFollyF14FastMap:
            std::cout << "Folly requires removing '-march=native' flag" << std::endl;
            exit(1);
//            return groupByHashFollyF14FastMap(n, inputGroupBy, inputAggregate, aggregator);
        case GroupBy::HashAbseilFlatHashMap:
            return groupByHashAbseilFlatHashMap(n, inputGroupBy, inputAggregate, aggregator);
        case GroupBy::HashTessilRobinMap:
            return groupByHashTessilRobinMap(n, inputGroupBy, inputAggregate, aggregator);
        case GroupBy::HashTessilHopscotchMap:
            return groupByHashTessilHopscotchMap(n, inputGroupBy, inputAggregate, aggregator);
        case GroupBy::SortRadix:
            return groupBySortRadix(n, inputGroupBy, inputAggregate, aggregator);
        case GroupBy::SortRadixOpt:
            return groupBySortRadixOpt(n, inputGroupBy, inputAggregate, aggregator);
        case GroupBy::SingleRadixPassThenHash:
            return groupBySingleRadixPassThenHash(n, inputGroupBy, inputAggregate, aggregator);
        case GroupBy::DoubleRadixPassThenHash:
            return groupByDoubleRadixPassThenHash(n, inputGroupBy, inputAggregate, aggregator);
        case GroupBy::Adaptive:
            return groupByAdaptive(n, inputGroupBy, inputAggregate, aggregator);
        default:
            std::cout << "Invalid selection of 'GroupBy' implementation!" << std::endl;
            exit(1);
    }
}

std::string getGroupByName(GroupBy groupByImplementation) {
    switch(groupByImplementation) {
        case GroupBy::Hash:
            return "GroupBy_Hash";
        case GroupBy::HashGoogleDenseHashMap:
            return "GroupBy_HashGoogleDenseHashMap";
        case GroupBy::HashFollyF14FastMap:
            return "GroupBy_HashFollyF14FastMap";
        case GroupBy::HashAbseilFlatHashMap:
            return "GroupBy_HashAbseilFlatHashMap";
        case GroupBy::HashTessilRobinMap:
            return "GroupBy_HashTessilRobinMap";
        case GroupBy::HashTessilHopscotchMap:
            return "GroupBy_HashTessilHopscotchMap";
        case GroupBy::SortRadix:
            return "GroupBy_SortRadix";
        case GroupBy::SortRadixOpt:
            return "GroupBy_SortRadixOptimal";
        case GroupBy::SingleRadixPassThenHash:
            return "GroupBy_SingleRadixPassThenHash";
        case GroupBy::DoubleRadixPassThenHash:
            return "GroupBy_DoubleRadixPassThenHash";
        case GroupBy::Adaptive:
            return "GroupBy_Adaptive";
        default:
            std::cout << "Invalid selection of 'GroupBy' implementation!" << std::endl;
            exit(1);
    }
}