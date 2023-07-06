#ifndef MABPL_GROUPBYCOUNTIMPLEMENTATION_H
#define MABPL_GROUPBYCOUNTIMPLEMENTATION_H

#include <iostream>
#include <sparsehash/dense_hash_map>
//#include <folly/container/F14Map.h>
#include <absl/container/flat_hash_map.h>
#include <tsl/robin_map.h>
#include <tsl/hopscotch_map.h>
#include <type_traits>

#include "groupByCount.h"
#include "utils.h"
#include "../utils/papiHelpers.h"
#include "../library/papi.h"


#define BITS_PER_PASS 10


template<typename T>
vectorOfPairs groupByCountHash(int n, const T *inputData) {
    static_assert(std::is_integral<T>::value, "Type T must be an integer type");
    tsl::robin_map<T, int> map;

    for (auto i = 0; i < n; ++i) {
        map[inputData[i]]++;
    }

    return {map.begin(), map.end()};
}

template<typename T>
vectorOfPairs groupByCountHashGoogleDenseHashMap(int n, const T *inputData) {
    // Google SparseHash
    static_assert(std::is_integral<T>::value, "Type T must be an integer type");
    google::dense_hash_map<T, int> map;
    map.set_empty_key(-1);

    for (auto i = 0; i < n; ++i) {
        int value = inputData[i];
        auto it = map.find(value);
        if (it != map.end()) {
            ++(it->second);
        } else {
            map[value] = 1;
        }
    }

    return {map.begin(), map.end()};
}

/*template<typename T>
vectorOfPairs groupByCountHashFollyF14FastMap(int n, const T *inputData) {
    // Facebook Folly
    static_assert(std::is_integral<T>::value, "Type T must be an integer type");
    folly::F14FastMap<T, int> map;

    for (auto i = 0; i < n; ++i) {
        map[inputData[i]]++;
    }

    return {map.begin(), map.end()};
}*/

template<typename T>
vectorOfPairs groupByCountHashAbseilFlatHashMap(int n, const T *inputData) {
    // Google Abseil
    static_assert(std::is_integral<T>::value, "Type T must be an integer type");
    absl::flat_hash_map<T, int> map;

    for (auto i = 0; i < n; ++i) {
        map[inputData[i]]++;
    }

    return {map.begin(), map.end()};
}

template<typename T>
vectorOfPairs groupByCountHashTessilRobinMap(int n, const T *inputData) {
    // Tessil Robin Map
    static_assert(std::is_integral<T>::value, "Type T must be an integer type");
    tsl::robin_map<T, int> map;

    for (auto i = 0; i < n; ++i) {
        map[inputData[i]]++;
    }

    return {map.begin(), map.end()};
}

template<typename T>
vectorOfPairs groupByCountHashTessilHopscotchMap(int n, const T *inputData) {
    // Tessil Hopscotch Map
    static_assert(std::is_integral<T>::value, "Type T must be an integer type");
    tsl::hopscotch_map<T, int> map;

    for (auto i = 0; i < n; ++i) {
        map[inputData[i]]++;
    }

    return {map.begin(), map.end()};
}

template<typename T>
vectorOfPairs groupByCountSortRadix(int n, T *inputData) {
    static_assert(std::is_integral<T>::value, "Type T must be an integer type");
    int i;
    T *buffer = new T[n];
    int buckets = 1 << BITS_PER_PASS;
    int mask = buckets - 1;
    int* bucket = new int[buckets];

    int passes = static_cast<int>(std::ceil(static_cast<double>(sizeof(T) * 8) / BITS_PER_PASS)) - 1;
    bool finalCopyRequired = (passes + 1) % 2;

    for (int pass = 0; pass <= passes; pass++) {
        for (i = 0; i < buckets; i++) {
            bucket[i] = 0;
        }

        for (i = 0; i < n; i++) {
            bucket[(inputData[i] >> (pass * BITS_PER_PASS)) & mask]++;
        }

        for (i = 1; i < buckets; i++) {
            bucket[i] += bucket[i - 1];
        }

        for (i = n - 1; i >= 0; i--) {
            buffer[--bucket[(inputData[i] >> (pass * BITS_PER_PASS)) & mask]] = inputData[i];
        }

        std::swap(inputData, buffer);
    }

    if (finalCopyRequired) {
        std::swap(inputData, buffer);
        std::copy(buffer, buffer + n, inputData);
    }

    delete []buffer;
    delete []bucket;

    vectorOfPairs result;
    result.emplace_back(inputData[0], 1);

    for (i = 1; i < n; i++) {
        if (inputData[i] == result.back().first) {
            result.back().second += 1;
        } else {
            result.emplace_back(inputData[i], 1);
        }
    }

    return result;
}

template<typename T>
void groupByCountSortRadixOptAuxAgg(int start, int end, const T *inputData, int mask, int buckets, vectorOfPairs &result) {
    static_assert(std::is_integral<T>::value, "Type T must be an integer type");
    int i;
    int bucket[1 << BITS_PER_PASS] = {0};

    for (i = start; i < end; i++) {
        bucket[inputData[i] & mask]++;
    }

    int valuePrefix = inputData[start] & ~mask;

    for (i = 0; i < buckets; i++) {
        if (bucket[i] > 0) {
            result.emplace_back(valuePrefix | i, bucket[i]);
        }
    }
}

template<typename T>
void groupByCountSortRadixOptAux(int start, int end, T *inputData, T *buffer, int mask, int buckets, int pass, vectorOfPairs &result) {
    static_assert(std::is_integral<T>::value, "Type T must be an integer type");
    int i;
    int bucket[1 << BITS_PER_PASS] = {0};

    for (i = start; i < end; i++) {
        bucket[(inputData[i] >> (pass * BITS_PER_PASS)) & mask]++;
    }

    for (i = 1; i < buckets; i++) {
        bucket[i] += bucket[i - 1];
    }

    std::vector<int> partitions(bucket, bucket + buckets);
    for (i = 0; i < buckets; i++) {
        partitions[i] += start;
    }

    for (i = end - 1; i >= start; i--) {
        buffer[start + --bucket[(inputData[i] >> (pass * BITS_PER_PASS)) & mask]] = inputData[i];
    }

    std::swap(inputData, buffer);
    --pass;

    if (pass > 0) {
        if (partitions[0] > start) {
            groupByCountSortRadixOptAux(start, partitions[0], inputData, buffer, mask, buckets, pass, result);
        }
        for (i = 1; i < buckets; i++) {
            if (partitions[i] > partitions[i - 1]) {
                groupByCountSortRadixOptAux(partitions[i - 1], partitions[i], inputData, buffer, mask, buckets, pass,
                                            result);
            }
        }
    } else {
        if (partitions[0] > start) {
            groupByCountSortRadixOptAuxAgg(start, partitions[0], inputData, mask, buckets, result);
        }
        for (i = 1; i < buckets; i++) {
            if (partitions[i] > partitions[i - 1]) {
                groupByCountSortRadixOptAuxAgg(partitions[i - 1], partitions[i], inputData, mask, buckets, result);
            }
        }
    }
}

template<typename T>
vectorOfPairs groupByCountSortRadixOpt(int n, T *inputData) {
    static_assert(std::is_integral<T>::value, "Type T must be an integer type");
    int i;
    int buckets = 1 << BITS_PER_PASS;
    int mask = buckets - 1;
    int largest = 0;

    for (i = 0; i < n; i++) {
        if (inputData[i] > largest) {
            largest = inputData[i];
        }
    }
    int msbPosition = 0;
    while (largest != 0) {
        largest >>= 1;
        msbPosition++;
    }

    int passes = static_cast<int>(std::ceil(static_cast<double>(msbPosition) / BITS_PER_PASS)) - 1;
    vectorOfPairs result;

    if (passes > 0) {
        T *buffer = new T[n];
        groupByCountSortRadixOptAux(0, n, inputData, buffer, mask, buckets, passes, result);
        delete []buffer;
    } else {
        groupByCountSortRadixOptAuxAgg(0, n, inputData, mask, buckets, result);
    }

    return result;
}

template<typename T>
vectorOfPairs groupByCountSingleRadixPassThenHash(int n, T *inputData) {
    static_assert(std::is_integral<T>::value, "Type T must be an integer type");
    int i;
    T *buffer = new T[n];
    int buckets = 1 << BITS_PER_PASS;
    int mask = buckets - 1;
    int bucket[buckets];

    for (i = 0; i < buckets; i++) {
        bucket[i] = 0;
    }

    for (i = 0; i < n; i++) {
        bucket[inputData[i] & mask]++;
    }

    for (i = 1; i < buckets; i++) {
        bucket[i] += bucket[i - 1];
    }

    std::vector<int> partitions(bucket, bucket + buckets);

    for (i = n - 1; i >= 0; i--) {
        buffer[--bucket[inputData[i] & mask]] = inputData[i];
    }

    std::swap(inputData, buffer);

    vectorOfPairs result;
    if (partitions[0] > 0) {
        vectorOfPairs newResults = groupByCountHash(partitions[0], inputData);
        result.insert(result.end(), newResults.begin(), newResults.end());
    }
    for (i = 1; i < buckets; i++) {
        if (partitions[i] > partitions[i - 1]) {
            vectorOfPairs newResults = groupByCountHash(partitions[i] - partitions[i - 1],
                                                        inputData + partitions[i - 1]);
            result.insert(result.end(), newResults.begin(), newResults.end());
        }
    }

    std::swap(inputData, buffer);
    delete []buffer;

    return result;
}

template<typename T>
vectorOfPairs groupByDoubleRadixPassThenHash(int n, T *inputData) {
    static_assert(std::is_integral<T>::value, "Type T must be an integer type");
    int i;
    T *buffer = new T[n];
    int buckets = 1 << BITS_PER_PASS;
    int mask = buckets - 1;
    int bucket[buckets];
    int partitions[buckets];

    for (int pass = 0; pass < 2; pass++) {
        for (i = 0; i < buckets; i++) {
            bucket[i] = 0;
        }

        for (i = 0; i < n; i++) {
            bucket[(inputData[i] >> (pass * BITS_PER_PASS)) & mask]++;
        }

        for (i = 1; i < buckets; i++) {
            bucket[i] += bucket[i - 1];
        }

        if (pass == 1) {
            std::copy(bucket, bucket + buckets, partitions);
        }

        for (i = n - 1; i >= 0; i--) {
            buffer[--bucket[(inputData[i] >> (pass * BITS_PER_PASS)) & mask]] = inputData[i];
        }

        std::swap(inputData, buffer);

    }

    vectorOfPairs result;

    if (partitions[0] > 0) {
        vectorOfPairs newResults = groupByCountHash(partitions[0], inputData);
        result.insert(result.end(), newResults.begin(), newResults.end());
    }
    for (i = 1; i < buckets; i++) {
        if (partitions[i] > partitions[i - 1]) {
            vectorOfPairs newResults = groupByCountHash(partitions[i] - partitions[i - 1],
                                                        inputData + partitions[i - 1]);
            result.insert(result.end(), newResults.begin(), newResults.end());
        }
    }

    delete []buffer;

    return result;
}

template<typename T>
vectorOfPairs groupByCountAdaptive(int n, T *inputData) {
    static_assert(std::is_integral<T>::value, "Type T must be an integer type");
    int tuplesPerCheck = 50000;
    tsl::robin_map<T, int> map;

//    float tuplesPerLastLevelCacheMissThreshold = (0.8 * getBytesPerCacheLine()) / sizeof(T);
    float tuplesPerLastLevelCacheMissThreshold = ((7.0 / 16.0) * bytesPerCacheLine()) / sizeof(T);

    std::vector<std::string> counters = {"PERF_COUNT_HW_CACHE_MISSES"};
    long_long *counterValues = Counters::getInstance().getEvents(counters);
    long_long lastLevelCacheMisses = 0;
    long_long rowsProcessed = 0;

    int index = 0;
    int tuplesToProcess;

    unsigned long warmUpRows = l3cacheSize() / (sizeof(int) + sizeof(int));
    for (auto _ = 0; _ < static_cast<int>(warmUpRows); ++_) {map[inputData[index++]]++;}

    while (index < n) {

        tuplesToProcess = std::min(tuplesPerCheck, n - index);

        Counters::getInstance().readEventSet();
        for (auto _ = 0; _ < tuplesToProcess; ++_) {map[inputData[index++]]++;}
        Counters::getInstance().readEventSet();

        lastLevelCacheMisses += counterValues[0];
        rowsProcessed += tuplesToProcess;

        if (__builtin_expect((static_cast<float>(rowsProcessed) / lastLevelCacheMisses)
                             < tuplesPerLastLevelCacheMissThreshold, false)) {
            return groupByCountSortRadixOpt(n, inputData);
        }
    }

    return {map.begin(), map.end()};
}

template<typename T>
vectorOfPairs runGroupByCountFunction(GroupByCount groupByImplementation, int n, T *inputData) {static_assert(std::is_integral<T>::value, "Type T must be an integer type");
    switch(groupByImplementation) {
        case GroupByCount::Count_Hash:
            return groupByCountHash(n, inputData);
        case GroupByCount::Count_HashGoogleDenseHashMap:
            return groupByCountHashGoogleDenseHashMap(n, inputData);
        case GroupByCount::Count_HashFollyF14FastMap:
            std::cout << "Folly requires removing '-march=native' flag" << std::endl;
            exit(1);
//            return groupByHashFollyF14FastMap(n, inputData);
        case GroupByCount::Count_HashAbseilFlatHashMap:
            return groupByCountHashAbseilFlatHashMap(n, inputData);
        case GroupByCount::Count_HashTessilRobinMap:
            return groupByCountHashTessilRobinMap(n, inputData);
        case GroupByCount::Count_HashTessilHopscotchMap:
            return groupByCountHashTessilHopscotchMap(n, inputData);
        case GroupByCount::Count_SortRadix:
            return groupByCountSortRadix(n, inputData);
        case GroupByCount::Count_SortRadixOpt:
            return groupByCountSortRadixOpt(n, inputData);
        case GroupByCount::Count_SingleRadixPassThenHash:
            return groupByCountSingleRadixPassThenHash(n, inputData);
        case GroupByCount::Count_DoubleRadixPassThenHash:
            return groupByDoubleRadixPassThenHash(n, inputData);
        case GroupByCount::Count_Adaptive:
            return groupByCountAdaptive(n, inputData);
        default:
            std::cout << "Invalid selection of 'GroupBy' implementation!" << std::endl;
            exit(1);
    }
}

#endif //MABPL_GROUPBYCOUNTIMPLEMENTATION_H
