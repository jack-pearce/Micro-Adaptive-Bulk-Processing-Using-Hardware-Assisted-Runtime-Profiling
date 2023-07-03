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

#include "groupBy.h"
#include "utils.h"
#include "../utils/papiHelpers.h"
#include "../library/papi.h"


#define BITS_PER_PASS 10


vectorPair groupByHash(int n, const int *inputData) {
    absl::flat_hash_map<int, int> map;

    for (auto i = 0; i < n; ++i) {
        map[inputData[i]]++;
    }

    vectorPair result;
    for (const auto& pair : map) {
        result.first.push_back(pair.first);
        result.second.push_back(pair.second);
    }

    return result;
}

vectorPair groupByHashGoogleDenseHashMap(int n, const int *inputData) {
    // Google SparseHash
    google::dense_hash_map<int, int> map;
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

    vectorPair result;
    for (const auto& pair : map) {
        result.first.push_back(pair.first);
        result.second.push_back(pair.second);
    }

    return result;
}

/*vectorOfPairs groupByHashFollyF14FastMap(int n, const int *inputData) {
    // Facebook Folly
    folly::F14FastMap<int, int> map;

    for (auto i = 0; i < n; ++i) {
        map[inputData[i]]++;
    }

    vectorPair result;
    for (const auto& pair : map) {
        result.first.push_back(pair.first);
        result.second.push_back(pair.second);
    }

    return result;
}*/

vectorPair groupByHashAbseilFlatHashMap(int n, const int *inputData) {
    // Google Abseil
    absl::flat_hash_map<int, int> map;

    for (auto i = 0; i < n; ++i) {
        map[inputData[i]]++;
    }

    vectorPair result;
    for (const auto& pair : map) {
        result.first.push_back(pair.first);
        result.second.push_back(pair.second);
    }

    return result;
}

vectorPair groupByHashTessilRobinMap(int n, const int *inputData) {
    // Tessil Robin Map
    tsl::robin_map<int, int> map;

    for (auto i = 0; i < n; ++i) {
        map[inputData[i]]++;
    }

    vectorPair result;
    for (const auto& pair : map) {
        result.first.push_back(pair.first);
        result.second.push_back(pair.second);
    }

    return result;
}

vectorPair groupByHashTessilHopscotchMap(int n, const int *inputData) {
    // Tessil Hopscotch Map
    tsl::hopscotch_map<int, int> map;

    for (auto i = 0; i < n; ++i) {
        map[inputData[i]]++;
    }

    vectorPair result;
    for (const auto& pair : map) {
        result.first.push_back(pair.first);
        result.second.push_back(pair.second);
    }

    return result;
}

vectorPair groupBySortRadix(int n, int *inputData) {
    int i;
    int *buffer = new int[n];
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

    vectorPair result;
    result.first.push_back(inputData[0]);
    result.second.push_back(1);

    for (i = 1; i < n; i++) {
        if (inputData[i] == result.first.back()) {
            result.second.back() += 1;
        } else {
            result.first.push_back(inputData[i]);
            result.second.push_back(1);
        }
    }

    return result;
}

void groupBySortRadixOptAuxAgg(int start, int end, const int *inputData, int mask, int buckets, vectorPair &result) {
    int i;
    int bucket[1 << BITS_PER_PASS] = {0};

    for (i = start; i < end; i++) {
        bucket[inputData[i] & mask]++;
    }

    int valuePrefix = inputData[start] & ~mask;

    for (i = 0; i < buckets; i++) {
        if (bucket[i] > 0) {
            result.first.push_back(valuePrefix | i);
            result.second.push_back(bucket[i]);
        }
    }
}

void groupBySortRadixOptAux(int start, int end, int *inputData, int *buffer, int mask, int buckets, int pass, vectorPair &result) {
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
            groupBySortRadixOptAux(start, partitions[0], inputData, buffer, mask, buckets, pass, result);
        }
        for (i = 1; i < buckets; i++) {
            if (partitions[i] > partitions[i - 1]) {
                groupBySortRadixOptAux(partitions[i - 1], partitions[i], inputData, buffer, mask, buckets, pass, result);
            }
        }
    } else {
        if (partitions[0] > start) {
            groupBySortRadixOptAuxAgg(start, partitions[0], inputData, mask, buckets, result);
        }
        for (i = 1; i < buckets; i++) {
            if (partitions[i] > partitions[i - 1]) {
                groupBySortRadixOptAuxAgg(partitions[i - 1], partitions[i], inputData, mask, buckets, result);
            }
        }
    }
}

vectorPair groupBySortRadixOpt(int n, int *inputData) {
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
    vectorPair result;

    if (passes > 0) {
        int *buffer = new int[n];
        groupBySortRadixOptAux(0, n, inputData, buffer, mask, buckets, passes, result);
        delete []buffer;
    } else {
        groupBySortRadixOptAuxAgg(0, n, inputData, mask, buckets, result);
    }

    return result;
}

vectorPair groupBySingleRadixPassThenHash(int n, int *inputData) {
    int i;
    int *buffer = new int[n];
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

    vectorPair result;
    if (partitions[0] > 0) {
        vectorPair newResults = groupByHash(partitions[0], inputData);
        result.first.insert(result.first.end(), newResults.first.begin(), newResults.first.end());
        result.second.insert(result.second.end(), newResults.second.begin(), newResults.second.end());
    }
    for (i = 1; i < buckets; i++) {
        if (partitions[i] > partitions[i - 1]) {
            vectorPair newResults = groupByHash(partitions[i] - partitions[i - 1], inputData + partitions[i - 1]);
            result.first.insert(result.first.end(), newResults.first.begin(), newResults.first.end());
            result.second.insert(result.second.end(), newResults.second.begin(), newResults.second.end());
        }
    }

    std::swap(inputData, buffer);
    delete []buffer;

    return result;
}

vectorPair groupByDoubleRadixPassThenHash(int n, int *inputData) {
    int i;
    int *buffer = new int[n];
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

    vectorPair result;

    if (partitions[0] > 0) {
        vectorPair newResults = groupByHash(partitions[0], inputData);
        result.first.insert(result.first.end(), newResults.first.begin(), newResults.first.end());
        result.second.insert(result.second.end(), newResults.second.begin(), newResults.second.end());
    }
    for (i = 1; i < buckets; i++) {
        if (partitions[i] > partitions[i - 1]) {
            vectorPair newResults = groupByHash(partitions[i] - partitions[i - 1], inputData + partitions[i - 1]);
            result.first.insert(result.first.end(), newResults.first.begin(), newResults.first.end());
            result.second.insert(result.second.end(), newResults.second.begin(), newResults.second.end());
        }
    }

    delete []buffer;

    return result;
}

vectorPair groupByAdaptive(int n, int *inputData) {
    int tuplesPerCheck = 50000;
    absl::flat_hash_map<int, int> map;

    float tuplesPerLastLevelCacheMissThreshold = 12.5;

    std::vector<std::string> counters = {"PERF_COUNT_HW_CACHE_MISSES"};
    long_long *counterValues = Counters::getInstance().getEvents(counters);
    long_long lastLevelCacheMisses = 0;
    long_long rowsProcessed = 0;

    int index = 0;
    int tuplesToProcess;

    unsigned long warmUpRows = getL3cacheSize() / (sizeof(int) + sizeof(int));
    for (auto _ = 0; _ < warmUpRows; ++_) {map[inputData[index++]]++;}

    while (index < n) {

        tuplesToProcess = std::min(tuplesPerCheck, n - index);

        Counters::getInstance().readEventSet();
        for (auto _ = 0; _ < tuplesToProcess; ++_) {map[inputData[index++]]++;}
        Counters::getInstance().readEventSet();

        lastLevelCacheMisses += counterValues[0];
        rowsProcessed += tuplesToProcess;

        if (__builtin_expect((static_cast<float>(rowsProcessed) / lastLevelCacheMisses)
                                < tuplesPerLastLevelCacheMissThreshold, false)) {
            return groupBySortRadixOpt(n, inputData);
        }
    }

    vectorPair result;
    for (const auto& pair : map) {
        result.first.push_back(pair.first);
        result.second.push_back(pair.second);
    }

    return result;
}

vectorPair runGroupByFunction(GroupBy groupByImplementation, int n, int *inputData) {
    switch(groupByImplementation) {
        case GroupBy::Hash:
            return groupByHash(n, inputData);
        case GroupBy::HashGoogleDenseHashMap:
            return groupByHashGoogleDenseHashMap(n, inputData);
        case GroupBy::HashFollyF14FastMap:
            std::cout << "Folly requires removing '-march=native' flag" << std::endl;
            exit(1);
//            return groupByHashFollyF14FastMap(n, inputData);
        case GroupBy::HashAbseilFlatHashMap:
            return groupByHashAbseilFlatHashMap(n, inputData);
        case GroupBy::HashTessilRobinMap:
            return groupByHashTessilRobinMap(n, inputData);
        case GroupBy::HashTessilHopscotchMap:
            return groupByHashTessilHopscotchMap(n, inputData);
        case GroupBy::SortRadix:
            return groupBySortRadix(n, inputData);
        case GroupBy::SortRadixOpt:
            return groupBySortRadixOpt(n, inputData);
        case GroupBy::SingleRadixPassThenHash:
            return groupBySingleRadixPassThenHash(n, inputData);
        case GroupBy::DoubleRadixPassThenHash:
            return groupByDoubleRadixPassThenHash(n, inputData);
        case GroupBy::Adaptive:
            return groupByAdaptive(n, inputData);
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