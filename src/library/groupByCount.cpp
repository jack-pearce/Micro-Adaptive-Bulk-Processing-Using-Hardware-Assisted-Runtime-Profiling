#include <iostream>
#include <sparsehash/dense_hash_map>
//#include <folly/container/F14Map.h>
#include <absl/container/flat_hash_map.h>
#include <tsl/robin_map.h>
#include <tsl/hopscotch_map.h>

#include "groupByCount.h"
#include "utils.h"
#include "../utils/papiHelpers.h"
#include "../library/papi.h"


#define BITS_PER_PASS 10


vectorOfPairs groupByCountHash(int n, const int *inputData) {
    absl::flat_hash_map<int, int> map;

    absl::flat_hash_map<int, int>::iterator it;
    for (auto i = 0; i < n; ++i) {
        it = map.find(inputData[i]);
        if (it != map.end()) {
            ++(it->second);
        } else {
            map.insert({inputData[i], 1});
        }
    }

    return {map.begin(), map.end()};
}

vectorOfPairs groupByCountHashGoogleDenseHashMap(int n, const int *inputData) {
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

    return {map.begin(), map.end()};
}

/*vectorOfPairs groupByCountHashFollyF14FastMap(int n, const int *inputData) {
    // Facebook Folly
    folly::F14FastMap<int, int> map;

    for (auto i = 0; i < n; ++i) {
        map[inputData[i]]++;
    }

    return {map.begin(), map.end()};
}*/

vectorOfPairs groupByCountHashAbseilFlatHashMap(int n, const int *inputData) {
    // Google Abseil
    absl::flat_hash_map<int, int> map;

    for (auto i = 0; i < n; ++i) {
        map[inputData[i]]++;
    }

    return {map.begin(), map.end()};
}

vectorOfPairs groupByCountHashTessilRobinMap(int n, const int *inputData) {
    // Tessil Robin Map
    tsl::robin_map<int, int> map;

    for (auto i = 0; i < n; ++i) {
        map[inputData[i]]++;
    }

    return {map.begin(), map.end()};
}

vectorOfPairs groupByCountHashTessilHopscotchMap(int n, const int *inputData) {
    // Tessil Hopscotch Map
    tsl::hopscotch_map<int, int> map;

    for (auto i = 0; i < n; ++i) {
        map[inputData[i]]++;
    }

    return {map.begin(), map.end()};
}

vectorOfPairs groupByCountSortRadix(int n, int *inputData) {
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

void groupByCountSortRadixOptAuxAgg(int start, int end, const int *inputData, int mask, int buckets, vectorOfPairs &result) {
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

void groupByCountSortRadixOptAux(int start, int end, int *inputData, int *buffer, int mask, int buckets, int pass, vectorOfPairs &result) {
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

vectorOfPairs groupByCountSortRadixOpt(int n, int *inputData) {
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
        int *buffer = new int[n];
        groupByCountSortRadixOptAux(0, n, inputData, buffer, mask, buckets, passes, result);
        delete []buffer;
    } else {
        groupByCountSortRadixOptAuxAgg(0, n, inputData, mask, buckets, result);
    }

    return result;
}

vectorOfPairs groupByCountSingleRadixPassThenHash(int n, int *inputData) {
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

vectorOfPairs groupByDoubleRadixPassThenHash(int n, int *inputData) {
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

vectorOfPairs groupByCountAdaptive(int n, int *inputData) {
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

vectorOfPairs runGroupByCountFunction(GroupByCount groupByImplementation, int n, int *inputData) {
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

std::string getGroupByCountName(GroupByCount groupByImplementation) {
    switch(groupByImplementation) {
        case GroupByCount::Count_Hash:
            return "GroupByCount_Hash";
        case GroupByCount::Count_HashGoogleDenseHashMap:
            return "GroupByCount_HashGoogleDenseHashMap";
        case GroupByCount::Count_HashFollyF14FastMap:
            return "GroupByCount_HashFollyF14FastMap";
        case GroupByCount::Count_HashAbseilFlatHashMap:
            return "GroupByCount_HashAbseilFlatHashMap";
        case GroupByCount::Count_HashTessilRobinMap:
            return "GroupByCount_HashTessilRobinMap";
        case GroupByCount::Count_HashTessilHopscotchMap:
            return "GroupByCount_HashTessilHopscotchMap";
        case GroupByCount::Count_SortRadix:
            return "GroupByCount_SortRadix";
        case GroupByCount::Count_SortRadixOpt:
            return "GroupByCount_SortRadixOptimal";
        case GroupByCount::Count_SingleRadixPassThenHash:
            return "GroupByCount_SingleRadixPassThenHash";
        case GroupByCount::Count_DoubleRadixPassThenHash:
            return "GroupByCount_DoubleRadixPassThenHash";
        case GroupByCount::Count_Adaptive:
            return "GroupByCount_Adaptive";
        default:
            std::cout << "Invalid selection of 'GroupByCount' implementation!" << std::endl;
            exit(1);
    }
}