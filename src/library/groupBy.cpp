#include <iostream>
#include <sparsehash/dense_hash_map>
#include <list>
#include <memory>
#include <algorithm>
#include <cmath>
#include <folly/container/F14Map.h>
#include <absl/container/flat_hash_map.h>
#include <tsl/robin_map.h>
#include <tsl/hopscotch_map.h>

#include "groupBy.h"
#include "utils.h"
#include "../utils/papiHelpers.h"
#include "../library/papi.h"


#define BITS_PER_PASS 12
#define PARTITION_BUCKETS 256


Run groupByHash(int n, const int *inputData) {
    tsl::hopscotch_map<int, int> map;  // Tessil Hopscotch Map

    for (auto i = 0; i < n; ++i) {
        map[inputData[i]]++;
    }

    Run result(map.begin(), map.end());
    return result;
}

Run groupByHashGoogleDenseHashMap(int n, const int *inputData) {
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

    Run result(map.begin(), map.end());
    return result;
}

/*Run groupByHashFollyF14FastMap(int n, const int *inputData) {
    // Facebook Folly
    folly::F14FastMap<int, int> map;

    for (auto i = 0; i < n; ++i) {
        map[inputData[i]]++;
    }

    Run result(map.begin(), map.end());
    return result;
}*/

Run groupByHashAbseilFlatHashMap(int n, const int *inputData) {
    // Google Abseil
    absl::flat_hash_map<int, int> map;

    for (auto i = 0; i < n; ++i) {
        map[inputData[i]]++;
    }

    Run result(map.begin(), map.end());
    return result;
}

Run groupByHashTessilRobinMap(int n, const int *inputData) {
    // Tessil Robin Map
    tsl::robin_map<int, int> map;

    for (auto i = 0; i < n; ++i) {
        map[inputData[i]]++;
    }

    Run result(map.begin(), map.end());
    return result;
}

Run groupByHashTessilHopscotchMap(int n, const int *inputData) {
    // Tessil Hopscotch Map
    tsl::hopscotch_map<int, int> map;

    for (auto i = 0; i < n; ++i) {
        map[inputData[i]]++;
    }

    Run result(map.begin(), map.end());
    return result;
}

Run groupBySortRadix(int n, int *inputData) {
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

    Run result;
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

void groupBySortRadixOptAuxAgg(int start, int end, const int *inputData, int mask, int buckets, Run &result) {
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

void groupBySortRadixOptAux(int start, int end, int *inputData, int *buffer, int mask, int buckets, int pass, Run &result) {
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

Run groupBySortRadixOpt(int n, int *inputData) {
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
    Run result;

    if (passes > 0) {
        int *buffer = new int[n];
        groupBySortRadixOptAux(0, n, inputData, buffer, mask, buckets, passes, result);
        delete []buffer;
    } else {
        groupBySortRadixOptAuxAgg(0, n, inputData, mask, buckets, result);
    }

    return result;
}

inline void initialiseMap(google::dense_hash_map<int, int> &map) {
    map.set_empty_key(-1);
//    map.resize(getL3cacheSize() / (4 * sizeof(int)));
}

Runs groupByAdaptiveHash(Run& inputRun, int level) {

//    google::dense_hash_map<int, int> map;

    std::unique_ptr<google::dense_hash_map<int, int>> map = std::make_unique<google::dense_hash_map<int, int>>();
    initialiseMap(*map);

    std::vector<std::unique_ptr<google::dense_hash_map<int, int>>> maps;

    for (auto &pair : inputRun) {

        auto it = map->find(pair.first);
        if (it != map->end()) {
            (it->second) += level == 0 ? 1: pair.second;
        } else {
            (*map)[pair.first] = level == 0 ? 1: pair.second;
        }

//        if (static_cast<double>(map->size()) / static_cast<double>(map->bucket_count()) >= 0.25) {
/*        if (static_cast<double>(map->bucket_count()) >= 500000) {
            maps.push_back(std::move(map));
            map = std::make_unique<google::dense_hash_map<int, int>>();
            initialiseMap(*map);
        }*/
    }

    // CHANGE TO A LIST OF VECTORS WHEN WE CHANGE LINEAR PROBING TO WORK IN BLOCKS ONLY
    Runs partitionedRuns(PARTITION_BUCKETS);

    for (auto& runPtr : partitionedRuns) {
        runPtr = std::make_unique<Run>();
    }

    auto hashFunc = map->hash_function();

//    maps.push_back(std::move(map));

    // CHANGE LINEAR PROBING TO WORK IN BLOCKS ONLY, THEN WE CAN SPLIT HASH MAP DIRECTLY
    for (const auto& pair : *map) {
        const std::size_t hashValue = hashFunc(pair.first);
        unsigned long partitionIndex = (hashValue >> (8 * level)) & 0xFF;
        partitionedRuns[partitionIndex]->push_back(pair);
    }

/*    for (auto& mapPtr : maps) {
        auto& mapRef = *mapPtr;
        for (const auto& pair : mapRef) {
            const std::size_t hashValue = hashFunc(pair.first);
            unsigned long partitionIndex = (hashValue >> (8 * level)) & 0xFF;
            partitionedRuns[partitionIndex]->push_back(pair);
        }
    }*/

    return partitionedRuns;
}

Run groupByAdaptiveAux(Runs &inputRuns, int level) {

    if (level != 0 && inputRuns.size() == 1 && inputRuns[0]->size() == 1) {
        return *inputRuns[0];
    }

    std::vector<Runs> producedGroupsOfRuns;
    producedGroupsOfRuns.reserve(inputRuns.size());

    for (auto &run : inputRuns) {
        producedGroupsOfRuns.push_back(groupByAdaptiveHash(*run, level));
    }

    Run result;

    for (auto partitionNumber = 0; partitionNumber < PARTITION_BUCKETS; ++partitionNumber) {
        Runs runsInPartition;
        runsInPartition.push_back(std::make_unique<Run>());
        Run& runInPartition = *runsInPartition.back();

        for (auto &producedGroupOfRuns : producedGroupsOfRuns) {

            if (!producedGroupOfRuns[partitionNumber]->empty()) {
                runInPartition.insert(
                        runInPartition.end(),
                        std::make_move_iterator(producedGroupOfRuns[partitionNumber]->begin()),
                        std::make_move_iterator(producedGroupOfRuns[partitionNumber]->end())
                );

            }
        }

        if (!runsInPartition[0]->empty()) {
            Run aggregatedPartitionResult = groupByAdaptiveAux(runsInPartition, level + 1);
            result.insert(result.end(),
                          std::make_move_iterator(aggregatedPartitionResult.begin()),
                          std::make_move_iterator(aggregatedPartitionResult.end()));
        }
    }

    return result;
}


Run groupByAdaptive(int n, const int *inputData) {

//    int elementsPerRun = std::max(n/PARTITION_BUCKETS, 500000);

    int elementsPerRun = 500000;

    Runs runs;
    int index = 0;

    while (index < n) {
        runs.push_back(std::make_unique<Run>());
        Run& run = *runs.back();
        int elementsToAdd = std::min(elementsPerRun, n - index);
        run.reserve(elementsToAdd);
        for (int i = 0; i < elementsToAdd; ++i) {
            run.emplace_back(inputData[index++], 0);
        }
    }

//    std::cout << "Initial number of runs: " << runs.size() << std::endl;

//    printRuns(runs);

/*    Runs runs;
    runs.push_back(std::make_unique<Run>());
    Run& run = *runs.back();

    for (int i = 0; i < n; ++i) {
        run.emplace_back(inputData[i], 0);
    }*/

//    long_long cycles = *Counters::getInstance().readEventSet();

    auto result = groupByAdaptiveAux(runs, 0);

//    std::cout << *Counters::getInstance().readEventSet() - cycles << std::endl;

    return result;
}

std::vector<std::pair<int, int>> runGroupByFunction(GroupBy groupByImplementation, int n, int *inputData) {
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
        case GroupBy::Adaptive:
            return "GroupBy_Adaptive";
        default:
            std::cout << "Invalid selection of 'GroupBy' implementation!" << std::endl;
            exit(1);
    }
}