#include <iostream>
#include <sparsehash/dense_hash_map>
#include <list>
#include <memory>
#include <algorithm>
#include <cmath>
#include <stdio.h>

#include "groupBy.h"
#include "utils.h"
#include "../utils/papiHelpers.h"
#include "../library/papi.h"

#define PARTITION_BUCKETS 256


Run groupByHash(int n, const int *inputData) {

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

// Function to find the maximum element in the array
int getMax(const int* arr, int n) {
    int max = arr[0];
    for (int i = 1; i < n; ++i) {
        if (arr[i] > max) {
            max = arr[i];
        }
    }
    return max;
}

// Counting sort used as a subroutine in Radix Sort
void countingSort(int* arr, int n, int exp) {
    std::vector<int> output(n);
    std::vector<int> count(10, 0);

    // Store the count of occurrences in count[]
    for (int i = 0; i < n; ++i) {
        count[(arr[i] / exp) % 10]++;
    }

    // Change count[i] so that count[i] contains the actual
    // position of this digit in the output[]
    for (int i = 1; i < 10; ++i) {
        count[i] += count[i - 1];
    }

    // Build the output array
    for (int i = n - 1; i >= 0; --i) {
        output[count[(arr[i] / exp) % 10] - 1] = arr[i];
        count[(arr[i] / exp) % 10]--;
    }

    // Copy the output array to arr[] so that arr[] contains
    // sorted numbers according to the current digit
    for (int i = 0; i < n; ++i) {
        arr[i] = output[i];
    }
}

/*
Run groupBySort(int n, int *inputData) {
    // Find the maximum number to determine the number of digits
    int max = getMax(inputData, n);

    // Perform counting sort for every digit
    for (int exp = 1; max / exp > 0; exp *= 10) {
        countingSort(inputData, n, exp);
    }

    Run result;
    return result;
}
*/

#define BITS_PER_PASS 10

Run groupBySort(int n, int *inputData) {
    int i;
    int *output = new int[n];
    int buckets = 1 << BITS_PER_PASS;
    int mask = buckets - 1;
    int* bucket = new int[buckets];
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
    bool finalCopyRequired = (passes + 1) % 2;

    while (passes >= 0) {
        for (i = 0; i < buckets; i++) {
            bucket[i] = 0;
        }

        for (i = 0; i < n; i++) {
            bucket[(inputData[i] >> (passes * BITS_PER_PASS)) & mask]++;
        }

        for (i = 1; i < buckets; i++) {
            bucket[i] += bucket[i - 1];
        }

        for (i = n - 1; i >= 0; i--) {
            output[--bucket[(inputData[i] >> (passes * BITS_PER_PASS)) & mask]] = inputData[i];
        }


        std::swap(inputData, output);
        --passes;
    }

    // Need to work individually in partitions
    // Need to use insertion sort when partition gets small enough

    if (finalCopyRequired) {
        std::swap(inputData, output);
        std::copy(output, output + n, inputData);
    }

    delete []output;

    Run result;
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
        case GroupBy::Sort:
            return groupBySort(n, inputData);
        case GroupBy::Adaptive:
            return groupByAdaptive(n, inputData);
        default:
            std::cout << "Invalid selection of 'Select' implementation!" << std::endl;
            exit(1);
    }
}

std::string getGroupByName(GroupBy groupByImplementation) {
    switch(groupByImplementation) {
        case GroupBy::Hash:
            return "GroupBy_Hash";
        case GroupBy::Sort:
            return "GroupBy_Sort";
        case GroupBy::Adaptive:
            return "GroupBy_Adaptive";
        default:
            std::cout << "Invalid selection of 'GroupBy' implementation!" << std::endl;
            exit(1);
    }
}