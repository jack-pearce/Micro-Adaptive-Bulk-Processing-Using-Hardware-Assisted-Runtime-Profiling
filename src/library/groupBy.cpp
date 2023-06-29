#include <iostream>
#include <sparsehash/dense_hash_map>
#include <list>
#include <memory>
#include <algorithm>

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

Run groupBySort(int n, const int *inputData) {
    std::vector<int> input(inputData, inputData + n);
    std::sort(input.begin(), input.end());

    Run result;
    result.emplace_back(input[0], 1);

    for (auto it = std::next(std::begin(input)); it != std::end(input); ++it) {
        if (*it == result.back().first) {
            result.back().second += 1;
        } else {
            result.emplace_back(*it, 1);
        }
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

void printRun(Run &run) {
    for (auto pair : run) {
        std::cout << pair.first << ", " << pair.second << std::endl;
    }
}

void printRuns(Runs &runs) {
    int count = 0;
    for (auto &run : runs) {
        if (run && !run->empty()) {
            std::cout << "Run: " << count++ << std::endl;
            printRun(*run);
        }
    }
}

Run groupByAdaptiveAux(Runs &inputRuns, int level) {

//    std::cout << "Level " << level << std::endl;

    if (level != 0 && inputRuns.size() == 1 && inputRuns[0]->size() == 1) {
        return *inputRuns[0];
    }

/*    if (level == 5) {
//        printRuns(inputRuns);
        exit(2);
    }*/

//    printRuns(inputRuns);
//    std::cout << inputRuns.size() << std::endl;

    std::vector<Runs> producedGroupsOfRuns;
    producedGroupsOfRuns.reserve(inputRuns.size());

    for (auto &run : inputRuns) {
        producedGroupsOfRuns.push_back(groupByAdaptiveHash(*run, level));
    }

//    printRuns(producedGroupsOfRuns[0]);
//    printRuns(producedGroupsOfRuns[1]);

    Run result;

    for (auto partitionNumber = 0; partitionNumber < PARTITION_BUCKETS; ++partitionNumber) {
        Runs runsInPartition;
        runsInPartition.push_back(std::make_unique<Run>());
        Run& runInPartition = *runsInPartition.back();

        for (auto &producedGroupOfRuns : producedGroupsOfRuns) {
//            printRuns(producedGroupOfRuns);

            if (!producedGroupOfRuns[partitionNumber]->empty()) {
//                std::cout << "Here, partition number: " << partitionNumber << std::endl;
//                printRun(*(producedGroupOfRuns[partitionNumber]));
//                runsInPartition.push_back(producedGroupOfRuns[partitionNumber]);
                runInPartition.insert(
                        runInPartition.end(),
                        std::make_move_iterator(producedGroupOfRuns[partitionNumber]->begin()),
                        std::make_move_iterator(producedGroupOfRuns[partitionNumber]->end())
                );

            }
        }
//        printRuns(runsInPartition);
        if (!runsInPartition[0]->empty()) {
            Run aggregatedPartitionResult = groupByAdaptiveAux(runsInPartition, level + 1);
            result.insert(result.end(),
                          std::make_move_iterator(aggregatedPartitionResult.begin()),
                          std::make_move_iterator(aggregatedPartitionResult.end()));
//            result.insert(result.end(), aggregatedPartitionResult.begin(), aggregatedPartitionResult.end());
//            printRun(resultRun);
        }
    }

//    printRun(result);

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

std::vector<std::pair<int, int>> runGroupByFunction(GroupBy groupByImplementation, int n, const int *inputData) {
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