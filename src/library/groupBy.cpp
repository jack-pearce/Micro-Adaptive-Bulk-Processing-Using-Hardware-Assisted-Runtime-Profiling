#include <iostream>
#include <sparsehash/dense_hash_map>
#include <list>

#include "groupBy.h"
#include "utils.h"


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

//PairVector groupBySort(int n, const int *inputData) {
//}

/*Runs groupByAdaptiveHashLevelZero(int n, const int *inputData) {
    int level = 0;

    auto *map = new google::dense_hash_map<int, int>();
    initialiseMap(*map);

//    std::vector<std::unique_ptr<google::dense_hash_map<int, int>>> maps;

    for (auto i = 0; i < n; ++i) {
        int value = inputData[i];

        auto it = map->find(value);
        if (it != map->end()) {
            ++(it->second);
        } else {
            (*map)[value] = 1;
        }

*//*        if (static_cast<double>(map->size()) / static_cast<double>(map->bucket_count()) >= 0.25) {
            maps.push_back(std::make_unique<google::dense_hash_map<int, int>>(*map));
            map = new google::dense_hash_map<int, int>();
            initialiseMap(*map);
        }*//*
    }

    // CHANGE TO A LIST OF VECTORS WHEN WE CHANGE LINEAR PROBING TO WORK IN BLOCKS ONLY
    std::vector<Run*> resultRuns(256, nullptr);

    for (auto& runPtr : resultRuns) {
        runPtr = new Run();  // Create a new empty Run
    }

    auto hashFunc = map->hash_function();

    for (const auto& entry : *map) {
        const std::size_t hash_value = hashFunc(entry.first);

        // Extract the 8 bits according to the level (0 is 8 LSBs, 1 is next 8 LSBs, etc.)
        unsigned long partition_index = (hash_value & (0xFF << (8 * level))) >> (8 * level);

        // CHANGE LINEAR PROBING TO WORK IN BLOCKS ONLY, THEN WE CAN SPLIT HASH MAP DIRECTLY

        resultRuns[partition_index]->push_back(entry);
    }

    return resultRuns;

// NEED TO COMBINE MULTIPLE TABLES HERE. OUTPUT SHOULD BE AN ARRAY OF PAIRS FOR EACH RUN (I.E. PARTITION / HASH VALUE)
// THIS IS WHAT LEADS TO A SECOND PASS IF THE CARDINALITY IS HIGHER THAN THE SIZE OF THE CACHE
// THIS WILL BE THE SAME OUTPUT AS THAT FOR THE PARTITIONING FUNCTION

//    Run result(map->begin(), map->end());
//    return result;
}
// STORE AS A LIST OF ARRAYS*/

inline void initialiseMap(google::dense_hash_map<int, int> &map) {
    map.set_empty_key(-1);
    map.resize(getL3cacheSize() / (sizeof(int) + sizeof(int)));
}

Runs groupByAdaptiveHash(Run& inputRun, int level) {

    auto *map = new google::dense_hash_map<int, int>();
    initialiseMap(*map);

//    std::vector<std::unique_ptr<google::dense_hash_map<int, int>>> maps;

    for (auto &pair : inputRun) {

        //  CHANGE GOOGLE:DENSE_HASH_MAP SO THAT IT ONLY DOES LINEAR PROBING WITHIN BLOCKS TO SPLIT WITHOUT
        //  RECALCULATING HASH VALUES

        auto it = map->find(pair.first);
        if (it != map->end()) {
            (it->second) += level == 0 ? 1: pair.second;
        } else {
            (*map)[pair.first] = level == 0 ? 1: pair.second;
        }

/*        if (static_cast<double>(map->size()) / static_cast<double>(map->bucket_count()) >= 0.25) {
            maps.push_back(std::make_unique<google::dense_hash_map<int, int>>(*map));
            map = new google::dense_hash_map<int, int>();
            initialiseMap(*map);
        }*/
    }

    // CHANGE TO A LIST OF VECTORS WHEN WE CHANGE LINEAR PROBING TO WORK IN BLOCKS ONLY
    Runs partitionedRuns(256, nullptr);

    for (auto& runPtr : partitionedRuns) {
        runPtr = new Run();  // Create a new empty Run
    }

    auto hashFunc = map->hash_function();

    for (const auto& pair : *map) {
        const std::size_t hashValue = hashFunc(pair.first);

        // Extract the 8 bits according to the level (0 is 8 LSBs, 1 is next 8 LSBs, etc.)
        unsigned long partitionIndex = (hashValue & (0xFF << (8 * level))) >> (8 * level);

        // CHANGE LINEAR PROBING TO WORK IN BLOCKS ONLY, THEN WE CAN SPLIT HASH MAP DIRECTLY

        partitionedRuns[partitionIndex]->push_back(pair);
    }

    return partitionedRuns;
}

void printRun(Run &run) {
    for (auto pair : run) {
        std::cout << pair.first << ", " << pair.second << std::endl;
    }
}

void printRuns(Runs &runs) {
    int count = 0;
    for (auto run : runs) {
        std::cout << "Run: " << count++ << std::endl;
        printRun(*run);
    }
}

Run groupByAdaptiveAux(std::vector<Run*> &inputRuns, int level) {

//    std::cout << "Level " << level << std::endl;

    if (level != 0 && inputRuns.size() == 1 && inputRuns[0]->size() == 1) {
        return *inputRuns[0];
    }

/*    if (level == 1) {
        printRuns(runs);
        exit(2);
    }*/

//    printRuns(runs);

    std::vector<Runs> producedGroupsOfRuns;
    producedGroupsOfRuns.reserve(inputRuns.size());

    for (auto &run : inputRuns) {
        producedGroupsOfRuns.push_back(groupByAdaptiveHash(*run, level));
    }

//    printRuns(producedRuns[0]);

    Run result;

    for (auto partitionNumber = 0; partitionNumber < 256; ++partitionNumber) {
        Runs runsInPartition;
        for (auto &producedGroupOfRuns : producedGroupsOfRuns) {
//            printRuns(oneSetOfProducedRuns);
            if (!producedGroupOfRuns[partitionNumber]->empty()) {
//                std::cout << "Here" << std::endl;
//                printRun(*oneSetOfProducedRuns[i]);
                runsInPartition.push_back(producedGroupOfRuns[partitionNumber]);
            }
        }
//        printRuns(runsToComplete);
        if (!runsInPartition.empty()) {
            Run aggregatedPartitionResult = groupByAdaptiveAux(runsInPartition, level + 1);
            result.insert(result.end(), aggregatedPartitionResult.begin(), aggregatedPartitionResult.end());
//            printRun(resultRun);
        }
    }

//    printRun(result);

    return result;
}


Run groupByAdaptive(int n, const int *inputData) {

    // Initially just do a single run for initial call - multiple runs is just for parallelization

    // Split into runs here
    // Split by ranges of 256 i.e. 256 partitions

    Run run;
    run.reserve(n);

    for (int i = 0; i < n; ++i) {
        run.emplace_back(inputData[i], 0);
    }

    Runs runs;
    runs.push_back(&run);

    return groupByAdaptiveAux(runs, 0);
}

std::vector<std::pair<int, int>> runGroupByFunction(GroupBy groupByImplementation, int n, const int *inputData) {
    switch(groupByImplementation) {
        case GroupBy::Hash:
            return groupByHash(n, inputData);
        case GroupBy::Sort:
//            return groupBySort(n, inputData);
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