#include <iostream>
#include <sparsehash/dense_hash_map>

#include "groupBy.h"


std::vector<std::pair<int, int>> groupByHash(int n, const int *inputData) {

    google::dense_hash_map<int, int> hashmap;
    hashmap.set_empty_key(-1);  // Specify the empty key

//    hashmap.resize(getL3cacheSize() / (sizeof(int) + sizeof(int)));

    for (auto i = 0; i < n; ++i) {
        int value = inputData[i];

        auto it = hashmap.find(value);
        if (it != hashmap.end()) {
            ++(it->second);
        } else {
            hashmap[value] = 1;
        }

//        if (static_cast<double>(hashmap.size()) / static_cast<double>(hashmap.bucket_count()) >= 0.25) {
//            std::cout << "Hash table is full";
//        }
    }

    std::vector<std::pair<int, int>> result(hashmap.begin(), hashmap.end());
    return result;
}

std::vector<std::pair<int, int>> runGroupByFunction(GroupBy groupByImplementation, int n, const int *inputData) {
    switch(groupByImplementation) {
        case GroupBy::Hash:
            return groupByHash(n, inputData);
        case GroupBy::Sort:
            exit(-1);
        case GroupBy::Adaptive:
            exit(-1);
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