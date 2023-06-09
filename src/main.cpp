#include <string>
#include <vector>
#include <iostream>

#include "benchmarking/selectTests.h"
#include "data_generation/data_generator.h"
#include "utils/dataHelpers.h"
#include "library/select.h"
#include "perf_counters/hpcTests.h"
#include "benchmark/benchmark.h"

void runBenchmark(int argc, char** argv) {
    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();
}

void selectTest1(int argc, char** argv) {
    selectBranchTest_1();
    selectPredicationTest_1();
    runBenchmark(argc, argv);
}

void dataDistributionTest() {
    std::string filePath = "/home/jack/CLionProjects/micro-adaptive-bulk-processing-library/data/input/uniformIntDistribution.csv";
    int numElements = 4000000 / sizeof(int);

    generateUniformDistributionCSV(filePath, numElements);
    std::vector<int> data;
    loadDataToVector(filePath, data);
    displayDistribution(data);
}

void selectFunctionalityTest() {
    std::string filePath = "/home/jack/CLionProjects/micro-adaptive-bulk-processing-library/data/input/uniformIntDistribution.csv";
    int numElements = 4000000 / sizeof(int);

    generateUniformDistributionCSV(filePath, numElements);
    std::vector<int> inputData;
    std::vector<int> selection;
    loadDataToVector(filePath, inputData);
    selection.reserve(inputData.size());

    for (int i = 0; i <= 100; i += 10) {
        int selected = selectBranch(numElements, inputData, selection, i);
        std::cout << i << "%: " << static_cast<float>(selected) / static_cast<float>(numElements) << std::endl;
    }

    for (int i = 0; i <= 100; i += 10) {
        int selected = selectPredication(numElements, inputData, selection, i);
        std::cout << i << "%: " << static_cast<float>(selected) / static_cast<float>(numElements) << std::endl;
    }
}


int main(int argc, char** argv) {
    hpcSelectTester();
    return 0;
}
