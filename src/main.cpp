#include <string>
#include <vector>
#include <iostream>
#include <memory>

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

void selectTest1() {
    std::string filePath = "/home/jack/CLionProjects/micro-adaptive-bulk-processing-library/data/input/uniformIntDistribution.csv";
    int numElements = 4000000 / sizeof(int);
    selectBranchTest_1(filePath, numElements);
    selectPredicationTest_1(filePath, numElements);
    runBenchmark(0, nullptr);
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
    std::unique_ptr<int[]> inputData(new int[numElements]);
    std::unique_ptr<int[]> selection(new int[numElements]);
    loadDataToArray(filePath, inputData.get());

    for (int i = 0; i <= 100; i += 10) {
        int selected = selectBranch(numElements, inputData.get(), selection.get(), i);
        std::cout << i << "%: " << static_cast<float>(selected) / static_cast<float>(numElements) << std::endl;
    }

    for (int i = 0; i <= 100; i += 10) {
        int selected = selectPredication(numElements, inputData.get(), selection.get(), i);
        std::cout << i << "%: " << static_cast<float>(selected) / static_cast<float>(numElements) << std::endl;
    }
}


int main(int argc, char** argv) {
//    hpcSelectTester();
    selectTest1();
    return 0;
}
