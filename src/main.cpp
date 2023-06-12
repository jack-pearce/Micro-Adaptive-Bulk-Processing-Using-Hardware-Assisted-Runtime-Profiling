#include <string>
#include <vector>
#include <iostream>
#include <memory>

#include "time_benchmarking/timeBM_select.h"
#include "data_generation/dataGenerators.h"
#include "utils/dataHelpers.h"
#include "library/select.h"
#include "perf_counters/counterTests.h"
#include "perf_counters/counterBM.h"
#include "benchmark/benchmark.h"
#include "utils/papiHelpers.h"


void runTimeBM(int argc, char** argv) {
    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();
}

void selectTest1() {
    std::string filePath = "/home/jack/CLionProjects/micro-adaptive-bulk-processing-library/data/input/uniformIntDistribution.csv";
    int numElements = 1000000000 / sizeof(int);
    selectBranchTimeBM_1(filePath, numElements);
    selectPredicationTimeBM_1(filePath, numElements);
    runTimeBM(0, nullptr);
}

void selectTest2() {
    std::string filePath = "/home/jack/CLionProjects/micro-adaptive-bulk-processing-library/data/input/uniformIntDistribution.csv";
    int numElements = 1000000000 / sizeof(int);
    selectPredicationTimeBM_2(filePath, numElements);
    selectAdaptiveTimeBM_2(filePath, numElements);
    selectBranchTimeBM_2(filePath, numElements);
    runTimeBM(0, nullptr);
}

void dataDistributionTest() {
    std::string filePath = "/home/jack/CLionProjects/micro-adaptive-bulk-processing-library/data/input/uniformIntDistribution.csv";
    int numElements = 1000000000 / sizeof(int);

//    generateUniformDistributionCSV(filePath, numElements);
    std::vector<int> data;
    loadDataToVector(filePath, data);
    displayDistribution(data);
}

void selectFunctionalityTest() {
    std::string filePath = "/home/jack/CLionProjects/micro-adaptive-bulk-processing-library/data/input/uniformIntDistribution.csv";
    int numElements = 1000000000 / sizeof(int);

//    generateUniformDistributionCSV(filePath, numElements);
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

void selectCounterBM_1(SelectImplementation selectImplementation, const std::string& fileName) {
    int numElements = 1000000000 / sizeof(int);
    int sensitivityStride = 5;

    std::vector<std::string> counters = {"INSTRUCTION_RETIRED",
                                         "PERF_COUNT_HW_CPU_CYCLES",
                                         "UNHALTED_CORE_CYCLES"};
    selectCounterBM(selectImplementation, numElements, sensitivityStride, counters, fileName);
}

void selectCounterBM_2(SelectImplementation selectImplementation) {
    int numElements = 1000000000 / sizeof(int);
    int sensitivityStride = 5;
    selectCounterBM_2(selectImplementation, numElements, sensitivityStride);
}

int main(int argc, char** argv) {
//    selectCounterBM_1(SelectImplementation::Predication, "Predication");
//    selectCounterBM_1(SelectImplementation::Branch, "Branch");
//    selectCounterBM_1(SelectImplementation::Adaptive, "Adaptive");

    selectCounterBM_2(SelectImplementation::Predication);
    selectCounterBM_2(SelectImplementation::Branch);
    selectCounterBM_2(SelectImplementation::Adaptive);

    return 0;
}
