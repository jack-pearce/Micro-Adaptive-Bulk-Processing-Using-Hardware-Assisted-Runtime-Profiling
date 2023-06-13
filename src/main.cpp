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
#include "data_generation/dataFiles.h"


void runTimeBM(int argc, char** argv) {
    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();
}

void selectTimeBM_1() {
    selectBranchTimeBM_1();
    selectPredicationTimeBM_1();
    selectAdaptiveTimeBM_1();
    runTimeBM(0, nullptr);
}

void selectTimeBM_2() {
    selectPredicationTimeBM_2();
    selectAdaptiveTimeBM_2();
    selectBranchTimeBM_2();
    runTimeBM(0, nullptr);
}

void dataDistributionTest(const std::string& filePath) {
    std::vector<int> data;
    loadDataToVector(filePath, data);
    std::cout << data.size() << std::endl;
    displayDistribution(data);
}

void selectFunctionalityTest() {
    std::string filePath = uniformIntDistribution250mValues;
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

    if (selectImplementation == SelectImplementation::Adaptive) {
        std::cout << "Can't benchmark adaptive select using counters since it is already using counters" << std::endl;
        exit(1);
    }

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

void selectCounterBM_3(SelectImplementation selectImplementation, int iterations, const std::string& fileName) {
    int numElements = 100000 / sizeof(int);
    int sensitivityStride = 25;
    selectCyclesCounterBM(selectImplementation, numElements, sensitivityStride, fileName, iterations);
}

int main(int argc, char** argv) {

//    selectCounterBM_3(SelectImplementation::Branch, 1, "select_branch_cpu_cycles");
//    selectCounterBM_3(SelectImplementation::Predication, 1, "select_branch_cpu_cycles");
    selectCounterBM_3(SelectImplementation::Adaptive, 1, "select_branch_cpu_cycles");

    return 0;
}
