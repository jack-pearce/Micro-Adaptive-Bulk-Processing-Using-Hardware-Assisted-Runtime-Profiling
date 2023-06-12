#include <string>
#include <vector>
#include <iostream>
#include <memory>

#include "benchmarking/selectBenchmarks.h"
#include "data_generation/dataGenerators.h"
#include "utils/dataHelpers.h"
#include "library/select.h"
#include "perf_counters/hpcTests.h"
#include "perf_counters/benchmarking.h"
#include "benchmark/benchmark.h"
#include "utils/papiHelpers.h"

void runBenchmark(int argc, char** argv) {
    benchmark::Initialize(&argc, argv);
    benchmark::RunSpecifiedBenchmarks();
    benchmark::Shutdown();
}

void selectTest1() {
    std::string filePath = "/home/jack/CLionProjects/micro-adaptive-bulk-processing-library/data/input/uniformIntDistribution.csv";
    int numElements = 1000000000 / sizeof(int);
    selectBranchTest_1(filePath, numElements);
    selectPredicationTest_1(filePath, numElements);
    runBenchmark(0, nullptr);
}

void selectTest2() {
    std::string filePath = "/home/jack/CLionProjects/micro-adaptive-bulk-processing-library/data/input/uniformIntDistribution.csv";
    int numElements = 1000000000 / sizeof(int);
    selectPredicationTest_2(filePath, numElements);
    selectAdaptiveTest_2(filePath, numElements);
    selectBranchTest_2(filePath, numElements);
    runBenchmark(0, nullptr);
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

void selectHPCbenchmark(SelectImplementation selectImplementation, std::string fileName) {
    std::string filePath = "/home/jack/CLionProjects/micro-adaptive-bulk-processing-library/data/input/uniformIntDistribution.csv";
    int numElements = 1000000000 / sizeof(int);
    int sensitivityStride = 5;
    int numTests = 1 + (100 / sensitivityStride);

    std::unique_ptr<int[]> inputData(new int[numElements]);
    std::unique_ptr<int[]> selection(new int[numElements]);
    loadDataToArray(filePath, inputData.get());

    std::vector<std::string> hpcs = {"INSTRUCTION_RETIRED",
                                     "PERF_COUNT_HW_CPU_CYCLES",
                                     "UNHALTED_CORE_CYCLES"};
    long_long hpcValues[hpcs.size()];
    int EventSet = initialisePAPIandCreateEventSet(hpcs);

    if (PAPI_start(EventSet) != PAPI_OK) {
        std::cerr << "PAPI could not start counting events!" << std::endl;
        exit(1);
    }

    std::vector<std::vector<long_long>> results(numTests, std::vector<long_long>(hpcs.size() + 1, 0));
    int count = 0;

    SelectFunctionPtr selectFunctionPtr;
    setSelectFuncPtr(selectFunctionPtr, selectImplementation);

    for (int i = 0; i <= 100; i += sensitivityStride) {
        if (PAPI_reset(EventSet) != PAPI_OK)
            exit(1);

        selectFunctionPtr(numElements, inputData.get(), selection.get(), i);

        if (PAPI_read(EventSet, hpcValues) != PAPI_OK)
            exit(1);

        results[count][0] = static_cast<long_long>(i);
        for (int j = 0; j < static_cast<int>(hpcs.size()); ++j) {
            results[count][j + 1] = hpcValues[j];
        }
        count++;
    }

    std::vector<std::string> headers(hpcs);
    headers.insert(headers.begin(), "Sensitivity");
    std::string outputFilePath = "/home/jack/CLionProjects/micro-adaptive-bulk-processing-library/data/output/";
    std::string outputFileName = "Sensitivity_Output_";
    std::string outputFullFilePath = outputFilePath + outputFileName + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, outputFullFilePath);

    teardownPAPI(EventSet, hpcValues);
}

int main(int argc, char** argv) {
    selectHPCbenchmark(SelectImplementation::Predication, "Predication");
    selectHPCbenchmark(SelectImplementation::Branch, "Branch");
    selectHPCbenchmark(SelectImplementation::Adaptive, "Adaptive");
    return 0;
}
