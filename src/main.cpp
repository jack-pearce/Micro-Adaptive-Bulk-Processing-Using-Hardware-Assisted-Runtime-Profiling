#include <string>
#include <vector>
#include <iostream>
#include <memory>

#include "time_benchmarking/timeBenchmarkSelect.h"
#include "time_benchmarking/timeBenchmarkHelpers.h"
#include "data_generation/dataGenerators.h"
#include "utils/dataHelpers.h"
#include "library/select.h"
#include "perf_counters/counterBenchmark.h"
#include "data_generation/dataFiles.h"


void dataDistributionTest(const DataFile& dataFile) {
    int* inputData = LoadedData::getInstance(dataFile.getFilepath(), dataFile.getNumElements()).getData();
    std::cout << dataFile.getNumElements() << " elements" << std::endl;
    displayDistribution(dataFile);
}

void selectFunctionalityTest(const DataFile& dataFile, SelectImplementation selectImplementation) {
    std::unique_ptr<int[]> selection(new int[dataFile.getNumElements()]);
    int* inputData = LoadedData::getInstance(dataFile.getFilepath(), dataFile.getNumElements()).getData();

    SelectFunctionPtr selectFunctionPtr;
    setSelectFuncPtr(selectFunctionPtr, selectImplementation);

    for (int i = 0; i <= 100; i += 10) {
        int selected = selectFunctionPtr(dataFile.getNumElements(), inputData, selection.get(), i);
        std::cout << i << "%: " << static_cast<float>(selected) / static_cast<float>(dataFile.getNumElements()) << std::endl;
    }
}

void runSelectTimeBenchmark(const DataFile& dataFile, SelectImplementation selectImplementation, int sensitivityStride) {
    selectTimeBenchmark(dataFile, selectImplementation, sensitivityStride);
    runTimeBenchmark(0, nullptr);
}

void runSelectTimeBenchmarkSetIterations(const DataFile& dataFile, SelectImplementation selectImplementation, int sensitivityStride, int iterations) {
    selectTimeBenchmarkSetIterations(dataFile, selectImplementation, sensitivityStride, iterations);
    runTimeBenchmark(0, nullptr);
}

void selectCyclesBenchmarkConfiguration_1(const DataFile &dataFile, SelectImplementation selectImplementation,
                                          int sensitivityStride, int iterations) {
    std::vector<std::string> benchmarkCounters = {"PERF_COUNT_HW_CPU_CYCLES"}; // Try adding in other CPU cycle counter
    selectCyclesBenchmark(dataFile,
                   selectImplementation,
                   sensitivityStride,
                   iterations,
                   benchmarkCounters);
}

void selectCyclesBenchmarkConfiguration_2(const DataFile &dataFile, SelectImplementation selectImplementation,
                                          int sensitivityStride, int iterations) {
    std::vector<std::string> benchmarkCounters = {"PERF_COUNT_HW_CPU_CYCLES",
                                                  "INSTRUCTION_RETIRED",
                                                  "UNHALTED_CORE_CYCLES"};
    selectCyclesBenchmark(dataFile,
                          selectImplementation,
                          sensitivityStride,
                          iterations,
                          benchmarkCounters);
}


int main(int argc, char** argv) {

    selectCyclesBenchmarkConfiguration_1(DataFiles::uniformIntDistribution25kValues,
                                         SelectImplementation::Predication,
                                         25,
                                         1);

    return 0;
}