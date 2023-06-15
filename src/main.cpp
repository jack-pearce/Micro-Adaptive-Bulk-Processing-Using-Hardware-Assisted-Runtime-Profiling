#include <string>
#include <vector>
#include <iostream>
#include <memory>

#include "time_benchmarking/timeBenchmarkSelect.h"
#include "time_benchmarking/timeBenchmarkHelpers.h"
#include "data_generation/dataGenerators.h"
#include "utils/dataHelpers.h"
#include "library/select.h"
#include "library/papi.h"
#include "perf_counters/counterBenchmark.h"
#include "data_generation/dataFiles.h"
#include "utils/papiHelpers.h"



void dataDistributionTest(const DataFile& dataFile) {
    LoadedData::getInstance(dataFile);
    std::cout << dataFile.getNumElements() << " elements" << std::endl;
    displayDistribution(dataFile);
}

void selectFunctionalityTest(const DataFile& dataFile, SelectImplementation selectImplementation) {
    std::unique_ptr<int[]> selection(new int[dataFile.getNumElements()]);
    int* inputData = LoadedData::getInstance(dataFile).getData();

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

void selectCountersBenchmarkConfiguration_1(const DataFile &dataFile, SelectImplementation selectImplementation,
                                            int sensitivityStride, int iterations) {
//    std::vector<std::string> benchmarkCounters = {"PERF_COUNT_HW_CPU_CYCLES"}; // Try adding in other CPU cycle counter
    std::vector<std::string> benchmarkCounters = {"INSTRUCTION_RETIRED"}; // Try adding in other CPU cycle counter

    selectBenchmarkWithExtraCounters(dataFile,
                                     selectImplementation,
                                     sensitivityStride,
                                     iterations,
                                     benchmarkCounters);
}

void selectCountersBenchmarkConfiguration_2(const DataFile &dataFile, SelectImplementation selectImplementation,
                                            int sensitivityStride, int iterations) {
    std::vector<std::string> benchmarkCounters = {"PERF_COUNT_HW_CPU_CYCLES",
                                                  "INSTRUCTION_RETIRED",
                                                  "LLC_REFERENCES",
                                                  "LLC_MISSES",
                                                  "MISPREDICTED_BRANCH_RETIRED",
                                                  "PERF_COUNT_HW_CACHE_REFERENCES",
                                                  "PERF_COUNT_HW_CACHE_MISSES",
                                                  "PERF_COUNT_HW_BRANCH_MISSES",
                                                  "PERF_COUNT_HW_CACHE_L1D"};
//    std::vector<std::string> benchmarkCounters = {"PERF_COUNT_HW_CPU_CYCLES",
//                                                  "INSTRUCTION_RETIRED",
//                                                  "PERF_COUNT_HW_CACHE_L1D",
//                                                  "L1-DCACHE-LOADS",
//                                                  "L1-DCACHE-LOAD-MISSES",
//                                                  "L1-DCACHE-STORES"};
    selectBenchmarkWithExtraCounters(dataFile,
                                     selectImplementation,
                                     sensitivityStride,
                                     iterations,
                                     benchmarkCounters);
}

void selectCpuCyclesBenchmarkConfiguration_1(const DataFile &dataFile, SelectImplementation selectImplementation,
                                             int sensitivityStride, int iterations) {
    selectCpuCyclesMultipleBenchmarks(dataFile,
                                      selectImplementation,
                                      sensitivityStride,
                                      iterations);
}


int main(int argc, char** argv) {

    selectCountersBenchmarkConfiguration_2(DataFiles::uniformIntDistribution250mValues,
                                           SelectImplementation::Adaptive,
                                           1,
                                           1);

/*    selectCountersBenchmarkConfiguration_2(DataFiles::uniformIntDistribution250mValues,
                                           SelectImplementation::Branch,
                                           1,
                                           1);*/

/*    selectCountersBenchmarkConfiguration_2(DataFiles::uniformIntDistribution250mValues,
                                           SelectImplementation::Predication,
                                           1,
                                           1);*/

/*    selectCountersBenchmarkConfiguration_2(DataFiles::uniformIntDistribution250mValues,
                                           SelectImplementation::Branch,
                                           1,
                                           1);*/

/*    selectCpuCyclesMultipleBenchmarks(DataFiles::uniformIntDistribution250mValues,
                             SelectImplementation::Adaptive,
                             1,
                             1);*/

//    selectCpuCyclesSingleBenchmark(DataFiles::uniformIntDistribution250mValues,
//                                   SelectImplementation::Adaptive, 1, 70);

    return 0;

}
