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

void runSelectTimeBenchmark(const DataFile& dataFile, SelectImplementation selectImplementation, int selectivityStride) {
    selectTimeBenchmark(dataFile, selectImplementation, selectivityStride);
    runTimeBenchmark(0, nullptr);
}

void runSelectTimeBenchmarkSetIterations(const DataFile& dataFile, SelectImplementation selectImplementation, int selectivityStride, int iterations) {
    selectTimeBenchmarkSetIterations(dataFile, selectImplementation, selectivityStride, iterations);
    runTimeBenchmark(0, nullptr);
}

void selectBenchmarkWithExtraCountersConfigurations(const DataFile &dataFile, SelectImplementation selectImplementation,
                                                    int selectivityStride, int iterations) {
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
                                     selectivityStride,
                                     iterations,
                                     benchmarkCounters);
}

void allSelectIndexesTests() {
    // Graph 1: Selectivity range on uniform data
    selectCpuCyclesMultipleInputBenchmark(DataFiles::uniformIntDistribution250mValues,
                                          {SelectImplementation::IndexesBranch,
                                           SelectImplementation::IndexesPredication,
                                           SelectImplementation::IndexesAdaptive},
                                          1, 3);

    // Graph 2: Randomness range on sorted data
    selectCpuCyclesSweepBenchmark(DataSweeps::logSortedIntDistribution250mValuesRandomnessSweep,
                                  {SelectImplementation::IndexesBranch,
                                   SelectImplementation::IndexesPredication,
                                   SelectImplementation::IndexesAdaptive}, 50, 5);

    // Graph 3: Period range on linearly varying selectivity
    selectCpuCyclesSweepBenchmark(DataSweeps::varyingIntDistribution250mValuesSweep,
                                  {SelectImplementation::IndexesBranch,
                                   SelectImplementation::IndexesPredication,
                                   SelectImplementation::IndexesAdaptive}, 50, 5);

    // Graph 4: Period range on step varying selectivity
    selectCpuCyclesSweepBenchmark(DataSweeps::step50IntDistribution250mValuesSweep,
                                  {SelectImplementation::IndexesBranch,
                                   SelectImplementation::IndexesPredication,
                                   SelectImplementation::IndexesAdaptive}, 50, 5);

    // Graph 5: Best case - tuned unequal step varying selectivity
    selectCpuCyclesSingleInputBenchmark(DataFiles::bestCaseTunedUnequalStep50IntDistribution250mValues,
                                        {SelectImplementation::IndexesBranch,
                                         SelectImplementation::IndexesPredication,
                                         SelectImplementation::IndexesAdaptive},
                                         50,
                                         5);

    // Graph 6: Worst case - tuned period range on step varying selectivity
    selectCpuCyclesSingleInputBenchmark(DataFiles::worstCaseTunedStep50IntDistribution250mValues,
                                        {SelectImplementation::IndexesBranch,
                                         SelectImplementation::IndexesPredication,
                                         SelectImplementation::IndexesAdaptive},
                                        50,
                                        5);
}


int main(int argc, char** argv) {

//    selectCpuCyclesMultipleInputBenchmark2(DataFiles::uniformIntDistribution250mValues,
//                                          {SelectImplementation::ValuesBranch,
//                                           SelectImplementation::ValuesPredication,
//                                           SelectImplementation::ValuesVectorized},
//                                          1, 1);

    // Check correctness of vectorise function. - Do they all give the same result?

    return 0;

}
