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

void selectFunctionalityTest(const DataFile& dataFile, Select selectImplementation) {

    for (auto i = 0; i <= 100; i += 10) {
        auto inputData = new int[dataFile.getNumElements()];
        auto inputFilter = new int[dataFile.getNumElements()];
        auto selection = new int[dataFile.getNumElements()];
        copyArray(LoadedData::getInstance(dataFile).getData(), inputData, dataFile.getNumElements());
        copyArray(LoadedData::getInstance(dataFile).getData(), inputFilter, dataFile.getNumElements());

        auto selected = runSelectFunction(selectImplementation,
                                         dataFile.getNumElements(), inputData, inputFilter, selection, i);
        std::cout << i << "%: " << static_cast<float>(selected) / static_cast<float>(dataFile.getNumElements()) << std::endl;

        delete[] inputData;
        delete[] inputFilter;
        delete[] selection;
    }
}

void runSelectTimeBenchmark(const DataFile& dataFile, Select selectImplementation, int selectivityStride) {
    selectTimeBenchmark(dataFile, selectImplementation, selectivityStride);
    runTimeBenchmark(0, nullptr);
}

void runSelectTimeBenchmarkSetIterations(const DataFile& dataFile, Select selectImplementation, int selectivityStride, int iterations) {
    selectTimeBenchmarkSetIterations(dataFile, selectImplementation, selectivityStride, iterations);
    runTimeBenchmark(0, nullptr);
}

void selectBenchmarkWithExtraCountersConfigurations(const DataFile &dataFile, Select selectImplementation, int iterations) {
    // HPC set 1
    std::vector<std::string> benchmarkCounters = {"PERF_COUNT_HW_CPU_CYCLES",
                                                  "INSTRUCTION_RETIRED",
                                                  "LLC_REFERENCES",
                                                  "LLC_MISSES",
                                                  "MISPREDICTED_BRANCH_RETIRED",
                                                  "PERF_COUNT_HW_CACHE_REFERENCES",
                                                  "PERF_COUNT_HW_CACHE_MISSES",
                                                  "PERF_COUNT_HW_BRANCH_MISSES",
                                                  "PERF_COUNT_HW_CACHE_L1D"};
    // HPC set 2
//    std::vector<std::string> benchmarkCounters = {"PERF_COUNT_HW_CPU_CYCLES",
//                                                  "INSTRUCTION_RETIRED",
//                                                  "PERF_COUNT_HW_CACHE_L1D",
//                                                  "L1-DCACHE-LOADS",
//                                                  "L1-DCACHE-LOAD-MISSES",
//                                                  "L1-DCACHE-STORES"};

    std::vector<float> inputThresholdDistribution;

    // Update min & max to match dataFile
    generateLinearDistribution(30, 1, 10*1000, inputThresholdDistribution);
    // Update min & max to match dataFile
//    generateLogDistribution(30, 1, 10*1000, inputThresholdDistribution);

    selectBenchmarkWithExtraCounters(dataFile,
                                     selectImplementation,
                                     inputThresholdDistribution,
                                     iterations,
                                     benchmarkCounters, "");
}

void allSelectIndexesTests() {
    // Graph 1: Selectivity range on uniform data
    std::vector<float> inputThresholdDistribution;
    generateLogDistribution(30, 1, 10*1000, inputThresholdDistribution);
    selectCpuCyclesInputSweepBenchmark(DataFiles::uniformIntDistribution250mValuesMax10000,
                                       {Select::ImplementationIndexesBranch,
                                        Select::ImplementationIndexesPredication,
                                        Select::ImplementationIndexesAdaptive},
                                       inputThresholdDistribution,
                                       5, "Indexes");

    // Graph 2: Randomness range on sorted data
    selectCpuCyclesSweepBenchmark(DataSweeps::logSortedIntDistribution250mValuesRandomnessSweep,
                                  {Select::ImplementationIndexesBranch,
                                   Select::ImplementationIndexesPredication,
                                   Select::ImplementationIndexesAdaptive},
                                   50, 5, "Indexes");

    // Graph 3: Period range on linearly varying selectivity
    selectCpuCyclesSweepBenchmark(DataSweeps::varyingIntDistribution250mValuesSweep,
                                  {Select::ImplementationIndexesBranch,
                                   Select::ImplementationIndexesPredication,
                                   Select::ImplementationIndexesAdaptive},
                                   50, 5, "Indexes");

    // Graph 4: Period range on step varying selectivity
    selectCpuCyclesSweepBenchmark(DataSweeps::lowerStep50IntDistribution250mValuesSweep,
                                  {Select::ImplementationIndexesBranch,
                                   Select::ImplementationIndexesPredication,
                                   Select::ImplementationIndexesAdaptive},
                                   50, 5, "Indexes");

    // Graph 5: Best case - tuned unequal step varying selectivity
    selectCpuCyclesSingleInputBenchmark(DataFiles::bestCaseIndexesTunedUnequalLowerStep50IntDistribution250mValues,
                                        {Select::ImplementationIndexesBranch,
                                         Select::ImplementationIndexesPredication,
                                         Select::ImplementationIndexesAdaptive},
                                        50,
                                        5, "Indexes");

    // Graph 6: Worst case - tuned period range on step varying selectivity
    selectCpuCyclesSingleInputBenchmark(DataFiles::worstCaseIndexesTunedUpperStep50IntDistribution250mValues,
                                        {Select::ImplementationIndexesBranch,
                                         Select::ImplementationIndexesPredication,
                                         Select::ImplementationIndexesAdaptive},
                                        50,
                                        5, "Indexes");
}

void allSelectValuesTests() {
    // Graph 1: Selectivity range on uniform data
    std::vector<float> inputThresholdDistribution;
    generateLogDistribution(30, 1, 10 * 1000, inputThresholdDistribution);
    selectCpuCyclesInputSweepBenchmark(DataFiles::uniformIntDistribution250mValuesMax10000,
                                       {Select::ImplementationValuesBranch,
                                        Select::ImplementationValuesVectorized,
                                        Select::ImplementationValuesPredication,
                                        Select::ImplementationValuesAdaptive},
                                       inputThresholdDistribution,
                                       5, "Values");

    // Graph 2: Randomness range on sorted data
    selectCpuCyclesSweepBenchmark(DataSweeps::logSortedIntDistribution250mValuesRandomnessSweep,
                                  {Select::ImplementationValuesBranch,
                                   Select::ImplementationValuesVectorized,
                                   Select::ImplementationValuesAdaptive},
                                   50, 5, "Values");


    // Graph 3: Period range on linearly varying selectivity
    selectCpuCyclesSweepBenchmark(DataSweeps::varyingIntDistribution250mValuesSweep,
                                  {Select::ImplementationValuesBranch,
                                   Select::ImplementationValuesVectorized,
                                   Select::ImplementationValuesAdaptive},
                                   50, 5, "Values");

    // Graph 4: Period range on step varying selectivity
    selectCpuCyclesSweepBenchmark(DataSweeps::lowerStep50IntDistribution250mValuesSweep,
                                  {Select::ImplementationValuesBranch,
                                   Select::ImplementationValuesVectorized,
                                   Select::ImplementationValuesAdaptive},
                                   50, 5, "Values");

    // Graph 5: Best case - tuned unequal step varying selectivity
    selectCpuCyclesSingleInputBenchmark(DataFiles::bestCaseValuesTunedUnequalLowerStep50IntDistribution250mValues,
                                        {Select::ImplementationValuesBranch,
                                         Select::ImplementationValuesVectorized,
                                         Select::ImplementationValuesAdaptive},
                                        50,
                                        5, "Values");

    // Graph 6: Worst case - tuned period range on step varying selectivity
    selectCpuCyclesSingleInputBenchmark(DataFiles::worstCaseValuesTunedLowerStep50IntDistribution250mValues,
                                        {Select::ImplementationValuesBranch,
                                         Select::ImplementationValuesVectorized,
                                         Select::ImplementationValuesAdaptive},
                                        50,
                                        5, "Values");

}


int main(int argc, char** argv) {

    selectSingleRunNoCounters(DataFiles::uniformIntDistribution250mValuesMax100,
                              Select::ImplementationValuesVectorized, 50 , 1);

    return 0;

}
