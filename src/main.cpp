#include <string>
#include <vector>
#include <iostream>

#include "time_benchmarking/selectTimeBenchmark.h"
#include "time_benchmarking/timeBenchmarkHelpers.h"
#include "data_generation/dataGenerators.h"
#include "utilities/dataHelpers.h"
#include "cycles_benchmarking//selectCyclesBenchmark.h"
#include "cycles_benchmarking/groupByCyclesBenchmark.h"
#include "data_generation/dataFiles.h"
#include "library/mabpl.h"

using MABPL::Select;
using MABPL::GroupBy;

using MABPL::MinAggregation;
using MABPL::MaxAggregation;
using MABPL::SumAggregation;
using MABPL::CountAggregation;

void dataDistributionTest(const DataFile& dataFile) {
    LoadedData<int>::getInstance(dataFile);
    std::cout << dataFile.getNumElements() << " elements" << std::endl;
    displayDistribution<int>(dataFile);
}

void selectFunctionalityTest(const DataFile& dataFile, Select selectImplementation) {

    for (auto i = 0; i <= 100; i += 10) {
        auto inputData = new int[dataFile.getNumElements()];
        auto inputFilter = new int[dataFile.getNumElements()];
        auto selection = new int[dataFile.getNumElements()];
        copyArray<int>(LoadedData<int>::getInstance(dataFile).getData(), inputData, dataFile.getNumElements());
        copyArray<int>(LoadedData<int>::getInstance(dataFile).getData(), inputFilter, dataFile.getNumElements());

        auto selected = MABPL::runSelectFunction(selectImplementation,
                                         dataFile.getNumElements(), inputData, inputFilter, selection, i);
        std::cout << i << "%: " << static_cast<float>(selected) / static_cast<float>(dataFile.getNumElements()) << std::endl;

        delete[] inputData;
        delete[] inputFilter;
        delete[] selection;

    }
}

void runSelectTimeBenchmark(const DataFile& dataFile, Select selectImplementation, int selectivityStride) {
    selectTimeBenchmark<int>(dataFile, selectImplementation, selectivityStride);
    runTimeBenchmark(0, nullptr);
}

void runSelectTimeBenchmarkSetIterations(const DataFile& dataFile, Select selectImplementation, int selectivityStride, int iterations) {
    selectTimeBenchmarkSetIterations<int>(dataFile, selectImplementation, selectivityStride, iterations);
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
//    // HPC set 2
//    std::vector<std::string> benchmarkCounters = {"PERF_COUNT_HW_CPU_CYCLES",
//                                                  "INSTRUCTION_RETIRED",
//                                                  "PERF_COUNT_HW_CACHE_L1D",
//                                                  "L1-DCACHE-LOADS",
//                                                  "L1-DCACHE-LOAD-MISSES",
//                                                  "L1-DCACHE-STORES"};

    std::vector<float> inputThresholdDistribution;
    generateLinearDistribution(2, 0., 1, inputThresholdDistribution);

    // Update min & max to match dataFile
//    generateLinearDistribution(10, 0, 100, inputThresholdDistribution);
    // Update min & max to match dataFile
//    generateLogDistribution(30, 1, 10*1000, inputThresholdDistribution);

    selectBenchmarkWithExtraCounters(dataFile,
                                     selectImplementation,
                                     inputThresholdDistribution,
                                     iterations,
                                     benchmarkCounters, "");
}

void selectIndexesCompareResultsTest(const DataFile& dataFile, Select selectImpOne, Select selectImpTwo) {
    int threshold = 3;
    auto inputFilter = new int[dataFile.getNumElements()];
    auto selectionOne = new int[dataFile.getNumElements()];
    copyArray<int>(LoadedData<int>::getInstance(dataFile).getData(), inputFilter,
              dataFile.getNumElements());

    std::cout << "Running " << getSelectName(selectImpOne) << "..." << std::endl;

    int resultOne = MABPL::runSelectFunction(selectImpOne, dataFile.getNumElements(),
                                             inputFilter, inputFilter, selectionOne, threshold);
    std::sort(selectionOne, selectionOne + resultOne);

    auto selectionTwo = new int[dataFile.getNumElements()];

    std::cout << std::endl << "Running " << getSelectName(selectImpTwo) << "..." << std::endl;

    int resultTwo = MABPL::runSelectFunction(selectImpTwo, dataFile.getNumElements(),
                                             inputFilter, inputFilter, selectionTwo, threshold);
    std::sort(selectionTwo, selectionTwo + resultTwo);

    if (resultOne != resultTwo) {
        std::cout << "Size of results are different" << std::endl;
    }

/*    for (auto i = 0; i < 400; i++) {
        std::cout << inputFilter[i] << std::endl;
    }
    std::cout << std::endl;

    for (auto i = 0; i < resultOne; i++) {
        std::cout << selectionOne[i] << std::endl;
    }
    std::cout << std::endl;

    for (auto i = 0; i < resultTwo; i++) {
        std::cout << selectionTwo[i] << std::endl;
    }
    std::cout << std::endl;*/

    for (auto i = 0; i < static_cast<int>(resultOne); ++i) {
        if (selectionOne[i] != selectionTwo[i]) {
            std::cout << "Different index found" << std::endl;
        }
    }

    delete[] inputFilter;
    delete[] selectionOne;
    delete[] selectionTwo;
}

void selectValuesCompareResultsTest(const DataFile& dataFile, Select selectImpOne, Select selectImpTwo) {
    int threshold = 3;
    auto inputFilter = new int[dataFile.getNumElements()];
    auto inputData = new int[dataFile.getNumElements()];
    auto selectionOne = new int[dataFile.getNumElements()];
    copyArray<int>(LoadedData<int>::getInstance(dataFile).getData(), inputFilter,
              dataFile.getNumElements());
    copyArray<int>(LoadedData<int>::getInstance(dataFile).getData(), inputData,
              dataFile.getNumElements());

    std::cout << "Running " << getSelectName(selectImpOne) << "..." << std::endl;

    int resultOne = MABPL::runSelectFunction(selectImpOne, dataFile.getNumElements(),
                                             inputData, inputFilter, selectionOne, threshold);
    std::sort(selectionOne, selectionOne + resultOne);

    auto selectionTwo = new int[dataFile.getNumElements()];

    std::cout << std::endl << "Running " << getSelectName(selectImpTwo) << "..." << std::endl;

    int resultTwo = MABPL::runSelectFunction(selectImpTwo, dataFile.getNumElements(),
                                             inputData, inputFilter, selectionTwo, threshold);
    std::sort(selectionTwo, selectionTwo + resultTwo);

    if (resultOne != resultTwo) {
        std::cout << "Size of results are different" << std::endl;
        std::cout << "Result one size: " << resultOne << ", Result two size: " << resultTwo << std::endl;
    }

/*    for (auto i = 0; i < 400; i++) {
        std::cout << inputFilter[i] << std::endl;
    }
    std::cout << std::endl;

    for (auto i = 0; i < resultOne; i++) {
        std::cout << selectionOne[i] << std::endl;
    }
    std::cout << std::endl;

    for (auto i = 0; i < resultTwo; i++) {
        std::cout << selectionTwo[i] << std::endl;
    }
    std::cout << std::endl;*/

    for (auto i = 0; i < static_cast<int>(resultOne); ++i) {
        if (selectionOne[i] != selectionTwo[i]) {
            std::cout << "Different index found" << std::endl;
        }
    }

    delete[] inputFilter;
    delete[] inputData;
    delete[] selectionOne;
    delete[] selectionTwo;
}

void selectWallTimeDopSweepBenchmarkCalcDopRange(DataSweep &dataSweep, Select selectImplementation,
                                                 int threshold, int iterations,
                                                 const std::string &fileNamePrefix) {
    int dop = 2;
    std::vector<int> dopValues;
    while (dop <= MABPL::maxDop()) {
        dopValues.push_back(dop);
        dop *= 2;
    }

    selectWallTimeDopSweepBenchmark(dataSweep, selectImplementation, threshold, iterations,
                                    fileNamePrefix, dopValues);
}

void selectWallTimeDopAndInputSweepBenchmarkCalcDopRange(const DataFile &dataFile, Select selectImplementation,
                                                         std::vector<float> &thresholds, int iterations,
                                                         const std::string &fileNamePrefix) {
    int dop = 2;
    std::vector<int> dopValues;
    while (dop <= MABPL::maxDop()) {
        dopValues.push_back(dop);
        dop *= 2;
    }

    selectWallTimeDopAndInputSweepBenchmark(dataFile, selectImplementation, thresholds, iterations,
                                            fileNamePrefix, dopValues);
}

void allSelectIndexesSingleThreadedTests() {
    // Graph 1: Selectivity range on uniform data
    std::vector<float> inputThresholdDistribution;
    generateLogDistribution(30, 1, 10*1000, inputThresholdDistribution);
    selectCpuCyclesInputSweepBenchmark(DataFiles::uniformIntDistribution250mValuesMax10000,
                                       {Select::ImplementationIndexesBranch,
                                        Select::ImplementationIndexesPredication,
                                        Select::ImplementationIndexesAdaptive},
                                       inputThresholdDistribution,
                                       5, "1-Indexes");

    // Graph 2: Randomness range on sorted data
    selectCpuCyclesSweepBenchmark(DataSweeps::logSortedIntDistribution250mValuesRandomnessSweep,
                                  {Select::ImplementationIndexesBranch,
                                   Select::ImplementationIndexesPredication,
                                   Select::ImplementationIndexesAdaptive},
                                   50, 5, "2-Indexes");

    // Graph 2.5: Selectivity on fully sorted data
    std::vector<float> inputThresholdDistribution2;
    generateLogDistribution(30, 1, 100, inputThresholdDistribution2);
    selectCpuCyclesInputSweepBenchmark(DataFiles::fullySortedIntDistribution250mValues,
                                       {Select::ImplementationIndexesBranch,
                                        Select::ImplementationIndexesPredication,
                                        Select::ImplementationIndexesAdaptive},
                                       inputThresholdDistribution,
                                       5, "2-5-Indexes");

    // Graph 3: Period range on linearly varying selectivity
/*    selectCpuCyclesSweepBenchmark(DataSweeps::varyingIntDistribution250mValuesSweep,
                                  {Select::ImplementationIndexesBranch,
                                   Select::ImplementationIndexesPredication,
                                   Select::ImplementationIndexesAdaptive},
                                   50, 5, "Indexes");*/

    // Graph 4: Period range on step varying selectivity
    selectCpuCyclesSweepBenchmark(DataSweeps::lowerStep50IntDistribution250mValuesSweep,
                                  {Select::ImplementationIndexesBranch,
                                   Select::ImplementationIndexesPredication,
                                   Select::ImplementationIndexesAdaptive},
                                   50, 5, "4-Indexes");

    // Graph 5: Best case - tuned unequal step varying selectivity
    selectCpuCyclesSweepBenchmark(DataSweeps::lowerStep50IntDistribution250mValuesPercentageStepSweep,
                                  {Select::ImplementationIndexesBranch,
                                   Select::ImplementationIndexesPredication,
                                   Select::ImplementationIndexesAdaptive},
                                  50, 5, "5-Indexes");
/*    selectCpuCyclesSingleInputBenchmark(DataFiles::bestCaseIndexesTunedUnequalLowerStep50IntDistribution250mValues,
                                        {Select::ImplementationIndexesBranch,
                                         Select::ImplementationIndexesPredication,
                                         Select::ImplementationIndexesAdaptive},
                                        50,
                                        5, "Indexes");*/

    // Graph 6: Worst case - tuned period range on step varying selectivity
    selectCpuCyclesSingleInputBenchmark(DataFiles::worstCaseIndexesTunedUpperStep50IntDistribution250mValues,
                                        {Select::ImplementationIndexesBranch,
                                         Select::ImplementationIndexesPredication,
                                         Select::ImplementationIndexesAdaptive},
                                        50,
                                        5, "6-Indexes");
}

void allSelectValuesSingleThreadedTests() {
    // Graph 1: Selectivity range on uniform data
    std::vector<float> inputThresholdDistribution;
    generateLogDistribution(30, 1, 10 * 1000, inputThresholdDistribution);
    selectCpuCyclesInputSweepBenchmark(DataFiles::uniformIntDistribution250mValuesMax10000,
                                       {Select::ImplementationValuesBranch,
                                        Select::ImplementationValuesVectorized,
                                        Select::ImplementationValuesPredication,
                                        Select::ImplementationValuesAdaptive},
                                       inputThresholdDistribution,
                                       5, "1-Values");

    // Graph 2: Randomness range on sorted data
    selectCpuCyclesSweepBenchmark(DataSweeps::logSortedIntDistribution250mValuesRandomnessSweep,
                                  {Select::ImplementationValuesBranch,
                                   Select::ImplementationValuesVectorized,
                                   Select::ImplementationValuesAdaptive},
                                   50, 5, "2-Values");

    // Graph 2.5: Selectivity on fully sorted data
    std::vector<float> inputThresholdDistribution2;
    generateLogDistribution(30, 1, 100, inputThresholdDistribution2);
    selectCpuCyclesInputSweepBenchmark(DataFiles::fullySortedIntDistribution250mValues,
                                       {Select::ImplementationValuesBranch,
                                        Select::ImplementationValuesVectorized,
                                        Select::ImplementationValuesAdaptive},
                                       inputThresholdDistribution,
                                       5, "2-5-Values");


    // Graph 3: Period range on linearly varying selectivity
/*    selectCpuCyclesSweepBenchmark(DataSweeps::varyingIntDistribution250mValuesSweep,
                                  {Select::ImplementationValuesBranch,
                                   Select::ImplementationValuesVectorized,
                                   Select::ImplementationValuesAdaptive},
                                   50, 5, "Values");*/

    // Graph 4: Period range on step varying selectivity
    selectCpuCyclesSweepBenchmark(DataSweeps::lowerStep50IntDistribution250mValuesSweep,
                                  {Select::ImplementationValuesBranch,
                                   Select::ImplementationValuesVectorized,
                                   Select::ImplementationValuesAdaptive},
                                   50, 5, "4-Values");

    // Graph 5: Best case - tuned unequal step varying selectivity
    selectCpuCyclesSweepBenchmark(DataSweeps::lowerStep50IntDistribution250mValuesPercentageStepSweep,
                                  {Select::ImplementationValuesBranch,
                                   Select::ImplementationValuesVectorized,
                                   Select::ImplementationValuesAdaptive},
                                  50, 5, "5-Values");
/*    selectCpuCyclesSingleInputBenchmark(DataFiles::bestCaseValuesTunedUnequalLowerStep50IntDistribution250mValues,
                                        {Select::ImplementationValuesBranch,
                                         Select::ImplementationValuesVectorized,
                                         Select::ImplementationValuesAdaptive},
                                        50,
                                        5, "Values");*/

    // Graph 6: Worst case - tuned period range on step varying selectivity
    selectCpuCyclesSingleInputBenchmark(DataFiles::worstCaseValuesTunedLowerStep50IntDistribution250mValues,
                                        {Select::ImplementationValuesBranch,
                                         Select::ImplementationValuesVectorized,
                                         Select::ImplementationValuesAdaptive},
                                        50,
                                        5, "6-Values");

}

void allSelectIndexesParallelTests() {
    std::vector<float> inputThresholdDistribution;
    generateLogDistribution(30, 1, 10*1000, inputThresholdDistribution);
    selectWallTimeInputSweepBenchmark(DataFiles::uniformIntDistribution250mValuesMax10000,
                                      {Select::ImplementationIndexesAdaptive},
                                      inputThresholdDistribution,
                                      5, "7-DOP-1-Indexes-SelectivitySweepSingle");

    selectWallTimeDopAndInputSweepBenchmarkCalcDopRange(DataFiles::uniformIntDistribution250mValuesMax10000,
                                                        Select::ImplementationIndexesAdaptiveParallel,
                                                        inputThresholdDistribution,
                                                        5, "7-DOP-1-Indexes-SelectivitySweepParallel");

    selectWallTimeSweepBenchmark(DataSweeps::lowerStep50IntDistribution250mValuesSweep,
                                 {Select::ImplementationIndexesAdaptive},
                                 50, 5, "7-DOP-2-Indexes-StepPeriodSweepSingle");

    selectWallTimeDopSweepBenchmarkCalcDopRange(DataSweeps::lowerStep50IntDistribution250mValuesSweep,
                                                Select::ImplementationIndexesAdaptiveParallel,
                                                50, 5, "7-DOP-2-Indexes-StepPeriodSweepParallel");

    selectWallTimeSweepBenchmark(DataSweeps::lowerStep50IntDistribution250mValuesPercentageStepSweep,
                                 {Select::ImplementationIndexesAdaptive},
                                 50, 5, "7-DOP-3-Indexes-StepPercentageSweepSingle");

    selectWallTimeDopSweepBenchmarkCalcDopRange(DataSweeps::lowerStep50IntDistribution250mValuesPercentageStepSweep,
                                                Select::ImplementationIndexesAdaptiveParallel,
                                                50, 5, "7-DOP-3-Indexes-StepPercentageSweepParallel");
}

void allSelectValuesParallelTests() {
    std::vector<float> inputThresholdDistribution;
    generateLogDistribution(30, 1, 10*1000, inputThresholdDistribution);
    selectWallTimeInputSweepBenchmark(DataFiles::uniformIntDistribution250mValuesMax10000,
                                      {Select::ImplementationValuesAdaptive},
                                      inputThresholdDistribution,
                                      5, "7-DOP-1-Values-SelectivitySweepSingle");

    selectWallTimeDopAndInputSweepBenchmarkCalcDopRange(DataFiles::uniformIntDistribution250mValuesMax10000,
                                                        Select::ImplementationValuesAdaptiveParallel,
                                                        inputThresholdDistribution,
                                                        5, "7-DOP-1-Values-SelectivitySweepParallel");

    selectWallTimeSweepBenchmark(DataSweeps::lowerStep50IntDistribution250mValuesSweep,
                                 {Select::ImplementationValuesAdaptive},
                                 50, 5, "7-DOP-2-Values-StepPeriodSweepSingle");

    selectWallTimeDopSweepBenchmarkCalcDopRange(DataSweeps::lowerStep50IntDistribution250mValuesSweep,
                                                Select::ImplementationValuesAdaptiveParallel,
                                                50, 5, "7-DOP-2-Values-StepPeriodSweepParallel");

    selectWallTimeSweepBenchmark(DataSweeps::lowerStep50IntDistribution250mValuesPercentageStepSweep,
                                 {Select::ImplementationValuesAdaptive},
                                 50, 5, "7-DOP-3-Values-StepPercentageSweepSingle");

    selectWallTimeDopSweepBenchmarkCalcDopRange(DataSweeps::lowerStep50IntDistribution250mValuesPercentageStepSweep,
                                                Select::ImplementationValuesAdaptiveParallel,
                                                50, 5, "7-DOP-3-Values-StepPercentageSweepParallel");
}

void groupByCompareResultsTest(const DataFile& dataFile, GroupBy groupByImpOne, GroupBy groupByImpTwo) {
    auto inputGroupBy = new int[dataFile.getNumElements()];
    auto inputAggregate = new int[dataFile.getNumElements()];
    copyArray<int>(LoadedData<int>::getInstance(dataFile).getData(), inputGroupBy, dataFile.getNumElements());
    generateUniformDistributionInMemory(inputAggregate, dataFile.getNumElements(), 10);

    auto resultOne = MABPL::runGroupByFunction<MaxAggregation>(groupByImpOne,
                                                               dataFile.getNumElements(), inputGroupBy, inputAggregate,
                                                               10000000);
    sortVectorOfPairs(resultOne);

    auto resultTwo = MABPL::runGroupByFunction<MaxAggregation>(groupByImpTwo,
                                                               dataFile.getNumElements(), inputGroupBy, inputAggregate,
                                                               10000000);
    sortVectorOfPairs(resultTwo);

    if (resultOne.size() != resultTwo.size()) {
        std::cout << "Size of results are different" << std::endl;
    }

    for (auto i = 0; i < static_cast<int> (resultOne.size()); ++i) {
        if ((resultOne[i].first != resultTwo[i].first) || (resultOne[i].second != resultTwo[i].second)) {
            std::cout << "Different row found" << std::endl;
        }
    }

    delete []inputGroupBy;
    delete []inputAggregate;
}

void groupByBenchmarkWithExtraCountersConfigurations(DataSweep &dataSweep,
                                                     GroupBy groupByImplementation,
                                                     int iterations,
                                                     const std::string &fileNamePrefix) {
    std::vector<std::string> benchmarkCounters = {"PERF_COUNT_HW_CPU_CYCLES",
                                                  "INSTRUCTION_RETIRED",
                                                  "LLC_REFERENCES",
                                                  "PERF_COUNT_HW_CACHE_REFERENCES",
                                                  "PERF_COUNT_HW_CACHE_MISSES",
                                                  "PERF_COUNT_HW_CACHE_L1D",
                                                  "L1-DCACHE-LOADS",
                                                  "L1-DCACHE-LOAD-MISSES",
                                                  "L1-DCACHE-STORES"};

    groupByBenchmarkWithExtraCounters(dataSweep, groupByImplementation, iterations, benchmarkCounters, fileNamePrefix);
}

void groupByBenchmarkWithExtraCountersDuringRunConfigurations(const DataFile &dataFile,
                                                              const std::string &fileNamePrefix) {
    // HPC set 1
    std::vector<std::string> benchmarkCounters = {"PERF_COUNT_HW_CPU_CYCLES",
                                                  "INSTRUCTION_RETIRED",
                                                  "LLC_REFERENCES",
                                                  "PERF_COUNT_HW_CACHE_REFERENCES",
                                                  "PERF_COUNT_HW_CACHE_MISSES",
                                                  "PERF_COUNT_HW_CACHE_L1D",
                                                  "L1-DCACHE-LOADS",
                                                  "L1-DCACHE-LOAD-MISSES",
                                                  "L1-DCACHE-STORES"};

    groupByBenchmarkWithExtraCountersDuringRun(dataFile, benchmarkCounters, fileNamePrefix);
}

void groupByWallTimeDopSweepBenchmarkCalcDopRange(DataSweep &dataSweep, int iterations,
                                                  const std::string &fileNamePrefix) {
    int dop = 2;
    std::vector<int> dopValues;
    while (dop <= MABPL::maxDop()) {
        dopValues.push_back(dop);
        dop *= 2;
    }

    groupByWallTimeDopSweepBenchmark(dataSweep, iterations, fileNamePrefix, dopValues);
}

//void allGroupByTestsOLD() {
/*
// SET ONE - BITS_PER_PASS = 10
    // Graph 1: Cardinality range on uniform data (variable max) for different hashmaps - compile with -march=native removed
*//*    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepVariableMax,
                                   {GroupBy::HashGoogleDenseHashMap,
//                                    GroupBy::HashFollyF14FastMap,   // Need to turn off -march=native for this one
                                    GroupBy::HashAbseilFlatHashMap,
                                    GroupBy::HashTessilRobinMap,
                                    GroupBy::HashTessilHopscotchMap}, countAggregation,
                                   1, "1-MapCompare");*//*

    // Graph 2: Cardinality range (variable max) for simple radix sort - manually run with BITS_PER_PASS of 8, 10, 12
    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepVariableMax,
                                   {GroupBy::SortRadix}, countAggregation,
                                   1, "2-RadixSimple10");

    // Graph 3: Cardinality range (variable max) for optimised radix sort - manually run with BITS_PER_PASS of 8, 10, 12
    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepVariableMax,
                                   {GroupBy::SortRadixOpt}, countAggregation,
                                   1, "3-RadixOpt10");

    // Graph 5: Cardinality range on uniform data (fixed max) for different hashmaps - compile with -march=native removed
*//*    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMax,
                                   {GroupBy::HashGoogleDenseHashMap,
//                                    GroupBy::HashFollyF14FastMap,   // Need to turn off -march=native for this one
                                    GroupBy::HashAbseilFlatHashMap,
                                    GroupBy::HashTessilRobinMap,
                                    GroupBy::HashTessilHopscotchMap}, countAggregation,
                                   1, "5-MapCompare");*//*

    // Graph 6: Cardinality range (fixed max) for simple radix sort - manually run with BITS_PER_PASS of 8, 10, 12
    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMax,
                                   {GroupBy::SortRadix}, countAggregation,
                                   1, "6-RadixSimple10");

    // Graph 7: Cardinality range (fixed max) for optimised radix sort - manually run with BITS_PER_PASS of 8, 10, 12
    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMax,
                                   {GroupBy::SortRadixOpt}, countAggregation,
                                   1, "7-RadixOpt10");

    // Graph 9: Cardinality range (fixed max) for single / double radix10 pass
    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMax,
                                   {GroupBy::SingleRadixPassThenHash,
                                    GroupBy::DoubleRadixPassThenHash}, countAggregation,
                                   1, "9-SingleDoubleRadix10PassThenHash");

    // Graph 10: Cardinality range (fixed max) for levels of clustering

    // Graph 2: Cardinality range (variable max) for simple radix sort - manually run with BITS_PER_PASS of 8, 10, 12
    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMax,
                                   {GroupBy::Hash,GroupBy::SortRadixOpt,
                                    GroupBy::Adaptive}, countAggregation,
                                   1, "10-NoClustering");

    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMaxClustered1,
                                   {GroupBy::Hash,GroupBy::SortRadixOpt,
                                    GroupBy::Adaptive}, countAggregation,
                                   1, "10-Clustered1");

    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMaxClustered1k,
                                   {GroupBy::Hash,GroupBy::SortRadixOpt,
                                    GroupBy::Adaptive}, countAggregation,
                                   1, "10-Clustered1k");

    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMaxClustered100k,
                                   {GroupBy::Hash,GroupBy::SortRadixOpt,
                                    GroupBy::Adaptive}, countAggregation,
                                   1, "10-Clustered100k");

// HPC CHARTS

    groupByBenchmarkWithExtraCountersConfigurations(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMax,
                                                    GroupBy::Hash, countAggregation, 1,
                                                    "1HPC-HashCounters");

    groupByBenchmarkWithExtraCountersConfigurations(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMaxClustered1,
                                                    GroupBy::Hash, countAggregation, 1,
                                                    "2-1HPC-HashCounters");

    groupByBenchmarkWithExtraCountersConfigurations(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMaxClustered1k,
                                                    GroupBy::Hash, countAggregation, 1,
                                                    "2-1kHPC-HashCounters");

    groupByBenchmarkWithExtraCountersConfigurations(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMaxClustered100k,
                                                    GroupBy::Hash, countAggregation, 1,
                                                    "2-100kHPC-HashCounters");

    groupByBenchmarkWithExtraCountersConfigurations(DataSweeps::linearUniformIntDistribution200mValuesCardinalitySweepFixedMaxCrossOverPoint,
                                                    GroupBy::Hash, countAggregation, 1,
                                                    "4HPC-HashCounters");

    groupByBenchmarkWithExtraCountersDuringRunConfigurations(DataFiles::uniformIntDistribution200mValuesCardinality400kMax200m,
                                                             countAggregation,
                                                             "5HPC-HashCounters");


    // SET TWO - BITS_PER_PASS = 8
*//*    // Graph 2: Cardinality range (variable max) for simple radix sort - manually run with BITS_PER_PASS of 8, 10, 12
    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepVariableMax,
                                   {GroupBy::SortRadix}, countAggregation,
                                   1, "2-RadixSimple8");

    // Graph 3: Cardinality range (variable max) for optimised radix sort - manually run with BITS_PER_PASS of 8, 10, 12
    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepVariableMax,
                                   {GroupBy::SortRadixOpt}, countAggregation,
                                   1, "3-RadixOpt8");


    // Graph 6: Cardinality range (fixed max) for simple radix sort - manually run with BITS_PER_PASS of 8, 10, 12
    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMax,
                                   {GroupBy::SortRadix}, countAggregation,
                                   1, "6-RadixSimple8");

    // Graph 7: Cardinality range (fixed max) for optimised radix sort - manually run with BITS_PER_PASS of 8, 10, 12
    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMax,
                                   {GroupBy::SortRadixOpt}, countAggregation,
                                   1, "7-RadixOpt8");

    // Graph 9: Cardinality range (fixed max) for single / double radix10 pass
    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMax,
                                   {GroupBy::SingleRadixPassThenHash,
                                    GroupBy::DoubleRadixPassThenHash}, countAggregation,
                                   1, "9-SingleDoubleRadix8PassThenHash");*//*


    // SET THREE - BITS_PER_PASS = 12
 *//*   // Graph 2: Cardinality range (variable max) for simple radix sort - manually run with BITS_PER_PASS of 8, 10, 12
    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepVariableMax,
                                   {GroupBy::SortRadix}, countAggregation,
                                   1, "2-RadixSimple12");

    // Graph 3: Cardinality range (variable max) for optimised radix sort - manually run with BITS_PER_PASS of 8, 10, 12
    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepVariableMax,
                                   {GroupBy::SortRadixOpt}, countAggregation,
                                   1, "3-RadixOpt12");


    // Graph 6: Cardinality range (fixed max) for simple radix sort - manually run with BITS_PER_PASS of 8, 10, 12
    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMax,
                                   {GroupBy::SortRadix}, countAggregation,
                                   1, "6-RadixSimple12");

    // Graph 7: Cardinality range (fixed max) for optimised radix sort - manually run with BITS_PER_PASS of 8, 10, 12
    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMax,
                                   {GroupBy::SortRadixOpt}, countAggregation,
                                   1, "7-RadixOpt12");

    // Graph 9: Cardinality range (fixed max) for single / double radix10 pass
    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMax,
                                   {GroupBy::SingleRadixPassThenHash,
                                    GroupBy::DoubleRadixPassThenHash}, countAggregation,
                                   1, "9-SingleDoubleRadix8PassThenHash");*/
//}

void allGroupBySingleThreadedTests() {
    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution20mValuesCardinalitySweepFixedMax,
                                   {GroupBy::Hash, GroupBy::Sort, GroupBy::Adaptive},
                                   10, "1-NoClustering");

    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution20mValuesCardinalitySweepFixedMaxClustered10,
                                   {GroupBy::Hash, GroupBy::Sort, GroupBy::Adaptive},
                                   10, "1-Clustered10");

    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution20mValuesCardinalitySweepFixedMaxClustered1k,
                                   {GroupBy::Hash, GroupBy::Sort, GroupBy::Adaptive},
                                   10, "1-Clustered1k");

    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution20mValuesCardinalitySweepFixedMaxClustered100k,
                                   {GroupBy::Hash, GroupBy::Sort, GroupBy::Adaptive},
                                   10, "1-Clustered100k");

    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution20mValuesCardinalitySweepVariableMax,
                                   {GroupBy::Hash, GroupBy::Sort, GroupBy::Adaptive},
                                   10, "1-NoClustering-VariableUpperBound");

//    groupByCpuCyclesSweepBenchmark64(DataSweeps::logUniformInt64Distribution20mValuesCardinalitySweepFixedMax,
//                                   {GroupBy::Hash, GroupBy::Sort, GroupBy::Adaptive},
//                                   10, "1-NoClustering-64bitInts");

    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution40mValuesCardinalitySweepFixedMax,
                                     {GroupBy::Hash, GroupBy::Sort, GroupBy::Adaptive},
                                     10, "1-NoClustering-40mValues");

    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMax,
                                     {GroupBy::Hash, GroupBy::Sort, GroupBy::Adaptive},
                                     10, "1-NoClustering-200mValues");

    groupByCpuCyclesSweepBenchmark(DataSweeps::linearUniformIntDistribution20mValuesCardinalitySections_100_3m_Max20m,
                                     {GroupBy::Hash, GroupBy::Sort, GroupBy::Adaptive},
                                     10, "2-TwoSection_100_3m");

    groupByCpuCyclesSweepBenchmark(DataSweeps::linearUniformIntDistribution20mValuesCardinalitySections_3m_100_Max20m,
                                   {GroupBy::Hash, GroupBy::Sort, GroupBy::Adaptive},
                                   10, "2-TwoSection_3m_100");

    groupByCpuCyclesSweepBenchmark(DataSweeps::linearUniformIntDistribution200mValuesMultipleCardinalitySections_100_10m_Max100m,
                                   {GroupBy::Hash, GroupBy::Sort, GroupBy::Adaptive},
                                   10, "3-MultipleSection_100_10m");

    groupByCpuCyclesSweepBenchmark(DataSweeps::linearUniformIntDistribution200mValuesMultipleCardinalitySections_10m_100_Max100m,
                                   {GroupBy::Hash, GroupBy::Sort, GroupBy::Adaptive},
                                   10, "3-MultipleSection_10m_100");

    tessilRobinMapInitialisationBenchmark("4-MapOverheadCosts");
}

void allGroupByParallelTests() {
    groupByWallTimeSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMax,
                                  {GroupBy::Adaptive},
                                  5, "5-DOP-1-CardinalitySweepSingle");

    groupByWallTimeDopSweepBenchmarkCalcDopRange(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMax,
                                                 5, "5-DOP-1-CardinalitySweepParallel");

    groupByWallTimeSweepBenchmark(DataSweeps::linearUniformIntDistribution200mValuesMultipleCardinalitySections_100_10m_Max100m,
                                  {GroupBy::Adaptive},
                                  5, "5-DOP-2-MultipleCardinalitySectionsFwdSingle");

    groupByWallTimeDopSweepBenchmarkCalcDopRange(DataSweeps::linearUniformIntDistribution200mValuesMultipleCardinalitySections_100_10m_Max100m,
                                                 5, "5-DOP-2-MultipleCardinalitySectionsFwdParallel");

    groupByWallTimeSweepBenchmark(DataSweeps::linearUniformIntDistribution200mValuesMultipleCardinalitySections_10m_100_Max100m,
                                  {GroupBy::Adaptive},
                                  5, "5-DOP-2-MultipleCardinalitySectionsBwdSingle");

    groupByWallTimeDopSweepBenchmarkCalcDopRange(DataSweeps::linearUniformIntDistribution200mValuesMultipleCardinalitySections_10m_100_Max100m,
                                                 5, "5-DOP-2-MultipleCardinalitySectionsBwdParallel");
}

int main() {

    MABPL::calculateMissingMachineConstants();

//    std::vector<float> inputThresholdDistribution;
//    generateLogDistribution(30, 1, 10*1000, inputThresholdDistribution);
//    selectCpuCyclesInputSweepBenchmark(DataFiles::uniformIntDistribution250mValuesMax10000,
//                                       {Select::ImplementationIndexesBranch,
//                                        Select::ImplementationIndexesPredication,
//                                        Select::ImplementationIndexesAdaptive},
//                                       inputThresholdDistribution,
//                                       1, "1-Indexes");

    return 0;
}