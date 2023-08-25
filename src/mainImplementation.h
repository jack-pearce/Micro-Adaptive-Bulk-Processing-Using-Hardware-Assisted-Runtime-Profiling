#ifndef MABPL_MAINIMPLEMENTATION_H
#define MABPL_MAINIMPLEMENTATION_H

#include <string>
#include <vector>
#include <iostream>

#include "time_benchmarking/selectTimeBenchmark.h"
#include "time_benchmarking/timeBenchmarkHelpers.h"
#include "data_generation/dataGenerators.h"
#include "utilities/dataHelpers.h"
#include "cycles_benchmarking/selectCyclesBenchmark.h"
#include "cycles_benchmarking/groupByCyclesBenchmark.h"
#include "cycles_benchmarking/radixPartitionCyclesBenchmark.h"
#include "data_generation/dataFiles.h"
#include "library/mabpl.h"

using MABPL::Select;
using MABPL::GroupBy;

using MABPL::MinAggregation;
using MABPL::MaxAggregation;
using MABPL::SumAggregation;
using MABPL::CountAggregation;


template <typename T>
void dataDistributionTest(const DataFile& dataFile) {
    LoadedData<T>::getInstance(dataFile);
    std::cout << dataFile.getNumElements() << " elements" << std::endl;
    displayDistribution<T>(dataFile);
}

template <typename T1, typename T2>
void selectFunctionalityTest(const DataFile& dataFile, Select selectImplementation) {

    for (T1 i = 0; i <= 100; i += 10) {
        auto inputData = new T2[dataFile.getNumElements()];
        auto inputFilter = new T1[dataFile.getNumElements()];
        auto selection = new T2[dataFile.getNumElements()];
        copyArray<T2>(LoadedData<T2>::getInstance(dataFile).getData(), inputData, dataFile.getNumElements());
        copyArray<T1>(LoadedData<T1>::getInstance(dataFile).getData(), inputFilter, dataFile.getNumElements());

        auto selected = MABPL::runSelectFunction(selectImplementation,
                                                 dataFile.getNumElements(), inputData, inputFilter, selection, i);
        std::cout << i << "%: " << static_cast<float>(selected) / static_cast<float>(dataFile.getNumElements()) << std::endl;

        delete[] inputData;
        delete[] inputFilter;
        delete[] selection;

    }
}

template <typename T>
void runSelectTimeBenchmark(const DataFile& dataFile, Select selectImplementation, int selectivityStride) {
    selectTimeBenchmark<T>(dataFile, selectImplementation, selectivityStride);
    runTimeBenchmark(0, nullptr);
}

template <typename T>
void runSelectTimeBenchmarkSetIterations(const DataFile& dataFile, Select selectImplementation, int selectivityStride, int iterations) {
    selectTimeBenchmarkSetIterations<T>(dataFile, selectImplementation, selectivityStride, iterations);
    runTimeBenchmark(0, nullptr);
}

template <typename T1, typename T2>
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

    selectBenchmarkWithExtraCounters<T1,T2>(dataFile,
                                     selectImplementation,
                                     inputThresholdDistribution,
                                     iterations,
                                     benchmarkCounters, "");
}

template <typename T>
void selectIndexesCompareResultsTest(const DataFile& dataFile, Select selectImpOne, Select selectImpTwo) {
    int threshold = 3;
    auto inputFilter = new T[dataFile.getNumElements()];
    auto selectionOne = new int[dataFile.getNumElements()];
    int *inputData;
    copyArray<T>(LoadedData<T>::getInstance(dataFile).getData(), inputFilter,
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

    for (auto i = 0; i < resultOne; ++i) {
        if (selectionOne[i] != selectionTwo[i]) {
            std::cout << "Different index found" << std::endl;
        }
    }

    delete[] inputFilter;
    delete[] selectionOne;
    delete[] selectionTwo;
}

template <typename T1, typename T2>
void selectValuesCompareResultsTest(const DataFile& dataFile, Select selectImpOne, Select selectImpTwo) {
    T1 threshold = 3;
    auto inputFilter = new T1[dataFile.getNumElements()];
    auto inputData = new T2[dataFile.getNumElements()];
    auto selectionOne = new T2[dataFile.getNumElements()];
    copyArray<T1>(LoadedData<T1>::getInstance(dataFile).getData(), inputFilter,
                   dataFile.getNumElements());
    copyArray<T2>(LoadedData<T2>::getInstance(dataFile).getData(), inputData,
                   dataFile.getNumElements());

    std::cout << "Running " << getSelectName(selectImpOne) << "..." << std::endl;

    int resultOne = MABPL::runSelectFunction(selectImpOne, dataFile.getNumElements(),
                                             inputData, inputFilter, selectionOne, threshold);
    std::sort(selectionOne, selectionOne + resultOne);

    auto selectionTwo = new T2[dataFile.getNumElements()];

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

    for (auto i = 0; i < resultOne; ++i) {
        if (selectionOne[i] != selectionTwo[i]) {
            std::cout << "Different index found" << std::endl;
        }
    }

    delete[] inputFilter;
    delete[] inputData;
    delete[] selectionOne;
    delete[] selectionTwo;
}

template <typename T1, typename T2>
void selectWallTimeDopSweepBenchmarkCalcDopRange(DataSweep &dataSweep, Select selectImplementation,
                                                 int threshold, int iterations,
                                                 const std::string &fileNamePrefix) {
    int dop = 2;
    std::vector<int> dopValues;
    while (dop <= MABPL::maxDop()) {
        dopValues.push_back(dop);
        dop *= 2;
    }

    selectWallTimeDopSweepBenchmark<T1,T2>(dataSweep, selectImplementation, threshold, iterations,
                                    fileNamePrefix, dopValues);
}

template <typename T1, typename T2>
void selectWallTimeDopAndInputSweepBenchmarkCalcDopRange(const DataFile &dataFile, Select selectImplementation,
                                                         std::vector<float> &thresholds, int iterations,
                                                         const std::string &fileNamePrefix) {
    int dop = 2;
    std::vector<int> dopValues;
    while (dop <= MABPL::maxDop()) {
        dopValues.push_back(dop);
        dop *= 2;
    }

    selectWallTimeDopAndInputSweepBenchmark<T1,T2>(dataFile, selectImplementation, thresholds, iterations,
                                            fileNamePrefix, dopValues);
}

template <typename T1, typename T2>
void groupByCompareResultsTest(const DataFile& dataFile, GroupBy groupByImpOne, GroupBy groupByImpTwo) {
    auto inputGroupBy = new T1[dataFile.getNumElements()];
    auto inputAggregate = new T2[dataFile.getNumElements()];
    copyArray<T1>(LoadedData<T1>::getInstance(dataFile).getData(), inputGroupBy, dataFile.getNumElements());
    generateUniformDistributionInMemory<T2>(inputAggregate, dataFile.getNumElements(), 10);

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

    for (auto i = 0; i < static_cast<int>(resultOne.size()); ++i) {
        if ((resultOne[i].first != resultTwo[i].first) || (resultOne[i].second != resultTwo[i].second)) {
            std::cout << "Different row found" << std::endl;
        }
    }

    delete []inputGroupBy;
    delete []inputAggregate;
}

template <typename T1, typename T2>
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

    groupByBenchmarkWithExtraCounters<T1,T2>(dataSweep, groupByImplementation, iterations, benchmarkCounters, fileNamePrefix);
}

template <typename T1, typename T2>
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

    groupByBenchmarkWithExtraCountersDuringRun<T1,T2>(dataFile, benchmarkCounters, fileNamePrefix);
}

template <typename T1, typename T2>
void groupByWallTimeDopSweepBenchmarkCalcDopRange(DataSweep &dataSweep, int iterations,
                                                  const std::string &fileNamePrefix) {
    int dop = 2;
    std::vector<int> dopValues;
    while (dop <= MABPL::maxDop()) {
        dopValues.push_back(dop);
        dop *= 2;
    }

    groupByWallTimeDopSweepBenchmark<T1,T2>(dataSweep, iterations, fileNamePrefix, dopValues);
}

template<typename T1, typename T2>
void radixPartitionBitsSweepBenchmarkWithExtraCountersConfigurations(const DataFile &dataFile, int startBits,
                                                                     int endBits, const std::string &fileNamePrefix,
                                                                     int iterations) {
    {
        std::vector<std::string> benchmarkCounters = {"PERF_COUNT_HW_CPU_CYCLES",
                                                      "INSTRUCTION_RETIRED",
                                                      "DTLB-STORE-MISSES",
                                                      "DTLB-STORES",
                                                      "DTLB-LOAD-MISSES",
                                                      "PERF_COUNT_HW_CACHE_L1D",
                                                      "PERF_COUNT_HW_CACHE_REFERENCES",
                                                      "PERF_COUNT_HW_CACHE_MISSES"};

        radixPartitionBitsSweepBenchmarkWithExtraCounters<T1, T2>(dataFile, startBits, endBits, benchmarkCounters,
                                                                  fileNamePrefix, iterations);
    }

/*    {
        std::vector<std::string> benchmarkCounters = {"PERF_COUNT_HW_CPU_CYCLES",
                                                      "DTLB-STORES",
                                                      "DTLB-STORE-MISSES",
                                                      "DTLB-LOADS",
                                                      "DTLB-LOAD-MISSES"};

        radixPartitionBitsSweepBenchmarkWithExtraCounters<T1, T2>(dataFile, startBits, endBits, benchmarkCounters,
                                                                  fileNamePrefix + "1_", iterations);
    }*/

/*    {
        std::vector<std::string> benchmarkCounters = {"PERF_COUNT_HW_CPU_CYCLES",
                                                      "INSTRUCTION_RETIRED",
                                                      "PERF_COUNT_HW_CACHE_L1D",
                                                      "PERF_COUNT_HW_CACHE_REFERENCES",
                                                      "PERF_COUNT_HW_CACHE_MISSES"};

        radixPartitionBitsSweepBenchmarkWithExtraCounters<T1, T2>(dataFile, startBits, endBits, benchmarkCounters,
                                                                  fileNamePrefix + "2_", iterations);
    }*/
}

template<typename T1, typename T2>
void radixPartitionSweepBenchmarkWithExtraCountersConfigurations(DataSweep &dataSweep, int radixBits,
                                                                 const std::string &fileNamePrefix,
                                                                 int iterations) {
    {
        std::vector<std::string> benchmarkCounters = {"PERF_COUNT_HW_CPU_CYCLES",
                                                      "INSTRUCTION_RETIRED",
                                                      "DTLB-STORE-MISSES",
                                                      "DTLB-STORES",
                                                      "DTLB-LOAD-MISSES",
                                                      "PERF_COUNT_HW_CACHE_L1D",
                                                      "PERF_COUNT_HW_CACHE_REFERENCES",
                                                      "PERF_COUNT_HW_CACHE_MISSES"};

        radixPartitionSweepBenchmarkWithExtraCounters<T1, T2>(dataSweep, radixBits, benchmarkCounters,
                                                              fileNamePrefix, iterations);
    }

/*    {
        std::vector<std::string> benchmarkCounters = {"PERF_COUNT_HW_CPU_CYCLES",
                                                      "DTLB-STORES",
                                                      "DTLB-STORE-MISSES",
                                                      "DTLB-LOADS",
                                                      "DTLB-LOAD-MISSES"};

        radixPartitionSweepBenchmarkWithExtraCounters<T1, T2>(dataSweep, radixBits, benchmarkCounters,
                                                                  fileNamePrefix + "1_", iterations);
    }

    {
        std::vector<std::string> benchmarkCounters = {"PERF_COUNT_HW_CPU_CYCLES",
                                                      "INSTRUCTION_RETIRED",
                                                      "PERF_COUNT_HW_CACHE_L1D",
                                                      "PERF_COUNT_HW_CACHE_REFERENCES",
                                                      "PERF_COUNT_HW_CACHE_MISSES"};

        radixPartitionSweepBenchmarkWithExtraCounters<T1, T2>(dataSweep, radixBits, benchmarkCounters,
                                                                  fileNamePrefix + "2_", iterations);
    }*/
}



#endif //MABPL_MAINIMPLEMENTATION_H
