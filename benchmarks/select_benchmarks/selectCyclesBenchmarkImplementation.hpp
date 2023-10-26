#ifndef HAQP_SELECTCYCLESBENCHMARKIMPLEMENTATION_HPP
#define HAQP_SELECTCYCLESBENCHMARKIMPLEMENTATION_HPP


#include <iostream>
#include <functional>
#include <vector>
#include <string>
#include <cassert>

#include "utilities/dataHelpers.hpp"
#include "utilities/papiHelpers.hpp"
#include "selectCyclesBenchmark.hpp"
#include "data_generation/dataFiles.hpp"

using HAQP::Counters;


template <typename T1, typename T2>
void selectFunctionalityTest(const DataFile& dataFile, Select selectImplementation) {

    for (T1 i = 0; i <= 100; i += 10) {
        auto inputData = new T2[dataFile.getNumElements()];
        auto inputFilter = new T1[dataFile.getNumElements()];
        auto selection = new T2[dataFile.getNumElements()];
        copyArray<T2>(LoadedData<T2>::getInstance(dataFile).getData(), inputData, dataFile.getNumElements());
        copyArray<T1>(LoadedData<T1>::getInstance(dataFile).getData(), inputFilter, dataFile.getNumElements());

        auto selected = HAQP::runSelectFunction(selectImplementation,
                                                dataFile.getNumElements(), inputData, inputFilter, selection, i);
        std::cout << i << "%: " << static_cast<float>(selected) / static_cast<float>(dataFile.getNumElements()) << std::endl;

        delete[] inputData;
        delete[] inputFilter;
        delete[] selection;

    }
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

    int resultOne = HAQP::runSelectFunction(selectImpOne, dataFile.getNumElements(),
                                            inputData, inputFilter, selectionOne, threshold);
    std::sort(selectionOne, selectionOne + resultOne);

    auto selectionTwo = new int[dataFile.getNumElements()];

    std::cout << std::endl << "Running " << getSelectName(selectImpTwo) << "..." << std::endl;

    int resultTwo = HAQP::runSelectFunction(selectImpTwo, dataFile.getNumElements(),
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

    int resultOne = HAQP::runSelectFunction(selectImpOne, dataFile.getNumElements(),
                                            inputData, inputFilter, selectionOne, threshold);
    std::sort(selectionOne, selectionOne + resultOne);

    auto selectionTwo = new T2[dataFile.getNumElements()];

    std::cout << std::endl << "Running " << getSelectName(selectImpTwo) << "..." << std::endl;

    int resultTwo = HAQP::runSelectFunction(selectImpTwo, dataFile.getNumElements(),
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

template<typename T1, typename T2>
void selectSingleRunNoCounters(const DataFile &dataFile, Select selectImplementation, T1 threshold,
                               int iterations) {
    for (auto j = 0; j < iterations; ++j) {
        auto inputData = new T2[dataFile.getNumElements()];
        auto inputFilter = new T1[dataFile.getNumElements()];
        auto selection = new T2[dataFile.getNumElements()];
        copyArray<T2>(LoadedData<T2>::getInstance(dataFile).getData(), inputData, dataFile.getNumElements());
        copyArray<T1>(LoadedData<T1>::getInstance(dataFile).getData(), inputFilter, dataFile.getNumElements());

        std::cout << "Running threshold " << threshold << ", iteration " << j + 1 << "... ";

        HAQP::runSelectFunction(selectImplementation,
                                 dataFile.getNumElements(), inputData, inputFilter, selection, threshold);

        delete[] inputData;
        delete[] inputFilter;
        delete[] selection;

        std::cout << "Completed" << std::endl;
    }
}

template<typename T1, typename T2>
void selectCpuCyclesSingleInputBenchmark(const DataFile &dataFile,
                                         const std::vector<Select> &selectImplementations, T1 threshold,
                                         int iterations, const std::string &fileNamePrefix) {
    int numTests = static_cast<int>(selectImplementations.size());
    long_long cycles;
    std::vector<std::vector<long_long>> results(iterations, std::vector<long_long>(numTests + 1, 0));

    for (auto i = 0; i < iterations; ++i) {
        results[i][0] = static_cast<long_long>(threshold);

        for (auto j = 0; j < numTests; ++j) {
            auto inputData = new T2[dataFile.getNumElements()];
            auto inputFilter = new T1[dataFile.getNumElements()];
            auto selection = new T2[dataFile.getNumElements()];
            copyArray<T2>(LoadedData<T2>::getInstance(dataFile).getData(), inputData, dataFile.getNumElements());
            copyArray<T1>(LoadedData<T1>::getInstance(dataFile).getData(), inputFilter, dataFile.getNumElements());

            std::cout << "Running " << getSelectName(selectImplementations[j]) << ", iteration " << i + 1 << "... ";

            cycles = *Counters::getInstance().readSharedEventSet();

            HAQP::runSelectFunction(selectImplementations[j],
                                     dataFile.getNumElements(), inputData, inputFilter, selection, threshold);

            results[i][1 + j] = *Counters::getInstance().readSharedEventSet() - cycles;

            delete[] inputData;
            delete[] inputFilter;
            delete[] selection;

            std::cout << "Completed" << std::endl;
        }
    }

    std::vector<std::string> headers(1 + numTests);
    headers [0] = "Input";
    for (auto i = 0; i < numTests; ++i) {
        headers[1 + i] = getSelectName(selectImplementations[i]);
    }

    std::string fileName =
            fileNamePrefix +
            "SingleCyclesBM_" +
            dataFile.getFileName() +
            "_threshold_" +
            std::to_string(threshold);
    std::string fullFilePath = FilePaths::getInstance().getSelectCyclesFolderPath() + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);
}

template<typename T1, typename T2>
void selectCpuCyclesMultipleInputBenchmark(const DataFile &dataFile,
                                           const std::vector<Select> &selectImplementations,
                                           int selectivityStride, int iterations, const std::string &fileNamePrefix) {
    int numTests = 1 + (100 / selectivityStride);
    int implementations = static_cast<int>(selectImplementations.size());
    int dataCols = implementations * iterations;

    long_long cycles;
    std::vector<std::vector<long_long>> results(numTests, std::vector<long_long>(dataCols + 1, 0));
    for (auto i = 0; i < implementations; ++i) {
        auto count = 0;

        for (auto j = 0; j <= 100; j += selectivityStride) {
            results[count][0] = static_cast<long_long>(j);

            for (auto k = 0; k < iterations; ++k) {
                auto inputData = new T2[dataFile.getNumElements()];
                auto inputFilter = new T1[dataFile.getNumElements()];
                auto selection = new T2[dataFile.getNumElements()];
                copyArray<T2>(LoadedData<T2>::getInstance(dataFile).getData(), inputData, dataFile.getNumElements());
                copyArray<T1>(LoadedData<T1>::getInstance(dataFile).getData(), inputFilter, dataFile.getNumElements());

                std::cout << "Running " << getSelectName(selectImplementations[i]) << ", threshold ";
                std::cout << j << ", iteration " << k + 1 << "... ";

                cycles = *Counters::getInstance().readSharedEventSet();

                HAQP::runSelectFunction(selectImplementations[j],
                                         dataFile.getNumElements(), inputData, inputFilter, selection, j);

                results[count][1 + (i * iterations) + k] = *Counters::getInstance().readSharedEventSet() - cycles;

                delete[] inputData;
                delete[] inputFilter;
                delete[] selection;

                std::cout << "Completed" << std::endl;
            }

            count++;
        }
    }

    std::vector<std::string> headers(1 + dataCols);
    headers[0] = "Threshold";
    for (auto i = 0; i < dataCols; ++i) {
        headers[1 + i] =
                getSelectName(selectImplementations[i / iterations]) + " iteration_" + std::to_string(i % iterations);
    }

    std::string fileName =
            fileNamePrefix +
            "MultiplesCyclesBM_" +
            dataFile.getFileName() +
            "_selectivityStride_" +
            std::to_string(selectivityStride);
    std::string fullFilePath = FilePaths::getInstance().getSelectCyclesFolderPath() + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);
}

template<typename T1, typename T2>
void selectBenchmarkWithExtraCounters(const DataFile &dataFile, Select selectImplementation,
                                      std::vector<float> &thresholds, int iterations,
                                      std::vector<std::string> &benchmarkCounters, const std::string &fileNamePrefix) {
    if (selectImplementation == Select::ImplementationIndexesAdaptive ||
        selectImplementation == Select::ImplementationValuesAdaptive)
        std::cout << "Cannot benchmark adaptive select using counters as adaptive select is already using these counters" << std::endl;

    int numTests = static_cast<int>(thresholds.size());

    long_long benchmarkCounterValues[benchmarkCounters.size()];
    int benchmarkEventSet = initialisePAPIandCreateEventSet(benchmarkCounters);

    std::vector<std::vector<long_long>> results(numTests, std::vector<long_long>((iterations * benchmarkCounters.size()) + 1, 0));
    auto count = 0;

    for (auto i = 0; i < numTests; ++i) {
        results[count][0] = static_cast<long_long>(thresholds[i]);

        for (auto j = 0; j < iterations; ++j) {
            auto inputData = new T2[dataFile.getNumElements()];
            auto inputFilter = new T1[dataFile.getNumElements()];
            auto selection = new T2[dataFile.getNumElements()];
            copyArray<T2>(LoadedData<T2>::getInstance(dataFile).getData(), inputData, dataFile.getNumElements());
            copyArray<T1>(LoadedData<T1>::getInstance(dataFile).getData(), inputFilter, dataFile.getNumElements());

            std::cout << "Running threshold " << static_cast<int>(thresholds[i]) << ", iteration " << j + 1 << "... ";

            if (PAPI_reset(benchmarkEventSet) != PAPI_OK)
                exit(1);

            HAQP::runSelectFunction(selectImplementation,
                                     dataFile.getNumElements(), inputData, inputFilter, selection,
                                     static_cast<T1>(thresholds[i]));

            if (PAPI_read(benchmarkEventSet, benchmarkCounterValues) != PAPI_OK)
                exit(1);

            delete[] inputData;
            delete[] inputFilter;
            delete[] selection;

            for (int k = 0; k < static_cast<int>(benchmarkCounters.size()); ++k) {
                results[count][1 + (j * benchmarkCounters.size()) + k] = benchmarkCounterValues[k];
            }

            std::cout << "Completed" << std::endl;
        }

        count++;
    }

    std::vector<std::string> headers(benchmarkCounters.size() * iterations);
    for (auto i = 0; i < iterations; ++i) {
        std::copy(benchmarkCounters.begin(), benchmarkCounters.end(), headers.begin() + i * benchmarkCounters.size());
    }
    headers.insert(headers.begin(), "Selectivity");

    std::string fileName =
            fileNamePrefix +
            "ExtraCountersCyclesBM_" +
            getSelectName(selectImplementation) +
            dataFile.getFileName();
    std::string fullFilePath = FilePaths::getInstance().getSelectCyclesFolderPath() + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);

    shutdownPAPI(benchmarkEventSet, benchmarkCounterValues);
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

template<typename T1, typename T2>
void selectCpuCyclesSweepBenchmark(DataSweep &dataSweep, const std::vector<Select> &selectImplementations,
                                   T1 threshold, int iterations, const std::string &fileNamePrefix) {
    assert(!selectImplementations.empty());
    if (std::count(selectImplementations.begin(), selectImplementations.end(), Select::ImplementationIndexesAdaptiveParallel) ||
        std::count(selectImplementations.begin(), selectImplementations.end(), Select::ImplementationValuesAdaptiveParallel )) {
        std::cout << "Cannot use cpu cycles to time multi-threaded programs, use wall time instead" << std::endl;
        exit(1);
    }


    int dataCols = iterations * static_cast<int>(selectImplementations.size());
    long_long cycles;
    std::vector<std::vector<double>> results(dataSweep.getTotalRuns(),
                                             std::vector<double>(dataCols + 1, 0));

    for (auto i = 0; i < iterations; ++i) {
        for (auto j = 0; j < static_cast<int>(selectImplementations.size()); ++j) {
            for (auto k = 0; k < dataSweep.getTotalRuns(); ++k) {
                results[k][0] = static_cast<double>(dataSweep.getRunInput());
                auto inputData = new T2[dataSweep.getNumElements()];
                auto inputFilter = new T1[dataSweep.getNumElements()];
                auto selection = new T2[dataSweep.getNumElements()];

                std::cout << "Running " << getSelectName(selectImplementations[j]) << " for input ";
                std::cout << dataSweep.getRunInput() << "... ";

                dataSweep.loadNextDataSetIntoMemory<T1>(inputFilter);
                generateUniformDistributionInMemory<T2>(inputData, dataSweep.getNumElements(), 10);

                cycles = *Counters::getInstance().readSharedEventSet();

                HAQP::runSelectFunction(selectImplementations[j],
                                         dataSweep.getNumElements(), inputData, inputFilter, selection, threshold);

                results[k][1 + (i * selectImplementations.size()) + j] =
                        static_cast<double>(*Counters::getInstance().readSharedEventSet() - cycles);

                delete[] inputData;
                delete[] inputFilter;
                delete[] selection;

                std::cout << "Completed" << std::endl;
            }
            dataSweep.restartSweep();
        }
    }

    std::vector<std::string> headers(1 + dataCols);
    headers [0] = "Input";
    for (auto i = 0; i < dataCols; ++i) {
        headers[1 + i] = getSelectName(selectImplementations[i % selectImplementations.size()]);
    }

    std::string fileName =
            fileNamePrefix +
            "SweepCyclesBM_" +
            dataSweep.getSweepName() +
            "_threshold_" +
            std::to_string(threshold);
    std::string fullFilePath = FilePaths::getInstance().getSelectCyclesFolderPath() + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);
}

template <typename T1, typename T2>
void selectWallTimeSweepBenchmark(DataSweep &dataSweep, const std::vector<Select> &selectImplementations,
                                  T1 threshold, int iterations, const std::string &fileNamePrefix) {
    assert(!selectImplementations.empty());

    int dataCols = iterations * static_cast<int>(selectImplementations.size());
    long_long wallTime;
    std::vector<std::vector<double>> results(dataSweep.getTotalRuns(),
                                             std::vector<double>(dataCols + 1, 0));

    for (auto i = 0; i < iterations; ++i) {
        for (auto j = 0; j < static_cast<int>(selectImplementations.size()); ++j) {
            for (auto k = 0; k < dataSweep.getTotalRuns(); ++k) {
                results[k][0] = static_cast<double>(dataSweep.getRunInput());
                auto inputData = new T2[dataSweep.getNumElements()];
                auto inputFilter = new T1[dataSweep.getNumElements()];
                auto selection = new T2[dataSweep.getNumElements()];

                std::cout << "Running " << getSelectName(selectImplementations[j]) << " for input ";
                std::cout << dataSweep.getRunInput() << "... ";

                dataSweep.loadNextDataSetIntoMemory<T1>(inputFilter);
                generateUniformDistributionInMemory<T2>(inputData, dataSweep.getNumElements(), 10);

                wallTime = PAPI_get_real_usec();

                HAQP::runSelectFunction(selectImplementations[j],
                                         dataSweep.getNumElements(), inputData, inputFilter, selection, threshold);

                results[k][1 + (i * selectImplementations.size()) + j] =
                        static_cast<double>(PAPI_get_real_usec() - wallTime);

                delete[] inputData;
                delete[] inputFilter;
                delete[] selection;

                std::cout << "Completed" << std::endl;
            }
            dataSweep.restartSweep();
        }
    }

    std::vector<std::string> headers(1 + dataCols);
    headers [0] = "Input";
    for (auto i = 0; i < dataCols; ++i) {
        headers[1 + i] = getSelectName(selectImplementations[i % selectImplementations.size()]);
    }

    std::string fileName =
            fileNamePrefix +
            "SweepWallTimeBM_" +
            dataSweep.getSweepName() +
            "_threshold_" +
            std::to_string(threshold);
    std::string fullFilePath = FilePaths::getInstance().getSelectCyclesFolderPath() + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);
}

template <typename T1, typename T2>
void selectWallTimeDopSweepBenchmark(DataSweep &dataSweep, Select selectImplementation, T1 threshold,
                                     int iterations, const std::string &fileNamePrefix, std::vector<int> dop) {
    assert(selectImplementation == Select::ImplementationValuesAdaptiveParallel ||
           selectImplementation == Select::ImplementationIndexesAdaptiveParallel);

    int dataCols = iterations * static_cast<int>(dop.size());
    long_long wallTime;
    std::vector<std::vector<double>> results(dataSweep.getTotalRuns(),
                                             std::vector<double>(dataCols + 1, 0));

    for (auto i = 0; i < iterations; ++i) {
        for (auto j = 0; j < static_cast<int>(dop.size()); ++j) {
            for (auto k = 0; k < dataSweep.getTotalRuns(); ++k) {
                results[k][0] = static_cast<double>(dataSweep.getRunInput());
                auto inputData = new T2[dataSweep.getNumElements()];
                auto inputFilter = new T1[dataSweep.getNumElements()];
                auto selection = new T2[dataSweep.getNumElements()];

                std::cout << "Running dop of " << dop[j] << " for input ";
                std::cout << dataSweep.getRunInput() << "... ";

                dataSweep.loadNextDataSetIntoMemory<T1>(inputFilter);
                generateUniformDistributionInMemory<T2>(inputData, dataSweep.getNumElements(), 10);

                wallTime = PAPI_get_real_usec();

                HAQP::runSelectFunction(selectImplementation,dataSweep.getNumElements(), inputData, inputFilter,
                                         selection, threshold, dop[j]);

                results[k][1 + (i * dop.size()) + j] =
                        static_cast<double>(PAPI_get_real_usec() - wallTime);

                delete[] inputData;
                delete[] inputFilter;
                delete[] selection;

                std::cout << "Completed" << std::endl;
            }
            dataSweep.restartSweep();
        }
    }

    std::vector<std::string> headers(1 + dataCols);
    headers [0] = "Input";
    for (auto i = 0; i < dataCols; ++i) {
        headers[1 + i] = "dop-" + std::to_string(dop[i % dop.size()]);
    }

    std::string fileName =
            fileNamePrefix +
            "DopSweepWallTimeBM_" +
            dataSweep.getSweepName() +
            "_threshold_" +
            std::to_string(threshold);
    std::string fullFilePath = FilePaths::getInstance().getSelectCyclesFolderPath() + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);
}

template <typename T1, typename T2>
void selectWallTimeDopSweepBenchmarkCalcDopRange(DataSweep &dataSweep, Select selectImplementation,
                                                 int threshold, int iterations,
                                                 const std::string &fileNamePrefix) {
    int dop = 2;
    std::vector<int> dopValues;
    while (dop <= HAQP::logicalCoresCount()) {
        dopValues.push_back(dop);
        dop *= 2;
    }

    selectWallTimeDopSweepBenchmark<T1,T2>(dataSweep, selectImplementation, threshold, iterations,
                                           fileNamePrefix, dopValues);
}

template <typename T1, typename T2>
void selectCpuCyclesInputSweepBenchmark(const DataFile &dataFile,
                                        const std::vector<Select> &selectImplementations,
                                        std::vector<float> &thresholds, int iterations,
                                        const std::string &fileNamePrefix) {
    assert(!selectImplementations.empty());
    if (std::count(selectImplementations.begin(), selectImplementations.end(), Select::ImplementationIndexesAdaptiveParallel) ||
        std::count(selectImplementations.begin(), selectImplementations.end(), Select::ImplementationValuesAdaptiveParallel )) {
        std::cout << "Cannot use cpu cycles to time multi-threaded programs, use wall time instead" << std::endl;
        exit(1);
    }

    int dataCols = iterations * static_cast<int>(selectImplementations.size());
    long_long cycles;
    std::vector<std::vector<double>> results(thresholds.size(),
                                             std::vector<double>(dataCols + 1, 0));

    for (auto i = 0; i < iterations; ++i) {
        for (auto j = 0; j < static_cast<int>(selectImplementations.size()); ++j) {
            for (auto k = 0; k < static_cast<int>(thresholds.size()); ++k) {
                results[k][0] = static_cast<int>(thresholds[k]);
                auto inputData = new T2[dataFile.getNumElements()];
                auto inputFilter = new T1[dataFile.getNumElements()];
                auto selection = new T2[dataFile.getNumElements()];
                copyArray<T2>(LoadedData<T2>::getInstance(dataFile).getData(), inputData, dataFile.getNumElements());
                copyArray<T1>(LoadedData<T1>::getInstance(dataFile).getData(), inputFilter, dataFile.getNumElements());

                std::cout << "Running " << getSelectName(selectImplementations[j]) << " for threshold ";
                std::cout << std::to_string(static_cast<int>(thresholds[k])) << "... ";

                cycles = *Counters::getInstance().readSharedEventSet();

                HAQP::runSelectFunction(selectImplementations[j],
                                         dataFile.getNumElements(), inputData, inputFilter,
                                         selection, static_cast<T1>(thresholds[k]));

                results[k][1 + (i * selectImplementations.size()) + j] =
                        static_cast<double>(*Counters::getInstance().readSharedEventSet() - cycles);

                delete[] inputData;
                delete[] inputFilter;
                delete[] selection;

                std::cout << "Completed" << std::endl;
            }
        }
    }

    std::vector<std::string> headers(1 + dataCols);
    headers [0] = "Input";
    for (auto i = 0; i < dataCols; ++i) {
        headers[1 + i] = getSelectName(selectImplementations[i % selectImplementations.size()]);
    }

    std::string fileName = fileNamePrefix + "InputSweepCyclesBM_" + dataFile.getFileName();
    std::string fullFilePath = FilePaths::getInstance().getSelectCyclesFolderPath() + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);
}

template <typename T1, typename T2>
void selectWallTimeInputSweepBenchmark(const DataFile &dataFile,
                                       const std::vector<Select> &selectImplementations,
                                       std::vector<float> &thresholds, int iterations,
                                       const std::string &fileNamePrefix) {
    assert(!selectImplementations.empty());

    int dataCols = iterations * static_cast<int>(selectImplementations.size());
    long_long wallTime;
    std::vector<std::vector<double>> results(thresholds.size(),
                                             std::vector<double>(dataCols + 1, 0));

    for (auto i = 0; i < iterations; ++i) {
        for (auto j = 0; j < static_cast<int>(selectImplementations.size()); ++j) {
            for (auto k = 0; k < static_cast<int>(thresholds.size()); ++k) {
                results[k][0] = static_cast<int>(thresholds[k]);
                auto inputData = new T2[dataFile.getNumElements()];
                auto inputFilter = new T1[dataFile.getNumElements()];
                auto selection = new T2[dataFile.getNumElements()];
                copyArray<T2>(LoadedData<T2>::getInstance(dataFile).getData(), inputData, dataFile.getNumElements());
                copyArray<T1>(LoadedData<T1>::getInstance(dataFile).getData(), inputFilter, dataFile.getNumElements());

                std::cout << "Running " << getSelectName(selectImplementations[j]) << " for threshold ";
                std::cout << std::to_string(static_cast<int>(thresholds[k])) << "... ";

                wallTime = PAPI_get_real_usec();

                HAQP::runSelectFunction(selectImplementations[j],
                                         dataFile.getNumElements(), inputData, inputFilter,
                                         selection, static_cast<T1>(thresholds[k]));

                results[k][1 + (i * selectImplementations.size()) + j] =
                        static_cast<double>(PAPI_get_real_usec() - wallTime);

                delete[] inputData;
                delete[] inputFilter;
                delete[] selection;

                std::cout << "Completed" << std::endl;
            }
        }
    }

    std::vector<std::string> headers(1 + dataCols);
    headers [0] = "Input";
    for (auto i = 0; i < dataCols; ++i) {
        headers[1 + i] = getSelectName(selectImplementations[i % selectImplementations.size()]);
    }

    std::string fileName = fileNamePrefix + "InputSweepWallTimeBM_" + dataFile.getFileName();
    std::string fullFilePath = FilePaths::getInstance().getSelectCyclesFolderPath() + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);
}

template <typename T1, typename T2>
void selectWallTimeDopAndInputSweepBenchmark(const DataFile &dataFile,
                                             Select selectImplementation,
                                             std::vector<float> &thresholds, int iterations,
                                             const std::string &fileNamePrefix, std::vector<int> dop) {
    assert(selectImplementation == Select::ImplementationValuesAdaptiveParallel ||
           selectImplementation == Select::ImplementationIndexesAdaptiveParallel);

    int dataCols = iterations * static_cast<int>(dop.size());
    long_long wallTime;
    std::vector<std::vector<double>> results(thresholds.size(),
                                             std::vector<double>(dataCols + 1, 0));

    for (auto i = 0; i < iterations; ++i) {
        for (auto j = 0; j < static_cast<int>(dop.size()); ++j) {
            for (auto k = 0; k < static_cast<int>(thresholds.size()); ++k) {
                results[k][0] = static_cast<int>(thresholds[k]);
                auto inputData = new T2[dataFile.getNumElements()];
                auto inputFilter = new T1[dataFile.getNumElements()];
                auto selection = new T2[dataFile.getNumElements()];
                copyArray<T2>(LoadedData<T2>::getInstance(dataFile).getData(), inputData, dataFile.getNumElements());
                copyArray<T1>(LoadedData<T1>::getInstance(dataFile).getData(), inputFilter, dataFile.getNumElements());

                std::cout << "Running dop of " << dop[j] << " for input ";
                std::cout << std::to_string(static_cast<int>(thresholds[k])) << "... ";

                wallTime = PAPI_get_real_usec();

                HAQP::runSelectFunction(selectImplementation,
                                         dataFile.getNumElements(), inputData, inputFilter,
                                         selection, static_cast<T1>(thresholds[k]), dop[j]);

                results[k][1 + (i * dop.size()) + j] =
                        static_cast<double>(PAPI_get_real_usec() - wallTime);

                delete[] inputData;
                delete[] inputFilter;
                delete[] selection;

                std::cout << "Completed" << std::endl;
            }
        }
    }

    std::vector<std::string> headers(1 + dataCols);
    headers [0] = "Input";
    for (auto i = 0; i < dataCols; ++i) {
        headers[1 + i] = "dop-" + std::to_string(dop[i % dop.size()]);
    }

    std::string fileName = fileNamePrefix + "DopAndInputSweepWallTimeBM_" + dataFile.getFileName();
    std::string fullFilePath = FilePaths::getInstance().getSelectCyclesFolderPath() + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);
}

template <typename T1, typename T2>
void selectWallTimeDopAndInputSweepBenchmarkCalcDopRange(const DataFile &dataFile, Select selectImplementation,
                                                         std::vector<float> &thresholds, int iterations,
                                                         const std::string &fileNamePrefix) {
    int dop = 2;
    std::vector<int> dopValues;
    while (dop <= HAQP::logicalCoresCount()) {
        dopValues.push_back(dop);
        dop *= 2;
    }

    selectWallTimeDopAndInputSweepBenchmark<T1,T2>(dataFile, selectImplementation, thresholds, iterations,
                                                   fileNamePrefix, dopValues);
}


#endif //HAQP_SELECTCYCLESBENCHMARKIMPLEMENTATION_HPP
