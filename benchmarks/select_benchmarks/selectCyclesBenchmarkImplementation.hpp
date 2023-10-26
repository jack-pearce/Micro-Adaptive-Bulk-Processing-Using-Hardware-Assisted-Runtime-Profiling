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
