#include <iostream>
#include <functional>
#include <vector>
#include <string>
#include <cassert>

#include "../utils/dataHelpers.h"
#include "../utils/papiHelpers.h"
#include "selectCounterBenchmark.h"
#include "../data_generation/config.h"


void selectSingleRunNoCounters(const DataFile &dataFile, Select selectImplementation, int threshold,
                               int iterations) {
    for (auto j = 0; j < iterations; ++j) {
        auto inputData = new int[dataFile.getNumElements()];
        auto inputFilter = new int[dataFile.getNumElements()];
        auto selection = new int[dataFile.getNumElements()];
        copyArray(LoadedData::getInstance(dataFile).getData(), inputData, dataFile.getNumElements());
        copyArray(LoadedData::getInstance(dataFile).getData(), inputFilter, dataFile.getNumElements());

        std::cout << "Running threshold " << threshold << ", iteration " << j + 1 << "... ";

        runSelectFunction(selectImplementation,
                          dataFile.getNumElements(), inputData, inputFilter, selection, threshold);

        delete[] inputData;
        delete[] inputFilter;
        delete[] selection;

        std::cout << "Completed" << std::endl;
    }
}

void selectCpuCyclesSingleInputBenchmark(const DataFile &dataFile,
                                         const std::vector<Select> &selectImplementations, int threshold,
                                         int iterations, const std::string &fileNamePrefix) {
    int numTests = static_cast<int>(selectImplementations.size());
    long_long cycles;
    std::vector<std::vector<long_long>> results(iterations, std::vector<long_long>(numTests + 1, 0));

    for (auto i = 0; i < iterations; ++i) {
        results[i][0] = static_cast<long_long>(threshold);

        for (auto j = 0; j < numTests; ++j) {
            auto inputData = new int[dataFile.getNumElements()];
            auto inputFilter = new int[dataFile.getNumElements()];
            auto selection = new int[dataFile.getNumElements()];
            copyArray(LoadedData::getInstance(dataFile).getData(), inputData, dataFile.getNumElements());
            copyArray(LoadedData::getInstance(dataFile).getData(), inputFilter, dataFile.getNumElements());

            std::cout << "Running " << getSelectName(selectImplementations[j]) << ", iteration " << i + 1 << "... ";

            cycles = *Counters::getInstance().readEventSet();

            runSelectFunction(selectImplementations[j],
                              dataFile.getNumElements(), inputData, inputFilter, selection, threshold);

            results[i][1 + j] = *Counters::getInstance().readEventSet() - cycles;

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
    std::string fullFilePath = outputFilePath + selectCyclesFolder + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);
}

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
                auto inputData = new int[dataFile.getNumElements()];
                auto inputFilter = new int[dataFile.getNumElements()];
                auto selection = new int[dataFile.getNumElements()];
                copyArray(LoadedData::getInstance(dataFile).getData(), inputData, dataFile.getNumElements());
                copyArray(LoadedData::getInstance(dataFile).getData(), inputFilter, dataFile.getNumElements());

                std::cout << "Running " << getSelectName(selectImplementations[i]) << ", threshold ";
                std::cout << j << ", iteration " << k + 1 << "... ";

                cycles = *Counters::getInstance().readEventSet();

                runSelectFunction(selectImplementations[j],
                                  dataFile.getNumElements(), inputData, inputFilter, selection, j);

                results[count][1 + (i * iterations) + k] = *Counters::getInstance().readEventSet() - cycles;

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
    std::string fullFilePath = outputFilePath + selectCyclesFolder + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);
}

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
            auto inputData = new int[dataFile.getNumElements()];
            auto inputFilter = new int[dataFile.getNumElements()];
            auto selection = new int[dataFile.getNumElements()];
            copyArray(LoadedData::getInstance(dataFile).getData(), inputData, dataFile.getNumElements());
            copyArray(LoadedData::getInstance(dataFile).getData(), inputFilter, dataFile.getNumElements());

            std::cout << "Running threshold " << static_cast<int>(thresholds[i]) << ", iteration " << j + 1 << "... ";

            if (PAPI_reset(benchmarkEventSet) != PAPI_OK)
                exit(1);

            runSelectFunction(selectImplementation,
                              dataFile.getNumElements(), inputData, inputFilter, selection,
                              static_cast<int>(thresholds[i]));

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
    std::string fullFilePath = outputFilePath + selectCyclesFolder + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);

    shutdownPAPI(benchmarkEventSet, benchmarkCounterValues);
}

void selectCpuCyclesSweepBenchmark(DataSweep &dataSweep, const std::vector<Select> &selectImplementations,
                                   int threshold, int iterations, const std::string &fileNamePrefix) {
    assert(!selectImplementations.empty());

    int dataCols = iterations * static_cast<int>(selectImplementations.size());
    long_long cycles;
    std::vector<std::vector<double>> results(dataSweep.getTotalRuns(),
                                             std::vector<double>(dataCols + 1, 0));

    for (auto i = 0; i < iterations; ++i) {
        for (auto j = 0; j < static_cast<int>(selectImplementations.size()); ++j) {
            for (auto k = 0; k < dataSweep.getTotalRuns(); ++k) {
                results[k][0] = static_cast<double>(dataSweep.getRunInput());
                auto inputData = new int[dataSweep.getNumElements()];
                auto inputFilter = new int[dataSweep.getNumElements()];
                auto selection = new int[dataSweep.getNumElements()];

                std::cout << "Running " << getSelectName(selectImplementations[j]) << " for input ";
                std::cout << dataSweep.getRunInput() << "... ";

                dataSweep.loadNextDataSetIntoMemory(inputData);
                copyArray(inputData, inputFilter, dataSweep.getNumElements());

                cycles = *Counters::getInstance().readEventSet();

                runSelectFunction(selectImplementations[j],
                                  dataSweep.getNumElements(), inputData, inputFilter, selection, threshold);

                results[k][1 + (i * selectImplementations.size()) + j] =
                        static_cast<double>(*Counters::getInstance().readEventSet() - cycles);

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
    std::string fullFilePath = outputFilePath + selectCyclesFolder + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);
}

void selectCpuCyclesInputSweepBenchmark(const DataFile &dataFile,
                                        const std::vector<Select> &selectImplementations,
                                        std::vector<float> &thresholds, int iterations,
                                        const std::string &fileNamePrefix) {
    assert(!selectImplementations.empty());

    int dataCols = iterations * static_cast<int>(selectImplementations.size());
    long_long cycles;
    std::vector<std::vector<double>> results(thresholds.size(),
                                             std::vector<double>(dataCols + 1, 0));

    for (auto i = 0; i < iterations; ++i) {
        for (auto j = 0; j < static_cast<int>(selectImplementations.size()); ++j) {
            for (auto k = 0; k < static_cast<int>(thresholds.size()); ++k) {
                results[k][0] = static_cast<int>(thresholds[k]);
                auto inputData = new int[dataFile.getNumElements()];
                auto inputFilter = new int[dataFile.getNumElements()];
                auto selection = new int[dataFile.getNumElements()];
                copyArray(LoadedData::getInstance(dataFile).getData(), inputData, dataFile.getNumElements());
                copyArray(LoadedData::getInstance(dataFile).getData(), inputFilter, dataFile.getNumElements());

                std::cout << "Running " << getSelectName(selectImplementations[j]) << " for threshold ";
                std::cout << std::to_string(static_cast<int>(thresholds[k])) << "... ";

                cycles = *Counters::getInstance().readEventSet();

                runSelectFunction(selectImplementations[j],
                                  dataFile.getNumElements(), inputData, inputFilter,
                                  selection, static_cast<int>(thresholds[k]));

                results[k][1 + (i * selectImplementations.size()) + j] =
                        static_cast<double>(*Counters::getInstance().readEventSet() - cycles);

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
    std::string fullFilePath = outputFilePath + selectCyclesFolder + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);
}