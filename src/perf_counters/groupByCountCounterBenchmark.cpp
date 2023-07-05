#include <cassert>
#include <iostream>
#include <cmath>
#include <absl/container/flat_hash_map.h>

#include "groupByCountCounterBenchmark.h"
#include "../utils/dataHelpers.h"
#include "../data_generation/config.h"
#include "../library/papi.h"
#include "../utils/papiHelpers.h"

void groupByCountCpuCyclesSweepBenchmark(DataSweep &dataSweep, const std::vector<GroupByCount> &groupByImplementations,
                                         int iterations, const std::string &fileNamePrefix) {
    assert(!groupByImplementations.empty());

    int dataCols = iterations * static_cast<int>(groupByImplementations.size());
    long_long cycles;
    std::vector<std::vector<double>> results(dataSweep.getTotalRuns(),
                                             std::vector<double>(dataCols + 1, 0));

    for (auto i = 0; i < iterations; ++i) {
        for (auto j = 0; j < static_cast<int>(groupByImplementations.size()); ++j) {
            for (auto k = 0; k < dataSweep.getTotalRuns(); ++k) {
                results[k][0] = static_cast<int>(dataSweep.getRunInput());
                auto inputData = new int[dataSweep.getNumElements()];

                std::cout << "Running " << getGroupByCountName(groupByImplementations[j]) << " for input ";
                std::cout << static_cast<int>(dataSweep.getRunInput()) << "... ";

                dataSweep.loadNextDataSetIntoMemory(inputData);

                cycles = *Counters::getInstance().readEventSet();

                auto result = runGroupByCountFunction(groupByImplementations[j], dataSweep.getNumElements(), inputData);

                results[k][1 + (i * groupByImplementations.size()) + j] =
                        static_cast<double>(*Counters::getInstance().readEventSet() - cycles);

                delete[] inputData;

                std::cout << "Completed" << std::endl;

                std::cout << "Result vector length: " << result.size() << std::endl;

            }
            dataSweep.restartSweep();
        }
    }

    std::vector<std::string> headers(1 + dataCols);
    headers [0] = "Input";
    for (auto i = 0; i < dataCols; ++i) {
        headers[1 + i] = getGroupByCountName(groupByImplementations[i % groupByImplementations.size()]);
    }

    std::string fileName = fileNamePrefix + "_GroupBy_SweepCyclesBM_" + dataSweep.getSweepName();
    std::string fullFilePath = outputFilePath + groupByCyclesFolder + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);
}

void groupByCountCpuCyclesSweepBenchmarkVariableSize(DataSweep &dataSweep, const std::vector<GroupByCount> &groupByImplementations,
                                                     int iterations, const std::string &fileNamePrefix) {
    assert(!groupByImplementations.empty());

    int dataCols = iterations * static_cast<int>(groupByImplementations.size());
    long_long cycles;
    std::vector<std::vector<double>> results(dataSweep.getTotalRuns(),
                                             std::vector<double>(dataCols + 1, 0));

    for (auto i = 0; i < iterations; ++i) {
        for (auto j = 0; j < static_cast<int>(groupByImplementations.size()); ++j) {
            for (auto k = 0; k < dataSweep.getTotalRuns(); ++k) {
                results[k][0] = static_cast<int>(dataSweep.getRunInput());
                int numElements = static_cast<int>(dataSweep.getRunInput());;
                auto inputData = new int[numElements];

                std::cout << "Running " << getGroupByCountName(groupByImplementations[j]) << " for input ";
                std::cout << static_cast<int>(dataSweep.getRunInput()) << "... ";

                dataSweep.loadNextDataSetIntoMemory(inputData);

                cycles = *Counters::getInstance().readEventSet();

                auto result = runGroupByCountFunction(groupByImplementations[j], numElements, inputData);

                results[k][1 + (i * groupByImplementations.size()) + j] =
                        static_cast<double>(*Counters::getInstance().readEventSet() - cycles);

                delete[] inputData;

                std::cout << "Completed" << std::endl;

                std::cout << "Result vector length: " << result.size() << std::endl;

            }
            dataSweep.restartSweep();
        }
    }

    std::vector<std::string> headers(1 + dataCols);
    headers [0] = "Input";
    for (auto i = 0; i < dataCols; ++i) {
        headers[1 + i] = getGroupByCountName(groupByImplementations[i % groupByImplementations.size()]);
    }

    std::string fileName = fileNamePrefix + "_GroupBy_SweepCyclesBM_" + dataSweep.getSweepName();
    std::string fullFilePath = outputFilePath + groupByCyclesFolder + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);
}


void groupByCountBenchmarkWithExtraCounters(DataSweep &dataSweep, GroupByCount groupByImplementation,
                                            int iterations, std::vector<std::string> &benchmarkCounters,
                                            const std::string &fileNamePrefix) {
    if (groupByImplementation == GroupByCount::Count_Adaptive)
        std::cout << "Cannot benchmark adaptive groupBy using counters as adaptive select is already using these counters" << std::endl;

    int numTests = static_cast<int>(dataSweep.getTotalRuns());

    long_long benchmarkCounterValues[benchmarkCounters.size()];
    int benchmarkEventSet = initialisePAPIandCreateEventSet(benchmarkCounters);

    std::vector<std::vector<long_long>> results(numTests, std::vector<long_long>((iterations * benchmarkCounters.size()) + 1, 0));

    for (auto i = 0; i < iterations; ++i) {

        for (auto j = 0; j < numTests; ++j) {
            results[j][0] = static_cast<long_long>(dataSweep.getRunInput());

            int numElements = static_cast<int>(dataSweep.getNumElements());
            auto inputData = new int[numElements];

            std::cout << "Running " << getGroupByCountName(groupByImplementation) << " for input ";
            std::cout << static_cast<int>(dataSweep.getRunInput()) << ", iteration ";
            std::cout << i + 1 << "... ";

            dataSweep.loadNextDataSetIntoMemory(inputData);

            if (PAPI_reset(benchmarkEventSet) != PAPI_OK)
                exit(1);

            runGroupByCountFunction(groupByImplementation,numElements, inputData);

            if (PAPI_read(benchmarkEventSet, benchmarkCounterValues) != PAPI_OK)
                exit(1);

            delete[] inputData;

            for (int k = 0; k < static_cast<int>(benchmarkCounters.size()); ++k) {
                results[j][1 + (i * benchmarkCounters.size()) + k] = benchmarkCounterValues[k];
            }

            std::cout << "Completed" << std::endl;
        }
        dataSweep.restartSweep();
    }

    std::vector<std::string> headers(benchmarkCounters.size() * iterations);
    for (auto i = 0; i < iterations; ++i) {
        std::copy(benchmarkCounters.begin(), benchmarkCounters.end(), headers.begin() + i * benchmarkCounters.size());
    }
    headers.insert(headers.begin(), "Cardinality");

    std::string fileName =
            fileNamePrefix +
            "ExtraCountersCyclesBM_" +
            getGroupByCountName(groupByImplementation) +
            dataSweep.getSweepName();
    std::string fullFilePath = outputFilePath + groupByCyclesFolder + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);

    shutdownPAPI(benchmarkEventSet, benchmarkCounterValues);
}

void groupByCountBenchmarkWithExtraCountersDuringRun(const DataFile &dataFile,
                                                     std::vector<std::string> &benchmarkCounters,
                                                     const std::string &fileNamePrefix) {

    int numElements = dataFile.getNumElements();
    int tuplesPerMeasurement = 50000;
    int numMeasurements = std::ceil(static_cast<float>(numElements) / tuplesPerMeasurement);

    long_long benchmarkCounterValues[benchmarkCounters.size()];
    int benchmarkEventSet = initialisePAPIandCreateEventSet(benchmarkCounters);

    std::vector<std::vector<long_long>> results(numMeasurements, std::vector<long_long>(benchmarkCounters.size() + 1, 0));

    auto inputData = new int[numElements];
    copyArray(LoadedData::getInstance(dataFile).getData(), inputData, dataFile.getNumElements());

    int index = 0;
    int tuplesToProcess;
    absl::flat_hash_map<int, int> map;

    for (auto j = 0; j < numMeasurements; ++j) {
        tuplesToProcess = std::min(tuplesPerMeasurement, numElements - index);

        if (PAPI_reset(benchmarkEventSet) != PAPI_OK)
            exit(1);

        for (auto _ = 0; _ < tuplesToProcess; ++_) {map[inputData[index++]]++;}

        if (PAPI_read(benchmarkEventSet, benchmarkCounterValues) != PAPI_OK)
            exit(1);

        for (int k = 0; k < static_cast<int>(benchmarkCounters.size()); ++k) {
            results[j][1 + k] = benchmarkCounterValues[k];
        }

        results[j][0] = static_cast<long_long>(index);
    }

    delete[] inputData;

    std::vector<std::string> headers(benchmarkCounters.begin(), benchmarkCounters.end());

    headers.insert(headers.begin(), "Rows processed");

    std::string fileName =
            fileNamePrefix +
            "ExtraCountersDuringRun_" +
            dataFile.getFileName();
    std::string fullFilePath = outputFilePath + groupByCyclesFolder + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);

    shutdownPAPI(benchmarkEventSet, benchmarkCounterValues);
}