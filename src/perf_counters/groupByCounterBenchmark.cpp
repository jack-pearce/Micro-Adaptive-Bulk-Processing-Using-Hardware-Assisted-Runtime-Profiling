#include <cassert>
#include <iostream>
#include <cmath>
#include <tsl/robin_map.h>

#include "groupByCounterBenchmark.h"
#include "../utils/dataHelpers.h"
#include "../data_generation/config.h"
#include "../library/papi.h"
#include "../library/utils.h"
#include "../utils/papiHelpers.h"
#include "../data_generation/dataGenerators.h"

void groupByCpuCyclesSweepBenchmark(DataSweep &dataSweep, const std::vector<GroupBy> &groupByImplementations,
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
                auto inputGroupBy = new int[dataSweep.getNumElements()];
                auto inputAggregate = new int[dataSweep.getNumElements()];

                std::cout << "Running " << getGroupByName(groupByImplementations[j]) << " for input ";
                std::cout << static_cast<int>(dataSweep.getRunInput()) << "... ";

                dataSweep.loadNextDataSetIntoMemory(inputGroupBy);
                generateUniformDistributionInMemory(inputAggregate, dataSweep.getNumElements(), 10);

                cycles = *Counters::getInstance().readEventSet();

                auto result = runGroupByFunction<MinAggregation>(groupByImplementations[j], dataSweep.getNumElements(), inputGroupBy, inputAggregate);

                results[k][1 + (i * groupByImplementations.size()) + j] =
                        static_cast<double>(*Counters::getInstance().readEventSet() - cycles);

                delete[] inputGroupBy;
                delete []inputAggregate;

                std::cout << "Completed" << std::endl;

                std::cout << "Result vector length: " << result.size() << std::endl;

            }
            dataSweep.restartSweep();
        }
    }

    std::vector<std::string> headers(1 + dataCols);
    headers [0] = "Input";
    for (auto i = 0; i < dataCols; ++i) {
        headers[1 + i] = getGroupByName(groupByImplementations[i % groupByImplementations.size()]);
    }

    std::string fileName = fileNamePrefix + "_GroupBy_SweepCyclesBM_" + dataSweep.getSweepName();
    std::string fullFilePath = outputFilePath + groupByCyclesFolder + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);
}

void groupByCpuCyclesSweepBenchmarkVariableSize(DataSweep &dataSweep, const std::vector<GroupBy> &groupByImplementations,
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
                auto inputGroupBy = new int[numElements];
                auto inputAggregate = new int[numElements];

                std::cout << "Running " << getGroupByName(groupByImplementations[j]) << " for input ";
                std::cout << static_cast<int>(dataSweep.getRunInput()) << "... ";

                dataSweep.loadNextDataSetIntoMemory(inputGroupBy);
                generateUniformDistributionInMemory(inputAggregate, numElements, 10);

                cycles = *Counters::getInstance().readEventSet();

                auto result = runGroupByFunction<MaxAggregation>(groupByImplementations[j], numElements, inputGroupBy, inputAggregate);

                results[k][1 + (i * groupByImplementations.size()) + j] =
                        static_cast<double>(*Counters::getInstance().readEventSet() - cycles);

                delete []inputGroupBy;
                delete []inputAggregate;

                std::cout << "Completed" << std::endl;

                std::cout << "Result vector length: " << result.size() << std::endl;

            }
            dataSweep.restartSweep();
        }
    }

    std::vector<std::string> headers(1 + dataCols);
    headers [0] = "Input";
    for (auto i = 0; i < dataCols; ++i) {
        headers[1 + i] = getGroupByName(groupByImplementations[i % groupByImplementations.size()]);
    }

    std::string fileName = fileNamePrefix + "_GroupBy_SweepCyclesBM_" + dataSweep.getSweepName();
    std::string fullFilePath = outputFilePath + groupByCyclesFolder + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);
}


void groupByBenchmarkWithExtraCounters(DataSweep &dataSweep, GroupBy groupByImplementation, int iterations,
                                       std::vector<std::string> &benchmarkCounters, const std::string &fileNamePrefix) {
    if (groupByImplementation == GroupBy::Adaptive)
        std::cout << "Cannot benchmark adaptive groupBy using counters as adaptive select is already using these counters" << std::endl;

    int numTests = static_cast<int>(dataSweep.getTotalRuns());

    long_long benchmarkCounterValues[benchmarkCounters.size()];
    int benchmarkEventSet = initialisePAPIandCreateEventSet(benchmarkCounters);

    std::vector<std::vector<long_long>> results(numTests, std::vector<long_long>((iterations * benchmarkCounters.size()) + 1, 0));

    for (auto i = 0; i < iterations; ++i) {

        for (auto j = 0; j < numTests; ++j) {
            results[j][0] = static_cast<long_long>(dataSweep.getRunInput());

            int numElements = static_cast<int>(dataSweep.getNumElements());
            auto inputGroupBy = new int[numElements];
            auto inputAggregate = new int[numElements];

            std::cout << "Running " << getGroupByName(groupByImplementation) << " for input ";
            std::cout << static_cast<int>(dataSweep.getRunInput()) << ", iteration ";
            std::cout << i + 1 << "... ";

            dataSweep.loadNextDataSetIntoMemory(inputGroupBy);
            generateUniformDistributionInMemory(inputAggregate, numElements, 10);


            if (PAPI_reset(benchmarkEventSet) != PAPI_OK)
                exit(1);

            runGroupByFunction<MaxAggregation>(groupByImplementation, numElements, inputGroupBy, inputAggregate);

            if (PAPI_read(benchmarkEventSet, benchmarkCounterValues) != PAPI_OK)
                exit(1);

            delete[] inputGroupBy;
            delete[] inputAggregate;

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
            getGroupByName(groupByImplementation) +
            dataSweep.getSweepName();
    std::string fullFilePath = outputFilePath + groupByCyclesFolder + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);

    shutdownPAPI(benchmarkEventSet, benchmarkCounterValues);
}

void groupByBenchmarkWithExtraCountersDuringRun(const DataFile &dataFile,
                                                std::vector<std::string> &benchmarkCounters,
                                                const std::string &fileNamePrefix) {

    int numElements = dataFile.getNumElements();
    int tuplesPerMeasurement = 50000;
    int numMeasurements = std::ceil(static_cast<float>(numElements) / tuplesPerMeasurement);

    long_long benchmarkCounterValues[benchmarkCounters.size()];
    int benchmarkEventSet = initialisePAPIandCreateEventSet(benchmarkCounters);

    std::vector<std::vector<long_long>> results(numMeasurements, std::vector<long_long>(benchmarkCounters.size() + 1, 0));

    auto inputGroupBy = new int[numElements];
    auto inputAggregate = new int[numElements];
    copyArray(LoadedData::getInstance(dataFile).getData(), inputGroupBy, dataFile.getNumElements());
    generateUniformDistributionInMemory(inputAggregate, numElements, 10);


    int index = 0;
    int tuplesToProcess;
    tsl::robin_map<int, int> map(l3cacheSize() / (3 * (sizeof(int) + sizeof(int))));
    tsl::robin_map<int, int>::iterator it;

    for (auto j = 0; j < numMeasurements; ++j) {
        tuplesToProcess = std::min(tuplesPerMeasurement, numElements - index);

        if (PAPI_reset(benchmarkEventSet) != PAPI_OK)
            exit(1);

        for (auto _ = 0; _ < tuplesToProcess; ++_) {
            it = map.find(inputGroupBy[index]);
            if (it != map.end()) {
                it.value() = MaxAggregation<int>()(it->second, inputAggregate[index], false);
            } else {
                map.insert({inputGroupBy[index], MaxAggregation<int>()(0, inputAggregate[index], true)});
            }
            ++index;
        }

        if (PAPI_read(benchmarkEventSet, benchmarkCounterValues) != PAPI_OK)
            exit(1);

        for (int k = 0; k < static_cast<int>(benchmarkCounters.size()); ++k) {
            results[j][1 + k] = benchmarkCounterValues[k];
        }

        results[j][0] = static_cast<long_long>(index);
    }

    delete[] inputGroupBy;
    delete[] inputAggregate;

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
