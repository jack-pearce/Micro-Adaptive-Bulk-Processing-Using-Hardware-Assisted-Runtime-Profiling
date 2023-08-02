#ifndef MABPL_GROUPBYCYCLESBENCHMARKIMPLEMENTATION_H
#define MABPL_GROUPBYCYCLESBENCHMARKIMPLEMENTATION_H

#include <cassert>
#include <iostream>
#include <cmath>
#include <tsl/robin_map.h>

#include "groupByCyclesBenchmark.h"
#include "../utilities/dataHelpers.h"
#include "../utilities/papiHelpers.h"
#include "../data_generation/dataGenerators.h"

using MABPL::Counters;
using MABPL::MaxAggregation;
using MABPL::MaxAggregation;
using MABPL::SumAggregation;
using MABPL::CountAggregation;


template <typename T1, typename T2>
void groupByCpuCyclesSweepBenchmark(DataSweep &dataSweep, const std::vector<GroupBy> &groupByImplementations,
                                    int iterations, const std::string &fileNamePrefix) {
    assert(!groupByImplementations.empty());
    if (std::count(groupByImplementations.begin(), groupByImplementations.end(), GroupBy::AdaptiveParallel)) {
        std::cout << "Cannot use cpu cycles to time multi-threaded programs, use wall time instead" << std::endl;
        exit(1);
    }

    int dataCols = iterations * static_cast<int>(groupByImplementations.size());
    long_long cycles;
    std::vector<std::vector<double>> results(dataSweep.getTotalRuns(),
                                             std::vector<double>(dataCols + 1, 0));

    for (auto i = 0; i < iterations; ++i) {
        for (auto j = 0; j < static_cast<int>(groupByImplementations.size()); ++j) {
            for (auto k = 0; k < dataSweep.getTotalRuns(); ++k) {
                if (dataSweep.getSweepName().find("CardinalitySweep") != std::string::npos) {
                    results[k][0] = static_cast<int>(dataSweep.getRunInput());
                } else {
                    results[k][0] = dataSweep.getRunInput();
                }

                auto inputGroupBy = new T1[dataSweep.getNumElements()];
                auto inputAggregate = new T2[dataSweep.getNumElements()];

                int cardinality = dataSweep.getCardinality();

                std::cout << "Running " << getGroupByName(groupByImplementations[j]) << " for input ";
                std::cout << static_cast<int>(dataSweep.getRunInput()) << "... ";

                dataSweep.loadNextDataSetIntoMemory<T1>(inputGroupBy);
                generateUniformDistributionInMemory<T2>(inputAggregate, dataSweep.getNumElements(), 10);

                cycles = *Counters::getInstance().readSharedEventSet();

                auto result = MABPL::runGroupByFunction<MaxAggregation>(groupByImplementations[j],
                                                                        dataSweep.getNumElements(), inputGroupBy,
                                                                        inputAggregate, cardinality);

                results[k][1 + (i * groupByImplementations.size()) + j] =
                        static_cast<double>(*Counters::getInstance().readSharedEventSet() - cycles);

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
    std::string fullFilePath = FilePaths::getInstance().getGroupByCyclesFolderPath() + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);
}

template <typename T1, typename T2>
void groupByWallTimeSweepBenchmark(DataSweep &dataSweep, const std::vector<GroupBy> &groupByImplementations,
                                   int iterations, const std::string &fileNamePrefix) {
    assert(!groupByImplementations.empty());

    int dataCols = iterations * static_cast<int>(groupByImplementations.size());
    long_long wallTime;

    std::vector<std::vector<double>> results(dataSweep.getTotalRuns(),
                                             std::vector<double>(dataCols + 1, 0));

    for (auto i = 0; i < iterations; ++i) {
        for (auto j = 0; j < static_cast<int>(groupByImplementations.size()); ++j) {
            for (auto k = 0; k < dataSweep.getTotalRuns(); ++k) {
                if (dataSweep.getSweepName().find("CardinalitySweep") != std::string::npos) {
                    results[k][0] = static_cast<int>(dataSweep.getRunInput());
                } else {
                    results[k][0] = dataSweep.getRunInput();
                }

                auto inputGroupBy = new T1[dataSweep.getNumElements()];
                auto inputAggregate = new T2[dataSweep.getNumElements()];

                int cardinality = dataSweep.getCardinality();

                std::cout << "Running " << getGroupByName(groupByImplementations[j]) << " for input ";
                std::cout << static_cast<int>(dataSweep.getRunInput()) << "... ";

                dataSweep.loadNextDataSetIntoMemory<T1>(inputGroupBy);
                generateUniformDistributionInMemory<T2>(inputAggregate, dataSweep.getNumElements(), 10);

                wallTime = PAPI_get_real_usec();

                auto result = MABPL::runGroupByFunction<MaxAggregation>(groupByImplementations[j],
                                                                        dataSweep.getNumElements(), inputGroupBy,
                                                                        inputAggregate, cardinality);

                results[k][1 + (i * groupByImplementations.size()) + j] = static_cast<double>(PAPI_get_real_usec() - wallTime);

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

    std::string fileName = fileNamePrefix + "_GroupBy_SweepWallTimeBM_" + dataSweep.getSweepName();
    std::string fullFilePath = FilePaths::getInstance().getGroupByCyclesFolderPath() + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);
}

template <typename T1, typename T2>
void groupByWallTimeDopSweepBenchmark(DataSweep &dataSweep, int iterations, const std::string &fileNamePrefix,
                                      std::vector<int> dop) {

    int dataCols = iterations * static_cast<int>(dop.size());
    long_long wallTime;

    std::vector<std::vector<double>> results(dataSweep.getTotalRuns(),
                                             std::vector<double>(dataCols + 1, 0));

    for (auto i = 0; i < iterations; ++i) {
        for (auto j = 0; j < static_cast<int>(dop.size()); ++j) {
            for (auto k = 0; k < dataSweep.getTotalRuns(); ++k) {
                if (dataSweep.getSweepName().find("CardinalitySweep") != std::string::npos) {
                    results[k][0] = static_cast<int>(dataSweep.getRunInput());
                } else {
                    results[k][0] = dataSweep.getRunInput();
                }

                auto inputGroupBy = new T1[dataSweep.getNumElements()];
                auto inputAggregate = new T2[dataSweep.getNumElements()];

                int cardinality = dataSweep.getCardinality();

                std::cout << "Running dop of " << dop[j] << " for input ";
                std::cout << static_cast<int>(dataSweep.getRunInput()) << "... ";

                dataSweep.loadNextDataSetIntoMemory<T1>(inputGroupBy);
                generateUniformDistributionInMemory<T2>(inputAggregate, dataSweep.getNumElements(), 10);

                wallTime = PAPI_get_real_usec();

                auto result = MABPL::runGroupByFunction<MaxAggregation>(GroupBy::AdaptiveParallel,
                                                                        dataSweep.getNumElements(), inputGroupBy,
                                                                        inputAggregate, cardinality, dop[j]);

                results[k][1 + (i * dop.size()) + j] = static_cast<double>(PAPI_get_real_usec() - wallTime);

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
        headers[1 + i] = "dop-" + std::to_string(dop[i % dop.size()]);
    }

    std::string fileName = fileNamePrefix + "_GroupBy_DopSweepWallTimeBM_" + dataSweep.getSweepName();
    std::string fullFilePath = FilePaths::getInstance().getGroupByCyclesFolderPath() + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);
}

template <typename T1, typename T2>
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
            auto inputGroupBy = new T1[numElements];
            auto inputAggregate = new T2[numElements];

            int cardinality = static_cast<int>(dataSweep.getRunInput());

            std::cout << "Running " << getGroupByName(groupByImplementation) << " for input ";
            std::cout << static_cast<int>(dataSweep.getRunInput()) << ", iteration ";
            std::cout << i + 1 << "... ";

            dataSweep.loadNextDataSetIntoMemory<T1>(inputGroupBy);
            generateUniformDistributionInMemory<T2>(inputAggregate, numElements, 10);


            if (PAPI_reset(benchmarkEventSet) != PAPI_OK)
                exit(1);

            MABPL::runGroupByFunction<MaxAggregation>(groupByImplementation, numElements, inputGroupBy, inputAggregate,
                                                      cardinality);

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
    std::string fullFilePath = FilePaths::getInstance().getGroupByCyclesFolderPath() + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);

    shutdownPAPI(benchmarkEventSet, benchmarkCounterValues);
}

template <typename T1, typename T2>
void groupByBenchmarkWithExtraCountersDuringRun(const DataFile &dataFile,
                                                std::vector<std::string> &benchmarkCounters,
                                                const std::string &fileNamePrefix) {

    int numElements = dataFile.getNumElements();
    int tuplesPerMeasurement = 50000;
    int numMeasurements = std::ceil(static_cast<float>(numElements) / tuplesPerMeasurement);

    long_long benchmarkCounterValues[benchmarkCounters.size()];
    int benchmarkEventSet = initialisePAPIandCreateEventSet(benchmarkCounters);

    std::vector<std::vector<long_long>> results(numMeasurements, std::vector<long_long>(benchmarkCounters.size() + 1, 0));

    auto inputGroupBy = new T1[numElements];
    auto inputAggregate = new T2[numElements];
    copyArray<T1>(LoadedData<T1>::getInstance(dataFile).getData(), inputGroupBy, dataFile.getNumElements());
    generateUniformDistributionInMemory<T2>(inputAggregate, numElements, 10);


    int index = 0;
    int tuplesToProcess;

    //////////////////////////////////////////////////////////////////////////////////////////////////////
    int cardinality = 10*1000*1000; ////////////////// NEED TO UPDATE TO MATCH RUN ///////////////////////
    //////////////////////////////////////////////////////////////////////////////////////////////////////

    tsl::robin_map<T1, T2> map(std::max(static_cast<int>(2.5 * cardinality), 400000));

    typename tsl::robin_map<T1, T2>::iterator it;
    for (auto j = 0; j < numMeasurements; ++j) {
        tuplesToProcess = std::min(tuplesPerMeasurement, numElements - index);

        if (PAPI_reset(benchmarkEventSet) != PAPI_OK)
            exit(1);

        for (auto _ = 0; _ < tuplesToProcess; ++_) {
            it = map.find(inputGroupBy[index]);
            if (it != map.end()) {
                it.value() = MaxAggregation<T2>()(it->second, inputAggregate[index], false);
            } else {
                map.insert({inputGroupBy[index], MaxAggregation<T2>()(0, inputAggregate[index], true)});
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
    std::string fullFilePath = FilePaths::getInstance().getGroupByCyclesFolderPath() + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);

    shutdownPAPI(benchmarkEventSet, benchmarkCounterValues);
}

template <typename T1, typename T2>
void tessilRobinMapInitialisationBenchmark(const std::string &fileNamePrefix) {

    int totalPoints = 100;
    std::vector<float> points;
    generateLogDistribution(30, 1, 20*1000*1000, points);

    long_long cycles;
    std::vector<std::vector<double>> results(totalPoints,
                                             std::vector<double>(2, 0));

    for (auto i = 0; i < totalPoints; ++i) {

        results[i][0] = static_cast<int>(points[i]);

        cycles = *Counters::getInstance().readSharedEventSet();

        int initialSize = std::max(static_cast<int>(2.5 * points[i]), 400000);
        tsl::robin_map<T1, T2> map(initialSize);

        results[i][1] = static_cast<double>(*Counters::getInstance().readSharedEventSet() - cycles);

    }

    std::vector<std::string> headers(2);
    headers [0] = "Cardinality";
    headers [1] = "Cycles";

    std::string fileName = fileNamePrefix + "_tsl_robinMap_initialisationCostOnly_";
    std::string fullFilePath = FilePaths::getInstance().getGroupByCyclesFolderPath() + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);
}


#endif //MABPL_GROUPBYCYCLESBENCHMARKIMPLEMENTATION_H
