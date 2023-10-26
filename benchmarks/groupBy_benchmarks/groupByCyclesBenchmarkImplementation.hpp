#ifndef HAQP_GROUPBYCYCLESBENCHMARKIMPLEMENTATION_HPP
#define HAQP_GROUPBYCYCLESBENCHMARKIMPLEMENTATION_HPP

#include <cassert>
#include <iostream>
#include <cmath>
#include "tsl/robin_map.h"

#include "groupByCyclesBenchmark.hpp"
#include "utilities/dataHelpers.hpp"
#include "utilities/papiHelpers.hpp"
#include "data_generation/dataGenerators.hpp"
#include "data_generation/dataFiles.hpp"

using HAQP::Counters;
using HAQP::MaxAggregation;
using HAQP::MaxAggregation;
using HAQP::SumAggregation;
using HAQP::CountAggregation;


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

                auto result = HAQP::runGroupByFunction<MaxAggregation>(groupByImplementations[j],
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

                auto result = HAQP::runGroupByFunction<MaxAggregation>(groupByImplementations[j],
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

                auto result = HAQP::runGroupByFunction<MaxAggregation>(GroupBy::AdaptiveParallel,
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
void groupByWallTimeDopSweepBenchmarkCalcDopRange(DataSweep &dataSweep, int iterations,
                                                  const std::string &fileNamePrefix) {
    int dop = 2;
    std::vector<int> dopValues;
    while (dop <= HAQP::logicalCoresCount()) {
        dopValues.push_back(dop);
        dop *= 2;
    }

    groupByWallTimeDopSweepBenchmark<T1,T2>(dataSweep, iterations, fileNamePrefix, dopValues);
}


#endif //HAQP_GROUPBYCYCLESBENCHMARKIMPLEMENTATION_HPP
