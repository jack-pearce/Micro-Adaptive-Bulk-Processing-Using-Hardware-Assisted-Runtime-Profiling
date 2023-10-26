#ifndef HAQP_GROUPBYCYCLESBENCHMARK_HPP
#define HAQP_GROUPBYCYCLESBENCHMARK_HPP

#include "haqp.hpp"
#include "data_generation/dataFiles.hpp"

using HAQP::GroupBy;


template <typename T1, typename T2>
void groupByCompareResultsTest(const DataFile& dataFile, GroupBy groupByImpOne, GroupBy groupByImpTwo);

template <typename T1, typename T2>
void groupByCpuCyclesSweepBenchmark(DataSweep &dataSweep, const std::vector<GroupBy> &groupByImplementations,
                                    int iterations, const std::string &fileNamePrefix);

template <typename T1, typename T2>
void groupByWallTimeSweepBenchmark(DataSweep &dataSweep, const std::vector<GroupBy> &groupByImplementations,
                                   int iterations, const std::string &fileNamePrefix);

template <typename T1, typename T2>
void groupByWallTimeDopSweepBenchmark(DataSweep &dataSweep, int iterations, const std::string &fileNamePrefix,
                                      std::vector<int> dop);

template <typename T1, typename T2>
void groupByWallTimeDopSweepBenchmarkCalcDopRange(DataSweep &dataSweep, int iterations,
                                                  const std::string &fileNamePrefix);

template <typename T1, typename T2>
void groupByBenchmarkWithExtraCounters(DataSweep &dataSweep, GroupBy groupByImplementation, int iterations,
                                       std::vector<std::string> &benchmarkCounters, const std::string &fileNamePrefix);

template <typename T1, typename T2>
void groupByBenchmarkWithExtraCountersConfigurations(DataSweep &dataSweep,
                                                     GroupBy groupByImplementation,
                                                     int iterations,
                                                     const std::string &fileNamePrefix);

template <typename T1, typename T2>
void groupByBenchmarkWithExtraCountersDuringRun(const DataFile &dataFile,
                                                std::vector<std::string> &benchmarkCounters,
                                                const std::string &fileNamePrefix);

template <typename T1, typename T2>
void groupByBenchmarkWithExtraCountersDuringRunConfigurations(const DataFile &dataFile,
                                                              const std::string &fileNamePrefix);

template <typename T1, typename T2>
void tessilRobinMapInitialisationBenchmark(const std::string &fileNamePrefix);


#include "groupByCyclesBenchmarkImplementation.hpp"


#endif //HAQP_GROUPBYCYCLESBENCHMARK_HPP
