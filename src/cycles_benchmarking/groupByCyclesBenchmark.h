#ifndef MABPL_GROUPBYCYCLESBENCHMARK_H
#define MABPL_GROUPBYCYCLESBENCHMARK_H

#include "../library/mabpl.h"
#include "../data_generation/dataFiles.h"

using MABPL::GroupBy;


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
void groupByBenchmarkWithExtraCounters(DataSweep &dataSweep, GroupBy groupByImplementation, int iterations,
                                       std::vector<std::string> &benchmarkCounters, const std::string &fileNamePrefix);

template <typename T1, typename T2>
void groupByBenchmarkWithExtraCountersDuringRun(const DataFile &dataFile,
                                                std::vector<std::string> &benchmarkCounters,
                                                const std::string &fileNamePrefix);

template <typename T1, typename T2>
void tessilRobinMapInitialisationBenchmarkDefaultAllocator(const std::string &fileNamePrefix);

template <typename T1, typename T2>
void tessilRobinMapInitialisationBenchmarkCustomAllocator(const std::string &fileNamePrefix);


#include "groupByCyclesBenchmarkImplementation.h"


#endif //MABPL_GROUPBYCYCLESBENCHMARK_H
