#ifndef MABPL_GROUPBYCYCLESBENCHMARK_H
#define MABPL_GROUPBYCYCLESBENCHMARK_H

#include "../library/mabpl.h"
#include "../data_generation/dataFiles.h"

using MABPL::GroupBy;

void groupByCpuCyclesSweepBenchmark(DataSweep &dataSweep, const std::vector<GroupBy> &groupByImplementations,
                                     int iterations, const std::string &fileNamePrefix);

void groupByCpuCyclesSweepBenchmark64(DataSweep &dataSweep, const std::vector<GroupBy> &groupByImplementations,
                                      int iterations, const std::string &fileNamePrefix);

void groupByBenchmarkWithExtraCounters(DataSweep &dataSweep, GroupBy groupByImplementation, int iterations,
                                       std::vector<std::string> &benchmarkCounters, const std::string &fileNamePrefix);

void groupByBenchmarkWithExtraCountersDuringRun(const DataFile &dataFile, std::vector<std::string> &benchmarkCounters,
                                                const std::string &fileNamePrefix);

void tessilRobinMapInitialisationBenchmark(const std::string &fileNamePrefix);

#endif //MABPL_GROUPBYCYCLESBENCHMARK_H
