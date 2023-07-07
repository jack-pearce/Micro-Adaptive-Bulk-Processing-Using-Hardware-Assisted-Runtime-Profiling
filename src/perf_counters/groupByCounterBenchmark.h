#ifndef MABPL_GROUPBYCOUNTERBENCHMARK_H
#define MABPL_GROUPBYCOUNTERBENCHMARK_H

#include "../library/groupBy.h"
#include "../data_generation/dataFiles.h"


void groupByCpuCyclesSweepBenchmark(DataSweep &dataSweep, const std::vector<GroupBy> &groupByImplementations,
                                     int iterations, const std::string &fileNamePrefix);

void groupByCpuCyclesSweepBenchmarkVariableSize(DataSweep &dataSweep, const std::vector<GroupBy> &groupByImplementations,
                                           int iterations, const std::string &fileNamePrefix);

void groupByBenchmarkWithExtraCounters(DataSweep &dataSweep, GroupBy groupByImplementation, int iterations,
                                       std::vector<std::string> &benchmarkCounters, const std::string &fileNamePrefix);

void groupByBenchmarkWithExtraCountersDuringRun(const DataFile &dataFile, std::vector<std::string> &benchmarkCounters,
                                                const std::string &fileNamePrefix);

#endif //MABPL_GROUPBYCOUNTERBENCHMARK_H
