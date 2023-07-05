#ifndef MABPL_GROUPBYCOUNTCOUNTERBENCHMARK_H
#define MABPL_GROUPBYCOUNTCOUNTERBENCHMARK_H

#include "../library/groupByCount.h"
#include "../data_generation/dataFiles.h"


void groupByCountCpuCyclesSweepBenchmark(DataSweep &dataSweep, const std::vector<GroupByCount> &groupByImplementations,
                                         int iterations, const std::string &fileNamePrefix);

void groupByCountCpuCyclesSweepBenchmark64(DataSweep &dataSweep, const std::vector<GroupByCount> &groupByImplementations,
                                           int iterations, const std::string &fileNamePrefix);

void groupByCountCpuCyclesSweepBenchmarkVariableSize(DataSweep &dataSweep, const std::vector<GroupByCount> &groupByImplementations,
                                                     int iterations, const std::string &fileNamePrefix);

void groupByCountBenchmarkWithExtraCounters(DataSweep &dataSweep, GroupByCount groupByImplementation,
                                            int iterations, std::vector<std::string> &benchmarkCounters,
                                            const std::string &fileNamePrefix);

void groupByCountBenchmarkWithExtraCountersDuringRun(const DataFile &dataFile,
                                                     std::vector<std::string> &benchmarkCounters,
                                                     const std::string &fileNamePrefix);

#endif //MABPL_GROUPBYCOUNTCOUNTERBENCHMARK_H
