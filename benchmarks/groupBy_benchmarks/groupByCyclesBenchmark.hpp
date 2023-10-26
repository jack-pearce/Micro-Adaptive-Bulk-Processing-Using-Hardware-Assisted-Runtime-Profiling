#ifndef HAQP_GROUPBYCYCLESBENCHMARK_HPP
#define HAQP_GROUPBYCYCLESBENCHMARK_HPP

#include "haqp.hpp"
#include "data_generation/dataFiles.hpp"

using HAQP::GroupBy;


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


#include "groupByCyclesBenchmarkImplementation.hpp"


#endif //HAQP_GROUPBYCYCLESBENCHMARK_HPP
