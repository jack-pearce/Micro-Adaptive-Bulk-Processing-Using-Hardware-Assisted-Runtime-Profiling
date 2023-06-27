#ifndef MABPL_GROUPBYCOUNTERBENCHMARK_H
#define MABPL_GROUPBYCOUNTERBENCHMARK_H

#include "../library/groupBy.h"
#include "../data_generation/dataFiles.h"


void groupByCpuCyclesSweepBenchmark(DataSweep &dataSweep, const std::vector<GroupBy> &groupByImplementations,
                                    int iterations, const std::string &fileNamePrefix);

#endif //MABPL_GROUPBYCOUNTERBENCHMARK_H
