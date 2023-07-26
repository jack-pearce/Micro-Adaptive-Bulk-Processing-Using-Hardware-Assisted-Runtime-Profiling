#ifndef MICRO_ADAPTIVE_BULK_PROCESSING_COUNTERBENCHMARK_H
#define MICRO_ADAPTIVE_BULK_PROCESSING_COUNTERBENCHMARK_H

#include <vector>

#include "../library/mabpl.h"
#include "../data_generation/dataFiles.h"

using MABPL::Select;

void selectSingleRunNoCounters(const DataFile &dataFile,
                               Select selectImplementation,
                               int threshold,
                               int iterations);

void selectCpuCyclesSingleInputBenchmark(const DataFile &dataFile,
                                         const std::vector<Select> &selectImplementations, int threshold,
                                         int iterations, const std::string &fileNamePrefix);

void selectCpuCyclesMultipleInputBenchmark(const DataFile &dataFile,
                                           const std::vector<Select> &selectImplementations,
                                           int selectivityStride, int iterations, const std::string &fileNamePrefix);

void selectBenchmarkWithExtraCounters(const DataFile &dataFile, Select selectImplementation,
                                      std::vector<float> &thresholds, int iterations,
                                      std::vector<std::string> &benchmarkCounters, const std::string &fileNamePrefix);

void selectCpuCyclesSweepBenchmark(DataSweep &dataSweep, const std::vector<Select> &selectImplementations,
                                   int threshold, int iterations, const std::string &fileNamePrefix);

void selectWallTimeSweepBenchmark(DataSweep &dataSweep, const std::vector<Select> &selectImplementations,
                                   int threshold, int iterations, const std::string &fileNamePrefix);

void selectWallTimeDopSweepBenchmark(DataSweep &dataSweep, Select selectImplementation, int threshold, int iterations,
                                     const std::string &fileNamePrefix, std::vector<int> dop);

void selectCpuCyclesInputSweepBenchmark(const DataFile &dataFile,
                                        const std::vector<Select> &selectImplementations,
                                        std::vector<float> &thresholds, int iterations,
                                        const std::string &fileNamePrefix);

void selectWallTimeInputSweepBenchmark(const DataFile &dataFile,
                                        const std::vector<Select> &selectImplementations,
                                        std::vector<float> &thresholds, int iterations,
                                        const std::string &fileNamePrefix);

void selectWallTimeDopAndInputSweepBenchmark(const DataFile &dataFile,
                                             Select selectImplementation,
                                             std::vector<float> &thresholds, int iterations,
                                             const std::string &fileNamePrefix, std::vector<int> dop);




#endif //MICRO_ADAPTIVE_BULK_PROCESSING_COUNTERBENCHMARK_H
