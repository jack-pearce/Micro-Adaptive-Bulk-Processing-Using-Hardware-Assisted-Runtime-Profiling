#ifndef MICRO_ADAPTIVE_BULK_PROCESSING_COUNTERBENCHMARK_H
#define MICRO_ADAPTIVE_BULK_PROCESSING_COUNTERBENCHMARK_H

#include <vector>

#include "haqp.hpp"
#include "data_generation/dataFiles.hpp"

using HAQP::Select;


template<typename T1, typename T2>
void selectCpuCyclesSingleInputBenchmark(const DataFile &dataFile,
                                         const std::vector<Select> &selectImplementations, T1 threshold,
                                         int iterations, const std::string &fileNamePrefix);

template<typename T1, typename T2>
void selectCpuCyclesSweepBenchmark(DataSweep &dataSweep, const std::vector<Select> &selectImplementations,
                                   T1 threshold, int iterations, const std::string &fileNamePrefix);

template <typename T1, typename T2>
void selectWallTimeSweepBenchmark(DataSweep &dataSweep, const std::vector<Select> &selectImplementations,
                                  T1 threshold, int iterations, const std::string &fileNamePrefix);

template <typename T1, typename T2>
void selectWallTimeDopSweepBenchmark(DataSweep &dataSweep, Select selectImplementation, T1 threshold,
                                     int iterations, const std::string &fileNamePrefix, std::vector<int> dop);

template <typename T1, typename T2>
void selectWallTimeDopSweepBenchmarkCalcDopRange(DataSweep &dataSweep, Select selectImplementation,
                                                 int threshold, int iterations,
                                                 const std::string &fileNamePrefix);

template <typename T1, typename T2>
void selectCpuCyclesInputSweepBenchmark(const DataFile &dataFile,
                                        const std::vector<Select> &selectImplementations,
                                        std::vector<float> &thresholds, int iterations,
                                        const std::string &fileNamePrefix);

template <typename T1, typename T2>
void selectWallTimeInputSweepBenchmark(const DataFile &dataFile,
                                       const std::vector<Select> &selectImplementations,
                                       std::vector<float> &thresholds, int iterations,
                                       const std::string &fileNamePrefix);

template <typename T1, typename T2>
void selectWallTimeDopAndInputSweepBenchmark(const DataFile &dataFile,
                                             Select selectImplementation,
                                             std::vector<float> &thresholds, int iterations,
                                             const std::string &fileNamePrefix, std::vector<int> dop);

template <typename T1, typename T2>
void selectWallTimeDopAndInputSweepBenchmarkCalcDopRange(const DataFile &dataFile, Select selectImplementation,
                                                         std::vector<float> &thresholds, int iterations,
                                                         const std::string &fileNamePrefix);


#include "selectCyclesBenchmarkImplementation.hpp"


#endif //MICRO_ADAPTIVE_BULK_PROCESSING_COUNTERBENCHMARK_H
