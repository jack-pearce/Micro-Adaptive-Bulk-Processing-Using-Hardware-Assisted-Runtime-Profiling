#ifndef MICRO_ADAPTIVE_BULK_PROCESSING_COUNTERBENCHMARK_H
#define MICRO_ADAPTIVE_BULK_PROCESSING_COUNTERBENCHMARK_H

#include <vector>

#include "../library/mabpl.h"
#include "../data_generation/dataFiles.h"

using MABPL::Select;


template<typename T1, typename T2>
void selectSingleRunNoCounters(const DataFile &dataFile, Select selectImplementation, T1 threshold,
                               int iterations);

template<typename T1, typename T2>
void selectCpuCyclesSingleInputBenchmark(const DataFile &dataFile,
                                         const std::vector<Select> &selectImplementations, T1 threshold,
                                         int iterations, const std::string &fileNamePrefix);

template<typename T1, typename T2>
void selectCpuCyclesMultipleInputBenchmark(const DataFile &dataFile,
                                           const std::vector<Select> &selectImplementations,
                                           int selectivityStride, int iterations, const std::string &fileNamePrefix);

template<typename T1, typename T2>
void selectBenchmarkWithExtraCounters(const DataFile &dataFile, Select selectImplementation,
                                      std::vector<float> &thresholds, int iterations,
                                      std::vector<std::string> &benchmarkCounters, const std::string &fileNamePrefix);

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


#include "selectCyclesBenchmarkImplementation.h"


#endif //MICRO_ADAPTIVE_BULK_PROCESSING_COUNTERBENCHMARK_H
