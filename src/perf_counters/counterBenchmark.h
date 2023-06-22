#ifndef MICRO_ADAPTIVE_BULK_PROCESSING_COUNTERBENCHMARK_H
#define MICRO_ADAPTIVE_BULK_PROCESSING_COUNTERBENCHMARK_H

#include <vector>

#include "../library/select.h"
#include "../library/papi.h"
#include "../data_generation/dataFiles.h"


void selectSingleRunNoCounters(const DataFile &dataFile,
                               SelectImplementation selectImplementation,
                               int threshold,
                               int iterations);

void selectCpuCyclesSingleInputBenchmark(const DataFile &dataFile,
                                         const std::vector<SelectImplementation> &selectImplementations, int threshold,
                                         int iterations, const std::string &fileNamePrefix);

void selectCpuCyclesMultipleInputBenchmark(const DataFile &dataFile,
                                           const std::vector<SelectImplementation> &selectImplementations,
                                           int selectivityStride, int iterations, const std::string &fileNamePrefix);

void selectBenchmarkWithExtraCounters(const DataFile &dataFile, SelectImplementation selectImplementation,
                                      std::vector<float> &thresholds, int iterations,
                                      std::vector<std::string> &benchmarkCounters, const std::string &fileNamePrefix);

void selectCpuCyclesSweepBenchmark(DataSweep &dataSweep, const std::vector<SelectImplementation> &selectImplementations,
                                   int threshold, int iterations, const std::string &fileNamePrefix);

void selectCpuCyclesInputSweepBenchmark(const DataFile &dataFile,
                                        const std::vector<SelectImplementation> &selectImplementations,
                                        std::vector<float> &thresholds, int iterations,
                                        const std::string &fileNamePrefix);


#endif //MICRO_ADAPTIVE_BULK_PROCESSING_COUNTERBENCHMARK_H
