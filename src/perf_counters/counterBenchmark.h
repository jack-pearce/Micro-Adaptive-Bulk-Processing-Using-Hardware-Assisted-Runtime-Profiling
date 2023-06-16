#ifndef MICRO_ADAPTIVE_BULK_PROCESSING_COUNTERBENCHMARK_H
#define MICRO_ADAPTIVE_BULK_PROCESSING_COUNTERBENCHMARK_H

#include "../library/select.h"
#include "../library/papi.h"
#include "../data_generation/dataFiles.h"


void selectBenchmarkWithExtraCounters(const DataFile& dataFile,
                                      SelectImplementation selectImplementation,
                                      int sensitivityStride,
                                      int iterations,
                                      std::vector<std::string>& benchmarkCounters);

void selectCpuCyclesMultipleBenchmarks(const DataFile& dataFile,
                                       SelectImplementation selectImplementation,
                                       int sensitivityStride,
                                       int iterations);

void selectCpuCyclesSingleBenchmark(const DataFile &dataFile, SelectImplementation selectImplementation, int iterations,
                                    int threshold);

void selectSingleRunNoCounters(const DataFile &dataFile, SelectImplementation selectImplementation, int iterations,
                               int threshold);

#endif //MICRO_ADAPTIVE_BULK_PROCESSING_COUNTERBENCHMARK_H
