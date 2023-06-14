#ifndef MICRO_ADAPTIVE_BULK_PROCESSING_COUNTERBENCHMARK_H
#define MICRO_ADAPTIVE_BULK_PROCESSING_COUNTERBENCHMARK_H

#include "../library/select.h"
#include "../library/papi.h"
#include "../data_generation/dataFiles.h"


void selectCountersBenchmark(const DataFile& dataFile,
                             SelectImplementation selectImplementation,
                             int sensitivityStride,
                             int iterations,
                             std::vector<std::string>& benchmarkCounters);

void selectCpuCyclesBenchmark(const DataFile& dataFile,
                              SelectImplementation selectImplementation,
                              int sensitivityStride,
                              int iterations);

void selectCpuCyclesBenchmark(const DataFile& dataFile,
                              SelectImplementation selectImplementation,
                              int iterations);

#endif //MICRO_ADAPTIVE_BULK_PROCESSING_COUNTERBENCHMARK_H
