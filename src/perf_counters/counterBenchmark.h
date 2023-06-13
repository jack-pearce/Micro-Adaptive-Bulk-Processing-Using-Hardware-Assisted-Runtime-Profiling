#ifndef MICRO_ADAPTIVE_BULK_PROCESSING_COUNTERBENCHMARK_H
#define MICRO_ADAPTIVE_BULK_PROCESSING_COUNTERBENCHMARK_H

#include "../library/select.h"
#include "../data_generation/dataFiles.h"


void selectCyclesBenchmark(const DataFile& dataFile,
                           SelectImplementation selectImplementation,
                           int sensitivityStride,
                           int iterations,
                           std::vector<std::string>& benchmarkCounters);

#endif //MICRO_ADAPTIVE_BULK_PROCESSING_COUNTERBENCHMARK_H
