#ifndef MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_TIMEBENCHMARKSELECT_H
#define MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_TIMEBENCHMARKSELECT_H

#include "../library/select.h"
#include "../data_generation/dataFiles.h"

void selectTimeBenchmark(const DataFile &dataFile, SelectImplementation selectImplementation, int sensitivityStride);
void selectTimeBenchmarkSetIterations(const DataFile &dataFile, SelectImplementation selectImplementation,
                                      int sensitivityStride, int iterations);

#endif //MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_TIMEBENCHMARKSELECT_H
