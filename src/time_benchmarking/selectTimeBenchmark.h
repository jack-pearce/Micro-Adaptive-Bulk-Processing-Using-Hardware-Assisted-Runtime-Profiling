#ifndef MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_TIMEBENCHMARKSELECT_H
#define MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_TIMEBENCHMARKSELECT_H

#include "../library/mabpl.h"
#include "../data_generation/dataFiles.h"

using MABPL::Select;

void selectTimeBenchmark(const DataFile &dataFile, Select selectImplementation, int selectivityStride);
void selectTimeBenchmarkSetIterations(const DataFile &dataFile, Select selectImplementation,
                                      int selectivityStride, int iterations);

#endif //MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_TIMEBENCHMARKSELECT_H
