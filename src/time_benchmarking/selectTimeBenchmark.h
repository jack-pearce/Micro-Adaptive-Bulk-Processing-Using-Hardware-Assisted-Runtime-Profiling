#ifndef MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_TIMEBENCHMARKSELECT_H
#define MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_TIMEBENCHMARKSELECT_H

#include "../library/mabpl.h"
#include "../data_generation/dataFiles.h"

using MABPL::Select;


template <typename T>
void selectTimeBenchmark(const DataFile &dataFile, Select selectImplementation, int selectivityStride);

template <typename T>
void selectTimeBenchmarkSetIterations(const DataFile &dataFile, Select selectImplementation,
                                      int selectivityStride, int iterations);

#include "selectTimeBenchmarkImplementation.h"


#endif //MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_TIMEBENCHMARKSELECT_H