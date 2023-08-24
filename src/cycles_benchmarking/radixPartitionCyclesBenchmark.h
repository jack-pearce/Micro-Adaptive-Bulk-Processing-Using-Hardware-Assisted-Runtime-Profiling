#ifndef MABPL_RADIXPARTITIONCYCLESBENCHMARK_H
#define MABPL_RADIXPARTITIONCYCLESBENCHMARK_H

#include "../library/mabpl.h"
#include "../data_generation/dataFiles.h"


template<typename T1, typename T2>
void testRadixPartition(const DataFile &dataFile, int radixBits = 10);

template<typename T1, typename T2>
void radixPartitionBitsSweepBenchmarkWithExtraCounters(const DataFile &dataFile, int startBits, int endBits,
                                                       std::vector<std::string> &benchmarkCounters,
                                                       const std::string &fileNamePrefix, int iterations);


#include "radixPartitionCyclesBenchmarkImplementation.h"

#endif //MABPL_RADIXPARTITIONCYCLESBENCHMARK_H
