#ifndef MABPL_RADIXPARTITIONCYCLESBENCHMARK_H
#define MABPL_RADIXPARTITIONCYCLESBENCHMARK_H

#include "../library/mabpl.h"
#include "../data_generation/dataFiles.h"

using MABPL::RadixPartition;


template<typename T>
void testRadixSort(const DataFile &dataFile, RadixPartition radixPartitionImplementation, int radixBits = 10);

template<typename T>
void testRadixPartition(const DataFile &dataFile, RadixPartition radixPartitionImplementation, int radixBits = 10);

template<typename T>
void radixPartitionBitsSweepBenchmarkWithExtraCounters(const DataFile &dataFile,
                                                       RadixPartition radixPartitionImplementation,
                                                       int startBits, int endBits,
                                                       std::vector<std::string> &benchmarkCounters,
                                                       const std::string &fileNamePrefix, int iterations);

template<typename T>
void radixPartitionSweepBenchmarkWithExtraCounters(DataSweep &dataSweep,
                                                   RadixPartition radixPartitionImplementation, int radixBits,
                                                   std::vector<std::string> &benchmarkCounters,
                                                   const std::string &fileNamePrefix, int iterations);

template<typename T>
void radixPartitionBitsSweepBenchmark(const DataFile &dataFile, const std::vector<RadixPartition> &rpImplementations,
                                      int startBits, int endBits, const std::string &fileNamePrefix, int iterations);

template<typename T>
void radixPartitionSweepBenchmark(DataSweep &dataSweep, const std::vector<RadixPartition> &rpImplementations,
                                  int radixBits, const std::string &fileNamePrefix, int iterations);


#include "radixPartitionCyclesBenchmarkImplementation.h"

#endif //MABPL_RADIXPARTITIONCYCLESBENCHMARK_H
