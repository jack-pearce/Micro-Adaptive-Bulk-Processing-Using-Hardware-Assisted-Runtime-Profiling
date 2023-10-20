#ifndef HAQP_PARTITIONCYCLESBENCHMARK_H
#define HAQP_PARTITIONCYCLESBENCHMARK_H

#include "haqp.h"
#include "data_generation/dataFiles.h"

using HAQP::Partition;


template<typename T>
void testSort(const DataFile &dataFile, Partition partitionImplementation, int radixBits = 10);

template<typename T>
void testPartition(const DataFile &dataFile, Partition partitionImplementation, int radixBits = 10);

//template<typename T>
//void partitionBitsSweepBenchmarkWithExtraCounters(const DataFile &dataFile,
//                                                  Partition partitionImplementation,
//                                                  int startBits, int endBits,
//                                                  std::vector<std::string> &benchmarkCounters,
//                                                  const std::string &fileNamePrefix, int iterations);
//
//template<typename T>
//void partitionSweepBenchmarkWithExtraCounters(DataSweep &dataSweep,
//                                              Partition partitionImplementation, int radixBits,
//                                              std::vector<std::string> &benchmarkCounters,
//                                              const std::string &fileNamePrefix, int iterations);

template<typename T>
void partitionBitsSweepBenchmarkWithExtraCountersConfigurations(const DataFile &dataFile,
                                                                Partition partitionImplementation,
                                                                int startBits, int endBits,
                                                                const std::string &fileNamePrefix,
                                                                int iterations);

template<typename T>
void partitionSweepBenchmarkWithExtraCountersConfigurations(DataSweep &dataSweep,
                                                            Partition partitionImplementation,
                                                            int radixBits, const std::string &fileNamePrefix,
                                                            int iterations);

template<typename T>
void partitionBitsSweepBenchmark(const DataFile &dataFile, const std::vector<Partition> &partitionImplementations,
                                 int startBits, int endBits, const std::string &fileNamePrefix, int iterations);

template<typename T>
void partitionSweepBenchmark(DataSweep &dataSweep, const std::vector<Partition> &partitionImplementations,
                             int radixBits, const std::string &fileNamePrefix, int iterations);


#include "partitionCyclesBenchmarkImplementation.h"

#endif //HAQP_PARTITIONCYCLESBENCHMARK_H
