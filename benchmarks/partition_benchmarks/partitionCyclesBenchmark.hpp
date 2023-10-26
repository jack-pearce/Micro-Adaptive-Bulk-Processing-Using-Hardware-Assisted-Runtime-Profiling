#ifndef HAQP_PARTITIONCYCLESBENCHMARK_HPP
#define HAQP_PARTITIONCYCLESBENCHMARK_HPP

#include "haqp.hpp"
#include "data_generation/dataFiles.hpp"

using HAQP::PartitionOperators;


template<typename T>
void testSort(const DataFile &dataFile, PartitionOperators partitionImplementation, int radixBits = 10);

template<typename T>
void testPartition(const DataFile &dataFile, PartitionOperators partitionImplementation, int radixBits = 10);

template<typename T>
void partitionBitsSweepBenchmarkWithExtraCounters(const DataFile &dataFile,
                                                  PartitionOperators partitionImplementation,
                                                  int startBits, int endBits,
                                                  std::vector<std::string> &benchmarkCounters,
                                                  const std::string &fileNamePrefix, int iterations);

template<typename T>
void partitionSweepBenchmarkWithExtraCounters(DataSweep &dataSweep,
                                              PartitionOperators partitionImplementation, int radixBitsOperator,
                                              std::vector<std::string> &benchmarkCounters,
                                              const std::string &fileNamePrefix, int iterations);

template<typename T>
void partitionBitsSweepBenchmarkWithExtraCountersConfigurations(const DataFile &dataFile,
                                                                PartitionOperators partitionImplementation,
                                                                int startBits, int endBits,
                                                                const std::string &fileNamePrefix,
                                                                int iterations);

template<typename T>
void partitionSweepBenchmarkWithExtraCountersConfigurations(DataSweep &dataSweep,
                                                            PartitionOperators partitionImplementation,
                                                            int radixBits, const std::string &fileNamePrefix,
                                                            int iterations);

template<typename T>
void partitionBitsSweepBenchmark(const DataFile &dataFile, const std::vector<PartitionOperators> &partitionImplementations,
                                 int startBits, int endBits, const std::string &fileNamePrefix, int iterations);

template<typename T>
void partitionSweepBenchmark(DataSweep &dataSweep, const std::vector<PartitionOperators> &partitionImplementations,
                             int radixBits, const std::string &fileNamePrefix, int iterations);


#include "partitionCyclesBenchmarkImplementation.hpp"

#endif //HAQP_PARTITIONCYCLESBENCHMARK_HPP
