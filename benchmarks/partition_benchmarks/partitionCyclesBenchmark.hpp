#ifndef HAQP_PARTITIONCYCLESBENCHMARK_HPP
#define HAQP_PARTITIONCYCLESBENCHMARK_HPP

#include "haqp.hpp"
#include "data_generation/dataFiles.hpp"

using HAQP::PartitionOperators;


template<typename T>
void partitionSweepBenchmark(DataSweep &dataSweep, const std::vector<PartitionOperators> &partitionImplementations,
                             int radixBits, const std::string &fileNamePrefix, int iterations);


#include "partitionCyclesBenchmarkImplementation.hpp"

#endif //HAQP_PARTITIONCYCLESBENCHMARK_HPP
