#ifndef HAQP_MAIN_H
#define HAQP_MAIN_H


#include "haqp.h"
#include "data_generation/dataFiles.h"

using HAQP::Select;
using HAQP::GroupBy;
using HAQP::Partition;


template <typename T>
void dataDistributionTest(const DataFile& dataFile);

template <typename T1, typename T2>
void selectFunctionalityTest(const DataFile& dataFile, Select selectImplementation);

template <typename T>
void runSelectTimeBenchmark(const DataFile& dataFile, Select selectImplementation, int selectivityStride);

template <typename T>
void runSelectTimeBenchmarkSetIterations(const DataFile& dataFile, Select selectImplementation, int selectivityStride, int iterations);

template <typename T1, typename T2>
void selectBenchmarkWithExtraCountersConfigurations(const DataFile &dataFile, Select selectImplementation, int iterations);

template <typename T>
void selectIndexesCompareResultsTest(const DataFile& dataFile, Select selectImpOne, Select selectImpTwo);

template <typename T1, typename T2>
void selectValuesCompareResultsTest(const DataFile& dataFile, Select selectImpOne, Select selectImpTwo);

template <typename T1, typename T2>
void selectWallTimeDopSweepBenchmarkCalcDopRange(DataSweep &dataSweep, Select selectImplementation,
                                                 int threshold, int iterations,
                                                 const std::string &fileNamePrefix);

template <typename T1, typename T2>
void selectWallTimeDopAndInputSweepBenchmarkCalcDopRange(const DataFile &dataFile, Select selectImplementation,
                                                         std::vector<float> &thresholds, int iterations,
                                                         const std::string &fileNamePrefix);

template <typename T1, typename T2>
void groupByCompareResultsTest(const DataFile& dataFile, GroupBy groupByImpOne, GroupBy groupByImpTwo);

template <typename T1, typename T2>
void groupByBenchmarkWithExtraCountersConfigurations(DataSweep &dataSweep,
                                                     GroupBy groupByImplementation,
                                                     int iterations,
                                                     const std::string &fileNamePrefix);

template <typename T1, typename T2>
void groupByBenchmarkWithExtraCountersDuringRunConfigurations(const DataFile &dataFile,
                                                              const std::string &fileNamePrefix);

template <typename T1, typename T2>
void groupByWallTimeDopSweepBenchmarkCalcDopRange(DataSweep &dataSweep, int iterations,
                                                  const std::string &fileNamePrefix);

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


#include "mainImplementation.h"


#endif //HAQP_MAIN_H
