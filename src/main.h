#ifndef MABPL_MAIN_H
#define MABPL_MAIN_H


#include "library/mabpl.h"
#include "data_generation/dataFiles.h"

using MABPL::Select;
using MABPL::GroupBy;


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
void radixPartitionBitsSweepBenchmarkWithExtraCountersConfigurations(const DataFile &dataFile, int startBits,
                                                                     int endBits, const std::string &fileNamePrefix,
                                                                     int iterations);

template<typename T>
void radixPartitionSweepBenchmarkWithExtraCountersConfigurations(DataSweep &dataSweep, int radixBits,
                                                                 const std::string &fileNamePrefix,
                                                                 int iterations);


#include "mainImplementation.h"


#endif //MABPL_MAIN_H
