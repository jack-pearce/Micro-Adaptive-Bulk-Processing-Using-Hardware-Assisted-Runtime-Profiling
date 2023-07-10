#include <string>
#include <vector>
#include <iostream>

#include "time_benchmarking/selectTimeBenchmark.h"
#include "time_benchmarking/timeBenchmarkHelpers.h"
#include "data_generation/dataGenerators.h"
#include "utilities//dataHelpers.h"
#include "cycles_benchmarking//selectCyclesBenchmark.h"
#include "cycles_benchmarking/groupByCyclesBenchmark.h"
#include "data_generation/dataFiles.h"
#include "library/mabpl.h"

using MABPL::Select;
using MABPL::GroupBy;

using MABPL::MaxAggregation;
using MABPL::MaxAggregation;
using MABPL::SumAggregation;
using MABPL::CountAggregation;

void dataDistributionTest(const DataFile& dataFile) {
    LoadedData::getInstance(dataFile);
    std::cout << dataFile.getNumElements() << " elements" << std::endl;
    displayDistribution(dataFile);
}

void selectFunctionalityTest(const DataFile& dataFile, Select selectImplementation) {

    for (auto i = 0; i <= 100; i += 10) {
        auto inputData = new int[dataFile.getNumElements()];
        auto inputFilter = new int[dataFile.getNumElements()];
        auto selection = new int[dataFile.getNumElements()];
        copyArray(LoadedData::getInstance(dataFile).getData(), inputData, dataFile.getNumElements());
        copyArray(LoadedData::getInstance(dataFile).getData(), inputFilter, dataFile.getNumElements());

        auto selected = MABPL::runSelectFunction(selectImplementation,
                                         dataFile.getNumElements(), inputData, inputFilter, selection, i);
        std::cout << i << "%: " << static_cast<float>(selected) / static_cast<float>(dataFile.getNumElements()) << std::endl;

        delete[] inputData;
        delete[] inputFilter;
        delete[] selection;

    }
}

void runSelectTimeBenchmark(const DataFile& dataFile, Select selectImplementation, int selectivityStride) {
    selectTimeBenchmark(dataFile, selectImplementation, selectivityStride);
    runTimeBenchmark(0, nullptr);
}

void runSelectTimeBenchmarkSetIterations(const DataFile& dataFile, Select selectImplementation, int selectivityStride, int iterations) {
    selectTimeBenchmarkSetIterations(dataFile, selectImplementation, selectivityStride, iterations);
    runTimeBenchmark(0, nullptr);
}

void selectBenchmarkWithExtraCountersConfigurations(const DataFile &dataFile, Select selectImplementation, int iterations) {
    // HPC set 1
    std::vector<std::string> benchmarkCounters = {"PERF_COUNT_HW_CPU_CYCLES",
                                                  "INSTRUCTION_RETIRED",
                                                  "LLC_REFERENCES",
                                                  "LLC_MISSES",
                                                  "MISPREDICTED_BRANCH_RETIRED",
                                                  "PERF_COUNT_HW_CACHE_REFERENCES",
                                                  "PERF_COUNT_HW_CACHE_MISSES",
                                                  "PERF_COUNT_HW_BRANCH_MISSES",
                                                  "PERF_COUNT_HW_CACHE_L1D"};
//    // HPC set 2
//    std::vector<std::string> benchmarkCounters = {"PERF_COUNT_HW_CPU_CYCLES",
//                                                  "INSTRUCTION_RETIRED",
//                                                  "PERF_COUNT_HW_CACHE_L1D",
//                                                  "L1-DCACHE-LOADS",
//                                                  "L1-DCACHE-LOAD-MISSES",
//                                                  "L1-DCACHE-STORES"};

    std::vector<float> inputThresholdDistribution;
    generateLinearDistribution(2, 0., 1, inputThresholdDistribution);

    // Update min & max to match dataFile
//    generateLinearDistribution(10, 0, 100, inputThresholdDistribution);
    // Update min & max to match dataFile
//    generateLogDistribution(30, 1, 10*1000, inputThresholdDistribution);

    selectBenchmarkWithExtraCounters(dataFile,
                                     selectImplementation,
                                     inputThresholdDistribution,
                                     iterations,
                                     benchmarkCounters, "");
}

void allSelectIndexesTests() {
    // Graph 1: Selectivity range on uniform data
    std::vector<float> inputThresholdDistribution;
    generateLogDistribution(30, 1, 10*1000, inputThresholdDistribution);
    selectCpuCyclesInputSweepBenchmark(DataFiles::uniformIntDistribution250mValuesMax10000,
                                       {Select::ImplementationIndexesBranch,
                                        Select::ImplementationIndexesPredication,
                                        Select::ImplementationIndexesAdaptive},
                                       inputThresholdDistribution,
                                       5, "Indexes");

    // Graph 2: Randomness range on sorted data
    selectCpuCyclesSweepBenchmark(DataSweeps::logSortedIntDistribution250mValuesRandomnessSweep,
                                  {Select::ImplementationIndexesBranch,
                                   Select::ImplementationIndexesPredication,
                                   Select::ImplementationIndexesAdaptive},
                                   50, 5, "Indexes");

    // Graph 3: Period range on linearly varying selectivity
    selectCpuCyclesSweepBenchmark(DataSweeps::varyingIntDistribution250mValuesSweep,
                                  {Select::ImplementationIndexesBranch,
                                   Select::ImplementationIndexesPredication,
                                   Select::ImplementationIndexesAdaptive},
                                   50, 5, "Indexes");

    // Graph 4: Period range on step varying selectivity
    selectCpuCyclesSweepBenchmark(DataSweeps::lowerStep50IntDistribution250mValuesSweep,
                                  {Select::ImplementationIndexesBranch,
                                   Select::ImplementationIndexesPredication,
                                   Select::ImplementationIndexesAdaptive},
                                   50, 5, "Indexes");

    // Graph 5: Best case - tuned unequal step varying selectivity
    selectCpuCyclesSingleInputBenchmark(DataFiles::bestCaseIndexesTunedUnequalLowerStep50IntDistribution250mValues,
                                        {Select::ImplementationIndexesBranch,
                                         Select::ImplementationIndexesPredication,
                                         Select::ImplementationIndexesAdaptive},
                                        50,
                                        5, "Indexes");

    // Graph 6: Worst case - tuned period range on step varying selectivity
    selectCpuCyclesSingleInputBenchmark(DataFiles::worstCaseIndexesTunedUpperStep50IntDistribution250mValues,
                                        {Select::ImplementationIndexesBranch,
                                         Select::ImplementationIndexesPredication,
                                         Select::ImplementationIndexesAdaptive},
                                        50,
                                        5, "Indexes");
}

void allSelectValuesTests() {
    // Graph 1: Selectivity range on uniform data
    std::vector<float> inputThresholdDistribution;
    generateLogDistribution(30, 1, 10 * 1000, inputThresholdDistribution);
    selectCpuCyclesInputSweepBenchmark(DataFiles::uniformIntDistribution250mValuesMax10000,
                                       {Select::ImplementationValuesBranch,
                                        Select::ImplementationValuesVectorized,
                                        Select::ImplementationValuesPredication,
                                        Select::ImplementationValuesAdaptive},
                                       inputThresholdDistribution,
                                       5, "Values");

    // Graph 2: Randomness range on sorted data
    selectCpuCyclesSweepBenchmark(DataSweeps::logSortedIntDistribution250mValuesRandomnessSweep,
                                  {Select::ImplementationValuesBranch,
                                   Select::ImplementationValuesVectorized,
                                   Select::ImplementationValuesAdaptive},
                                   50, 5, "Values");


    // Graph 3: Period range on linearly varying selectivity
    selectCpuCyclesSweepBenchmark(DataSweeps::varyingIntDistribution250mValuesSweep,
                                  {Select::ImplementationValuesBranch,
                                   Select::ImplementationValuesVectorized,
                                   Select::ImplementationValuesAdaptive},
                                   50, 5, "Values");

    // Graph 4: Period range on step varying selectivity
    selectCpuCyclesSweepBenchmark(DataSweeps::lowerStep50IntDistribution250mValuesSweep,
                                  {Select::ImplementationValuesBranch,
                                   Select::ImplementationValuesVectorized,
                                   Select::ImplementationValuesAdaptive},
                                   50, 5, "Values");

    // Graph 5: Best case - tuned unequal step varying selectivity
    selectCpuCyclesSingleInputBenchmark(DataFiles::bestCaseValuesTunedUnequalLowerStep50IntDistribution250mValues,
                                        {Select::ImplementationValuesBranch,
                                         Select::ImplementationValuesVectorized,
                                         Select::ImplementationValuesAdaptive},
                                        50,
                                        5, "Values");

    // Graph 6: Worst case - tuned period range on step varying selectivity
    selectCpuCyclesSingleInputBenchmark(DataFiles::worstCaseValuesTunedLowerStep50IntDistribution250mValues,
                                        {Select::ImplementationValuesBranch,
                                         Select::ImplementationValuesVectorized,
                                         Select::ImplementationValuesAdaptive},
                                        50,
                                        5, "Values");

}

void groupByCompareResultsTest(const DataFile& dataFile, GroupBy groupByImpOne, GroupBy groupByImpTwo) {
    auto inputGroupBy = new int[dataFile.getNumElements()];
    auto inputAggregate = new int[dataFile.getNumElements()];
    copyArray(LoadedData::getInstance(dataFile).getData(), inputGroupBy, dataFile.getNumElements());
    generateUniformDistributionInMemory(inputAggregate, dataFile.getNumElements(), 10);

    auto resultOne = MABPL::runGroupByFunction<MaxAggregation>(groupByImpOne,
                                                               dataFile.getNumElements(), inputGroupBy, inputAggregate,
                                                               10000000);
    sortVectorOfPairs(resultOne);

    auto resultTwo = MABPL::runGroupByFunction<MaxAggregation>(groupByImpTwo,
                                                               dataFile.getNumElements(), inputGroupBy, inputAggregate,
                                                               10000000);
   sortVectorOfPairs(resultTwo);

    if (resultOne.size() != resultTwo.size()) {
        std::cout << "Size of results are different" << std::endl;
    }

    for (auto i = 0; i < static_cast<int> (resultOne.size()); ++i) {
        if ((resultOne[i].first != resultTwo[i].first) || (resultOne[i].second != resultTwo[i].second)) {
            std::cout << "Different row found" << std::endl;
        }
    }

    delete []inputGroupBy;
    delete []inputAggregate;
}

void groupByBenchmarkWithExtraCountersConfigurations(DataSweep &dataSweep,
                                                     GroupBy groupByImplementation,
                                                     int iterations,
                                                     const std::string &fileNamePrefix) {
    std::vector<std::string> benchmarkCounters = {"PERF_COUNT_HW_CPU_CYCLES",
                                                  "INSTRUCTION_RETIRED",
                                                  "LLC_REFERENCES",
                                                  "PERF_COUNT_HW_CACHE_REFERENCES",
                                                  "PERF_COUNT_HW_CACHE_MISSES",
                                                  "PERF_COUNT_HW_CACHE_L1D",
                                                  "L1-DCACHE-LOADS",
                                                  "L1-DCACHE-LOAD-MISSES",
                                                  "L1-DCACHE-STORES"};

    groupByBenchmarkWithExtraCounters(dataSweep, groupByImplementation, iterations, benchmarkCounters, fileNamePrefix);
}

void groupByBenchmarkWithExtraCountersDuringRunConfigurations(const DataFile &dataFile,
                                                              const std::string &fileNamePrefix) {
    // HPC set 1
    std::vector<std::string> benchmarkCounters = {"PERF_COUNT_HW_CPU_CYCLES",
                                                  "INSTRUCTION_RETIRED",
                                                  "LLC_REFERENCES",
                                                  "PERF_COUNT_HW_CACHE_REFERENCES",
                                                  "PERF_COUNT_HW_CACHE_MISSES",
                                                  "PERF_COUNT_HW_CACHE_L1D",
                                                  "L1-DCACHE-LOADS",
                                                  "L1-DCACHE-LOAD-MISSES",
                                                  "L1-DCACHE-STORES"};

    groupByBenchmarkWithExtraCountersDuringRun(dataFile, benchmarkCounters, fileNamePrefix);
}

void allGroupByTestsOLD() {
/*
// SET ONE - BITS_PER_PASS = 10
    // Graph 1: Cardinality range on uniform data (variable max) for different hashmaps - compile with -march=native removed
*//*    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepVariableMax,
                                   {GroupBy::HashGoogleDenseHashMap,
//                                    GroupBy::HashFollyF14FastMap,   // Need to turn off -march=native for this one
                                    GroupBy::HashAbseilFlatHashMap,
                                    GroupBy::HashTessilRobinMap,
                                    GroupBy::HashTessilHopscotchMap}, countAggregation,
                                   1, "1-MapCompare");*//*

    // Graph 2: Cardinality range (variable max) for simple radix sort - manually run with BITS_PER_PASS of 8, 10, 12
    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepVariableMax,
                                   {GroupBy::SortRadix}, countAggregation,
                                   1, "2-RadixSimple10");

    // Graph 3: Cardinality range (variable max) for optimised radix sort - manually run with BITS_PER_PASS of 8, 10, 12
    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepVariableMax,
                                   {GroupBy::SortRadixOpt}, countAggregation,
                                   1, "3-RadixOpt10");

    // Graph 5: Cardinality range on uniform data (fixed max) for different hashmaps - compile with -march=native removed
*//*    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMax,
                                   {GroupBy::HashGoogleDenseHashMap,
//                                    GroupBy::HashFollyF14FastMap,   // Need to turn off -march=native for this one
                                    GroupBy::HashAbseilFlatHashMap,
                                    GroupBy::HashTessilRobinMap,
                                    GroupBy::HashTessilHopscotchMap}, countAggregation,
                                   1, "5-MapCompare");*//*

    // Graph 6: Cardinality range (fixed max) for simple radix sort - manually run with BITS_PER_PASS of 8, 10, 12
    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMax,
                                   {GroupBy::SortRadix}, countAggregation,
                                   1, "6-RadixSimple10");

    // Graph 7: Cardinality range (fixed max) for optimised radix sort - manually run with BITS_PER_PASS of 8, 10, 12
    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMax,
                                   {GroupBy::SortRadixOpt}, countAggregation,
                                   1, "7-RadixOpt10");

    // Graph 9: Cardinality range (fixed max) for single / double radix10 pass
    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMax,
                                   {GroupBy::SingleRadixPassThenHash,
                                    GroupBy::DoubleRadixPassThenHash}, countAggregation,
                                   1, "9-SingleDoubleRadix10PassThenHash");

    // Graph 10: Cardinality range (fixed max) for levels of clustering
    // Graph 2: Cardinality range (variable max) for simple radix sort - manually run with BITS_PER_PASS of 8, 10, 12
    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMax,
                                   {GroupBy::Hash,GroupBy::SortRadixOpt,
                                    GroupBy::Adaptive}, countAggregation,
                                   1, "10-NoClustering");

    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMaxClustered1,
                                   {GroupBy::Hash,GroupBy::SortRadixOpt,
                                    GroupBy::Adaptive}, countAggregation,
                                   1, "10-Clustered1");

    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMaxClustered1k,
                                   {GroupBy::Hash,GroupBy::SortRadixOpt,
                                    GroupBy::Adaptive}, countAggregation,
                                   1, "10-Clustered1k");

    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMaxClustered100k,
                                   {GroupBy::Hash,GroupBy::SortRadixOpt,
                                    GroupBy::Adaptive}, countAggregation,
                                   1, "10-Clustered100k");

// HPC CHARTS

    groupByBenchmarkWithExtraCountersConfigurations(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMax,
                                                    GroupBy::Hash, countAggregation, 1,
                                                    "1HPC-HashCounters");

    groupByBenchmarkWithExtraCountersConfigurations(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMaxClustered1,
                                                    GroupBy::Hash, countAggregation, 1,
                                                    "2-1HPC-HashCounters");

    groupByBenchmarkWithExtraCountersConfigurations(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMaxClustered1k,
                                                    GroupBy::Hash, countAggregation, 1,
                                                    "2-1kHPC-HashCounters");

    groupByBenchmarkWithExtraCountersConfigurations(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMaxClustered100k,
                                                    GroupBy::Hash, countAggregation, 1,
                                                    "2-100kHPC-HashCounters");

    groupByBenchmarkWithExtraCountersConfigurations(DataSweeps::linearUniformIntDistribution200mValuesCardinalitySweepFixedMaxCrossOverPoint,
                                                    GroupBy::Hash, countAggregation, 1,
                                                    "4HPC-HashCounters");

    groupByBenchmarkWithExtraCountersDuringRunConfigurations(DataFiles::uniformIntDistribution200mValuesCardinality400kMax200m,
                                                             countAggregation,
                                                             "5HPC-HashCounters");


    // SET TWO - BITS_PER_PASS = 8
*//*    // Graph 2: Cardinality range (variable max) for simple radix sort - manually run with BITS_PER_PASS of 8, 10, 12
    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepVariableMax,
                                   {GroupBy::SortRadix}, countAggregation,
                                   1, "2-RadixSimple8");

    // Graph 3: Cardinality range (variable max) for optimised radix sort - manually run with BITS_PER_PASS of 8, 10, 12
    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepVariableMax,
                                   {GroupBy::SortRadixOpt}, countAggregation,
                                   1, "3-RadixOpt8");


    // Graph 6: Cardinality range (fixed max) for simple radix sort - manually run with BITS_PER_PASS of 8, 10, 12
    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMax,
                                   {GroupBy::SortRadix}, countAggregation,
                                   1, "6-RadixSimple8");

    // Graph 7: Cardinality range (fixed max) for optimised radix sort - manually run with BITS_PER_PASS of 8, 10, 12
    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMax,
                                   {GroupBy::SortRadixOpt}, countAggregation,
                                   1, "7-RadixOpt8");

    // Graph 9: Cardinality range (fixed max) for single / double radix10 pass
    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMax,
                                   {GroupBy::SingleRadixPassThenHash,
                                    GroupBy::DoubleRadixPassThenHash}, countAggregation,
                                   1, "9-SingleDoubleRadix8PassThenHash");*//*


    // SET THREE - BITS_PER_PASS = 12
 *//*   // Graph 2: Cardinality range (variable max) for simple radix sort - manually run with BITS_PER_PASS of 8, 10, 12
    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepVariableMax,
                                   {GroupBy::SortRadix}, countAggregation,
                                   1, "2-RadixSimple12");

    // Graph 3: Cardinality range (variable max) for optimised radix sort - manually run with BITS_PER_PASS of 8, 10, 12
    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepVariableMax,
                                   {GroupBy::SortRadixOpt}, countAggregation,
                                   1, "3-RadixOpt12");


    // Graph 6: Cardinality range (fixed max) for simple radix sort - manually run with BITS_PER_PASS of 8, 10, 12
    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMax,
                                   {GroupBy::SortRadix}, countAggregation,
                                   1, "6-RadixSimple12");

    // Graph 7: Cardinality range (fixed max) for optimised radix sort - manually run with BITS_PER_PASS of 8, 10, 12
    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMax,
                                   {GroupBy::SortRadixOpt}, countAggregation,
                                   1, "7-RadixOpt12");

    // Graph 9: Cardinality range (fixed max) for single / double radix10 pass
    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMax,
                                   {GroupBy::SingleRadixPassThenHash,
                                    GroupBy::DoubleRadixPassThenHash}, countAggregation,
                                   1, "9-SingleDoubleRadix8PassThenHash");*/
}

void allGroupByTests() {
    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution20mValuesCardinalitySweepVariableMax,
                                   {GroupBy::Hash,GroupBy::SortRadixOpt,
                                    GroupBy::AdaptiveSwitchToSortOnly},
                                   1, "VariableMax");

    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution20mValuesCardinalitySweepFixedMax,
                                   {GroupBy::Hash,GroupBy::SortRadixOpt,
                                    GroupBy::AdaptiveSwitchToSortOnly},
                                   1, "NoClustering");

    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution20mValuesCardinalitySweepFixedMaxClustered10,
                                   {GroupBy::Hash,GroupBy::SortRadixOpt,
                                    GroupBy::AdaptiveSwitchToSortOnly},
                                   1, "Clustered10");

    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution20mValuesCardinalitySweepFixedMaxClustered1k,
                                   {GroupBy::Hash,GroupBy::SortRadixOpt,
                                    GroupBy::AdaptiveSwitchToSortOnly},
                                   1, "Clustered1k");

    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution20mValuesCardinalitySweepFixedMaxClustered100k,
                                   {GroupBy::Hash,GroupBy::SortRadixOpt,
                                    GroupBy::AdaptiveSwitchToSortOnly},
                                   1, "Clustered100k");

    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution40mValuesCardinalitySweepFixedMax,
                                   {GroupBy::Hash,GroupBy::SortRadixOpt,
                                    GroupBy::AdaptiveSwitchToSortOnly},
                                   1, "NoClustering40M");

    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMax,
                                   {GroupBy::Hash,GroupBy::SortRadixOpt,
                                    GroupBy::AdaptiveSwitchToSortOnly},
                                   1, "NoClustering200M");

    groupByCpuCyclesSweepBenchmark64(DataSweeps::logUniformInt64Distribution20mValuesCardinalitySweepFixedMax,
                                   {GroupBy::Hash,GroupBy::SortRadixOpt,
                                    GroupBy::AdaptiveSwitchToSortOnly},
                                   1, "NoClustering64bitInt");
}

int main() {

    groupByCpuCyclesSweepBenchmark(DataSweeps::linearUniformIntDistribution20mValuesCardinalitySections_3m_100_Max20m,
                                   {GroupBy::Hash, GroupBy::SortRadixOpt , GroupBy::AdaptiveSwitchToSortOnly, GroupBy::Adaptive},
                                   1, "");

//    groupByCpuCyclesSweepBenchmark(DataSweeps::linearUniformIntDistribution20mValuesCardinalitySections_100_10m_Max20m,
//                                   {GroupBy::Hash,GroupBy::SortRadixOpt ,GroupBy::Adaptive, GroupBy::Adaptive2},
//                                   1, "");

//    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution20mValuesCardinalitySweepFixedMax,
//                                   {GroupBy::Adaptive2},
//                                   1, "JustAdaptiveNEW");

//    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution20mValuesCardinalitySweepFixedMax,
//                                   {GroupBy::Hash,GroupBy::SortRadixOpt ,GroupBy::Adaptive, GroupBy::Adaptive2},
//                                   1, "");

//    groupByCpuCyclesSweepBenchmark(DataSweeps::linearUniformIntDistribution20mValuesCardinalitySections_100_10m_Max20m,
//                           {GroupBy::Hash,GroupBy::SortRadixOpt ,GroupBy::Adaptive, GroupBy::Adaptive2},
//                           1, "");


//    groupByCompareResultsTest(DataFiles::uniformIntDistribution20mValuesTwo10mCardinalitySections_100_10m_Max20m,
//                              GroupBy::Hash, GroupBy::Adaptive2);

//    groupByBenchmarkWithExtraCountersDuringRunConfigurations(DataFiles::uniformIntDistribution20mValuesCardinality10mMax20mClustered1k, "");

//    groupByCpuCyclesSweepBenchmark(DataSweeps::linearUniformIntDistribution200mValuesCardinalitySections_100_10m_Max20m,
//                                   {GroupBy::Hash,GroupBy::SortRadixOpt ,GroupBy::Adaptive},
//                                   2, "TwoCardinalitySections200m_2");

//    groupByCpuCyclesSweepBenchmark(DataSweeps::linearUniformIntDistribution200mValuesCardinalitySections_100_100m_Max200m,
//                                   {GroupBy::Hash,GroupBy::SortRadixOpt ,GroupBy::Adaptive},
//                                   1, "TwoCardinalitySections200m");

//    groupByCpuCyclesSweepBenchmark(DataSweeps::linearUniformIntDistribution200mValuesCardinalitySections_100_10m_Max20m,
//                                   {GroupBy::Adaptive},
//                                   1, "Test");

/*    groupByCpuCyclesSweepBenchmark(DataSweeps::linearUniformIntDistribution20mValuesCardinalitySections_100_10m_Max20m,
                                   {GroupBy::Hash,GroupBy::SortRadixOpt ,GroupBy::Adaptive},
                                   2, "TwoCardinalitySections20m");

    groupByCpuCyclesSweepBenchmark(DataSweeps::linearUniformIntDistribution200mValuesCardinalitySections_100_10m_Max20m,
                                   {GroupBy::Hash,GroupBy::SortRadixOpt ,GroupBy::Adaptive},
                                   2, "TwoCardinalitySections200m");*/

//    groupByCompareResultsTest(DataFiles::uniformIntDistribution200mValuesCardinality400kMax200m,
//                              GroupBy::Hash, GroupBy::Adaptive);

//    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution20mValuesCardinalitySweepFixedMax,
//                                   {GroupBy::Adaptive},
//                                   1, "");

//    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution20mValuesCardinalitySweepFixedMax,
//                                   {GroupBy::Hash, GroupBy::Adaptive},
//                                   1, "");
//
//    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMax,
//                                   {GroupBy::Hash,GroupBy::SortRadixOpt ,GroupBy::Adaptive},
//                                   1, "");

//    groupByBenchmarkWithExtraCountersDuringRunConfigurations(DataFiles::uniformIntDistribution20mValuesCardinality50kMax20m,
//                                                             "WarmUpTest");
//
//    groupByBenchmarkWithExtraCountersConfigurations(DataSweeps::logUniformIntDistribution20mValuesCardinalitySweepFixedMaxClustered100k,
//                                      GroupBy::Hash,1, "");

//    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution20mValuesCardinalitySweepFixedMaxClustered100k,
//                                   {GroupBy::Hash},
//                                   1, "HashCheck");

/*    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution20mValuesCardinalitySweepFixedMax,
                                   {GroupBy::Hash,GroupBy::SortRadixOpt},
                                   1, "");

    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution20mValuesCardinalitySweepFixedMaxClustered10,
                                   {GroupBy::Hash,GroupBy::SortRadixOpt},
                                   1, "");

    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution20mValuesCardinalitySweepFixedMaxClustered1k,
                                   {GroupBy::Hash,GroupBy::SortRadixOpt},
                                   1, "");

    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution20mValuesCardinalitySweepFixedMaxClustered100k,
                                   {GroupBy::Hash,GroupBy::SortRadixOpt},
                                   1, "");

    groupByCpuCyclesSweepBenchmark(DataSweeps::logUniformIntDistribution20mValuesCardinalitySweepVariableMax,
                                   {GroupBy::Hash,GroupBy::SortRadixOpt},
                                   1, "");*/

    return 0;
}