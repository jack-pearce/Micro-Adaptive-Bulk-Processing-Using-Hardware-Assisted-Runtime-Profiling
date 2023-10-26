#include <vector>

#include "haqp.hpp"
#include "select_benchmarks/selectCyclesBenchmark.hpp"
#include "groupBy_benchmarks/groupByCyclesBenchmark.hpp"
#include "partition_benchmarks/partitionCyclesBenchmark.hpp"
#include "data_generation/dataFiles.hpp"
#include "utilities/dataHelpers.hpp"

using HAQP::Select;
using HAQP::GroupBy;
using HAQP::PartitionOperators;

using HAQP::MinAggregation;
using HAQP::MaxAggregation;
using HAQP::SumAggregation;
using HAQP::CountAggregation;


void allSelectIndexesSingleThreadedTests(int iterations) {
    // Graph 1: Selectivity range on uniform data
    std::vector<float> inputThresholdDistribution;
    generateLogDistribution(30, 1, 10*1000, inputThresholdDistribution);
    selectCpuCyclesInputSweepBenchmark<int,int>(DataFiles::uniformIntDistribution250mValuesMax10000,
                                       {Select::ImplementationIndexesBranch,
                                        Select::ImplementationIndexesPredication,
                                        Select::ImplementationIndexesAdaptive},
                                       inputThresholdDistribution,
                                       iterations, "1-Indexes");

    // Graph 2: Randomness range on sorted data
    selectCpuCyclesSweepBenchmark<int,int>(DataSweeps::logSortedIntDistribution250mValuesRandomnessSweep,
                                  {Select::ImplementationIndexesBranch,
                                   Select::ImplementationIndexesPredication,
                                   Select::ImplementationIndexesAdaptive},
                                   50, iterations, "2-Indexes");

    // Graph 3: Selectivity on fully sorted data
    std::vector<float> inputThresholdDistribution2;
    generateLogDistribution(30, 1, 100, inputThresholdDistribution2);
    selectCpuCyclesInputSweepBenchmark<int,int>(DataFiles::fullySortedIntDistribution250mValues,
                                       {Select::ImplementationIndexesBranch,
                                        Select::ImplementationIndexesPredication,
                                        Select::ImplementationIndexesAdaptive},
                                       inputThresholdDistribution2,
                                       iterations, "3-Indexes");

    // Graph 4: Varying section length on step varying selectivity
    selectCpuCyclesSweepBenchmark<int,int>(DataSweeps::lowerStep50IntDistribution250mValuesSweepSectionLength_1,
                                           {Select::ImplementationIndexesBranch,
                                            Select::ImplementationIndexesPredication,
                                            Select::ImplementationIndexesAdaptive},
                                           50, iterations, "4_1-Indexes");

    selectCpuCyclesSweepBenchmark<int,int>(DataSweeps::lowerStep50IntDistribution250mValuesSweepSectionLength_2,
                                           {Select::ImplementationIndexesBranch,
                                            Select::ImplementationIndexesPredication,
                                            Select::ImplementationIndexesAdaptive},
                                           50, iterations, "4_2-Indexes");

    selectCpuCyclesSweepBenchmark<int,int>(DataSweeps::lowerStep50IntDistribution250mValuesSweepSectionLength_3,
                                           {Select::ImplementationIndexesBranch,
                                            Select::ImplementationIndexesPredication,
                                            Select::ImplementationIndexesAdaptive},
                                           50, iterations, "4_3-Indexes");

    selectCpuCyclesSingleInputBenchmark<int,int>(DataFiles::worstCaseIndexesTunedUpperStep50IntDistribution250mValues,
                                             {Select::ImplementationIndexesBranch,
                                              Select::ImplementationIndexesPredication,
                                              Select::ImplementationIndexesAdaptive},
                                             50,
                                             iterations, "4_4-Indexes");

    // Graph 5: Varying relative section length on step varying selectivity
    selectCpuCyclesSweepBenchmark<int,int>(DataSweeps::lowerStep50IntDistribution250mValuesPercentageStepSweep,
                                  {Select::ImplementationIndexesBranch,
                                   Select::ImplementationIndexesPredication,
                                   Select::ImplementationIndexesAdaptive},
                                  50, iterations, "5-Indexes");
}

void allSelectValuesSingleThreadedTests(int iterations) {
    // Graph 1: Selectivity range on uniform data
    std::vector<float> inputThresholdDistribution;
    generateLogDistribution(30, 1, 10 * 1000, inputThresholdDistribution);
    selectCpuCyclesInputSweepBenchmark<int,int>(DataFiles::uniformIntDistribution250mValuesMax10000,
                                       {Select::ImplementationValuesBranch,
                                        Select::ImplementationValuesVectorized,
                                        Select::ImplementationValuesPredication,
                                        Select::ImplementationValuesAdaptive},
                                       inputThresholdDistribution,
                                       iterations, "1-Values");

    // Graph 2: Randomness range on sorted data
    selectCpuCyclesSweepBenchmark<int,int>(DataSweeps::logSortedIntDistribution250mValuesRandomnessSweep,
                                  {Select::ImplementationValuesBranch,
                                   Select::ImplementationValuesVectorized,
                                   Select::ImplementationValuesAdaptive},
                                   50, iterations, "2-Values");

    // Graph 3: Selectivity on fully sorted data
    std::vector<float> inputThresholdDistribution2;
    generateLogDistribution(30, 1, 100, inputThresholdDistribution2);
    selectCpuCyclesInputSweepBenchmark<int,int>(DataFiles::fullySortedIntDistribution250mValues,
                                       {Select::ImplementationValuesBranch,
                                        Select::ImplementationValuesVectorized,
                                        Select::ImplementationValuesAdaptive},
                                       inputThresholdDistribution2,
                                       iterations, "3-Values");

    // Graph 4: Varying section length on step varying selectivity
    selectCpuCyclesSweepBenchmark<int,int>(DataSweeps::lowerStep50IntDistribution250mValuesSweep,
                                  {Select::ImplementationValuesBranch,
                                   Select::ImplementationValuesVectorized,
                                   Select::ImplementationValuesAdaptive},
                                   50, iterations, "4_1-Values");

    selectCpuCyclesSingleInputBenchmark<int,int>(DataFiles::worstCaseValuesTunedLowerStep50IntDistribution250mValues,
                                 {Select::ImplementationValuesBranch,
                                  Select::ImplementationValuesVectorized,
                                  Select::ImplementationValuesAdaptive},
                                 50,
                                 iterations, "4_2-Values");

    // Graph 5: Varying relative section length on step varying selectivity
    selectCpuCyclesSweepBenchmark<int,int>(DataSweeps::lowerStep50IntDistribution250mValuesPercentageStepSweep,
                                  {Select::ImplementationValuesBranch,
                                   Select::ImplementationValuesVectorized,
                                   Select::ImplementationValuesAdaptive},
                                  50, iterations, "5-Values");
}

void allSelectIndexesParallelTests(int iterations) {
    std::vector<float> inputThresholdDistribution;
    generateLogDistribution(30, 1, 10*1000, inputThresholdDistribution);
    selectWallTimeInputSweepBenchmark<int,int>(DataFiles::uniformIntDistribution250mValuesMax10000,
                                      {Select::ImplementationIndexesAdaptive},
                                      inputThresholdDistribution,
                                      iterations, "7-DOP-1-Indexes-SelectivitySweepSingle");

    selectWallTimeDopAndInputSweepBenchmarkCalcDopRange<int,int>(DataFiles::uniformIntDistribution250mValuesMax10000,
                                                        Select::ImplementationIndexesAdaptiveParallel,
                                                        inputThresholdDistribution,
                                                        iterations, "7-DOP-1-Indexes-SelectivitySweepParallel");

    selectWallTimeSweepBenchmark<int,int>(DataSweeps::lowerStep50IntDistribution250mValuesSweep,
                                 {Select::ImplementationIndexesAdaptive},
                                 50, iterations, "7-DOP-2-Indexes-StepPeriodSweepSingle");

    selectWallTimeDopSweepBenchmarkCalcDopRange<int,int>(DataSweeps::lowerStep50IntDistribution250mValuesSweep,
                                                Select::ImplementationIndexesAdaptiveParallel,
                                                50, iterations, "7-DOP-2-Indexes-StepPeriodSweepParallel");

    selectWallTimeSweepBenchmark<int,int>(DataSweeps::lowerStep50IntDistribution250mValuesPercentageStepSweep,
                                 {Select::ImplementationIndexesAdaptive},
                                 50, iterations, "7-DOP-3-Indexes-StepPercentageSweepSingle");

    selectWallTimeDopSweepBenchmarkCalcDopRange<int,int>(DataSweeps::lowerStep50IntDistribution250mValuesPercentageStepSweep,
                                                Select::ImplementationIndexesAdaptiveParallel,
                                                50, iterations, "7-DOP-3-Indexes-StepPercentageSweepParallel");
}

void allSelectValuesParallelTests(int iterations) {
    std::vector<float> inputThresholdDistribution;
    generateLogDistribution(30, 1, 10*1000, inputThresholdDistribution);
    selectWallTimeInputSweepBenchmark<int,int>(DataFiles::uniformIntDistribution250mValuesMax10000,
                                      {Select::ImplementationValuesAdaptive},
                                      inputThresholdDistribution,
                                      iterations, "7-DOP-1-Values-SelectivitySweepSingle");

    selectWallTimeDopAndInputSweepBenchmarkCalcDopRange<int,int>(DataFiles::uniformIntDistribution250mValuesMax10000,
                                                        Select::ImplementationValuesAdaptiveParallel,
                                                        inputThresholdDistribution,
                                                        iterations, "7-DOP-1-Values-SelectivitySweepParallel");

    selectWallTimeSweepBenchmark<int,int>(DataSweeps::lowerStep50IntDistribution250mValuesSweep,
                                 {Select::ImplementationValuesAdaptive},
                                 50, iterations, "7-DOP-2-Values-StepPeriodSweepSingle");

    selectWallTimeDopSweepBenchmarkCalcDopRange<int,int>(DataSweeps::lowerStep50IntDistribution250mValuesSweep,
                                                Select::ImplementationValuesAdaptiveParallel,
                                                50, iterations, "7-DOP-2-Values-StepPeriodSweepParallel");

    selectWallTimeSweepBenchmark<int,int>(DataSweeps::lowerStep50IntDistribution250mValuesPercentageStepSweep,
                                 {Select::ImplementationValuesAdaptive},
                                 50, iterations, "7-DOP-3-Values-StepPercentageSweepSingle");

    selectWallTimeDopSweepBenchmarkCalcDopRange<int,int>(DataSweeps::lowerStep50IntDistribution250mValuesPercentageStepSweep,
                                                Select::ImplementationValuesAdaptiveParallel,
                                                50, iterations, "7-DOP-3-Values-StepPercentageSweepParallel");
}

void allGroupBySingleThreadedTests(int iterations) {
    groupByCpuCyclesSweepBenchmark<int,int>(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMax,
                                     {GroupBy::Hash, GroupBy::Sort, GroupBy::Adaptive},
                                     iterations, "1-NoClustering-200mValues");

    groupByCpuCyclesSweepBenchmark<int,int>(DataSweeps::logUniformIntDistribution250mValuesClusteredSweepFixedCardinality10mMax250m,
                                    {GroupBy::Hash, GroupBy::Sort, GroupBy::Adaptive},
                                    iterations, "2-ClusteringSweep-250mValues");

    groupByCpuCyclesSweepBenchmark<int,int>(DataSweeps::linearUniformIntDistribution20mValuesCardinalitySections_100_3m_Max20m,
                                     {GroupBy::Hash, GroupBy::Sort, GroupBy::Adaptive},
                                     iterations, "3-TwoSection_100_3m");

    groupByCpuCyclesSweepBenchmark<int,int>(DataSweeps::linearUniformIntDistribution20mValuesCardinalitySections_3m_100_Max20m,
                                   {GroupBy::Hash, GroupBy::Sort, GroupBy::Adaptive},
                                   iterations, "3-TwoSection_3m_100");

    groupByCpuCyclesSweepBenchmark<int,int>(DataSweeps::linearUniformIntDistribution200mValuesMultipleCardinalitySections_100_10m_Max100m,
                                   {GroupBy::Hash, GroupBy::Sort, GroupBy::Adaptive},
                                   iterations, "4-MultipleSection_100_10m");

    groupByCpuCyclesSweepBenchmark<int,int>(DataSweeps::linearUniformIntDistribution200mValuesMultipleCardinalitySections_10m_100_Max100m,
                                   {GroupBy::Hash, GroupBy::Sort, GroupBy::Adaptive},
                                   iterations, "4-MultipleSection_10m_100");
}

void allGroupByParallelTests(int iterations) {
    groupByWallTimeSweepBenchmark<int,int>(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMax,
                                  {GroupBy::Adaptive},
                                  iterations, "5-DOP-1-CardinalitySweepSingle");

    groupByWallTimeDopSweepBenchmarkCalcDopRange<int,int>(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMax,
                                                          iterations, "5-DOP-1-CardinalitySweepParallel");

    groupByWallTimeSweepBenchmark<int,int>(DataSweeps::linearUniformIntDistribution200mValuesMultipleCardinalitySections_100_10m_Max100m,
                                  {GroupBy::Adaptive},
                                  iterations, "5-DOP-2-MultipleCardinalitySectionsFwdSingle");

    groupByWallTimeDopSweepBenchmarkCalcDopRange<int,int>(DataSweeps::linearUniformIntDistribution200mValuesMultipleCardinalitySections_100_10m_Max100m,
                                                          iterations, "5-DOP-2-MultipleCardinalitySectionsFwdParallel");

    groupByWallTimeSweepBenchmark<int,int>(DataSweeps::linearUniformIntDistribution200mValuesMultipleCardinalitySections_10m_100_Max100m,
                                  {GroupBy::Adaptive},
                                  iterations, "5-DOP-2-MultipleCardinalitySectionsBwdSingle");

    groupByWallTimeDopSweepBenchmarkCalcDopRange<int,int>(DataSweeps::linearUniformIntDistribution200mValuesMultipleCardinalitySections_10m_100_Max100m,
                                                          iterations, "5-DOP-2-MultipleCardinalitySectionsBwdParallel");
}

void allSelectAndGroupBySingleThreadedDataSizeTests(int iterations) {
    std::vector<float> inputThresholdDistribution;
    generateLogDistribution(30, 1, 10*1000, inputThresholdDistribution);
    selectCpuCyclesInputSweepBenchmark<int,int>(DataFiles::uniformIntDistribution250mValuesMax10000,
                                                {Select::ImplementationIndexesBranch,
                                                 Select::ImplementationIndexesPredication,
                                                 Select::ImplementationIndexesAdaptive},
                                                inputThresholdDistribution,
                                                iterations, "1-SelectIndexes-32");

    selectCpuCyclesInputSweepBenchmark<int64_t,int>(DataFiles::uniformIntDistribution250mValuesMax10000,
                                                        {Select::ImplementationIndexesBranch,
                                                         Select::ImplementationIndexesPredication,
                                                         Select::ImplementationIndexesAdaptive},
                                                        inputThresholdDistribution,
                                                         iterations, "1-SelectIndexes-64");


    selectCpuCyclesInputSweepBenchmark<int,int>(DataFiles::uniformIntDistribution250mValuesMax10000,
                                                {Select::ImplementationValuesBranch,
                                                 Select::ImplementationValuesVectorized,
                                                 Select::ImplementationValuesPredication,
                                                 Select::ImplementationValuesAdaptive},
                                                inputThresholdDistribution,
                                                iterations, "1-SelectValues-32-32");

    selectCpuCyclesInputSweepBenchmark<int64_t,int>(DataFiles::uniformIntDistribution250mValuesMax10000,
                                                    {Select::ImplementationValuesBranch,
                                                     Select::ImplementationValuesVectorized,
                                                     Select::ImplementationValuesPredication,
                                                     Select::ImplementationValuesAdaptive},
                                                    inputThresholdDistribution,
                                                    iterations, "1-SelectValues-64-32");

    selectCpuCyclesInputSweepBenchmark<int,int64_t>(DataFiles::uniformIntDistribution250mValuesMax10000,
                                                    {Select::ImplementationValuesBranch,
                                                     Select::ImplementationValuesVectorized,
                                                     Select::ImplementationValuesPredication,
                                                     Select::ImplementationValuesAdaptive},
                                                    inputThresholdDistribution,
                                                    iterations, "1-SelectValues-32-64");

    selectCpuCyclesInputSweepBenchmark<int64_t,int64_t>(DataFiles::uniformIntDistribution250mValuesMax10000,
                                                        {Select::ImplementationValuesBranch,
                                                         Select::ImplementationValuesVectorized,
                                                         Select::ImplementationValuesPredication,
                                                         Select::ImplementationValuesAdaptive},
                                                        inputThresholdDistribution,
                                                        iterations, "1-SelectValues-64-64");


    groupByCpuCyclesSweepBenchmark<int,int>(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMax,
                                            {GroupBy::Hash, GroupBy::Sort, GroupBy::Adaptive},
                                            iterations, "1-GroupBy-32-32");

    groupByCpuCyclesSweepBenchmark<int64_t,int>(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMax,
                                                {GroupBy::Hash, GroupBy::Sort, GroupBy::Adaptive},
                                                iterations, "1-GroupBy-64-32");

    groupByCpuCyclesSweepBenchmark<int,int64_t>(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMax,
                                                {GroupBy::Hash, GroupBy::Sort, GroupBy::Adaptive},
                                                iterations, "1-GroupBy-32-64");

    groupByCpuCyclesSweepBenchmark<int64_t,int64_t>(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMax,
                                                    {GroupBy::Hash, GroupBy::Sort, GroupBy::Adaptive},
                                                    iterations, "1-GroupBy-64-64");
}

void allSelectAndGroupByParallelDataSizeTests(int iterations) {
    std::vector<float> inputThresholdDistribution;
    generateLogDistribution(30, 1, 10*1000, inputThresholdDistribution);
    selectWallTimeInputSweepBenchmark<int,int>(DataFiles::uniformIntDistribution250mValuesMax10000,
                                               {Select::ImplementationIndexesAdaptive},
                                               inputThresholdDistribution,
                                               iterations, "7-DOP-1-SelectIndexes-SelectivitySweepSingle-32");

    selectWallTimeDopAndInputSweepBenchmarkCalcDopRange<int,int>(DataFiles::uniformIntDistribution250mValuesMax10000,
                                                                 Select::ImplementationIndexesAdaptiveParallel,
                                                                 inputThresholdDistribution,
                                                                 iterations, "7-DOP-1-SelectIndexes-SelectivitySweepParallel-32");

    selectWallTimeInputSweepBenchmark<int64_t,int>(DataFiles::uniformIntDistribution250mValuesMax10000,
                                                       {Select::ImplementationIndexesAdaptive},
                                                       inputThresholdDistribution,
                                                        iterations, "7-DOP-1-SelectIndexes-SelectivitySweepSingle-64");

    selectWallTimeDopAndInputSweepBenchmarkCalcDopRange<int64_t,int>(DataFiles::uniformIntDistribution250mValuesMax10000,
                                                                         Select::ImplementationIndexesAdaptiveParallel,
                                                                         inputThresholdDistribution,
                                                                         iterations, "7-DOP-1-SelectIndexes-SelectivitySweepParallel-64");


    selectWallTimeInputSweepBenchmark<int,int>(DataFiles::uniformIntDistribution250mValuesMax10000,
                                               {Select::ImplementationValuesAdaptive},
                                               inputThresholdDistribution,
                                               iterations, "7-DOP-1-SelectValues-SelectivitySweepSingle-32-32");

    selectWallTimeDopAndInputSweepBenchmarkCalcDopRange<int,int>(DataFiles::uniformIntDistribution250mValuesMax10000,
                                                                 Select::ImplementationValuesAdaptiveParallel,
                                                                 inputThresholdDistribution,
                                                                 iterations, "7-DOP-1-SelectValues-SelectivitySweepParallel-32-32");

    selectWallTimeInputSweepBenchmark<int64_t,int>(DataFiles::uniformIntDistribution250mValuesMax10000,
                                                   {Select::ImplementationValuesAdaptive},
                                                   inputThresholdDistribution,
                                                   iterations, "7-DOP-1-SelectValues-SelectivitySweepSingle-64-32");

    selectWallTimeDopAndInputSweepBenchmarkCalcDopRange<int64_t,int>(DataFiles::uniformIntDistribution250mValuesMax10000,
                                                                     Select::ImplementationValuesAdaptiveParallel,
                                                                     inputThresholdDistribution,
                                                                     iterations, "7-DOP-1-SelectValues-SelectivitySweepParallel-64-32");

    selectWallTimeInputSweepBenchmark<int,int64_t>(DataFiles::uniformIntDistribution250mValuesMax10000,
                                                   {Select::ImplementationValuesAdaptive},
                                                   inputThresholdDistribution,
                                                   iterations, "7-DOP-1-SelectValues-SelectivitySweepSingle-32-64");

    selectWallTimeDopAndInputSweepBenchmarkCalcDopRange<int,int64_t>(DataFiles::uniformIntDistribution250mValuesMax10000,
                                                                     Select::ImplementationValuesAdaptiveParallel,
                                                                     inputThresholdDistribution,
                                                                     iterations, "7-DOP-1-SelectValues-SelectivitySweepParallel-32-64");

    selectWallTimeInputSweepBenchmark<int64_t,int64_t>(DataFiles::uniformIntDistribution250mValuesMax10000,
                                                       {Select::ImplementationValuesAdaptive},
                                                       inputThresholdDistribution,
                                                       iterations, "7-DOP-1-SelectValues-SelectivitySweepSingle-64-64");

    selectWallTimeDopAndInputSweepBenchmarkCalcDopRange<int64_t,int64_t>(DataFiles::uniformIntDistribution250mValuesMax10000,
                                                                         Select::ImplementationValuesAdaptiveParallel,
                                                                         inputThresholdDistribution,
                                                                         iterations, "7-DOP-1-SelectValues-SelectivitySweepParallel-64-64");


    groupByWallTimeSweepBenchmark<int,int>(DataSweeps::logUniformIntDistribution200mValuesCardinalityUpTo10mSweepFixedMax,
                                           {GroupBy::Adaptive},
                                           iterations, "5-DOP-1-GroupBy-CardinalitySweepSingle-32-32");

    groupByWallTimeDopSweepBenchmarkCalcDopRange<int,int>(DataSweeps::logUniformIntDistribution200mValuesCardinalityUpTo10mSweepFixedMax,
                                                          iterations, "5-DOP-1-GroupBy-CardinalitySweepParallel-32-32");

    groupByWallTimeSweepBenchmark<int64_t,int>(DataSweeps::logUniformIntDistribution200mValuesCardinalityUpTo10mSweepFixedMax,
                                               {GroupBy::Adaptive},
                                               iterations, "5-DOP-1-GroupBy-CardinalitySweepSingle-64-32");

    groupByWallTimeDopSweepBenchmarkCalcDopRange<int64_t,int>(DataSweeps::logUniformIntDistribution200mValuesCardinalityUpTo10mSweepFixedMax,
                                                              iterations, "5-DOP-1-GroupBy-CardinalitySweepParallel-64-32");

    groupByWallTimeSweepBenchmark<int,int64_t>(DataSweeps::logUniformIntDistribution200mValuesCardinalityUpTo10mSweepFixedMax,
                                               {GroupBy::Adaptive},
                                               iterations, "5-DOP-1-GroupBy-CardinalitySweepSingle-32-64");

    groupByWallTimeDopSweepBenchmarkCalcDopRange<int,int64_t>(DataSweeps::logUniformIntDistribution200mValuesCardinalityUpTo10mSweepFixedMax,
                                                              iterations, "5-DOP-1-GroupBy-CardinalitySweepParallel-32-64");

    groupByWallTimeSweepBenchmark<int64_t,int64_t>(DataSweeps::logUniformIntDistribution200mValuesCardinalityUpTo10mSweepFixedMax,
                                                   {GroupBy::Adaptive},
                                                   iterations, "5-DOP-1-GroupBy-CardinalitySweepSingle-64-64");

    groupByWallTimeDopSweepBenchmarkCalcDopRange<int64_t,int64_t>(DataSweeps::logUniformIntDistribution200mValuesCardinalityUpTo10mSweepFixedMax,
                                                                  iterations, "5-DOP-1-GroupBy-CardinalitySweepParallel-64-64");
}

void allPartitionTests(int iterations) {
    std::string startMachineConstantName = "Partition_startRadixBits";
    std::string minMachineConstantName = "Partition_minRadixBits";

    int startMachineConstant = static_cast<int>(HAQP::MachineConstants::getInstance().
            getMachineConstant(startMachineConstantName));
    int minMachineConstant = static_cast<int>(HAQP::MachineConstants::getInstance().
            getMachineConstant(minMachineConstantName));

    std::string nameOne = "Int64_ClusterednessSweep_" + std::to_string(startMachineConstant);
    std::string nameTwo = "Int64_ClusterednessSweep_" + std::to_string(minMachineConstant);

    partitionSweepBenchmark<uint64_t>(DataSweeps::logUniformIntDistribution250mValuesClusteredSweepFixedCardinality10mMax250m,
                                      {PartitionOperators::RadixBitsFixed, PartitionOperators::RadixBitsAdaptive},
                                      startMachineConstant, nameOne, iterations);
    partitionSweepBenchmark<uint64_t>(DataSweeps::logUniformIntDistribution250mValuesClusteredSweepFixedCardinality10mMax250m,
                                      {PartitionOperators::RadixBitsFixed},
                                      minMachineConstant, nameTwo, iterations);

    for (int radixBits = minMachineConstant + 1; radixBits < startMachineConstant; radixBits++) {
        std::string name = "Int64_ClusterednessSweep_" + std::to_string(radixBits);
        partitionSweepBenchmark<uint64_t>(DataSweeps::logUniformIntDistribution250mValuesClusteredSweepFixedCardinality10mMax250m,
                                          {PartitionOperators::RadixBitsFixed},
                                          radixBits, name, iterations);
    }

    partitionSweepBenchmark<uint64_t>(DataSweeps::logUniformIntDistribution20mValuesClusteredSweepFixedCardinality1m,
                                      {PartitionOperators::RadixBitsFixed, PartitionOperators::RadixBitsAdaptive},
                                      startMachineConstant, nameOne, iterations);
    partitionSweepBenchmark<uint64_t>(DataSweeps::logUniformIntDistribution20mValuesClusteredSweepFixedCardinality1m,
                                      {PartitionOperators::RadixBitsFixed},
                                      minMachineConstant, nameTwo, iterations);

    for (int radixBits = minMachineConstant + 1; radixBits < startMachineConstant; radixBits++) {
        std::string name = "Int64_ClusterednessSweep_" + std::to_string(radixBits);
        partitionSweepBenchmark<uint64_t>(DataSweeps::logUniformIntDistribution20mValuesClusteredSweepFixedCardinality1m,
                                          {PartitionOperators::RadixBitsFixed},
                                          radixBits, name, iterations);
    }
}

void imdbSelectIndexesSweepMacroBenchmark(int startYear, int endYear, int iterations,
                                          const std::vector<Select> &selectImplementations) {
    int numImplementations = static_cast<int>(selectImplementations.size());

    std::string filePath = FilePaths::getInstance().getImdbInputFolderPath() + "title.basics.tsv";
    int n = getLengthOfFile(filePath);
    auto data = new int[n];
    readImdbStartYearColumnFromBasicsTable(filePath, data);

    long_long cycles;
    std::vector<std::vector<long_long>> results(endYear - startYear + 1,
                                                std::vector<long_long>((numImplementations * iterations) + 1, 0));

    for (auto year = startYear; year <= endYear; ++year) {
        results[year - startYear][0] = year;

        for (auto i = 0; i < iterations; ++i) {
            for (auto j = 0; j < numImplementations; ++j) {
                auto inputFilter = new int[n];
                copyArray<int>(data, inputFilter, n);
                auto selectedIndexes = new int[n];

                std::cout << "Running year " << year << ", iteration " << i + 1 << "... ";

                cycles = *Counters::getInstance().readSharedEventSet();

                HAQP::runSelectFunction(selectImplementations[j],
                                         n, inputFilter, inputFilter, selectedIndexes,year);

                results[year - startYear][1 + (i * numImplementations) + j] = *Counters::getInstance().readSharedEventSet() - cycles;

                delete[] inputFilter;
                delete[] selectedIndexes;

                std::cout << "Completed" << std::endl;
            }
        }
    }

    std::vector<std::string> headers((numImplementations * iterations) + 1);
    headers [0] = "Year";
    for (auto i = 0; i < (numImplementations * iterations); ++i) {
        headers[1 + i] = getSelectName(selectImplementations[i % numImplementations]);
    }

    std::string fileName = "IMDB_selectIndexes_year_sweep";
    std::string fullFilePath = FilePaths::getInstance().getImdbOutputFolderPath() + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);
}

void imdbGroupByMacroBenchmark_titleIdFromPrincipalsTable_clusteringSweep(int iterations, int numRuns) {
    std::string filePath = FilePaths::getInstance().getImdbInputFolderPath() + "title.principals.tsv";
    int n = getLengthOfFile(filePath);
    using T = uint32_t;

    auto data = new T[n];
    readImdbTitleIdColumnFromPrincipalsTable(filePath, data);

    int cardinality = 9135620;
    long_long cycles;
    std::vector<std::vector<long_long>> results(numRuns, std::vector<long_long>(1 + (3 * iterations), 0));

    std::vector<float> spreadInCluster;
    generateLogDistribution(numRuns, 1, n, spreadInCluster);

    for (int numRun = 0; numRun < numRuns; numRun++) {
        results[numRun][0] = static_cast<int>(spreadInCluster[numRun]);

        auto clusteredData = new T[n];
        copyArray(data, clusteredData, n);
        runClusteringOnData(clusteredData, n, static_cast<int>(spreadInCluster[numRun]));

        for (int iteration = 0; iteration < iterations; iteration++) {

            for (int j = 0; j < 3; j++) {

                auto inputGroupBy = new T[n];
                auto inputAggregate = new T[n];
                copyArray(clusteredData, inputGroupBy, n);
                copyArray(clusteredData, inputAggregate, n);

                std::cout << "Running input " << static_cast<int>(spreadInCluster[numRun]) << ", iteration ";
                std::cout << iteration + 1 << "... ";

                if (j == 0) {
                    cycles = *Counters::getInstance().readSharedEventSet();
                    HAQP::GroupByAdaptive<CountAggregation,T,T> groupByOperator(n, inputGroupBy, inputAggregate, cardinality);
                    auto result = groupByOperator.processInput();
                    cycles = *Counters::getInstance().readSharedEventSet() - cycles;
                } else if (j == 1) {
                    cycles = *Counters::getInstance().readSharedEventSet();
                    HAQP::GroupByHash<CountAggregation,T,T> groupByOperator(inputGroupBy, inputAggregate, cardinality);
                    groupByOperator.processMicroBatch(n);
                    auto result = groupByOperator.getOutput();
                    cycles = *Counters::getInstance().readSharedEventSet() - cycles;
                } else if (j == 2) {
                    cycles = *Counters::getInstance().readSharedEventSet();
                    HAQP::GroupBySort<CountAggregation,T,T> groupByOperator(inputGroupBy, inputAggregate);
                    groupByOperator.processMicroBatch(n);
                    auto result = groupByOperator.getOutput();
                    cycles = *Counters::getInstance().readSharedEventSet() - cycles;
                }

                results[numRun][1 + (iteration * 3) + j] = cycles;

                delete[] inputGroupBy;
                delete[] inputAggregate;
                std::cout << "Completed" << std::endl;
            }
        }

        delete[] clusteredData;
    }

    std::vector<std::string> headers(1 + (3 * iterations));
    headers[0] = "SpreadInCluster";
    std::string functionNames[] = {"GroupBy_Adaptive", "GroupBy_Hash", "GroupBy_Sort"};
    for (auto i = 0; i < (3 * iterations); ++i) {
        headers[1 + i] = functionNames[i % 3];
    }

    std::string fileName = "IMDB_groupBy_titleIdColumn_PrincipalsTable";
    std::string fullFilePath = FilePaths::getInstance().getImdbOutputFolderPath() + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);

    delete[] data;
}

void imdbPartitionMacroBenchmark_titleIdColumnBasicsTable(int iterations) {
    std::string filePath = FilePaths::getInstance().getImdbInputFolderPath() + "title.basics.tsv";
    std::string minRadixBits = "Partition_minRadixBits";
    std::string startRadixBits = "Partition_startRadixBits";
    int n = getLengthOfFile(filePath);
    using T = uint32_t;
    auto data = new T[n];
    readImdbTitleIdColumnFromBasicsTable(filePath, data);

    std::cout << n << std::endl;

    long_long cycles;
    std::vector<std::vector<long_long>> results(1, std::vector<long_long>((3 * iterations), 0));

    for (int i = 0; i < iterations; i++) {

        for (int j = 0; j < 3; j++) {

            auto keys = new T[n];
            copyArray(data, keys, n);

            std::cout << "Running iteration " << i + 1 << "... ";

            if (j == 0) {
                cycles = *Counters::getInstance().readSharedEventSet();
                HAQP::PartitionAdaptive<T> partitionOperator(n, keys);
                auto partitions =  partitionOperator.processInput();
                cycles = *Counters::getInstance().readSharedEventSet() - cycles;
            } else if (j == 1) {
                cycles = *Counters::getInstance().readSharedEventSet();
                HAQP::Partition<T> partitionOperator(n, keys,
                    static_cast<int>(HAQP::MachineConstants::getInstance().getMachineConstant(minRadixBits)));
                partitionOperator.processInput();
                auto partitions = partitionOperator.getOutput();
                cycles = *Counters::getInstance().readSharedEventSet() - cycles;
            } else if (j == 2) {
                cycles = *Counters::getInstance().readSharedEventSet();
                HAQP::Partition<T> partitionOperator(n, keys,
                    static_cast<int>(HAQP::MachineConstants::getInstance().getMachineConstant(startRadixBits)));
                partitionOperator.processInput();
                auto partitions = partitionOperator.getOutput();
                cycles = *Counters::getInstance().readSharedEventSet() - cycles;
            }

            results[0][(i * 3) + j] = cycles;

            delete[] keys;
            std::cout << "Completed" << std::endl;
        }
    }

    std::vector<std::string> headers(3 * iterations);
    std::string functionNames[] = {"RadixPartition_Adaptive", "RadixPartition_Static_", "RadixPartition_Static_"};
    functionNames[1].append(std::to_string(static_cast<int>(HAQP::MachineConstants::getInstance().getMachineConstant(minRadixBits))));
    functionNames[1].append("bits");
    functionNames[2].append(std::to_string(static_cast<int>(HAQP::MachineConstants::getInstance().getMachineConstant(startRadixBits))));
    functionNames[2].append("bits");
    for (auto i = 0; i < (3 * iterations); ++i) {
        headers[i] = functionNames[i % 3];
    }

    std::string fileName = "IMDB_partition_titleIdColumn_BasicsTable";
    std::string fullFilePath = FilePaths::getInstance().getImdbOutputFolderPath() + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);

    delete[] data;
}

void imdbPartitionMacroBenchmark_startYearColumnBasicsTable(int iterations) {
    std::string filePath = FilePaths::getInstance().getImdbInputFolderPath() + "title.basics.tsv";
    std::string minRadixBits = "Partition_minRadixBits";
    std::string startRadixBits = "Partition_startRadixBits";
    int n = getLengthOfFile(filePath);
    using T = uint32_t;
    auto data = new T[n];
    readImdbStartYearColumnFromBasicsTable(filePath, data);

    std::cout << n << std::endl;

    long_long cycles;
    std::vector<std::vector<long_long>> results(1, std::vector<long_long>((3 * iterations), 0));

    for (int i = 0; i < iterations; i++) {

        for (int j = 0; j < 3; j++) {

            auto keys = new T[n];
            copyArray(data, keys, n);

            std::cout << "Running iteration " << i + 1 << "... ";

            if (j == 0) {
                cycles = *Counters::getInstance().readSharedEventSet();
                HAQP::PartitionAdaptive<T> partitionOperator(n, keys);
                auto partitions =  partitionOperator.processInput();
                cycles = *Counters::getInstance().readSharedEventSet() - cycles;
            } else if (j == 1) {
                cycles = *Counters::getInstance().readSharedEventSet();
                HAQP::Partition<T> partitionOperator(n, keys,
                    static_cast<int>(HAQP::MachineConstants::getInstance().getMachineConstant(minRadixBits)));
                partitionOperator.processInput();
                auto partitions = partitionOperator.getOutput();
                cycles = *Counters::getInstance().readSharedEventSet() - cycles;
            } else if (j == 2) {
                cycles = *Counters::getInstance().readSharedEventSet();
                HAQP::Partition<T> partitionOperator(n, keys,
                    static_cast<int>(HAQP::MachineConstants::getInstance().getMachineConstant(startRadixBits)));
                partitionOperator.processInput();
                auto partitions = partitionOperator.getOutput();
                cycles = *Counters::getInstance().readSharedEventSet() - cycles;
            }

            results[0][(i * 3) + j] = cycles;

            delete[] keys;
            std::cout << "Completed" << std::endl;
        }
    }

    std::vector<std::string> headers(3 * iterations);
    std::string functionNames[] = {"RadixPartition_Adaptive", "RadixPartition_Static_", "RadixPartition_Static_"};
    functionNames[1].append(std::to_string(static_cast<int>(HAQP::MachineConstants::getInstance().getMachineConstant(minRadixBits))));
    functionNames[1].append("bits");
    functionNames[2].append(std::to_string(static_cast<int>(HAQP::MachineConstants::getInstance().getMachineConstant(startRadixBits))));
    functionNames[2].append("bits");
    for (auto i = 0; i < (3 * iterations); ++i) {
        headers[i] = functionNames[i % 3];
    }

    std::string fileName = "IMDB_partition_startYearColumn_BasicsTable";
    std::string fullFilePath = FilePaths::getInstance().getImdbOutputFolderPath() + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);

    delete[] data;
}

void imdbPartitionMacroBenchmark_personIdColumnPrincipalsTable(int iterations) {
    std::string filePath = FilePaths::getInstance().getImdbInputFolderPath() + "title.principals.tsv";
    std::string minRadixBits = "Partition_minRadixBits";
    std::string startRadixBits = "Partition_startRadixBits";
    int n = getLengthOfFile(filePath);
    using T = uint32_t;
    auto data = new T[n];
    readImdbPersonIdColumnFromPrincipalsTable(filePath, data);

    std::cout << n << std::endl;

    long_long cycles;
    std::vector<std::vector<long_long>> results(1, std::vector<long_long>((3 * iterations), 0));

    for (int i = 0; i < iterations; i++) {

        for (int j = 0; j < 3; j++) {

            auto keys = new T[n];
            copyArray(data, keys, n);

            std::cout << "Running iteration " << i + 1 << "... ";

            if (j == 0) {
                cycles = *Counters::getInstance().readSharedEventSet();
                HAQP::PartitionAdaptive<T> partitionOperator(n, keys);
                auto partitions =  partitionOperator.processInput();
                cycles = *Counters::getInstance().readSharedEventSet() - cycles;
            } else if (j == 1) {
                cycles = *Counters::getInstance().readSharedEventSet();
                HAQP::Partition<T> partitionOperator(n, keys,
                    static_cast<int>(HAQP::MachineConstants::getInstance().getMachineConstant(minRadixBits)));
                partitionOperator.processInput();
                auto partitions = partitionOperator.getOutput();
                cycles = *Counters::getInstance().readSharedEventSet() - cycles;
            } else if (j == 2) {
                cycles = *Counters::getInstance().readSharedEventSet();
                HAQP::Partition<T> partitionOperator(n, keys,
                    static_cast<int>(HAQP::MachineConstants::getInstance().getMachineConstant(startRadixBits)));
                partitionOperator.processInput();
                auto partitions = partitionOperator.getOutput();
                cycles = *Counters::getInstance().readSharedEventSet() - cycles;
            }

            results[0][(i * 3) + j] = cycles;

            delete[] keys;
            std::cout << "Completed" << std::endl;
        }
    }

    std::vector<std::string> headers(3 * iterations);
    std::string functionNames[] = {"RadixPartition_Adaptive", "RadixPartition_Static_", "RadixPartition_Static_"};
    functionNames[1].append(std::to_string(static_cast<int>(HAQP::MachineConstants::getInstance().getMachineConstant(minRadixBits))));
    functionNames[1].append("bits");
    functionNames[2].append(std::to_string(static_cast<int>(HAQP::MachineConstants::getInstance().getMachineConstant(startRadixBits))));
    functionNames[2].append("bits");
    for (auto i = 0; i < (3 * iterations); ++i) {
        headers[i] = functionNames[i % 3];
    }

    std::string fileName = "IMDB_partition_personIdColumn_PrincipalsTable";
    std::string fullFilePath = FilePaths::getInstance().getImdbOutputFolderPath() + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);

    delete[] data;
}

void allImdbMacroBenchmarks(int iterations) {
    imdbSelectIndexesSweepMacroBenchmark(1874, 2023, iterations,
                                         {Select::ImplementationIndexesBranch,
                                          Select::ImplementationIndexesPredication,
                                          Select::ImplementationIndexesAdaptive});

    imdbGroupByMacroBenchmark_titleIdFromPrincipalsTable_clusteringSweep(iterations, 30);

    imdbPartitionMacroBenchmark_titleIdColumnBasicsTable(iterations);
    imdbPartitionMacroBenchmark_startYearColumnBasicsTable(iterations);
    imdbPartitionMacroBenchmark_personIdColumnPrincipalsTable(iterations);
}

int main() {

    return 0;
}