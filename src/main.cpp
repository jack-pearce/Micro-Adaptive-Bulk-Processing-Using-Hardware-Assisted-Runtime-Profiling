#include <vector>

#include "main.h"
#include "cycles_benchmarking/groupByCyclesBenchmark.h"
#include "data_generation/dataFiles.h"
#include "utilities/dataHelpers.h"
#include "library/mabpl.h"

using MABPL::Select;
using MABPL::GroupBy;
using MABPL::Partition;

using MABPL::MinAggregation;
using MABPL::MaxAggregation;
using MABPL::SumAggregation;
using MABPL::CountAggregation;


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

    // Graph 2.5: Selectivity on fully sorted data
    std::vector<float> inputThresholdDistribution2;
    generateLogDistribution(30, 1, 100, inputThresholdDistribution2);
    selectCpuCyclesInputSweepBenchmark<int,int>(DataFiles::fullySortedIntDistribution250mValues,
                                       {Select::ImplementationIndexesBranch,
                                        Select::ImplementationIndexesPredication,
                                        Select::ImplementationIndexesAdaptive},
                                       inputThresholdDistribution,
                                       iterations, "2-5-Indexes");

    // Graph 3: Period range on linearly varying selectivity
/*    selectCpuCyclesSweepBenchmark<int,int>(DataSweeps::varyingIntDistribution250mValuesSweep,
                                  {Select::ImplementationIndexesBranch,
                                   Select::ImplementationIndexesPredication,
                                   Select::ImplementationIndexesAdaptive},
                                   50, iterations, "Indexes");*/

    // Graph 4: Period range on step varying selectivity
    selectCpuCyclesSweepBenchmark<int,int>(DataSweeps::lowerStep50IntDistribution250mValuesSweepSectionLength_1,
                                           {Select::ImplementationIndexesBranch,
                                            Select::ImplementationIndexesPredication,
                                            Select::ImplementationIndexesAdaptive},
                                           50, 5, "4_1-Indexes");

    selectCpuCyclesSweepBenchmark<int,int>(DataSweeps::lowerStep50IntDistribution250mValuesSweepSectionLength_2,
                                           {Select::ImplementationIndexesBranch,
                                            Select::ImplementationIndexesPredication,
                                            Select::ImplementationIndexesAdaptive},
                                           50, 5, "4_2-Indexes");

    // Graph 5: Best case - tuned unequal step varying selectivity
    selectCpuCyclesSweepBenchmark<int,int>(DataSweeps::lowerStep50IntDistribution250mValuesPercentageStepSweep,
                                  {Select::ImplementationIndexesBranch,
                                   Select::ImplementationIndexesPredication,
                                   Select::ImplementationIndexesAdaptive},
                                  50, iterations, "5-Indexes");
/*    selectCpuCyclesSingleInputBenchmark(DataFiles::bestCaseIndexesTunedUnequalLowerStep50IntDistribution250mValues,
                                        {Select::ImplementationIndexesBranch,
                                         Select::ImplementationIndexesPredication,
                                         Select::ImplementationIndexesAdaptive},
                                        50,
                                        iterations, "Indexes");*/

    // Graph 6: Worst case - tuned period range on step varying selectivity
    selectCpuCyclesSingleInputBenchmark<int,int>(DataFiles::worstCaseIndexesTunedUpperStep50IntDistribution250mValues,
                                        {Select::ImplementationIndexesBranch,
                                         Select::ImplementationIndexesPredication,
                                         Select::ImplementationIndexesAdaptive},
                                        50,
                                        iterations, "6-Indexes");
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

    // Graph 2.5: Selectivity on fully sorted data
    std::vector<float> inputThresholdDistribution2;
    generateLogDistribution(30, 1, 100, inputThresholdDistribution2);
    selectCpuCyclesInputSweepBenchmark<int,int>(DataFiles::fullySortedIntDistribution250mValues,
                                       {Select::ImplementationValuesBranch,
                                        Select::ImplementationValuesVectorized,
                                        Select::ImplementationValuesAdaptive},
                                       inputThresholdDistribution,
                                       iterations, "2-5-Values");


    // Graph 3: Period range on linearly varying selectivity
/*    selectCpuCyclesSweepBenchmark<int,int>(DataSweeps::varyingIntDistribution250mValuesSweep,
                                  {Select::ImplementationValuesBranch,
                                   Select::ImplementationValuesVectorized,
                                   Select::ImplementationValuesAdaptive},
                                   50, iterations, "Values");*/

    // Graph 4: Period range on step varying selectivity
    selectCpuCyclesSweepBenchmark<int,int>(DataSweeps::lowerStep50IntDistribution250mValuesSweep,
                                  {Select::ImplementationValuesBranch,
                                   Select::ImplementationValuesVectorized,
                                   Select::ImplementationValuesAdaptive},
                                   50, iterations, "4-Values");

    // Graph 5: Best case - tuned unequal step varying selectivity
    selectCpuCyclesSweepBenchmark<int,int>(DataSweeps::lowerStep50IntDistribution250mValuesPercentageStepSweep,
                                  {Select::ImplementationValuesBranch,
                                   Select::ImplementationValuesVectorized,
                                   Select::ImplementationValuesAdaptive},
                                  50, iterations, "5-Values");
/*    selectCpuCyclesSingleInputBenchmark(DataFiles::bestCaseValuesTunedUnequalLowerStep50IntDistribution250mValues,
                                        {Select::ImplementationValuesBranch,
                                         Select::ImplementationValuesVectorized,
                                         Select::ImplementationValuesAdaptive},
                                        50,
                                        iterations, "Values");*/

    // Graph 6: Worst case - tuned period range on step varying selectivity
    selectCpuCyclesSingleInputBenchmark<int,int>(DataFiles::worstCaseValuesTunedLowerStep50IntDistribution250mValues,
                                        {Select::ImplementationValuesBranch,
                                         Select::ImplementationValuesVectorized,
                                         Select::ImplementationValuesAdaptive},
                                        50,
                                        iterations, "6-Values");

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
    groupByCpuCyclesSweepBenchmark<int,int>(DataSweeps::logUniformIntDistribution20mValuesCardinalitySweepFixedMax,
                                   {GroupBy::Hash, GroupBy::Sort, GroupBy::Adaptive},
                                   iterations, "1-NoClustering");

    groupByCpuCyclesSweepBenchmark<int,int>(DataSweeps::logUniformIntDistribution20mValuesCardinalitySweepFixedMaxClustered10,
                                   {GroupBy::Hash, GroupBy::Sort, GroupBy::Adaptive},
                                   iterations, "1-Clustered10");

    groupByCpuCyclesSweepBenchmark<int,int>(DataSweeps::logUniformIntDistribution20mValuesCardinalitySweepFixedMaxClustered1k,
                                   {GroupBy::Hash, GroupBy::Sort, GroupBy::Adaptive},
                                   iterations, "1-Clustered1k");

    groupByCpuCyclesSweepBenchmark<int,int>(DataSweeps::logUniformIntDistribution20mValuesCardinalitySweepFixedMaxClustered100k,
                                   {GroupBy::Hash, GroupBy::Sort, GroupBy::Adaptive},
                                   iterations, "1-Clustered100k");

    groupByCpuCyclesSweepBenchmark<int,int>(DataSweeps::logUniformIntDistribution20mValuesCardinalitySweepVariableMax,
                                   {GroupBy::Hash, GroupBy::Sort, GroupBy::Adaptive},
                                   iterations, "1-NoClustering-VariableUpperBound");

    groupByCpuCyclesSweepBenchmark<int64_t,int64_t>(DataSweeps::logUniformIntDistribution20mValuesCardinalitySweepFixedMax,
                                            {GroupBy::Hash, GroupBy::Sort, GroupBy::Adaptive},
                                            iterations, "1-NoClustering-64bitInts");

    groupByCpuCyclesSweepBenchmark<int,int>(DataSweeps::logUniformIntDistribution40mValuesCardinalitySweepFixedMax,
                                     {GroupBy::Hash, GroupBy::Sort, GroupBy::Adaptive},
                                     iterations, "1-NoClustering-40mValues");

    groupByCpuCyclesSweepBenchmark<int,int>(DataSweeps::logUniformIntDistribution200mValuesCardinalitySweepFixedMax,
                                     {GroupBy::Hash, GroupBy::Sort, GroupBy::Adaptive},
                                     iterations, "1-NoClustering-200mValues");

    groupByCpuCyclesSweepBenchmark<int,int>(DataSweeps::linearUniformIntDistribution20mValuesCardinalitySections_100_3m_Max20m,
                                     {GroupBy::Hash, GroupBy::Sort, GroupBy::Adaptive},
                                     iterations, "2-TwoSection_100_3m");

    groupByCpuCyclesSweepBenchmark<int,int>(DataSweeps::linearUniformIntDistribution20mValuesCardinalitySections_3m_100_Max20m,
                                   {GroupBy::Hash, GroupBy::Sort, GroupBy::Adaptive},
                                   iterations, "2-TwoSection_3m_100");

    groupByCpuCyclesSweepBenchmark<int,int>(DataSweeps::linearUniformIntDistribution200mValuesMultipleCardinalitySections_100_10m_Max100m,
                                   {GroupBy::Hash, GroupBy::Sort, GroupBy::Adaptive},
                                   iterations, "3-MultipleSection_100_10m");

    groupByCpuCyclesSweepBenchmark<int,int>(DataSweeps::linearUniformIntDistribution200mValuesMultipleCardinalitySections_10m_100_Max100m,
                                   {GroupBy::Hash, GroupBy::Sort, GroupBy::Adaptive},
                                   iterations, "3-MultipleSection_10m_100");

    tessilRobinMapInitialisationBenchmark<int,int>("4-MapOverheadCosts");
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

void allSingleThreadedDataSizeTests(int iterations) {
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

void allParallelDataSizeTests(int iterations) {
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

void allPartitionTests() {

}

void runImdbSelectSweepMacroBenchmark(int startYear, int endYear, int iterations,
                                      const std::vector<Select> &selectImplementations) {
    int numImplementations = static_cast<int>(selectImplementations.size());

    std::string filePath = FilePaths::getInstance().getImdbInputFolderPath() + "title.basics.tsv";
    int n = getLengthOfTsv(filePath);
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

                MABPL::runSelectFunction(selectImplementations[j],
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

    std::string fileName = "IMDB_select_year_sweep";
    std::string fullFilePath = FilePaths::getInstance().getImdbOutputFolderPath() + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);
}

void runImdbPartitionMacroBenchmark_titleIdColumnBasicsTable(int iterations) {
    std::string filePath = FilePaths::getInstance().getImdbInputFolderPath() + "title.basics.tsv";
    std::string machineConstantName = "Partition_minRadixBits";
    int n = getLengthOfTsv(filePath);
    auto data = new uint32_t [n];
    readImdbTitleIdColumnFromBasicsTable(filePath, data);

    long_long cycles;
    std::vector<std::vector<long_long>> results(1, std::vector<long_long>((3 * iterations), 0));

    for (int i = 0; i < iterations; i++) {

        for (int j = 0; j < 3; j++) {

            auto keys = new uint32_t[n];
            copyArray(data, keys, n);

            std::cout << "Running iteration " << i + 1 << "... ";

            if (j == 0) {
                cycles = *Counters::getInstance().readSharedEventSet();
                auto partitions = MABPL::partitionAdaptive(n, keys);
                cycles = *Counters::getInstance().readSharedEventSet() - cycles;
            } else if (j == 1) {
                cycles = *Counters::getInstance().readSharedEventSet();
                auto partitions = MABPL::partitionFixed(n, keys,
                                                        static_cast<int>(MABPL::MachineConstants::getInstance().getMachineConstant(
                                                                machineConstantName)));
                cycles = *Counters::getInstance().readSharedEventSet() - cycles;
            } else if (j == 2) {
                cycles = *Counters::getInstance().readSharedEventSet();
                auto partitions = MABPL::partitionFixed(n, keys, 16);
                cycles = *Counters::getInstance().readSharedEventSet() - cycles;
            }

            results[0][(i * 3) + j] = cycles;

            delete[] keys;
            std::cout << "Completed" << std::endl;
        }
    }

    std::vector<std::string> headers(3 * iterations);
    std::string functionNames[] = {"RadixPartition_Adaptive", "RadixPartition_Static_", "RadixPartition_Static_16bits"};
    functionNames[1].append(std::to_string(static_cast<int>(MABPL::MachineConstants::getInstance().getMachineConstant(machineConstantName))));
    functionNames[1].append("bits");
    for (auto i = 0; i < (3 * iterations); ++i) {
        headers[i] = functionNames[i % 3];
    }

    std::string fileName = "IMDB_partition_titleIdColumn_BasicsTable";
    std::string fullFilePath = FilePaths::getInstance().getImdbOutputFolderPath() + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);

    delete[] data;
}

void runImdbPartitionMacroBenchmark_startYearColumnBasicsTable(int iterations) {
    std::string filePath = FilePaths::getInstance().getImdbInputFolderPath() + "title.basics.tsv";
    std::string machineConstantName = "Partition_minRadixBits";
    int n = getLengthOfTsv(filePath);
    auto data = new uint32_t [n];
    readImdbStartYearColumnFromBasicsTable(filePath, data);

    long_long cycles;
    std::vector<std::vector<long_long>> results(1, std::vector<long_long>((3 * iterations), 0));

    for (int i = 0; i < iterations; i++) {

        for (int j = 0; j < 3; j++) {

            auto keys = new uint32_t[n];
            copyArray(data, keys, n);

            std::cout << "Running iteration " << i + 1 << "... ";

            if (j == 0) {
                cycles = *Counters::getInstance().readSharedEventSet();
                auto partitions = MABPL::partitionAdaptive(n, keys);
                cycles = *Counters::getInstance().readSharedEventSet() - cycles;
            } else if (j == 1) {
                cycles = *Counters::getInstance().readSharedEventSet();
                auto partitions = MABPL::partitionFixed(n, keys,
                                                        static_cast<int>(MABPL::MachineConstants::getInstance().getMachineConstant(
                                                                machineConstantName)));
                cycles = *Counters::getInstance().readSharedEventSet() - cycles;
            } else if (j == 2) {
                cycles = *Counters::getInstance().readSharedEventSet();
                auto partitions = MABPL::partitionFixed(n, keys, 16);
                cycles = *Counters::getInstance().readSharedEventSet() - cycles;
            }

            results[0][(i * 3) + j] = cycles;

            delete[] keys;
            std::cout << "Completed" << std::endl;
        }
    }

    std::vector<std::string> headers(3 * iterations);
    std::string functionNames[] = {"RadixPartition_Adaptive", "RadixPartition_Static_", "RadixPartition_Static_16bits"};
    functionNames[1].append(std::to_string(static_cast<int>(MABPL::MachineConstants::getInstance().getMachineConstant(machineConstantName))));
    functionNames[1].append("bits");
    for (auto i = 0; i < (3 * iterations); ++i) {
        headers[i] = functionNames[i % 3];
    }

    std::string fileName = "IMDB_partition_startYearColumn_BasicsTable";
    std::string fullFilePath = FilePaths::getInstance().getImdbOutputFolderPath() + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);

    delete[] data;
}

void runImdbPartitionMacroBenchmark_personIdColumnPrincipalsTable(int iterations) {
    std::string filePath = FilePaths::getInstance().getImdbInputFolderPath() + "title.principals.tsv";
    std::string machineConstantName = "Partition_minRadixBits";
    int n = getLengthOfTsv(filePath);
    auto data = new uint32_t [n];
    readImdbPersonIdColumnFromPrincipalsTable(filePath, data);

    long_long cycles;
    std::vector<std::vector<long_long>> results(1, std::vector<long_long>((3 * iterations), 0));

    for (int i = 0; i < iterations; i++) {

        for (int j = 0; j < 3; j++) {

            auto keys = new uint32_t[n];
            copyArray(data, keys, n);

            std::cout << "Running iteration " << i + 1 << "... ";

            if (j == 0) {
                cycles = *Counters::getInstance().readSharedEventSet();
                auto partitions = MABPL::partitionAdaptive(n, keys);
                cycles = *Counters::getInstance().readSharedEventSet() - cycles;
            } else if (j == 1) {
                cycles = *Counters::getInstance().readSharedEventSet();
                auto partitions = MABPL::partitionFixed(n, keys,
                                                        static_cast<int>(MABPL::MachineConstants::getInstance().getMachineConstant(
                                                                machineConstantName)));
                cycles = *Counters::getInstance().readSharedEventSet() - cycles;
            } else if (j == 2) {
                cycles = *Counters::getInstance().readSharedEventSet();
                auto partitions = MABPL::partitionFixed(n, keys, 16);
                cycles = *Counters::getInstance().readSharedEventSet() - cycles;
            }

            results[0][(i * 3) + j] = cycles;

            delete[] keys;
            std::cout << "Completed" << std::endl;
        }
    }

    std::vector<std::string> headers(3 * iterations);
    std::string functionNames[] = {"RadixPartition_Adaptive", "RadixPartition_Static_", "RadixPartition_Static_16bits"};
    functionNames[1].append(std::to_string(static_cast<int>(MABPL::MachineConstants::getInstance().getMachineConstant(machineConstantName))));
    functionNames[1].append("bits");
    for (auto i = 0; i < (3 * iterations); ++i) {
        headers[i] = functionNames[i % 3];
    }

    std::string fileName = "IMDB_partition_personIdColumn_PrincipalsTable";
    std::string fullFilePath = FilePaths::getInstance().getImdbOutputFolderPath() + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);

    delete[] data;
}

void runImdbGroupByMacroBenchmark_titleIdColumnPrincipalsTable(int iterations, bool randomise) {
    std::string filePath = FilePaths::getInstance().getImdbInputFolderPath() + "title.principals.tsv";
    int n = getLengthOfTsv(filePath);
    auto data = new uint32_t[n];
    readImdbTitleIdColumnFromPrincipalsTable(filePath, data);

    if (randomise) {
        randomiseArray(data, n);
    }

    int cardinality = 9135620;
    long_long cycles;
    std::vector<std::vector<long_long>> results(1, std::vector<long_long>((3 * iterations), 0));

    for (int i = 0; i < iterations; i++) {

        for (int j = 0; j < 3; j++) {

            auto inputGroupBy = new uint32_t[n];
            auto inputAggregate = new uint32_t[n];
            copyArray(data, inputGroupBy, n);
            copyArray(data, inputAggregate, n);

            std::cout << "Running iteration " << i + 1 << "... ";

            if (j == 0) {
                cycles = *Counters::getInstance().readSharedEventSet();
                auto result = MABPL::groupByAdaptive<CountAggregation>(n, inputGroupBy, inputAggregate, cardinality);
                cycles = *Counters::getInstance().readSharedEventSet() - cycles;
            } else if (j == 1) {
                cycles = *Counters::getInstance().readSharedEventSet();
                auto result = MABPL::groupByHash<CountAggregation>(n, inputGroupBy, inputAggregate, cardinality);
                cycles = *Counters::getInstance().readSharedEventSet() - cycles;
            } else if (j == 2) {
                cycles = *Counters::getInstance().readSharedEventSet();
                auto result = MABPL::groupBySort<CountAggregation>(n, inputGroupBy, inputAggregate);
                cycles = *Counters::getInstance().readSharedEventSet() - cycles;
            }

            results[0][(i * 3) + j] = cycles;

            delete[] inputGroupBy;
            delete[] inputAggregate;
            std::cout << "Completed" << std::endl;
        }
    }

    std::vector<std::string> headers(3 * iterations);
    std::string functionNames[] = {"GroupBy_Adaptive", "GroupBy_Hash", "GroupBy_Sort"};
    for (auto i = 0; i < (3 * iterations); ++i) {
        headers[i] = functionNames[i % 3];
    }

    std::string fileName = "IMDB_groupBy_titleIdColumn_PrincipalsTable";
    if (randomise) {fileName += "_randomised";}
    std::string fullFilePath = FilePaths::getInstance().getImdbOutputFolderPath() + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);

    delete[] data;
}

void runImdbGroupByMacroBenchmark_titleIdFromAkasTable(int iterations, bool randomise) {
    std::string filePath = FilePaths::getInstance().getImdbInputFolderPath() + "title.akas.tsv";
    int n = getLengthOfTsv(filePath);
    auto data = new int [n];
    readImdbTitleIdColumnFromAkasTable(filePath, data);

    if (randomise) {
        randomiseArray(data, n);
    }

    int cardinality = 7247075;
    long_long cycles;
    std::vector<std::vector<long_long>> results(1, std::vector<long_long>((3 * iterations), 0));

    for (int i = 0; i < iterations; i++) {

        for (int j = 0; j < 3; j++) {

            auto inputGroupBy = new int[n];
            auto inputAggregate = new int[n];
            copyArray(data, inputGroupBy, n);
            copyArray(data, inputAggregate, n);

            std::cout << "Running iteration " << i + 1 << "... ";

            if (j == 0) {
                cycles = *Counters::getInstance().readSharedEventSet();
                auto result = MABPL::groupByAdaptive<CountAggregation>(n, inputGroupBy, inputAggregate, cardinality);
                cycles = *Counters::getInstance().readSharedEventSet() - cycles;
            } else if (j == 1) {
                cycles = *Counters::getInstance().readSharedEventSet();
                auto result = MABPL::groupByHash<CountAggregation>(n, inputGroupBy, inputAggregate, cardinality);
                cycles = *Counters::getInstance().readSharedEventSet() - cycles;
            } else if (j == 2) {
                cycles = *Counters::getInstance().readSharedEventSet();
                auto result = MABPL::groupBySort<CountAggregation>(n, inputGroupBy, inputAggregate);
                cycles = *Counters::getInstance().readSharedEventSet() - cycles;
            }

            results[0][(i * 3) + j] = cycles;

            delete[] inputGroupBy;
            delete[] inputAggregate;
            std::cout << "Completed" << std::endl;
        }
    }

    std::vector<std::string> headers(3 * iterations);
    std::string functionNames[] = {"GroupBy_Adaptive", "GroupBy_Hash", "GroupBy_Sort"};
    for (auto i = 0; i < (3 * iterations); ++i) {
        headers[i] = functionNames[i % 3];
    }

    std::string fileName = "IMDB_groupBy_titleIdColumn_AkasTable";
    if (randomise) {fileName += "_randomised";}
    std::string fullFilePath = FilePaths::getInstance().getImdbOutputFolderPath() + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);

    delete[] data;
}


int main() {

    std::string machineConstantName = "Partition_minRadixBits";

    partitionSweepBenchmark<uint32_t>(DataSweeps::logUniformIntDistribution250mValuesClusteredSweepFixedCardinality10mMax250m,
                                           {Partition::RadixBitsFixed, Partition::RadixBitsAdaptive},
                                           16, "ClusterednessSweep", 5);
    partitionSweepBenchmark<uint32_t>(DataSweeps::logUniformIntDistribution250mValuesClusteredSweepFixedCardinality10mMax250m,
                                           {Partition::RadixBitsFixed},
                                           MABPL::MachineConstants::getInstance().getMachineConstant(machineConstantName),
                                           "ClusterednessSweep_9", 5);

//    runImdbSelectSweepMacroBenchmark(1874, 2023, 5,
//                                     {Select::ImplementationIndexesBranch, Select::ImplementationIndexesPredication, Select::ImplementationIndexesAdaptive});

//    runImdbPartitionMacroBenchmark_titleIdColumnBasicsTable(1);
//    runImdbPartitionMacroBenchmark_startYearColumnBasicsTable(1);
//    runImdbPartitionMacroBenchmark_personIdColumnPrincipalsTable(1);
//
//    runImdbGroupByMacroBenchmark_titleIdColumnPrincipalsTable(1, true);
//    runImdbGroupByMacroBenchmark_titleIdFromAkasTable(1, true);

    return 0;
}