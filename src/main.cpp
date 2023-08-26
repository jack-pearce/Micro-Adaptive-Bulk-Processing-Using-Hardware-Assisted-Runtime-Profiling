#include <vector>
#include <bitset>

#include "main.h"
#include "cycles_benchmarking/groupByCyclesBenchmark.h"
#include "data_generation/dataFiles.h"
#include "utilities/dataHelpers.h"
#include "library/mabpl.h"

using MABPL::Select;
using MABPL::GroupBy;

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

void runOisstMacroBenchmark() {
    std::string inputDataFilePathOne = FilePaths::getInstance().getOisstInputFolderPath() + "1_month_(06-07)/" + "1982.csv";
    const int n1 = getLengthOfCsv(inputDataFilePathOne);
    std::string inputDataFilePathTwo = FilePaths::getInstance().getOisstInputFolderPath() + "1_month_(06-07)/" + "2023.csv";
    const int n2 = getLengthOfCsv(inputDataFilePathTwo);
    int n = n1 + n2;

    auto *yearLatLong = new int64_t[n];
    auto *monthDay = new int[n];
    auto *sst = new float[n];

    readOisstDataFromCsv(inputDataFilePathOne, n1, yearLatLong, monthDay, sst);
    readOisstDataFromCsv(inputDataFilePathTwo, n2, yearLatLong + n1, monthDay + n1, sst + n1);
    delete[] monthDay;

    float thresholdTemperature = 0;
    auto *selectedIndexes = new int[n];
    int selectedCount = MABPL::selectIndexesAdaptive<float>(n,sst, selectedIndexes, thresholdTemperature);

    auto *yearLatLong30 = new int64_t[selectedCount];
    auto *sst30 = new float[selectedCount];

    projectIndexesOnToArray(selectedIndexes, selectedCount, yearLatLong, yearLatLong30);
    projectIndexesOnToArray(selectedIndexes, selectedCount, sst, sst30);

    delete[] selectedIndexes;
    delete[] yearLatLong;
    delete[] sst;

    n = selectedCount;

    {
        auto *inputGroupBy = new int64_t[n];
        auto *inputAggregate = new float[n];

        copyArray(yearLatLong30, inputGroupBy, n);
        copyArray(sst30, inputAggregate, n);

        long_long cycles;

        cycles = *Counters::getInstance().readSharedEventSet();
        auto results = MABPL::groupByAdaptive<MaxAggregation>(n, inputGroupBy, inputAggregate, 2 * 720 * 1440);
        cycles = *Counters::getInstance().readSharedEventSet() - cycles;

        std::cout << "Adpt result size " << results.size() << " / " << n << ", Cycles: " << cycles << std::endl;

        delete[] inputGroupBy;
        delete[] inputAggregate;
    }

    {
        auto *inputGroupBy = new int64_t[n];
        auto *inputAggregate = new float[n];

        copyArray(yearLatLong30, inputGroupBy, n);
        copyArray(sst30, inputAggregate, n);

        long_long cycles;

        cycles = *Counters::getInstance().readSharedEventSet();
        auto results = MABPL::groupByHash<MaxAggregation>(n, inputGroupBy, inputAggregate, 2 * 720 * 1440);
        cycles = *Counters::getInstance().readSharedEventSet() - cycles;

        std::cout << "Hash result size " << results.size() << " / " << n << ", Cycles: " << cycles << std::endl;

        delete[] inputGroupBy;
        delete[] inputAggregate;
    }

    {
        auto *inputGroupBy = new int64_t[n];
        auto *inputAggregate = new float[n];

        copyArray(yearLatLong30, inputGroupBy, n);
        copyArray(sst30, inputAggregate, n);

        long_long cycles;

        cycles = *Counters::getInstance().readSharedEventSet();
        auto results = MABPL::groupBySort<MaxAggregation>(n, inputGroupBy, inputAggregate);
        cycles = *Counters::getInstance().readSharedEventSet() - cycles;

        std::cout << "Sort result size " << results.size() << " / " << n << ", Cycles: " << cycles << std::endl;

        delete[] inputGroupBy;
        delete[] inputAggregate;
    }

    delete[] yearLatLong30;
    delete[] sst30;
}

void runImdbSelectMacroBenchmark() {
    std::string filePath = FilePaths::getInstance().getImdbInputFolderPath() + "title.basics.tsv";
    int n = getLengthOfTsv(filePath);
    auto inputData = new int[n];
    readImdbStartYearColumn(filePath, inputData);

    long_long cycles;
    auto *selectedIndexes = new int[n];

    cycles = *Counters::getInstance().readSharedEventSet();
    int selectedCount = MABPL::selectIndexesBranch(n, inputData, selectedIndexes, 1960);
    cycles = *Counters::getInstance().readSharedEventSet() - cycles;

    std::cout << "Selected " << selectedCount << " / " << n << ", Cycles: " << cycles << std::endl;

    delete[] inputData;
}

void runImdbSelectSweepMacroBenchmark(int startYear, int endYear, int iterations,
                                      const std::vector<Select> &selectImplementations) {
    int numImplementations = static_cast<int>(selectImplementations.size());

    std::string filePath = FilePaths::getInstance().getImdbInputFolderPath() + "title.basics.tsv";
    int n = getLengthOfTsv(filePath);
    auto data = new int[n];
    readImdbStartYearColumn(filePath, data);

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

void runImdbGroupByMacroBenchmark1() {
    std::string filePath = FilePaths::getInstance().getImdbInputFolderPath() + "title.episode.tsv";
    int n = getLengthOfTsv(filePath);
    auto inputGroupBy = new int64_t[n];
    readImdbParentTvSeriesAndSeasonColumn(filePath, inputGroupBy);

    auto inputAggregate = new int64_t[n];
    copyArray(inputGroupBy, inputAggregate, n);

    long_long cycles;

    cycles = *Counters::getInstance().readSharedEventSet();
    auto results = MABPL::groupByHash<CountAggregation>(n, inputGroupBy, inputAggregate, 325000);
    cycles = *Counters::getInstance().readSharedEventSet() - cycles;

    std::cout << "Cardinality: " << results.size() << ", Total input size: " << n << ", Cycles: " << cycles << std::endl;

    delete[] inputGroupBy;
    delete[] inputAggregate;
}

void runImdbGroupByMacroBenchmark2() {
    std::string filePath = FilePaths::getInstance().getImdbInputFolderPath() + "title.principals.tsv";
    int n = getLengthOfTsv(filePath);
    auto inputGroupBy = new int[n];
    readImdbPrincipalsColumn(filePath, inputGroupBy);

    auto inputAggregate = new int[n];
    copyArray(inputGroupBy, inputAggregate, n);

    long_long cycles;

    cycles = *Counters::getInstance().readSharedEventSet();
    auto results = MABPL::groupBySort<CountAggregation>(n, inputGroupBy, inputAggregate);
    cycles = *Counters::getInstance().readSharedEventSet() - cycles;

    std::cout << "Cardinality: " << results.size() << ", Total input size: " << n << ", Cycles: " << cycles << std::endl;

    delete[] inputGroupBy;
    delete[] inputAggregate;
}

void runImdbGroupByMacroBenchmark3() {
    std::string filePath = FilePaths::getInstance().getImdbInputFolderPath() + "title.akas.tsv";
    int n = getLengthOfTsv(filePath);
    auto inputGroupBy = new int64_t [n];
    readImdbFilmColumn(filePath, inputGroupBy);

    auto inputAggregate = new int64_t[n];
    copyArray(inputGroupBy, inputAggregate, n);

    long_long cycles;

    cycles = *Counters::getInstance().readSharedEventSet();
    auto results = MABPL::groupByAdaptive<CountAggregation>(n, inputGroupBy, inputAggregate, 7250000);
    cycles = *Counters::getInstance().readSharedEventSet() - cycles;

    std::cout << "Cardinality: " << results.size() << ", Total input size: " << n << ", Cycles: " << cycles << std::endl;

    delete[] inputGroupBy;
    delete[] inputAggregate;
}

void printTlbSpecificationsFromSubLeaf0x18(unsigned int maxSubleaf) {
    unsigned int leaf = 0x18;
    unsigned int eax, ebx, ecx, edx;

    for (unsigned int subleaf = 0x1; subleaf <= maxSubleaf; subleaf++) {
        eax = leaf;
        ecx = subleaf;

        asm volatile (
                "cpuid"
                : "=a" (eax), "=b" (ebx), "=c" (ecx), "=d" (edx)
                : "a" (eax), "c" (ecx)
                );

        printf("0x%08x-", eax);
        printf("0x%08x-", ebx);
        printf("0x%08x-", ecx);
        printf("0x%08x", edx);
        std::cout << " [SL 0" << subleaf << "]" << std::endl;
    }
    std::cout << std::endl;

    for (unsigned int subleaf = 0x1; subleaf <= maxSubleaf; subleaf++) {
        eax = leaf;
        ecx = subleaf;

        asm volatile (
                "cpuid"
                : "=a" (eax), "=b" (ebx), "=c" (ecx), "=d" (edx)
                : "a" (eax), "c" (ecx)
                );

        std::string eax_binary = std::bitset<32>(eax).to_string();
        std::string ebx_binary = std::bitset<32>(ebx).to_string();
        std::string ecx_binary = std::bitset<32>(ecx).to_string();
        std::string edx_binary = std::bitset<32>(edx).to_string();

        for (int i = 4; i < 39; i += 5) {
            eax_binary.insert(i, " ");
            ebx_binary.insert(i, " ");
            ecx_binary.insert(i, " ");
            edx_binary.insert(i, " ");
        }

        std::cout << eax_binary << "---";
        std::cout << ebx_binary << "---";
        std::cout << ecx_binary << "---";
        std::cout << edx_binary;
        std::cout << " [SL 0" << subleaf << "]" << std::endl;
    }
    std::cout << std::endl;

    for (unsigned int subleaf = 0x1; subleaf <= maxSubleaf; subleaf++) {
        eax = leaf;
        ecx = subleaf;

        asm volatile (
                "cpuid"
                : "=a" (eax), "=b" (ebx), "=c" (ecx), "=d" (edx)
                : "a" (eax), "c" (ecx)
                );

        unsigned int translationCacheLevel = (edx >> 5) & 0x7;
        unsigned int translationCacheType = edx & 0x1F;
        unsigned int waysOfAssociativity = (ebx >> 16) & 0xFF;
        unsigned int numberOfSets = ecx;
        bool supports4KPage = ebx & 0x1;
        bool supports2MBPage = (ebx >> 1) & 0x1;
        bool supports4MBPage = (ebx >> 2) & 0x1;
        bool supports1GBPage = (ebx >> 3) & 0x1;

        std::cout << "L" << translationCacheLevel;
        if (translationCacheType == 1) std::cout << " Data TLB, ";
        if (translationCacheType == 2) std::cout << " Instruction TLB, ";
        if (translationCacheType == 3) std::cout << " Unified TLB, ";
        if (translationCacheType == 4) std::cout << " Load Only TLB, ";
        if (translationCacheType == 5) std::cout << " Store Only TLB, ";
        std::cout << waysOfAssociativity << " way associativity, ";
        std::cout << numberOfSets << " sets, ";
        std::cout << waysOfAssociativity * numberOfSets << " entries, ";
        std::cout << "supports ";
        if (supports4KPage) std::cout << "4K ";
        if (supports2MBPage) std::cout << "2MB ";
        if (supports4MBPage) std::cout << "4MB ";
        if (supports1GBPage) std::cout << "1GB ";
        std::cout << "page sizes" << std::endl;
    }
}

void printByteDescriptorFromLeaf0x2(unsigned char value) {
    switch (value) {
        case 0x00: std::cout << "Null descriptor"; break;
        case 0x01: std::cout << "Reserved (ignored)"; break;
        case 0x02: std::cout << "Instruction TLB: 4K, 4-way, 32 entries"; break;
        case 0x03: std::cout << "Instruction TLB: 4M, fully associative, 2 entries"; break;
        case 0x04: std::cout << "Data TLB: 4K, 4-way, 64 entries"; break;
        case 0x05: std::cout << "Data TLB1: 4M, 4-way, 32 entries"; break;
        case 0x06: std::cout << "1st-level instruction cache: 8KB, 4-way, 32 byte line size"; break;
        case 0x08: std::cout << "1st-level instruction cache: 16KB, 4-way, 32 byte line size"; break;
        case 0x09: std::cout << "1st-level instruction cache: 32KB, 4-way, 64 byte line size"; break;
        case 0x0A: std::cout << "1st-level data cache: 8KB, 2-way, 32 byte line size"; break;
        case 0x0B: std::cout << "Instruction TLB: 4M, 4-way, 4 entries"; break;
        case 0x0C: std::cout << "1st-level data cache: 16KB, 4-way, 32 byte line size"; break;
        case 0x0D: std::cout << "1st-level data cache: 16KB, 4-way, 64 byte line size"; break;
        case 0x0E: std::cout << "1st-level data cache: 24KB, 6-way, 64 byte line size"; break;
        case 0x1D: std::cout << "2nd-level cache: 128KB, 2-way, 64 byte line size"; break;
        case 0x21: std::cout << "2nd-level cache: 256KB, 8-way, 64 byte line size"; break;
        case 0x22: std::cout << "3rd-level cache: 512KB, 4-way, 64 byte line size, 2 lines per sector"; break;
        case 0x23: std::cout << "3rd-level cache: 1MB, 8-way, 64 byte line size, 2 lines per sector"; break;
        case 0x24: std::cout << "2nd-level cache: 1MB, 16-way, 64 byte line size"; break;
        case 0x25: std::cout << "3rd-level cache: 2MB, 8-way, 64 byte line size, 2 lines per sector"; break;
        case 0x29: std::cout << "3rd-level cache: 4MB, 8-way, 64 byte line size, 2 lines per sector"; break;
        case 0x2C: std::cout << "1st-level data cache: 32KB, 8-way, 64 byte line size"; break;
        case 0x30: std::cout << "1st-level instruction cache: 32KB, 8-way, 64 byte line size"; break;
        case 0x40: std::cout << "No 2nd-level cache or, if valid, no 3rd-level cache"; break;
        case 0x41: std::cout << "2nd-level cache: 128KB, 4-way, 32 byte line size"; break;
        case 0x42: std::cout << "2nd-level cache: 256KB, 4-way, 32 byte line size"; break;
        case 0x43: std::cout << "2nd-level cache: 512KB, 4-way, 32 byte line size"; break;
        case 0x44: std::cout << "2nd-level cache: 1MB, 4-way, 32 byte line size"; break;
        case 0x45: std::cout << "2nd-level cache: 2MB, 4-way, 32 byte line size"; break;
        case 0x46: std::cout << "3rd-level cache: 4MB, 4-way, 64 byte line size"; break;
        case 0x47: std::cout << "3rd-level cache: 8MB, 8-way, 64 byte line size"; break;
        case 0x48: std::cout << "2nd-level cache: 3MB, 12-way, 64 byte line size"; break;
        case 0x49: std::cout << "3rd-level cache: 4MB, 16-way, 64 byte line size"; break;
        case 0x4A: std::cout << "3rd-level cache: 6MB, 12-way, 64 byte line size"; break;
        case 0x4B: std::cout << "3rd-level cache: 8MB, 16-way, 64 byte line size"; break;
        case 0x4C: std::cout << "3rd-level cache: 12MB, 12-way, 64 byte line size"; break;
        case 0x4D: std::cout << "3rd-level cache: 16MB, 16-way, 64 byte line size"; break;
        case 0x4E: std::cout << "2nd-level cache: 6MB, 24-way, 64 byte line size"; break;
        case 0x4F: std::cout << "Instruction TLB: 4K, 32 entries"; break;
        case 0x50: std::cout << "Instruction TLB: 4K/2M pages, 64 entries"; break;
        case 0x51: std::cout << "Instruction TLB: 4K/2M pages, 128 entries"; break;
        case 0x52: std::cout << "Instruction TLB: 4K/2M pages, 256 entries"; break;
        case 0x55: std::cout << "Instruction TLB: 2M/4M pages, fully associative, 7 entries"; break;
        case 0x56: std::cout << "Data TLB0: 4M pages, 4-way, 16 entries"; break;
        case 0x57: std::cout << "Data TLB0: 4K pages, 4-way, 16 entries"; break;
        case 0x59: std::cout << "Data TLB0: 4K pages, fully associative, 16 entries"; break;
        case 0x5A: std::cout << "Data TLB0: 2M/4M pages, 4-way, 32 entries"; break;
        case 0x5B: std::cout << "Data TLB: 4K/4M pages, 64 entries"; break;
        case 0x5C: std::cout << "Data TLB: 4K/4M pages, 128 entries"; break;
        case 0x5D: std::cout << "Data TLB: 4K/4M pages, 256 entries"; break;
        case 0x60: std::cout << "1st-level data cache: 16KB, 8-way, 64 byte line size"; break;
        case 0x61: std::cout << "Instruction TLB: 4K pages, fully associative, 48 entries"; break;
        case 0x63: std::cout << "Data TLB: 2M/4M pages, 4-way, 32 entries and 1GB pages, 4-way, 4 entries"; break;
        case 0x64: std::cout << "Data TLB: 4K pages, 4-way, 512 entries"; break;
        case 0x66: std::cout << "1st-level data cache: 8KB, 4-way, 64 byte line size"; break;
        case 0x67: std::cout << "1st-level data cache: 16KB, 4-way, 64 byte line size"; break;
        case 0x68: std::cout << "1st-level data cache: 32KB, 4-way, 64 byte line size"; break;
        case 0x6A: std::cout << "uTLB: 4K pages, 8-way, 64 entries"; break;
        case 0x6B: std::cout << "DTLB: 4K pages, 8-way, 256 entries"; break;
        case 0x6C: std::cout << "DTLB: 2M/4M pages, 8-way, 128 entries"; break;
        case 0x6D: std::cout << "DTLB: 1GB pages, fully associative, 16 entries"; break;
        case 0x70: std::cout << "Trace cache: 12K-μop, 8-way"; break;
        case 0x71: std::cout << "Trace cache: 16K-μop, 8-way"; break;
        case 0x72: std::cout << "Trace cache: 32K-μop, 8-way"; break;
        case 0x76: std::cout << "Instruction TLB: 2M/4M pages, fully associative, 8 entries"; break;
        case 0x78: std::cout << "2nd-level cache: 1MB, 4-way, 64 byte line size"; break;
        case 0x79: std::cout << "2nd-level cache: 128KB, 8-way, 64 byte line size, 2 lines per sector"; break;
        case 0x7A: std::cout << "2nd-level cache: 256KB, 8-way, 64 byte line size, 2 lines per sector"; break;
        case 0x7B: std::cout << "2nd-level cache: 512KB, 8-way, 64 byte line size, 2 lines per sector"; break;
        case 0x7C: std::cout << "2nd-level cache: 1MB, 8-way, 64 byte line size, 2 lines per sector"; break;
        case 0x7D: std::cout << "2nd-level cache: 2MB, 8-way, 64 byte line size"; break;
        case 0x7F: std::cout << "2nd-level cache: 512KB, 2-way, 64 byte line size"; break;
        case 0x80: std::cout << "2nd-level cache: 512KB, 8-way, 64 byte line size"; break;
        case 0x82: std::cout << "2nd-level cache: 256KB, 8-way, 32 byte line size"; break;
        case 0x83: std::cout << "2nd-level cache: 512KB, 8-way, 32 byte line size"; break;
        case 0x84: std::cout << "2nd-level cache: 1MB, 8-way, 32 byte line size"; break;
        case 0x85: std::cout << "2nd-level cache: 2MB, 8-way, 32 byte line size"; break;
        case 0x86: std::cout << "2nd-level cache: 512KB, 4-way, 64 byte line size"; break;
        case 0x87: std::cout << "2nd-level cache: 1MB, 8-way, 64 byte line size"; break;
        case 0xA0: std::cout << "DTLB: 4k pages, fully associative, 32 entries"; break;
        case 0xB0: std::cout << "Instruction TLB: 4 KByte pages, 4-way set associative, 128 entries"; break;
        case 0xB1: std::cout << "Instruction TLB: 2M pages, 4-way, 8 entries or 4M pages, 4-way, 4 entries"; break;
        case 0xB2: std::cout << "Instruction TLB: 4KByte pages, 4-way set associative, 64 entries"; break;
        case 0xB3: std::cout << "Data TLB: 4 KByte pages, 4-way set associative, 128 entries"; break;
        case 0xB4: std::cout << "Data TLB: 4M pages, 4-way set associative, 8 entries"; break;
        case 0xB5: std::cout << "Data TLB1: 4M pages, 4-way associative, 256 entries"; break;
        case 0xB6: std::cout << "Instruction TLB: 4KByte pages, 8-way set associative, 64 entries"; break;
        case 0xB7: std::cout << "Instruction TLB: 2M pages, 8-way, 2 entries or 4M pages, 8-way, 1 entry"; break;
        case 0xB8: std::cout << "Instruction TLB: 4KByte pages, 8-way set associative, 128 entries"; break;
        case 0xBA: std::cout << "Data TLB1: 4 KByte pages, 4-way associative, 64 entries"; break;
        case 0xC0: std::cout << "Data TLB: 4 KByte and 4 MByte pages, 4-way associative, 8 entries"; break;
        case 0xC1: std::cout << "Shared 2nd-Level TLB: 4 KByte/2MByte pages, 8-way associative, 1024 entries"; break;
        case 0xC2: std::cout << "DTLB: 4 KByte/2 MByte pages, 4-way associative, 16 entries"; break;
        case 0xC3: std::cout << "Shared 2nd-Level TLB: 4 KByte/2 MByte pages, 6-way associative, 1536 entries"; break;
        case 0xC4: std::cout << "DTLB: 2M/4M Byte pages, 4-way associative, 32 entries"; break;
        case 0xCA: std::cout << "Shared 2nd-Level TLB: 4 KByte pages, 4-way associative, 512 entries"; break;
        case 0xD0: std::cout << "3rd-level cache: 512 KByte, 4-way set associative, 64 byte line size"; break;
        case 0xD1: std::cout << "3rd-level cache: 1 MByte, 4-way set associative, 64 byte line size"; break;
        case 0xD2: std::cout << "3rd-level cache: 2 MByte, 4-way set associative, 64 byte line size"; break;
        case 0xD6: std::cout << "3rd-level cache: 1 MByte, 8-way set associative, 64 byte line size"; break;
        case 0xD7: std::cout << "3rd-level cache: 2 MByte, 8-way set associative, 64 byte line size"; break;
        case 0xD8: std::cout << "3rd-level cache: 4 MByte, 8-way set associative, 64 byte line size"; break;
        case 0xDC: std::cout << "3rd-level cache: 1.5 MByte, 12-way set associative, 64 byte line size"; break;
        case 0xDD: std::cout << "3rd-level cache: 3 MByte, 12-way set associative, 64 byte line size"; break;
        case 0xDE: std::cout << "3rd-level cache: 6 MByte, 12-way set associative, 64 byte line size"; break;
        case 0xE2: std::cout << "3rd-level cache: 2 MByte, 16-way set associative, 64 byte line size"; break;
        case 0xE3: std::cout << "3rd-level cache: 4 MByte, 16-way set associative, 64 byte line size"; break;
        case 0xE4: std::cout << "3rd-level cache: 8 MByte, 16-way set associative, 64 byte line size"; break;
        case 0xEA: std::cout << "3rd-level cache: 12 MByte, 24-way set associative, 64 byte line size"; break;
        case 0xEB: std::cout << "3rd-level cache: 18 MByte, 24-way set associative, 64 byte line size"; break;
        case 0xEC: std::cout << "3rd-level cache: 24 MByte, 24-way set associative, 64 byte line size"; break;
        case 0xF0: std::cout << "Prefetch: 64-Byte prefetching"; break;
        case 0xF1: std::cout << "Prefetch: 128-Byte prefetching"; break;
        case 0xFE: std::cout << "CPUID leaf 2 does not report TLB descriptor information"; break;
        default: std::cout << "Unknown descriptor"; break;
    }
    std::cout << std::endl;
}

void printTlbInformation(unsigned int eax, unsigned int ebx, unsigned int ecx, unsigned int edx) {
    unsigned char descriptors[16] = {
            static_cast<unsigned char>(eax & 0xFF), static_cast<unsigned char>((eax >> 8) & 0xFF),
            static_cast<unsigned char>((eax >> 16) & 0xFF), static_cast<unsigned char>((eax >> 24) & 0xFF),
            static_cast<unsigned char>(ebx & 0xFF), static_cast<unsigned char>((ebx >> 8) & 0xFF),
            static_cast<unsigned char>((ebx >> 16) & 0xFF), static_cast<unsigned char>((ebx >> 24) & 0xFF),
            static_cast<unsigned char>(ecx & 0xFF), static_cast<unsigned char>((ecx >> 8) & 0xFF),
            static_cast<unsigned char>((ecx >> 16) & 0xFF), static_cast<unsigned char>((ecx >> 24) & 0xFF),
            static_cast<unsigned char>(edx & 0xFF), static_cast<unsigned char>((edx >> 8) & 0xFF),
            static_cast<unsigned char>((edx >> 16) & 0xFF), static_cast<unsigned char>((edx >> 24) & 0xFF)
    };

    for (int i = 0; i < 16; i++) {
        std::cout << "Byte " << i << ": ";
        printByteDescriptorFromLeaf0x2(descriptors[i]);
        std::cout << std::endl;
    }
}

bool has0xfeByteValue(unsigned int reg, unsigned char byteValue) {
    for (int i = 0; i < 4; i++) {
        if ((reg & 0xFF) == byteValue) {
            return true;
        }
        reg >>= 8;
    }
    return false;
}

void printTlbSpecifications() {
    unsigned int eax, ebx, ecx, edx;
    eax = 0x02;

    asm volatile (
            "cpuid"
            : "=a" (eax), "=b" (ebx), "=c" (ecx), "=d" (edx)
            : "a" (eax)
            );

    unsigned char byteValue = 0xFE;
    if (has0xfeByteValue(eax, byteValue) || has0xfeByteValue(ebx, byteValue) ||
        has0xfeByteValue(ecx, byteValue) || has0xfeByteValue(edx, byteValue)) {

        eax = 0x18;
        ecx = 0x0;

        asm volatile (
                "cpuid"
                : "=a" (eax), "=b" (ebx), "=c" (ecx), "=d" (edx)
                : "a" (eax), "c" (ecx)
                );

        printTlbSpecificationsFromSubLeaf0x18(eax);
        return;
    }

    printf("0x%08x-", eax);
    printf("0x%08x-", ebx);
    printf("0x%08x-", ecx);
    printf("0x%08x", edx);

    std::cout << std::endl;

    std::string eax_binary = std::bitset<32>(eax).to_string();
    std::string ebx_binary = std::bitset<32>(ebx).to_string();
    std::string ecx_binary = std::bitset<32>(ecx).to_string();
    std::string edx_binary = std::bitset<32>(edx).to_string();

    for (int i = 4; i < 39; i += 5) {
        eax_binary.insert(i, " ");
        ebx_binary.insert(i, " ");
        ecx_binary.insert(i, " ");
        edx_binary.insert(i, " ");
    }

    std::cout << eax_binary << "---";
    std::cout << ebx_binary << "---";
    std::cout << ecx_binary << "---";
    std::cout << edx_binary << std::endl;

    printTlbInformation(eax, ebx, ecx, edx);
}

int main() {

    printTlbSpecifications();

    return 0;

//    for (int i = 2; i < 20; i ++) {
//        testRadixPartition<int, int>(DataFiles::uniformIntDistribution250mValuesMax250m, i);
//    }
//    for (int i = 2; i < 20; i ++) {
//        testRadixPartition<int, int>(DataFiles::fullySortedIntDistribution250mValuesMax250m, i);
//    }


//    radixPartitionBitsSweepBenchmarkWithExtraCountersConfigurations<int,int>(DataFiles::uniformIntDistribution250mValuesMax250m,
//                                                                             4, 20,"4-20_Random_SinglePassRadixPartition_",1);
//    radixPartitionBitsSweepBenchmarkWithExtraCountersConfigurations<int,int>(DataFiles::fullySortedIntDistribution250mValuesMax250m,
//                                                                             4, 20,"4-20_Sorted_SinglePassRadixPartition_",1);
//    radixPartitionSweepBenchmarkWithExtraCountersConfigurations<int,int>(DataSweeps::linearUniqueIntDistribution250mValuesSortednessSweep,
//                                                                         16, "SortednessSweep_16", 1);
//    radixPartitionSweepBenchmarkWithExtraCountersConfigurations<int,int>(DataSweeps::logUniformIntDistribution250mValuesClusteredSweepFixedCardinality10mMax250m,
//                                                                         16, "ClusterednessSweep_16", 1);
//    radixPartitionSweepBenchmarkWithExtraCountersConfigurations<int,int>(DataSweeps::linearUniformIntDistribution250mValuesClusteredSweepFixedCardinality10mMax250m,
//                                                                         16, "ClusterednessSweep_16", 1);

//    radixPartitionSweepBenchmarkWithExtraCountersConfigurations<int,int>(DataSweeps::linearUniqueIntDistribution250mValuesSortednessSweep,
//                                                                         6, "SortednessSweep_6", 1);

/*    unsigned int seed = 1;
    std::mt19937 gen(seed);
    std::uniform_int_distribution<int> distribution(1, 1000000);

    int n = 50;
    int *keys = new int[n];
    int *payloads = new int[n];

    for (auto i = 0; i < n; ++i) {
        keys[i] = distribution(gen);
        payloads[i] = distribution(gen);
    }

    std::cout << "Before: " << std::endl;
    for (int i = 0; i < n; i++) {
        std::cout << keys[i] << " ";
    }
    std::cout << std::endl;
    for (int i = 0; i < n; i++) {
        std::cout << payloads[i] << " ";
    }
    std::cout << std::endl << std::endl;

    MABPL::radixPartition(n, keys, payloads);

    std::cout << "After: " << std::endl;
    for (int i = 0; i < n; i++) {
        std::cout << keys[i] << " ";
    }
    std::cout << std::endl;
    for (int i = 0; i < n; i++) {
        std::cout << payloads[i] << " ";
    }
    std::cout << std::endl << std::endl;

    delete[] keys;
    delete[] payloads;*/

    return 0;
}