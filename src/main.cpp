#include <vector>

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
//    std::string inputDataFilePathOne = FilePaths::getInstance().getOisstInputFolderPath() + "1_month_(06-07)/" + "1982.csv";
//    std::string inputDataFilePathTwo = FilePaths::getInstance().getOisstInputFolderPath() + "1_month_(06-07)/" + "2023.csv";
//    std::string outputDataFilePath = FilePaths::getInstance().getOisstOutputFolderPath() + "1982.csv";
//    const int n = getLengthOfCsv(inputDataFilePathOne);

/*    auto *yearLatLong = new std::string[n];
    auto *monthDay = new std::string[n];
    auto *sst1982 = new float[n];
    readOisstDataFromCsv(inputDataFilePathOne, n, yearLatLong, monthDay, sst1982);

    auto *sstDiff= new float[n];
    readOisstDataFromCsv(inputDataFilePathOne, n, yearLatLong, monthDay, sstDiff);
    delete[] monthDay;

    for (int i = 0; i < n; i++) {
        if (!std::isnan(sst1982[i])) {
            sstDiff[i] -= sst1982[i];
        }
    }
    delete[] sst1982;*/


/*    std::string inputDataFilePath = FilePaths::getInstance().getOisstInputFolderPath() + "1_month_(06-07)/" + "1982.csv";
    const int n = getLengthOfCsv(inputDataFilePath);

    auto *yearLatLong = new std::string[n];
    auto *monthDay = new std::string[n];
    auto *sst = new float[n];
    readOisstDataFromCsv(inputDataFilePath, n, yearLatLong, monthDay, sst);

    auto sst2 = new float[n];
    copyArray<float>(sst, sst2, n);

    long_long cycles, cyclesCount;
    auto *selectedValues= new float[n];

    cycles = *Counters::getInstance().readSharedEventSet();
    int selectedCount = MABPL::selectValuesVectorized<float,float>(n, sst, sst, sst2,15);
    cyclesCount = *Counters::getInstance().readSharedEventSet() - cycles;

    std::cout << "Selected " << selectedCount << " / " << n << ", Cycles: " << cyclesCount << std::endl;

//    projectIndexesOnToArray(selectedIndexes, selectedCount, yearLatLong);
//    projectIndexesOnToArray(selectedIndexes, selectedCount, sst);

//    writeOisstDataToCsv(outputDataFilePath, selectedCount, yearLatLong, sst);

    delete[] yearLatLong;
    delete[] monthDay;
//    delete[] sstDiff;
    delete[] sst;
    delete[] selectedValues;*/

    std::string inputDataFilePath = FilePaths::getInstance().getOisstInputFolderPath() + "1_month_(06-07)/" + "1982.csv";
    const int n = getLengthOfCsv(inputDataFilePath);

    auto *yearLatLong = new std::string[n];
    auto *monthDay = new std::string[n];
    auto *sst = new float[n];
    readOisstDataFromCsv(inputDataFilePath, n, yearLatLong, monthDay, sst);
    delete[] monthDay;

    auto sst2 = new float[n];
    copyArray<float>(sst, sst2, n);

    long_long cycles, cyclesCount;
    auto *selectedValues= new float[n];

    cycles = *Counters::getInstance().readSharedEventSet();
    int selectedCount = MABPL::selectValuesAdaptive<float,float>(n, sst2, sst, selectedValues,-1);
    cyclesCount = *Counters::getInstance().readSharedEventSet() - cycles;

    std::cout << "Selected " << selectedCount << " / " << n << ", Cycles: " << cyclesCount << std::endl;

    delete[] yearLatLong;
    delete[] sst;
    delete[] selectedValues;


}

void runImdbSelectMacroBenchmark() {
    std::string filePath = FilePaths::getInstance().getImdbInputFolderPath() + "title.basics.tsv";
    int n = getLengthOfTsv(filePath);
    auto inputData = new int[n];
    readImdbStartYearColumn(filePath, inputData);

    long_long cycles;
    auto *selectedIndexes = new int[n];

    cycles = *Counters::getInstance().readSharedEventSet();
    int selectedCount = MABPL::selectIndexesAdaptive(n, inputData, selectedIndexes, 1960);
    cycles = *Counters::getInstance().readSharedEventSet() - cycles;

    std::cout << "Selected " << selectedCount << " / " << n << ", Cycles: " << cycles << std::endl;

    delete[] inputData;
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


int main() {

    return 0;
}