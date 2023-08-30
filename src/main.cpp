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
    readImdbTitleIdColumnBasicsTable(filePath, data);

    long_long cycles;
    std::vector<std::vector<long_long>> results(1, std::vector<long_long>((3 * iterations), 0));

    for (int i = 0; i < iterations; i++) {

        for (int j = 0; j < 3; j++) {

            auto keys = new uint32_t[n];
            copyArray(data, keys, n);

            std::cout << "Running iteration " << i + 1 << "... ";

            if (j == 0) {
                cycles = *Counters::getInstance().readSharedEventSet();
                auto partitions = MABPL::radixPartitionAdaptive(n, keys);
                cycles = *Counters::getInstance().readSharedEventSet() - cycles;
            } else if (j == 1) {
                cycles = *Counters::getInstance().readSharedEventSet();
                auto partitions = MABPL::radixPartitionFixed(n, keys,  MABPL::MachineConstants::getInstance().getMachineConstant(machineConstantName));
                cycles = *Counters::getInstance().readSharedEventSet() - cycles;
            } else if (j == 2) {
                cycles = *Counters::getInstance().readSharedEventSet();
                auto partitions = MABPL::radixPartitionFixed(n, keys,  16);
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
                auto partitions = MABPL::radixPartitionAdaptive(n, keys);
                cycles = *Counters::getInstance().readSharedEventSet() - cycles;
            } else if (j == 1) {
                cycles = *Counters::getInstance().readSharedEventSet();
                auto partitions = MABPL::radixPartitionFixed(n, keys,  MABPL::MachineConstants::getInstance().getMachineConstant(machineConstantName));
                cycles = *Counters::getInstance().readSharedEventSet() - cycles;
            } else if (j == 2) {
                cycles = *Counters::getInstance().readSharedEventSet();
                auto partitions = MABPL::radixPartitionFixed(n, keys,  16);
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
                auto partitions = MABPL::radixPartitionAdaptive(n, keys);
                cycles = *Counters::getInstance().readSharedEventSet() - cycles;
            } else if (j == 1) {
                cycles = *Counters::getInstance().readSharedEventSet();
                auto partitions = MABPL::radixPartitionFixed(n, keys,  MABPL::MachineConstants::getInstance().getMachineConstant(machineConstantName));
                cycles = *Counters::getInstance().readSharedEventSet() - cycles;
            } else if (j == 2) {
                cycles = *Counters::getInstance().readSharedEventSet();
                auto partitions = MABPL::radixPartitionFixed(n, keys,  16);
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

/*void runImdbGroupByMacroBenchmark_titleIdColumnPrincipalsTable() {
    std::string filePath = FilePaths::getInstance().getImdbInputFolderPath() + "title.principals.tsv";
    int n = getLengthOfTsv(filePath);
    auto data = new uint32_t[n];
    readImdbPersonIdColumnFromPrincipalsTable(filePath, data);
//    randomiseArray(data, n);

    auto inputGroupBy = new uint32_t[n];
    auto inputAggregate = new uint32_t[n];
    copyArray(data, inputGroupBy, n);
    copyArray(data, inputAggregate, n);

    long_long cycles;

    cycles = *Counters::getInstance().readSharedEventSet();
    auto results = MABPL::groupByAdaptive<CountAggregation>(n, inputGroupBy, inputAggregate, 9200000);
    cycles = *Counters::getInstance().readSharedEventSet() - cycles;

    std::cout << "Cardinality: " << results.size() << ", Total input size: " << n << ", Cycles: " << cycles << std::endl;

    delete[] data;
    delete[] inputGroupBy;
    delete[] inputAggregate;
}*/

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



void runOisstMacroBenchmark() {
//    std::string inputDataFilePathOne = FilePaths::getInstance().getOisstInputFolderPath() + "1_month_(06-07)/" + "1982.csv";
//    std::string inputDataFilePathTwo = FilePaths::getInstance().getOisstInputFolderPath() + "1_month_(06-07)/" + "2023.csv";
    std::string inputDataFilePathOne = FilePaths::getInstance().getOisstInputFolderPath() + "3_months_(04-07)/" + "1982.csv";
    std::string inputDataFilePathTwo = FilePaths::getInstance().getOisstInputFolderPath() + "3_months_(04-07)/" + "2023.csv";
    const int n1 = getLengthOfCsv(inputDataFilePathOne);
    const int n2 = getLengthOfCsv(inputDataFilePathTwo);
    int n = n1 + n2;

    auto *yearLatLong = new uint32_t[n];
    auto *monthDay = new int[n];
    auto *sst = new float[n];

    readOisstDataFromCsv(inputDataFilePathOne, n1, yearLatLong, monthDay, sst);
    readOisstDataFromCsv(inputDataFilePathTwo, n2, yearLatLong + n1, monthDay + n1, sst + n1);
    delete[] monthDay;

//    randomiseArray(yearLatLong, n);
//    randomiseArray(sst, n);

    float thresholdTemperature = -1.7;
    int cardinality = 208165;

    auto *selectedIndexes = new int[n];
    int selectedCount = MABPL::selectIndexesAdaptive<float>(n,sst, selectedIndexes, thresholdTemperature);

    auto *yearLatLongFiltered = new uint32_t[selectedCount];
    auto *sstFiltered = new float[selectedCount];

    projectIndexesOnToArray(selectedIndexes, selectedCount, yearLatLong, yearLatLongFiltered);
    projectIndexesOnToArray(selectedIndexes, selectedCount, sst, sstFiltered);

    delete[] selectedIndexes;
    delete[] yearLatLong;
    delete[] sst;

    n = selectedCount;

    {
        auto *inputGroupBy = new uint32_t[n];
        auto *inputAggregate = new float[n];

        copyArray(yearLatLongFiltered, inputGroupBy, n);
        copyArray(sstFiltered, inputAggregate, n);

        for (int i = 0; i < 1000; i++) {
            std::cout << inputGroupBy[100000+i] << std::endl;
        }

        long_long cycles;

        cycles = *Counters::getInstance().readSharedEventSet();
        auto results = MABPL::groupByAdaptive<MaxAggregation>(n, inputGroupBy, inputAggregate, cardinality);
        cycles = *Counters::getInstance().readSharedEventSet() - cycles;

        std::cout << "Adpt result size " << results.size() << " / " << n << ", Cycles: " << cycles << std::endl;

        delete[] inputGroupBy;
        delete[] inputAggregate;
    }

    {
        auto *inputGroupBy = new uint32_t[n];
        auto *inputAggregate = new float[n];

        copyArray(yearLatLongFiltered, inputGroupBy, n);
        copyArray(sstFiltered, inputAggregate, n);

        long_long cycles;

        cycles = *Counters::getInstance().readSharedEventSet();
        auto results = MABPL::groupByHash<MaxAggregation>(n, inputGroupBy, inputAggregate, cardinality);
        cycles = *Counters::getInstance().readSharedEventSet() - cycles;

        std::cout << "Hash result size " << results.size() << " / " << n << ", Cycles: " << cycles << std::endl;

        delete[] inputGroupBy;
        delete[] inputAggregate;
    }

    {
        auto *inputGroupBy = new uint32_t[n];
        auto *inputAggregate = new float[n];

        copyArray(yearLatLongFiltered, inputGroupBy, n);
        copyArray(sstFiltered, inputAggregate, n);

        long_long cycles;

        cycles = *Counters::getInstance().readSharedEventSet();
        auto results = MABPL::groupBySort<MaxAggregation>(n, inputGroupBy, inputAggregate);
        cycles = *Counters::getInstance().readSharedEventSet() - cycles;

        std::cout << "Sort result size " << results.size() << " / " << n << ", Cycles: " << cycles << std::endl;

        delete[] inputGroupBy;
        delete[] inputAggregate;
    }

    delete[] yearLatLongFiltered;
    delete[] sstFiltered;
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

void runImdbGroupByMacroBenchmark4() {
    std::string filePath = FilePaths::getInstance().getImdbInputFolderPath() + "title.principals.tsv";
    int n = getLengthOfTsv(filePath);
    auto data = new int[n];
    readImdbPrincipalsColumn(filePath, data);
    randomiseArray(data, n);

    auto inputGroupBy = new int[n];
    auto inputAggregate = new int[n];
    copyArray(data, inputGroupBy, n);
    copyArray(data, inputAggregate, n);

    long_long cycles;

    cycles = *Counters::getInstance().readSharedEventSet();
    auto results = MABPL::groupByAdaptive<CountAggregation>(n, inputGroupBy, inputAggregate, 9200000);
    cycles = *Counters::getInstance().readSharedEventSet() - cycles;

    std::cout << "Cardinality: " << results.size() << ", Total input size: " << n << ", Cycles: " << cycles << std::endl;

    delete[] data;
    delete[] inputGroupBy;
    delete[] inputAggregate;
}

void runImdbGroupByMacroBenchmark5() {
/*    std::string filePath = FilePaths::getInstance().getImdbInputFolderPath() + "title.principals.tsv";
    int n = getLengthOfTsv(filePath);
    auto data = new int[n];
    readImdbFilmsColumnFromPrincipals(filePath, data);*/

    std::string filePath = FilePaths::getInstance().getImdbInputFolderPath() + "title.akas.tsv";
    int n = getLengthOfTsv(filePath);
    auto data = new int [n];
    readImdbTitleIdColumnFromAkasTable(filePath, data);


    randomiseArray(data, n);

    int cardinality = 7247075;

    {
        auto inputGroupBy = new int[n];
        auto inputAggregate = new int[n];
        copyArray(data, inputGroupBy, n);
        copyArray(data, inputAggregate, n);

        long_long cycles;

        cycles = *Counters::getInstance().readSharedEventSet();
        auto results = MABPL::groupByAdaptive<CountAggregation>(n, inputGroupBy, inputAggregate, cardinality);
        cycles = *Counters::getInstance().readSharedEventSet() - cycles;

        std::cout << "Adpt: Cardinality: " << results.size() << ", Total input size: " << n << ", Cycles: " << cycles
                  << std::endl;

        delete[] inputGroupBy;
        delete[] inputAggregate;
    }

    {
        auto inputGroupBy = new int[n];
        auto inputAggregate = new int[n];
        copyArray(data, inputGroupBy, n);
        copyArray(data, inputAggregate, n);

        long_long cycles;

        cycles = *Counters::getInstance().readSharedEventSet();
        auto results = MABPL::groupByHash<CountAggregation>(n, inputGroupBy, inputAggregate, cardinality);
        cycles = *Counters::getInstance().readSharedEventSet() - cycles;

        std::cout << "Hash: Cardinality: " << results.size() << ", Total input size: " << n << ", Cycles: " << cycles
                  << std::endl;

        delete[] inputGroupBy;
        delete[] inputAggregate;
    }

    {
        auto inputGroupBy = new int[n];
        auto inputAggregate = new int[n];
        copyArray(data, inputGroupBy, n);
        copyArray(data, inputAggregate, n);

        long_long cycles;

        cycles = *Counters::getInstance().readSharedEventSet();
        auto results = MABPL::groupBySort<CountAggregation>(n, inputGroupBy, inputAggregate);
        cycles = *Counters::getInstance().readSharedEventSet() - cycles;

        std::cout << "Sort: Cardinality: " << results.size() << ", Total input size: " << n << ", Cycles: " << cycles
                  << std::endl;

        delete[] inputGroupBy;
        delete[] inputAggregate;
    }

    delete[] data;
}

void runImdbGroupByMacroBenchmark6() {
    std::string filePath = FilePaths::getInstance().getImdbInputFolderPath() + "title.crew.tsv";
    int n = getLengthOfTsv(filePath);
    auto data = new int[n];
    readImdbDirectorsColumn(filePath, data);
    randomiseArray(data, n);

    auto inputGroupBy = new int[n];
    auto inputAggregate = new int[n];
    copyArray(data, inputGroupBy, n);
    copyArray(data, inputAggregate, n);

    long_long cycles;

    cycles = *Counters::getInstance().readSharedEventSet();
    auto results = MABPL::groupBySort<CountAggregation>(n, inputGroupBy, inputAggregate);
    cycles = *Counters::getInstance().readSharedEventSet() - cycles;

    std::cout << "Cardinality: " << results.size() << ", Total input size: " << n << ", Cycles: " << cycles << std::endl;

    delete[] data;
    delete[] inputGroupBy;
    delete[] inputAggregate;
}

int main() {

/*
    std::string filePath = FilePaths::getInstance().getImdbInputFolderPath() + "title.basics.tsv";
    int n = getLengthOfTsv(filePath);
    auto data = new int[n];
    readImdbStartYearColumnFromBasicsTable(filePath, data);

    int year = 1900;

    long_long cycles;

    {
        auto inputFilter = new int[n];
        copyArray<int>(data, inputFilter, n);
        auto selectedIndexes = new int[n];

        cycles = *Counters::getInstance().readSharedEventSet();
        MABPL::selectIndexesAdaptive(n, inputFilter, selectedIndexes, year);
        cycles = *Counters::getInstance().readSharedEventSet() - cycles;

        std::cout << cycles << std::endl;

        delete[] inputFilter;
        delete[] selectedIndexes;
    }

    {
        auto inputFilter = new int[n];
        copyArray<int>(data, inputFilter, n);
        auto selectedIndexes = new int[n];

        cycles = *Counters::getInstance().readSharedEventSet();
        MABPL::selectIndexesBranch(n, inputFilter, selectedIndexes, year);
        cycles = *Counters::getInstance().readSharedEventSet() - cycles;

        std::cout << cycles << std::endl;

        delete[] inputFilter;
        delete[] selectedIndexes;
    }

    {
        auto inputFilter = new int[n];
        copyArray<int>(data, inputFilter, n);
        auto selectedIndexes = new int[n];

        cycles = *Counters::getInstance().readSharedEventSet();
        MABPL::selectIndexesPredication(n, inputFilter, selectedIndexes, year);
        cycles = *Counters::getInstance().readSharedEventSet() - cycles;

        std::cout << cycles << std::endl;

        delete[] inputFilter;
        delete[] selectedIndexes;
    }
*/










//    runImdbSelectSweepMacroBenchmark(1874, 2023, 1,
//                                     {Select::ImplementationIndexesBranch,
//                                      Select::ImplementationIndexesPredication,
//                                      Select::ImplementationIndexesAdaptive});

//    runImdbPartitionMacroBenchmark_titleIdColumnBasicsTable(5);
//    runImdbPartitionMacroBenchmark_startYearColumnBasicsTable(5);
//    runImdbPartitionMacroBenchmark_personIdColumnPrincipalsTable(5);



/*    std::string machineConstantName = "Partition_minRadixBits";
    radixPartitionBitsSweepBenchmark<uint32_t>(DataFiles::Clustered1mDistribution250mValuesCardinality10mMax250m,
                       {RadixPartition::RadixBitsFixed, RadixPartition::RadixBitsAdaptive},
                       MABPL::MachineConstants::getInstance().getMachineConstant(machineConstantName),
                       MABPL::MachineConstants::getInstance().getMachineConstant(machineConstantName),
                       "TEST",5);*/

//    radixPartitionSweepBenchmark<uint32_t>(DataSweeps::logUniformIntDistribution250mValuesClusteredSweepFixedCardinality10mMax250m,
//                                           {RadixPartition::RadixBitsAdaptive},
//                                           16, "ClusterednessSweep", 5);
//    radixPartitionSweepBenchmark<uint32_t>(DataSweeps::logUniformIntDistribution250mValuesClusteredSweepFixedCardinality10mMax250m,
//                                           {RadixPartition::RadixBitsFixed},
//                                           MABPL::MachineConstants::getInstance().getMachineConstant(machineConstantName),
//                                           "ClusterednessSweep_9", 5);




//    runOisstMacroBenchmark();

//    runImdbPartitionMacroBenchmark1();


//    testRadixSort<int32_t>(DataFiles::uniformIntDistribution20mValuesMax20m, RadixPartition::RadixBitsFixed);

//    testRadixPartition<int32_t>(DataFiles::uniformIntDistribution20mValuesMax20m, RadixPartition::RadixBitsAdaptive);

    // Can test with a maximum partition size of 1 i.e. this will fully sort the data
/*    for (int i = 2; i < 20; i ++) {
        testRadixPartition<int32_t>(DataFiles::uniformIntDistribution20mValuesMax20m, i);
    }
    for (int i = 2; i < 20; i ++) {
        testRadixPartition<int32_t>(DataFiles::fullySortedIntDistribution20mValuesMax20m, i);
    }*/


//    radixPartitionSweepBenchmark<uint32_t>(DataSweeps::logUniformIntDistribution250mValuesClusteredSweepFixedCardinality10mMax250m,
//                                           {RadixPartition::RadixBitsAdaptive},
//                                           16, "ClusterednessSweep_16", 1);


//    radixPartitionSweepBenchmark<uint32_t>(DataSweeps::logUniformIntDistribution250mValuesClusteredSweepFixedCardinality10mMax250m,
//                                           {RadixPartition::RadixBitsFixed, RadixPartition::RadixBitsAdaptive},
//                                           16, "ClusterednessSweep_16", 5);
//    radixPartitionSweepBenchmark<uint32_t>(DataSweeps::logUniformIntDistribution250mValuesClusteredSweepFixedCardinality10mMax250m,
//                                           {RadixPartition::RadixBitsFixed},
//                                           7, "ClusterednessSweep_7", 5);

/*    radixPartitionSweepBenchmark<uint32_t>(DataSweeps::linearUniqueIntDistribution250mValuesSortednessSweep,
                                           {RadixPartition::RadixBitsFixed, RadixPartition::RadixBitsAdaptive},
                                           16, "SortednessSweep_16", 5);
    radixPartitionSweepBenchmark<uint32_t>(DataSweeps::linearUniqueIntDistribution250mValuesSortednessSweep,
                                           {RadixPartition::RadixBitsFixed},
                                           9, "SortednessSweep_9", 5);*/










/*    radixPartitionSweepBenchmark<uint32_t>(DataSweeps::logUniformIntDistribution250mValuesClusteredSweepFixedCardinality10mMax250m,
                                           {RadixPartition::RadixBitsFixed},
                                           10, "ClusterednessSweep_10", 1);
    radixPartitionSweepBenchmark<uint32_t>(DataSweeps::logUniformIntDistribution250mValuesClusteredSweepFixedCardinality10mMax250m,
                                           {RadixPartition::RadixBitsFixed},
                                           9, "ClusterednessSweep_9", 1);
    radixPartitionSweepBenchmark<uint32_t>(DataSweeps::logUniformIntDistribution250mValuesClusteredSweepFixedCardinality10mMax250m,
                                           {RadixPartition::RadixBitsFixed},
                                           8, "ClusterednessSweep_8", 1);
    radixPartitionSweepBenchmark<uint32_t>(DataSweeps::logUniformIntDistribution250mValuesClusteredSweepFixedCardinality10mMax250m,
                                           {RadixPartition::RadixBitsFixed},
                                           7, "ClusterednessSweep_7", 1);



    radixPartitionSweepBenchmark<uint32_t>(DataSweeps::linearUniqueIntDistribution250mValuesSortednessSweep,
                                           {RadixPartition::RadixBitsFixed, RadixPartition::RadixBitsAdaptive},
                                           16, "SortednessSweep_16", 1);
    radixPartitionSweepBenchmark<uint32_t>(DataSweeps::linearUniqueIntDistribution250mValuesSortednessSweep,
                                           {RadixPartition::RadixBitsFixed},
                                           10, "SortednessSweep_10", 1);
    radixPartitionSweepBenchmark<uint32_t>(DataSweeps::linearUniqueIntDistribution250mValuesSortednessSweep,
                                           {RadixPartition::RadixBitsFixed},
                                           9, "SortednessSweep_9", 1);
    radixPartitionSweepBenchmark<uint32_t>(DataSweeps::linearUniqueIntDistribution250mValuesSortednessSweep,
                                           {RadixPartition::RadixBitsFixed},
                                           8, "SortednessSweep_8", 1);
    radixPartitionSweepBenchmark<uint32_t>(DataSweeps::linearUniqueIntDistribution250mValuesSortednessSweep,
                                           {RadixPartition::RadixBitsFixed},
                                           7, "SortednessSweep_7", 1);*/



//    radixPartitionBitsSweepBenchmarkWithExtraCountersConfigurations<uint32_t>(DataFiles::slightlyClusteredDistribution250mValuesCardinality10mMax250m,
//                                                                              RadixPartition::RadixBitsFixed,
//                                                                              4, 20,"4-20_SlightlyClustered_RadixPartition_",1);
//    radixPartitionBitsSweepBenchmarkWithExtraCountersConfigurations<uint32_t>(DataFiles::uniformIntDistribution250mValuesMax250m,
//                                                                              RadixPartition::RadixBitsFixed,
//                                                                             4, 20,"4-20_Random_RadixPartition_",1);
//    radixPartitionBitsSweepBenchmarkWithExtraCountersConfigurations<uint32_t>(DataFiles::fullySortedIntDistribution250mValuesMax250m,
//                                                                              RadixPartition::RadixBitsFixed,
//                                                                             4, 20,"4-20_Sorted_RadixPartition_",1);
//
//    radixPartitionSweepBenchmarkWithExtraCountersConfigurations<uint32_t>(DataSweeps::linearUniqueIntDistribution250mValuesSortednessSweep,
//                                                                          RadixPartition::RadixBitsFixed,
//                                                                         16, "SortednessSweep_16", 1);
//    radixPartitionSweepBenchmarkWithExtraCountersConfigurations<uint32_t>(DataSweeps::logUniformIntDistribution250mValuesClusteredSweepFixedCardinality10mMax250m,
//                                                                          RadixPartition::RadixBitsFixed,
//                                                                         16, "ClusterednessSweep_16", 1);
//    radixPartitionSweepBenchmarkWithExtraCountersConfigurations<uint32_t>(DataSweeps::linearUniformIntDistribution250mValuesClusteredSweepFixedCardinality10mMax250m,
//                                                                          RadixPartition::RadixBitsFixed,
//                                                                         16, "ClusterednessSweep_16", 1);
//
//    radixPartitionSweepBenchmarkWithExtraCountersConfigurations<uint32_t>(DataSweeps::logUniformIntDistribution250mValuesClusteredSweepFixedCardinality10mMax250m,
//                                                                          RadixPartition::RadixBitsFixed,
//                                                                          6, "ClusterednessSweep_6", 1);

/*    unsigned int seed = 1;
    std::mt19937 gen(seed);
    std::uniform_int_distribution<uint32_t> distribution(1, 20000000);

    int n = 70000;
    auto *keys = new uint32_t[n];

    for (auto i = 0; i < n; ++i) {
        keys[i] = distribution(gen);
    }

    std::cout << "Before: " << std::endl;
    for (int i = 0; i < n; i++) {
        std::cout << keys[i] << " ";
    }
    std::cout << std::endl << std::endl;

    MABPL::radixPartitionAdaptive(n, keys);

    std::cout << "After: " << std::endl;
    for (int i = 0; i < n; i++) {
        std::cout << keys[i] << " ";
        if (keys[i-1] > keys[i]) {
            std::cout << "Error - not sorted" << std::endl;
        }
    }
    std::cout << std::endl << std::endl;

    delete[] keys;*/

    return 0;
}