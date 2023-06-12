#include <string>
#include <vector>
#include <memory>

#include "selectBenchmarks.h"
#include "../data_generation/dataGenerators.h"
#include "../library/select.h"
#include "../utils/dataHelpers.h"
#include "../../libs/benchmark/include/benchmark/benchmark.h"


static void BM_select(benchmark::State& state) {
    int selectivity = static_cast<int>(state.range(0));
    auto selectImplementation = static_cast<SelectImplementation>(state.range(1));

    std::string filePath = "/home/jack/CLionProjects/micro-adaptive-bulk-processing-library/data/input/uniformIntDistribution.csv";
//    int numElements = countElements(filePath);
    int numElements = 1000000000;
    std::unique_ptr<int[]> inputData(new int[numElements]);
    std::unique_ptr<int[]> selection(new int[numElements]);
    loadDataToArray(filePath, inputData.get());

    SelectFunctionPtr selectFunctionPtr;
    setSelectFuncPtr(selectFunctionPtr, selectImplementation);

    for (auto _: state) {
        selectFunctionPtr(numElements, inputData.get(), selection.get(), selectivity);
    }
}

void selectBranchTest_1(const std::string& filePath, int numElements) {
//    generateUniformDistributionCSV(filePath, numElements);
    BENCHMARK(BM_select)->Name("SelectBranch")->ArgsProduct({
        benchmark::CreateDenseRange(0,100,10), {SelectImplementation::Branch}});
}

void selectPredicationTest_1(const std::string& filePath, int numElements) {
//    generateUniformDistributionCSV(filePath, numElements);
    BENCHMARK(BM_select)->Name("SelectPredicate")->ArgsProduct({
        benchmark::CreateDenseRange(0,100,10), {SelectImplementation::Predication}});
}

void selectAdaptiveTest_1(const std::string& filePath, int numElements) {
//    generateUniformDistributionCSV(filePath, numElements);
    BENCHMARK(BM_select)->Name("SelectAdaptive")->ArgsProduct({
        benchmark::CreateDenseRange(0,100,10), {SelectImplementation::Adaptive}});
}

void selectBranchTest_2(const std::string& filePath, int numElements) {
    BENCHMARK(BM_select)->Name("SelectBranch")->Iterations(1)->ArgsProduct({
        benchmark::CreateDenseRange(0,50,10), {SelectImplementation::Branch}});
}

void selectPredicationTest_2(const std::string& filePath, int numElements) {
    BENCHMARK(BM_select)->Name("SelectPredication")->Iterations(1)->ArgsProduct({
        benchmark::CreateDenseRange(0,50,10), {SelectImplementation::Predication}});
}

void selectAdaptiveTest_2(const std::string& filePath, int numElements) {
    BENCHMARK(BM_select)->Name("SelectAdaptive")->Iterations(1)->ArgsProduct({
        benchmark::CreateDenseRange(0,50,10), {SelectImplementation::Adaptive}});
}
