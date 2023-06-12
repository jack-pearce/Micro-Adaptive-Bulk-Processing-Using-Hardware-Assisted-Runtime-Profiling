#include <string>
#include <vector>
#include <memory>

#include "selectTests.h"
#include "../data_generation/data_generator.h"
#include "../library/select.h"
#include "../utils/dataHelpers.h"
#include "../../libs/benchmark/include/benchmark/benchmark.h"


static void BM_select(benchmark::State& state) {
    int selectivity = static_cast<int>(state.range(0));
    auto implementation = static_cast<selectImplementation>(state.range(1));

    std::string filePath = "/home/jack/CLionProjects/micro-adaptive-bulk-processing-library/data/input/uniformIntDistribution.csv";
    int numElements = countElements(filePath);
    std::unique_ptr<int[]> inputData(new int[numElements]);
    std::unique_ptr<int[]> selection(new int[numElements]);
    loadDataToArray(filePath, inputData.get());

    if (implementation == selectImplementation::Branch) {
        for (auto _: state) {
            selectBranch(numElements, inputData.get(), selection.get(), selectivity);
        }
    } else if (implementation == selectImplementation::Predication) {
        for (auto _: state) {
            selectPredication(numElements, inputData.get(), selection.get(), selectivity);
        }
    }
}

void selectBranchTest_1(const std::string& filePath, int numElements) {
    generateUniformDistributionCSV(filePath, numElements);
    BENCHMARK(BM_select)->Name("SelectBranch")->ArgsProduct({
        benchmark::CreateDenseRange(0,100,10), {selectImplementation::Branch}});
}

void selectPredicationTest_1(const std::string& filePath, int numElements) {
    generateUniformDistributionCSV(filePath, numElements);
    BENCHMARK(BM_select)->Name("SelectPredicate")->ArgsProduct({
        benchmark::CreateDenseRange(0,100,10), {selectImplementation::Predication}});
}
