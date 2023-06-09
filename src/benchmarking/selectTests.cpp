#include <string>
#include <vector>

#include "selectTests.h"
#include "../data_generation/data_generator.h"
#include "../library/select.h"
#include "../utils/dataHelpers.h"
#include "../../libs/benchmark/include/benchmark/benchmark.h"


int BM_selectSetup(const std::string& filePath, std::vector<int>& inputData, std::vector<int>& selection) {
    loadDataToVector(filePath, inputData);
    selection.reserve(inputData.size());
    return static_cast<int>(inputData.size());
}

static void BM_select(benchmark::State& state) {
    int selectivity = static_cast<int>(state.range(0));
    auto implementation = static_cast<selectImplementation>(state.range(1));

    if (implementation == selectImplementation::Branch) {
        for (auto _: state) {
            state.PauseTiming();

            std::string filePath = "/home/jack/CLionProjects/micro-adaptive-bulk-processing-library/data/uniformIntDistribution.csv";
            std::vector<int> inputData;
            std::vector<int> selection;
            int elements = BM_selectSetup(filePath, inputData, selection);

            state.ResumeTiming();

            selectBranch(elements, inputData, selection, selectivity);
        }
    } else if (implementation == selectImplementation::Predication) {
        for (auto _: state) {
            state.PauseTiming();

            std::string filePath = "/home/jack/CLionProjects/micro-adaptive-bulk-processing-library/data/uniformIntDistribution.csv";
            std::vector<int> inputData;
            std::vector<int> selection;
            int elements = BM_selectSetup(filePath, inputData, selection);

            state.ResumeTiming();

            selectPredication(elements, inputData, selection, selectivity);
        }
    }
}

void selectBranchTest_1() {
    std::string filePath = "/home/jack/CLionProjects/micro-adaptive-bulk-processing-library/data/uniformIntDistribution.csv";
    int numElements = 4000000 / sizeof(int);

    generateUniformDistributionCSV(filePath, numElements);
    BENCHMARK(BM_select)->Name("SelectBranch")->ArgsProduct({
        benchmark::CreateDenseRange(0,100,10), {selectImplementation::Branch}});
}

void selectPredicationTest_1() {
    std::string filePath = "/home/jack/CLionProjects/micro-adaptive-bulk-processing-library/data/uniformIntDistribution.csv";
    int numElements = 4000000 / sizeof(int);

    generateUniformDistributionCSV(filePath, numElements);
    BENCHMARK(BM_select)->Name("SelectPredicate")->ArgsProduct({
        benchmark::CreateDenseRange(0,100,10), {selectImplementation::Predication}});
}
