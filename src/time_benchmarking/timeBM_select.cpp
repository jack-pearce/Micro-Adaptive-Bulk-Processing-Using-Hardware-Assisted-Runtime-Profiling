#include <string>
#include <vector>
#include <memory>

#include "timeBM_select.h"
#include "../library/select.h"
#include "../utils/dataHelpers.h"
#include "../../libs/benchmark/include/benchmark/benchmark.h"
#include "../data_generation/dataFiles.h"


class timeBM_setup {
private:
    std::unique_ptr<int[]> inputData;
    static timeBM_setup *instance;
    timeBM_setup(const std::string& filePath, int numElements) {
        inputData = std::make_unique<int[]>(numElements);
        loadDataToArray(filePath, inputData.get());
    }

public:
    static timeBM_setup &getInstance(const std::string& filePath, int numElements) {
        if (instance == nullptr) {
            instance = new timeBM_setup(filePath, numElements);
        }
        return *instance;
    }

    int *getInputData() {
        return inputData.get();
    }
};

timeBM_setup *timeBM_setup::instance = nullptr;

static void timeBM_select(benchmark::State& state) {
    int selectivity = static_cast<int>(state.range(0));
    auto selectImplementation = static_cast<SelectImplementation>(state.range(1));

    int numElements = 1000000000 / sizeof(int);
    std::unique_ptr<int[]> selection(new int[numElements]);

    int* inputData = timeBM_setup::getInstance(uniformInstDistribution250mValues, numElements).getInputData();

    SelectFunctionPtr selectFunctionPtr;
    setSelectFuncPtr(selectFunctionPtr, selectImplementation);

    for (auto _: state) {
        selectFunctionPtr(numElements, inputData, selection.get(), selectivity);
    }
}

void selectBranchTimeBM_1() {
    BENCHMARK(timeBM_select)->Name("SelectBranch")->ArgsProduct({
        benchmark::CreateDenseRange(0,100,10), {SelectImplementation::Branch}});
}

void selectPredicationTimeBM_1() {
    BENCHMARK(timeBM_select)->Name("SelectPredicate")->ArgsProduct({
        benchmark::CreateDenseRange(0,100,10), {SelectImplementation::Predication}});
}

void selectAdaptiveTimeBM_1() {
    BENCHMARK(timeBM_select)->Name("SelectAdaptive")->ArgsProduct({
        benchmark::CreateDenseRange(0,100,10), {SelectImplementation::Adaptive}});
}

void selectBranchTimeBM_2() {
    BENCHMARK(timeBM_select)->Name("SelectBranch")->Iterations(10)->ArgsProduct({
        benchmark::CreateDenseRange(0,100,10), {SelectImplementation::Branch}});
}

void selectPredicationTimeBM_2() {
    BENCHMARK(timeBM_select)->Name("SelectPredication")->Iterations(10)->ArgsProduct({
        benchmark::CreateDenseRange(0,100,10), {SelectImplementation::Predication}});
}

void selectAdaptiveTimeBM_2() {
    BENCHMARK(timeBM_select)->Name("SelectAdaptive")->Iterations(10)->ArgsProduct({
        benchmark::CreateDenseRange(0,100,10), {SelectImplementation::Adaptive}});
}
