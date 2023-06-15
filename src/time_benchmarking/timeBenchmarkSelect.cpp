#include <vector>
#include <memory>

#include "timeBenchmarkSelect.h"
#include "../utils/dataHelpers.h"
#include "../../libs/benchmark/include/benchmark/benchmark.h"

LoadedData* loadedDataFile;

static void selectTimeBenchmarker(benchmark::State& state) {
    int selectivity = static_cast<int>(state.range(0));
    auto selectImplementation = static_cast<SelectImplementation>(state.range(1));

    int numElements = loadedDataFile->getDataFile().getNumElements();
    int* inputData = loadedDataFile->getData();
    std::unique_ptr<int[]> selection(new int[numElements]);

    SelectFunctionPtr selectFunctionPtr;
    setSelectFuncPtr(selectFunctionPtr, selectImplementation);

    for (auto _: state) {
        selectFunctionPtr(numElements, inputData, selection.get(), selectivity);
    }
}

void selectTimeBenchmark(const DataFile &dataFile, SelectImplementation selectImplementation, int sensitivityStride) {
    loadedDataFile = &LoadedData::getInstance(dataFile);
    BENCHMARK(selectTimeBenchmarker)->Name(getName(selectImplementation))->ArgsProduct({
        benchmark::CreateDenseRange(0,100,sensitivityStride), {selectImplementation}});
}

void
selectTimeBenchmarkSetIterations(const DataFile &dataFile, SelectImplementation selectImplementation,
                                 int sensitivityStride, int iterations) {
    loadedDataFile = &LoadedData::getInstance(dataFile);
    BENCHMARK(selectTimeBenchmarker)->Name(getName(selectImplementation))->Iterations(iterations)->ArgsProduct({
        benchmark::CreateDenseRange(0,100,sensitivityStride), {selectImplementation}});
}

