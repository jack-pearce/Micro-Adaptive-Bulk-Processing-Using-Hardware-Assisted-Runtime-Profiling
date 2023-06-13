#include <vector>
#include <memory>

#include "timeBenchmarkSelect.h"
#include "../utils/dataHelpers.h"
#include "../../libs/benchmark/include/benchmark/benchmark.h"

LoadedData* selectedDataFile;

static void selectTimeBenchmarker(benchmark::State& state) {
    int selectivity = static_cast<int>(state.range(0));
    auto selectImplementation = static_cast<SelectImplementation>(state.range(1));

    int numElements = selectedDataFile->getSize();
    int* inputData = selectedDataFile->getData();
    std::unique_ptr<int[]> selection(new int[numElements]);

    SelectFunctionPtr selectFunctionPtr;
    setSelectFuncPtr(selectFunctionPtr, selectImplementation);

    for (auto _: state) {
        selectFunctionPtr(numElements, inputData, selection.get(), selectivity);
    }
}

void selectTimeBenchmark(const DataFile &dataFile, SelectImplementation selectImplementation, int sensitivityStride) {
    selectedDataFile = &LoadedData::getInstance(dataFile.getFilepath(), dataFile.getNumElements());
    BENCHMARK(selectTimeBenchmarker)->Name(getName(selectImplementation))->ArgsProduct({
        benchmark::CreateDenseRange(0,100,sensitivityStride), {selectImplementation}});
}

void
selectTimeBenchmarkSetIterations(const DataFile &dataFile, SelectImplementation selectImplementation,
                                 int sensitivityStride, int iterations) {
    selectedDataFile = &LoadedData::getInstance(dataFile.getFilepath(), dataFile.getNumElements());
    BENCHMARK(selectTimeBenchmarker)->Name(getName(selectImplementation))->Iterations(iterations)->ArgsProduct({
        benchmark::CreateDenseRange(0,100,sensitivityStride), {selectImplementation}});
}

