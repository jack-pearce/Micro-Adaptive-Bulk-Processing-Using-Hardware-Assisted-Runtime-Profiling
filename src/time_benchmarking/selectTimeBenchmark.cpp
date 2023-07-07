#include <vector>
#include <memory>

#include "selectTimeBenchmark.h"
#include "../utilities/dataHelpers.h"
#include "../../libs/benchmark/include/benchmark/benchmark.h"

LoadedData* loadedDataFile;

static void selectTimeBenchmarker(benchmark::State& state) {
    auto selectivity = static_cast<int>(state.range(0));
    auto selectImplementation = static_cast<Select>(state.range(1));

    auto numElements = loadedDataFile->getDataFile().getNumElements();
    auto inputData = loadedDataFile->getData();
    auto inputFilter = loadedDataFile->getData();
    auto selection = std::make_unique<int[]>(numElements);

    for (auto _: state) {
        MABPL::runSelectFunction<int>(selectImplementation, numElements, inputData,
                          inputFilter, selection.get(), selectivity);
    }
}

void selectTimeBenchmark(const DataFile &dataFile, Select selectImplementation, int selectivityStride) {
    loadedDataFile = &LoadedData::getInstance(dataFile);
    BENCHMARK(selectTimeBenchmarker)
    ->Name(getSelectName(selectImplementation))
    ->ArgsProduct({benchmark::CreateDenseRange(0, 100, selectivityStride),
                   {selectImplementation}});
}

void selectTimeBenchmarkSetIterations(const DataFile &dataFile, Select selectImplementation,
                                      int selectivityStride, int iterations) {
    loadedDataFile = &LoadedData::getInstance(dataFile);
    BENCHMARK(selectTimeBenchmarker)
    ->Name(getSelectName(selectImplementation))
    ->Iterations(iterations)
    ->ArgsProduct({benchmark::CreateDenseRange(0, 100, selectivityStride),
                   {selectImplementation}});
}

