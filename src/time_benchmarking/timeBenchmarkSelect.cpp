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
    int* inputFilter = loadedDataFile->getData();
    std::unique_ptr<int[]> selection(new int[numElements]);

//    SelectIndexesFunctionPtr<int> selectFunctionPtr;
//    setSelectIndexesFuncPtr(selectFunctionPtr, selectImplementation);

    for (auto _: state) {
//        selectFunctionPtr(numElements, inputData, selection.get(), selectivity);
        runSelectFunction<int>(selectImplementation, numElements, inputData,
                          inputFilter, selection.get(), selectivity);
    }
}

void selectTimeBenchmark(const DataFile &dataFile, SelectImplementation selectImplementation, int selectivityStride) {
    loadedDataFile = &LoadedData::getInstance(dataFile);
    BENCHMARK(selectTimeBenchmarker)
    ->Name(getSelectName(selectImplementation))
    ->ArgsProduct({benchmark::CreateDenseRange(0, 100, selectivityStride),
                   {selectImplementation}});
}

void selectTimeBenchmarkSetIterations(const DataFile &dataFile, SelectImplementation selectImplementation,
                                 int selectivityStride, int iterations) {
    loadedDataFile = &LoadedData::getInstance(dataFile);
    BENCHMARK(selectTimeBenchmarker)
    ->Name(getSelectName(selectImplementation))
    ->Iterations(iterations)
    ->ArgsProduct({benchmark::CreateDenseRange(0, 100, selectivityStride),
                   {selectImplementation}});
}

