#include <iostream>
#include <functional>
#include <vector>
#include <string>

#include "../utils/dataHelpers.h"
#include "../utils/papiHelpers.h"
#include "counterBenchmark.h"


void selectBenchmarkWithExtraCounters(const DataFile& dataFile,
                                      SelectImplementation selectImplementation,
                                      int sensitivityStride,
                                      int iterations,
                                      std::vector<std::string>& benchmarkCounters) {
    if (selectImplementation == SelectImplementation::Adaptive)
        std::cout << "Cannot benchmark adaptive select using counters as adaptive select is already using these counters" << std::endl;

    int numTests = 1 + (100 / sensitivityStride);

    long_long benchmarkCounterValues[benchmarkCounters.size()];
    int benchmarkEventSet = initialisePAPIandCreateEventSet(benchmarkCounters);

    std::vector<std::vector<long_long>> results(numTests, std::vector<long_long>((iterations * benchmarkCounters.size()) + 1, 0));
    int count = 0;

    SelectFunctionPtr selectFunctionPtr;
    setSelectFuncPtr(selectFunctionPtr, selectImplementation);

    for (int i = 0; i <= 100; i += sensitivityStride) {
        results[count][0] = static_cast<long_long>(i);

        for (int j = 0; j < iterations; ++j) {
            int* inputData = new int[dataFile.getNumElements()];
            int* selection = new int[dataFile.getNumElements()];
            copyArray(LoadedData::getInstance(dataFile).getData(), inputData, dataFile.getNumElements());

            std::cout << "Running sensitivity " << i << ", iteration " << j + 1 << "... ";

            if (PAPI_reset(benchmarkEventSet) != PAPI_OK)
                exit(1);

            selectFunctionPtr(dataFile.getNumElements(), inputData, selection, i);

            if (PAPI_read(benchmarkEventSet, benchmarkCounterValues) != PAPI_OK)
                exit(1);

            delete[] inputData;
            delete[] selection;

            for (int k = 0; k < static_cast<int>(benchmarkCounters.size()); ++k) {
                results[count][1 + (j * benchmarkCounters.size()) + k] = benchmarkCounterValues[k];
            }

            std::cout << "Completed" << std::endl;
        }

        count++;
    }

    std::vector<std::string> headers(benchmarkCounters.size() * iterations);
    for (int i = 0; i < iterations; ++i) {
        std::copy(benchmarkCounters.begin(), benchmarkCounters.end(), headers.begin() + i * benchmarkCounters.size());
    }
    headers.insert(headers.begin(), "Sensitivity");

    std::string fileName =
            getName(selectImplementation) +
            "_cyclesBM_" +
            dataFile.getFileName() +
            "_sensitivity_stride_" +
            std::to_string(sensitivityStride);
    std::string fullFilePath = outputFilePath + selectCyclesFolder + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);

    shutdownPAPI(benchmarkEventSet, benchmarkCounterValues);
}

void selectCpuCyclesMultipleBenchmarks(const DataFile& dataFile,
                                       SelectImplementation selectImplementation,
                                       int sensitivityStride,
                                       int iterations) {
    int numTests = 1 + (100 / sensitivityStride);

    long_long cycles;
    std::vector<std::vector<long_long>> results(numTests, std::vector<long_long>(iterations + 1, 0));
    int count = 0;

    SelectFunctionPtr selectFunctionPtr;
    setSelectFuncPtr(selectFunctionPtr, selectImplementation);

    for (int i = 0; i <= 100; i += sensitivityStride) {
        results[count][0] = static_cast<long_long>(i);

        for (int j = 0; j < iterations; ++j) {
            int* inputData = new int[dataFile.getNumElements()];
            int* selection = new int[dataFile.getNumElements()];
            copyArray(LoadedData::getInstance(dataFile).getData(), inputData, dataFile.getNumElements());

            std::cout << "Running sensitivity " << i << ", iteration " << j + 1 << "... ";

            cycles = *Counters::getInstance().readEventSet();

            selectFunctionPtr(dataFile.getNumElements(), inputData, selection, i);

            results[count][1 + j] = *Counters::getInstance().readEventSet() - cycles;

            delete[] inputData;
            delete[] selection;

            std::cout << "Completed" << std::endl;
        }

        count++;
    }

    std::vector<std::string> headers(1 + iterations, "PERF_COUNT_HW_CPU_CYCLES");
    headers [0] = "Sensitivity";

    std::string fileName =
            getName(selectImplementation) +
            "_cyclesBM_usingCounterSingleton_" +
            dataFile.getFileName() +
            "_sensitivity_stride_" +
            std::to_string(sensitivityStride);
    std::string fullFilePath = outputFilePath + selectCyclesFolder + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);
}


void selectCpuCyclesSingleBenchmark(const DataFile &dataFile, SelectImplementation selectImplementation, int iterations,
                                    int threshold) {
    int numTests = 1;


    long_long cycles;
    std::vector<std::vector<long_long>> results(numTests, std::vector<long_long>(iterations + 1, 0));
    int count = 0;

    SelectFunctionPtr selectFunctionPtr;
    setSelectFuncPtr(selectFunctionPtr, selectImplementation);

    results[count][0] = static_cast<long_long>(threshold);

    for (int j = 0; j < iterations; ++j) {
        int* inputData = new int[dataFile.getNumElements()];
        int* selection = new int[dataFile.getNumElements()];
        copyArray(LoadedData::getInstance(dataFile).getData(), inputData, dataFile.getNumElements());

        std::cout << "Running sensitivity " << threshold << ", iteration " << j + 1 << "... ";

        cycles = *Counters::getInstance().readEventSet();

        selectFunctionPtr(dataFile.getNumElements(), inputData, selection, threshold);

        results[count][1 + j] = *Counters::getInstance().readEventSet() - cycles;

        delete[] inputData;
        delete[] selection;

        std::cout << "Completed" << std::endl;
    }

    count++;

    std::vector<std::string> headers(1 + iterations, "PERF_COUNT_HW_CPU_CYCLES");
    headers [0] = "Sensitivity";

    std::string fileName =
            getName(selectImplementation) +
            "_cyclesBM_usingCounterSingleton_" +
            dataFile.getFileName() +
            "_threshold_" +
            std::to_string(threshold);
    std::string fullFilePath = outputFilePath + selectCyclesFolder + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);
}



