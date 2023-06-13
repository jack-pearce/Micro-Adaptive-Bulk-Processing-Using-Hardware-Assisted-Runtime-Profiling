#include <iostream>
#include <functional>
#include <vector>
#include <string>
#include <memory>

#include "../utils/dataHelpers.h"
#include "../utils/papiHelpers.h"
#include "counterBM.h"
#include "../data_generation/dataFiles.h"


void selectCounterBM(SelectImplementation selectImplementation, int numElements, int sensitivityStride,
                     std::vector<std::string> counters, const std::string& fileName) {
    std::string filePath = uniformIntDistribution250mValues;
    int numTests = 1 + (100 / sensitivityStride);

    std::unique_ptr<int[]> inputData(new int[numElements]);
    std::unique_ptr<int[]> selection(new int[numElements]);
    loadDataToArray(filePath, inputData.get());

    long_long counterValues[counters.size()];
    int EventSet = initialisePAPIandCreateEventSet(counters);

    std::vector<std::vector<long_long>> results(numTests, std::vector<long_long>(counters.size() + 1, 0));
    int count = 0;

    SelectFunctionPtr selectFunctionPtr;
    setSelectFuncPtr(selectFunctionPtr, selectImplementation);

    for (int i = 0; i <= 100; i += sensitivityStride) {
        if (PAPI_reset(EventSet) != PAPI_OK)
            exit(1);

        selectFunctionPtr(numElements, inputData.get(), selection.get(), i);

        if (PAPI_read(EventSet, counterValues) != PAPI_OK)
            exit(1);

        results[count][0] = static_cast<long_long>(i);
        for (int j = 0; j < static_cast<int>(counters.size()); ++j) {
            results[count][j + 1] = counterValues[j];
        }
        count++;
    }

    std::vector<std::string> headers(counters);
    headers.insert(headers.begin(), "Sensitivity");
    std::string outputFilePath = "/home/jack/CLionProjects/micro-adaptive-bulk-processing-library/data/output/";
    std::string outputFileName = "Sensitivity_Output_";
    std::string outputFullFilePath = outputFilePath + outputFileName + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, outputFullFilePath);

    shutdownPAPI(EventSet);
}

void selectCounterBM_2(SelectImplementation selectImplementation, int numElements, int sensitivityStride) {
    std::string filePath = uniformIntDistribution250mValues;

    std::unique_ptr<int[]> inputData(new int[numElements]);
    std::unique_ptr<int[]> selection(new int[numElements]);
    loadDataToArray(filePath, inputData.get());

    long_long start_cycles, end_cycles, start_usec, end_usec;

    SelectFunctionPtr selectFunctionPtr;
    setSelectFuncPtr(selectFunctionPtr, selectImplementation);

    for (int i = 0; i <= 100; i += sensitivityStride) {
        start_cycles = PAPI_get_real_cyc();
        start_usec = PAPI_get_real_usec();

        selectFunctionPtr(numElements, inputData.get(), selection.get(), i);

        end_cycles = PAPI_get_real_cyc();
        end_usec = PAPI_get_real_usec();

        printf("Wall clock cycles: %lld\n", end_cycles - start_cycles);
        printf("Wall clock time in microseconds: %lld\n", end_usec - start_usec);
    }
}

void selectCyclesCounterBM(SelectImplementation selectImplementation, int numElements,
                           int sensitivityStride, const std::string& fileName, int iterations) {
    std::string filePath = uniformIntDistribution25kValues;
    int numTests = 1 + (100 / sensitivityStride);

    // Try adding in other CPU cycle counter
    std::vector<std::string> benchmarkCounters = {"PERF_COUNT_HW_CPU_CYCLES"};

    std::unique_ptr<int[]> selection(new int[numElements]);
    int* inputData = LoadedData::getInstance(filePath, numElements).getData();

    long_long benchmarkCounterValues[benchmarkCounters.size()];
    int benchmarkEventSet = initialisePAPIandCreateEventSet(benchmarkCounters);

    std::vector<std::vector<long_long>> results(numTests, std::vector<long_long>((iterations * benchmarkCounters.size()) + 1, 0));
    int count = 0;

    SelectFunctionPtr selectFunctionPtr;
    setSelectFuncPtr(selectFunctionPtr, selectImplementation);

    for (int i = 0; i <= 100; i += sensitivityStride) {
        results[count][0] = static_cast<long_long>(i);

        for (int j = 0; j < iterations; ++j) {
            std::cout << "Running sensitivity " << i << ", iteration " << j + 1 << "... ";

            if (PAPI_reset(benchmarkEventSet) != PAPI_OK)
                exit(1);

            selectFunctionPtr(numElements, inputData, selection.get(), i);

            if (PAPI_read(benchmarkEventSet, benchmarkCounterValues) != PAPI_OK)
                exit(1);

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
    std::string outputFilePath = "/home/jack/CLionProjects/micro-adaptive-bulk-processing-library/data/output/";
    std::string outputFileName = "select_cycles_benchmark";
    std::string outputFullFilePath = outputFilePath + outputFileName + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, outputFullFilePath);

    shutdownPAPI(benchmarkEventSet);
}