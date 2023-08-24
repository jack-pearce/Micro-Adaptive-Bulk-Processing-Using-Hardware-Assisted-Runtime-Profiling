#ifndef MABPL_RADIXPARTITIONCYCLESBENCHMARKIMPLEMENTATION_H
#define MABPL_RADIXPARTITIONCYCLESBENCHMARKIMPLEMENTATION_H

#include "../utilities/dataHelpers.h"
#include "../utilities/papiHelpers.h"
#include "radixPartitionCyclesBenchmark.h"


template<typename T1, typename T2>
void testRadixPartition(const DataFile &dataFile, int radixBits) {
    auto keys = new T1[dataFile.getNumElements()];
    auto payloads = new T2[dataFile.getNumElements()];
    copyArray<T1>(LoadedData<T1>::getInstance(dataFile).getData(), keys, dataFile.getNumElements());
    copyArray<T2>(LoadedData<T2>::getInstance(dataFile).getData(), payloads, dataFile.getNumElements());

    std::cout << "Running test... ";

    MABPL::radixPartition(dataFile.getNumElements(), keys, payloads, radixBits);

    for (int i = 1; i < dataFile.getNumElements(); i++) {
        if (keys[i] < keys[i - 1]) {
            std::cout << "Test Failed! Results are not ordered" << std::endl;
            return;
        }
    }

    delete[] keys;
    delete[] payloads;

    std::cout << "Completed" << std::endl;
}

template<typename T1, typename T2>
void radixPartitionBitsSweepBenchmarkWithExtraCounters(const DataFile &dataFile, int startBits, int endBits,
                                                        std::vector<std::string> &benchmarkCounters,
                                                        const std::string &fileNamePrefix, int iterations) {

    long_long benchmarkCounterValues[benchmarkCounters.size()];
    int benchmarkEventSet = initialisePAPIandCreateEventSet(benchmarkCounters);

    std::vector<std::vector<long_long>> results(endBits - startBits + 1,
                                                std::vector<long_long>((iterations * benchmarkCounters.size()) + 1, 0));

    for (auto radixBits = startBits; radixBits <= endBits; ++radixBits) {
        results[radixBits - startBits][0] = radixBits;

        for (auto i = 0; i < iterations; ++i) {
            auto keys = new T1[dataFile.getNumElements()];
            auto payloads = new T2[dataFile.getNumElements()];
            copyArray<T1>(LoadedData<T1>::getInstance(dataFile).getData(), keys, dataFile.getNumElements());
            copyArray<T2>(LoadedData<T2>::getInstance(dataFile).getData(), payloads, dataFile.getNumElements());

            std::cout << "Running radixBits = " << radixBits << ", iteration = " << i + 1 << "... ";

            if (PAPI_reset(benchmarkEventSet) != PAPI_OK)
                exit(1);

            MABPL::radixPartition(dataFile.getNumElements(), keys, payloads, radixBits);

            if (PAPI_read(benchmarkEventSet, benchmarkCounterValues) != PAPI_OK)
                exit(1);

            delete[] keys;
            delete[] payloads;

            for (int k = 0; k < static_cast<int>(benchmarkCounters.size()); ++k) {
                results[radixBits - startBits][1 + (i * benchmarkCounters.size()) + k] = benchmarkCounterValues[k];
            }

            std::cout << "Completed" << std::endl;
        }
    }

    std::vector<std::string> headers(benchmarkCounters.size() * iterations);
    for (auto i = 0; i < iterations; ++i) {
        std::copy(benchmarkCounters.begin(), benchmarkCounters.end(), headers.begin() + i * benchmarkCounters.size());
    }
    headers.insert(headers.begin(), "RadixBits");

    std::string fileName =
            fileNamePrefix +
            "RadixPartitionRadixBitsSweep_" +
            dataFile.getFileName();
    std::string fullFilePath = FilePaths::getInstance().getRadixPartitionCyclesFolderPath() + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);

    shutdownPAPI(benchmarkEventSet, benchmarkCounterValues);
}


#endif //MABPL_RADIXPARTITIONCYCLESBENCHMARKIMPLEMENTATION_H
