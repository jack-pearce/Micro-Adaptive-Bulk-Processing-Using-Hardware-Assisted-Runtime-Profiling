#ifndef HAQP_PARTITIONCYCLESBENCHMARKIMPLEMENTATION_H
#define HAQP_PARTITIONCYCLESBENCHMARKIMPLEMENTATION_H

#include <limits>

#include "utilities/dataHelpers.h"
#include "utilities/papiHelpers.h"
#include "partitionCyclesBenchmark.h"
#include "data_generation/dataFiles.h"


template<typename T>
void testSort(const DataFile &dataFile, PartitionOperators partitionImplementation, int radixBits) {
    auto keys = new T[dataFile.getNumElements()];
    copyArray<T>(LoadedData<T>::getInstance(dataFile).getData(), keys, dataFile.getNumElements());

    std::cout << "Running test... ";

    HAQP::runPartitionFunction(partitionImplementation, dataFile.getNumElements(), keys, radixBits);

    for (int i = 1; i < dataFile.getNumElements(); i++) {
        if (keys[i] < keys[i - 1]) {
            std::cout << "Test Failed! Results are not ordered" << std::endl;
            return;
        }
    }

    delete[] keys;

    std::cout << "Completed" << std::endl;
}

template<typename T>
void testPartition(const DataFile &dataFile, PartitionOperators partitionImplementation, int radixBits) {
    auto keys = new T[dataFile.getNumElements()];
    copyArray<T>(LoadedData<T>::getInstance(dataFile).getData(), keys, dataFile.getNumElements());

    std::cout << "Running test... ";

    std::vector<int> partitions = HAQP::runPartitionFunction(partitionImplementation, dataFile.getNumElements(),
                                                              keys, radixBits);

    int previous = 0;
    int i;
    T smallest, largest;
    T prevLargest = std::numeric_limits<T>::min();

    for (int partition : partitions) {
        smallest = std::numeric_limits<T>::max();
        largest = std::numeric_limits<T>::min();

        for (i = previous; i < partition; ++i) {
            smallest = std::min(smallest, keys[i]);
            largest = std::max(largest, keys[i]);
        }

        if (smallest <= prevLargest) {
            std::cout << "Test Failed! Overlapping partition found!" << std::endl;
        }

        prevLargest = largest;
        previous = partition;
    }

    delete[] keys;

    std::cout << "Completed" << std::endl;
}

template<typename T>
void partitionBitsSweepBenchmarkWithExtraCounters(const DataFile &dataFile,
                                                  PartitionOperators partitionImplementation,
                                                  int startBits, int endBits,
                                                  std::vector<std::string> &benchmarkCounters,
                                                  const std::string &fileNamePrefix, int iterations) {
    if (partitionImplementation == PartitionOperators::RadixBitsAdaptive) {
        std::cout
                << "Cannot benchmark adaptive partition using counters as algorithm is already using these counters"
                << std::endl;
        exit(1);
    }

    long_long benchmarkCounterValues[benchmarkCounters.size()];
    int benchmarkEventSet = initialisePAPIandCreateEventSet(benchmarkCounters);

    std::vector<std::vector<long_long>> results(endBits - startBits + 1,
                                                std::vector<long_long>((iterations * benchmarkCounters.size()) + 1, 0));

    for (auto radixBits = startBits; radixBits <= endBits; ++radixBits) {
        results[radixBits - startBits][0] = radixBits;

        for (auto i = 0; i < iterations; ++i) {
            auto keys = new T[dataFile.getNumElements()];
            copyArray<T>(LoadedData<T>::getInstance(dataFile).getData(), keys, dataFile.getNumElements());

            std::cout << "Running radixBitsOperator = " << radixBits << ", iteration = " << i + 1 << "... ";

            if (PAPI_reset(benchmarkEventSet) != PAPI_OK)
                exit(1);

            HAQP::runPartitionFunction(partitionImplementation, dataFile.getNumElements(), keys, radixBits);

            if (PAPI_read(benchmarkEventSet, benchmarkCounterValues) != PAPI_OK)
                exit(1);

            delete[] keys;

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
            "PartitionRadixBitsSweep_" +
            dataFile.getFileName();
    std::string fullFilePath = FilePaths::getInstance().getPartitionCyclesFolderPath() + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);

    shutdownPAPI(benchmarkEventSet, benchmarkCounterValues);
}

template<typename T>
void partitionSweepBenchmarkWithExtraCounters(DataSweep &dataSweep,
                                              PartitionOperators partitionImplementation, int radixBits,
                                              std::vector<std::string> &benchmarkCounters,
                                              const std::string &fileNamePrefix, int iterations) {
    if (partitionImplementation == PartitionOperators::RadixBitsAdaptive) {
        std::cout
                << "Cannot benchmark adaptive partition using counters as algorithm is already using these counters"
                << std::endl;
        exit(1);
    }

    long_long benchmarkCounterValues[benchmarkCounters.size()];
    int benchmarkEventSet = initialisePAPIandCreateEventSet(benchmarkCounters);

    std::vector<std::vector<double>> results(dataSweep.getTotalRuns(),
                                                std::vector<double>((iterations * benchmarkCounters.size()) + 1, 0));

    int n = dataSweep.getNumElements();

    for (auto runNum = 0; runNum < dataSweep.getTotalRuns(); ++runNum) {
        results[runNum][0] = static_cast<double>(dataSweep.getRunInput());
        auto data = new T[n];
        dataSweep.loadNextDataSetIntoMemory<T>(data);

        for (auto iteration = 0; iteration < iterations; ++iteration) {
            auto keys = new T[n];
            copyArray<T>(data, keys, n);

            std::cout << "Running input " << results[runNum][0] << ", iteration " << iteration + 1 << "... ";

            if (PAPI_reset(benchmarkEventSet) != PAPI_OK)
                exit(1);

            HAQP::runPartitionFunction(partitionImplementation, n, keys, radixBits);

            if (PAPI_read(benchmarkEventSet, benchmarkCounterValues) != PAPI_OK)
                exit(1);

            delete[] keys;

            for (int i = 0; i < static_cast<int>(benchmarkCounters.size()); ++i) {
                results[runNum][1 + (iteration * benchmarkCounters.size()) + i] =
                        static_cast<double>(benchmarkCounterValues[i]);
            }

            std::cout << "Completed" << std::endl;
        }

        delete[] data;
    }

    dataSweep.restartSweep();

    std::vector<std::string> headers(benchmarkCounters.size() * iterations);
    for (auto i = 0; i < iterations; ++i) {
        std::copy(benchmarkCounters.begin(), benchmarkCounters.end(), headers.begin() + i * benchmarkCounters.size());
    }
    headers.insert(headers.begin(), "Input");

    std::string fileName =
            fileNamePrefix +
            "PartitionSweep_" +
            dataSweep.getSweepName();
    std::string fullFilePath = FilePaths::getInstance().getPartitionCyclesFolderPath() + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);

    shutdownPAPI(benchmarkEventSet, benchmarkCounterValues);
}

template<typename T>
void partitionBitsSweepBenchmarkWithExtraCountersConfigurations(const DataFile &dataFile,
                                                                PartitionOperators partitionImplementation,
                                                                int startBits, int endBits,
                                                                const std::string &fileNamePrefix,
                                                                int iterations) {
    {
        std::vector<std::string> benchmarkCounters = {"PERF_COUNT_HW_CPU_CYCLES",
                                                      "INSTRUCTION_RETIRED",
                                                      "DTLB-STORE-MISSES",
                                                      "DTLB-STORES",
                                                      "DTLB-LOAD-MISSES",
                                                      "PERF_COUNT_HW_CACHE_L1D",
                                                      "PERF_COUNT_HW_CACHE_REFERENCES",
                                                      "PERF_COUNT_HW_CACHE_MISSES"};

        partitionBitsSweepBenchmarkWithExtraCounters<T>(dataFile, partitionImplementation, startBits, endBits,
                                                        benchmarkCounters, fileNamePrefix, iterations);
    }

/*    {
        std::vector<std::string> benchmarkCounters = {"PERF_COUNT_HW_CPU_CYCLES",
                                                      "DTLB-STORES",
                                                      "DTLB-STORE-MISSES",
                                                      "DTLB-LOADS",
                                                      "DTLB-LOAD-MISSES"};

        partitionBitsSweepBenchmarkWithExtraCounters<T>(dataFile, partitionImplementation, startBits, endBits,
                                                            benchmarkCounters, fileNamePrefix + "1_", iterations);
    }*/

/*    {
        std::vector<std::string> benchmarkCounters = {"PERF_COUNT_HW_CPU_CYCLES",
                                                      "INSTRUCTION_RETIRED",
                                                      "PERF_COUNT_HW_CACHE_L1D",
                                                      "PERF_COUNT_HW_CACHE_REFERENCES",
                                                      "PERF_COUNT_HW_CACHE_MISSES"};

        partitionBitsSweepBenchmarkWithExtraCounters<T>(dataFile, partitionImplementation, startBits, endBits,
                                                            benchmarkCounters, fileNamePrefix + "2_", iterations);
    }*/
}

template<typename T>
void partitionSweepBenchmarkWithExtraCountersConfigurations(DataSweep &dataSweep,
                                                            PartitionOperators partitionImplementation,
                                                            int radixBits, const std::string &fileNamePrefix,
                                                            int iterations) {
    {
        std::vector<std::string> benchmarkCounters = {"PERF_COUNT_HW_CPU_CYCLES",
                                                      "INSTRUCTION_RETIRED",
                                                      "DTLB-STORE-MISSES",
                                                      "DTLB-STORES",
                                                      "DTLB-LOAD-MISSES",
                                                      "PERF_COUNT_HW_CACHE_L1D",
                                                      "PERF_COUNT_HW_CACHE_REFERENCES",
                                                      "PERF_COUNT_HW_CACHE_MISSES"};

        partitionSweepBenchmarkWithExtraCounters<T>(dataSweep, partitionImplementation, radixBits,
                                                    benchmarkCounters, fileNamePrefix, iterations);
    }

/*    {
        std::vector<std::string> benchmarkCounters = {"PERF_COUNT_HW_CPU_CYCLES",
                                                      "DTLB-STORES",
                                                      "DTLB-STORE-MISSES",
                                                      "DTLB-LOADS",
                                                      "DTLB-LOAD-MISSES"};

        partitionSweepBenchmarkWithExtraCounters<T>(dataSweep, partitionImplementation, radixBitsOperator,
                                                        benchmarkCounters, fileNamePrefix + "1_", iterations);
    }

    {
        std::vector<std::string> benchmarkCounters = {"PERF_COUNT_HW_CPU_CYCLES",
                                                      "INSTRUCTION_RETIRED",
                                                      "PERF_COUNT_HW_CACHE_L1D",
                                                      "PERF_COUNT_HW_CACHE_REFERENCES",
                                                      "PERF_COUNT_HW_CACHE_MISSES"};

        partitionSweepBenchmarkWithExtraCounters<T>(dataSweep, partitionImplementation, radixBitsOperator,
                                                        benchmarkCounters, fileNamePrefix + "2_", iterations);
    }*/
}

template<typename T>
void partitionBitsSweepBenchmark(const DataFile &dataFile, const std::vector<PartitionOperators> &partitionImplementations,
                                 int startBits, int endBits, const std::string &fileNamePrefix, int iterations) {

    int dataCols = iterations * static_cast<int>(partitionImplementations.size());
    long_long cycles;
    std::vector<std::vector<long_long>> results(endBits - startBits + 1,
                                                std::vector<long_long>(dataCols + 1, 0));

    for (auto radixBits = startBits; radixBits <= endBits; ++radixBits) {
        results[radixBits - startBits][0] = radixBits;

        for (auto iteration = 0; iteration < iterations; ++iteration) {

            for (size_t j = 0; j < partitionImplementations.size(); ++j) {
                auto keys = new T[dataFile.getNumElements()];
                copyArray<T>(LoadedData<T>::getInstance(dataFile).getData(), keys,
                             dataFile.getNumElements());

                std::cout << "Running radixBitsOperator = " << radixBits << ", iteration = " << iteration + 1;
                std::cout << ", implementation = ";
                std::cout << getPartitionName(partitionImplementations[j]) << "... ";

                cycles = *Counters::getInstance().readSharedEventSet();

                HAQP::runPartitionFunction(partitionImplementations[j],
                                            dataFile.getNumElements(), keys, radixBits);

                results[radixBits - startBits][1 + (iteration * partitionImplementations.size()) + j] =
                        *Counters::getInstance().readSharedEventSet() - cycles;

                delete[] keys;

                std::cout << "Completed" << std::endl;
            }
        }
    }

    std::vector<std::string> headers(1 + dataCols);
    headers [0] = "RadixBits";
    for (auto i = 0; i < dataCols; ++i) {
        headers[1 + i] = getPartitionName(partitionImplementations[i % partitionImplementations.size()]);
    }

    std::string fileName =
            fileNamePrefix +
            "PartitionRadixBitsSweep_" +
            dataFile.getFileName();
    std::string fullFilePath = FilePaths::getInstance().getPartitionCyclesFolderPath() + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);
}

template<typename T>
void partitionSweepBenchmark(DataSweep &dataSweep, const std::vector<PartitionOperators> &partitionImplementations,
                             int radixBits, const std::string &fileNamePrefix, int iterations) {

    int dataCols = iterations * static_cast<int>(partitionImplementations.size());
    long_long cycles;
    std::vector<std::vector<double>> results(dataSweep.getTotalRuns(),
                                             std::vector<double>(dataCols + 1, 0));

    int n = dataSweep.getNumElements();

    for (auto runNum = 0; runNum < dataSweep.getTotalRuns(); ++runNum) {

        results[runNum][0] = static_cast<double>(dataSweep.getRunInput());
        auto data = new T[n];
        dataSweep.loadNextDataSetIntoMemory<T>(data);

        for (auto iteration = 0; iteration < iterations; ++iteration) {

            for (size_t j = 0; j < partitionImplementations.size(); ++j) {
                auto keys = new T[n];
                copyArray<T>(data, keys, n);

                std::cout << "Running input " << results[runNum][0] << ", iteration " << iteration + 1;
                std::cout << ", implementation = " << getPartitionName(partitionImplementations[j]);
                std::cout << "... ";

                cycles = *Counters::getInstance().readSharedEventSet();

                HAQP::runPartitionFunction(partitionImplementations[j], n, keys, radixBits);

                results[runNum][1 + (iteration * partitionImplementations.size()) + j] =
                        static_cast<double>(*Counters::getInstance().readSharedEventSet() - cycles);

                delete[] keys;

                std::cout << "Completed" << std::endl;
            }
        }

        delete[] data;
    }

    dataSweep.restartSweep();

    std::vector<std::string> headers(1 + dataCols);
    headers [0] = "Input";
    for (auto i = 0; i < dataCols; ++i) {
        headers[1 + i] = getPartitionName(partitionImplementations[i % partitionImplementations.size()]);
    }

    std::string fileName =
            fileNamePrefix +
            "PartitionSweep_" +
            dataSweep.getSweepName();
    std::string fullFilePath = FilePaths::getInstance().getPartitionCyclesFolderPath() + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);
}



#endif //HAQP_PARTITIONCYCLESBENCHMARKIMPLEMENTATION_H
