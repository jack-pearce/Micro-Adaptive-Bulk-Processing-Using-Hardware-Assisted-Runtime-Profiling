#ifndef MABPL_RADIXPARTITIONCYCLESBENCHMARKIMPLEMENTATION_H
#define MABPL_RADIXPARTITIONCYCLESBENCHMARKIMPLEMENTATION_H

#include <limits>

#include "../utilities/dataHelpers.h"
#include "../utilities/papiHelpers.h"
#include "radixPartitionCyclesBenchmark.h"


template<typename T>
void testRadixSort(const DataFile &dataFile, RadixPartition radixPartitionImplementation, int radixBits) {
    auto keys = new T[dataFile.getNumElements()];
    copyArray<T>(LoadedData<T>::getInstance(dataFile).getData(), keys, dataFile.getNumElements());

    std::cout << "Running test... ";

    MABPL::runRadixPartitionFunction(radixPartitionImplementation,dataFile.getNumElements(), keys, radixBits);

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
void testRadixPartition(const DataFile &dataFile, RadixPartition radixPartitionImplementation, int radixBits) {
    auto keys = new T[dataFile.getNumElements()];
    copyArray<T>(LoadedData<T>::getInstance(dataFile).getData(), keys, dataFile.getNumElements());

    std::cout << "Running test... ";

    std::vector<int> partitions = MABPL::runRadixPartitionFunction(radixPartitionImplementation,dataFile.getNumElements(),
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
void radixPartitionBitsSweepBenchmarkWithExtraCounters(const DataFile &dataFile,
                                                       RadixPartition radixPartitionImplementation,
                                                       int startBits, int endBits,
                                                       std::vector<std::string> &benchmarkCounters,
                                                       const std::string &fileNamePrefix, int iterations) {
    if (radixPartitionImplementation == RadixPartition::RadixBitsAdaptive) {
        std::cout
                << "Cannot benchmark adaptive radix partition using counters as algorithm is already using these counters"
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

            std::cout << "Running radixBits = " << radixBits << ", iteration = " << i + 1 << "... ";

            if (PAPI_reset(benchmarkEventSet) != PAPI_OK)
                exit(1);

            MABPL::runRadixPartitionFunction(radixPartitionImplementation,dataFile.getNumElements(), keys, radixBits);

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
            "RadixPartitionRadixBitsSweep_" +
            dataFile.getFileName();
    std::string fullFilePath = FilePaths::getInstance().getRadixPartitionCyclesFolderPath() + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);

    shutdownPAPI(benchmarkEventSet, benchmarkCounterValues);
}

template<typename T>
void radixPartitionSweepBenchmarkWithExtraCounters(DataSweep &dataSweep,
                                                   RadixPartition radixPartitionImplementation, int radixBits,
                                                   std::vector<std::string> &benchmarkCounters,
                                                   const std::string &fileNamePrefix, int iterations) {
    if (radixPartitionImplementation == RadixPartition::RadixBitsAdaptive) {
        std::cout
                << "Cannot benchmark adaptive radix partition using counters as algorithm is already using these counters"
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

            MABPL::runRadixPartitionFunction(radixPartitionImplementation, n, keys, radixBits);

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
            "RadixPartitionSweep_" +
            dataSweep.getSweepName();
    std::string fullFilePath = FilePaths::getInstance().getRadixPartitionCyclesFolderPath() + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);

    shutdownPAPI(benchmarkEventSet, benchmarkCounterValues);
}

template<typename T>
void radixPartitionBitsSweepBenchmark(const DataFile &dataFile, const std::vector<RadixPartition> &rpImplementations,
                                      int startBits, int endBits, const std::string &fileNamePrefix, int iterations) {

    int dataCols = iterations * static_cast<int>(rpImplementations.size());
    long_long cycles;
    std::vector<std::vector<long_long>> results(endBits - startBits + 1,
                                                std::vector<long_long>(dataCols + 1, 0));

    for (auto radixBits = startBits; radixBits <= endBits; ++radixBits) {
        results[radixBits - startBits][0] = radixBits;

        for (auto iteration = 0; iteration < iterations; ++iteration) {

            for (size_t j = 0; j < rpImplementations.size(); ++j) {
                auto keys = new T[dataFile.getNumElements()];
                copyArray<T>(LoadedData<T>::getInstance(dataFile).getData(), keys,
                             dataFile.getNumElements());

                std::cout << "Running radixBits = " << radixBits << ", iteration = " << iteration + 1;
                std::cout << ", implementation = ";
                std::cout << getRadixPartitionName(rpImplementations[j]) << "... ";

                cycles = *Counters::getInstance().readSharedEventSet();

                MABPL::runRadixPartitionFunction(rpImplementations[j],
                                                 dataFile.getNumElements(), keys, radixBits);

                results[radixBits - startBits][1 + (iteration * rpImplementations.size()) + j] =
                        *Counters::getInstance().readSharedEventSet() - cycles;

                delete[] keys;

                std::cout << "Completed" << std::endl;
            }
        }
    }

    std::vector<std::string> headers(1 + dataCols);
    headers [0] = "RadixBits";
    for (auto i = 0; i < dataCols; ++i) {
        headers[1 + i] = getRadixPartitionName(rpImplementations[i % rpImplementations.size()]);
    }

    std::string fileName =
            fileNamePrefix +
            "RadixPartitionRadixBitsSweep_" +
            dataFile.getFileName();
    std::string fullFilePath = FilePaths::getInstance().getRadixPartitionCyclesFolderPath() + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);
}

template<typename T>
void radixPartitionSweepBenchmark(DataSweep &dataSweep, const std::vector<RadixPartition> &rpImplementations,
                                  int radixBits, const std::string &fileNamePrefix, int iterations) {

    int dataCols = iterations * static_cast<int>(rpImplementations.size());
    long_long cycles;
    std::vector<std::vector<double>> results(dataSweep.getTotalRuns(),
                                             std::vector<double>(dataCols + 1, 0));

    int n = dataSweep.getNumElements();

    for (auto runNum = 0; runNum < dataSweep.getTotalRuns(); ++runNum) {

        results[runNum][0] = static_cast<double>(dataSweep.getRunInput());
        auto data = new T[n];
        dataSweep.loadNextDataSetIntoMemory<T>(data);

        for (auto iteration = 0; iteration < iterations; ++iteration) {

            for (size_t j = 0; j < rpImplementations.size(); ++j) {
                auto keys = new T[n];
                copyArray<T>(data, keys, n);

                std::cout << "Running input " << results[runNum][0] << ", iteration " << iteration + 1;
                std::cout << ", implementation = " << getRadixPartitionName(rpImplementations[j]);
                std::cout << "... ";

                cycles = *Counters::getInstance().readSharedEventSet();

                MABPL::runRadixPartitionFunction(rpImplementations[j], n, keys, radixBits);

                results[runNum][1 + (iteration * rpImplementations.size()) + j] =
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
        headers[1 + i] = getRadixPartitionName(rpImplementations[i % rpImplementations.size()]);
    }

    std::string fileName =
            fileNamePrefix +
            "RadixPartitionSweep_" +
            dataSweep.getSweepName();
    std::string fullFilePath = FilePaths::getInstance().getRadixPartitionCyclesFolderPath() + fileName + ".csv";
    writeHeadersAndTableToCSV(headers, results, fullFilePath);
}



#endif //MABPL_RADIXPARTITIONCYCLESBENCHMARKIMPLEMENTATION_H
