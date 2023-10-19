#ifndef MABPL_PARTITIONCYCLESBENCHMARKIMPLEMENTATION_H
#define MABPL_PARTITIONCYCLESBENCHMARKIMPLEMENTATION_H

#include <limits>

#include "utilities/dataHelpers.h"
#include "utilities/papiHelpers.h"
#include "partitionCyclesBenchmark.h"


template<typename T>
void testSort(const DataFile &dataFile, Partition partitionImplementation, int radixBits) {
    auto keys = new T[dataFile.getNumElements()];
    copyArray<T>(LoadedData<T>::getInstance(dataFile).getData(), keys, dataFile.getNumElements());

    std::cout << "Running test... ";

    MABPL::runPartitionFunction(partitionImplementation, dataFile.getNumElements(), keys, radixBits);

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
void testPartition(const DataFile &dataFile, Partition partitionImplementation, int radixBits) {
    auto keys = new T[dataFile.getNumElements()];
    copyArray<T>(LoadedData<T>::getInstance(dataFile).getData(), keys, dataFile.getNumElements());

    std::cout << "Running test... ";

    std::vector<int> partitions = MABPL::runPartitionFunction(partitionImplementation, dataFile.getNumElements(),
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
                                                  Partition partitionImplementation,
                                                  int startBits, int endBits,
                                                  std::vector<std::string> &benchmarkCounters,
                                                  const std::string &fileNamePrefix, int iterations) {
    if (partitionImplementation == Partition::RadixBitsAdaptive) {
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

            std::cout << "Running radixBits = " << radixBits << ", iteration = " << i + 1 << "... ";

            if (PAPI_reset(benchmarkEventSet) != PAPI_OK)
                exit(1);

            MABPL::runPartitionFunction(partitionImplementation, dataFile.getNumElements(), keys, radixBits);

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
                                              Partition partitionImplementation, int radixBits,
                                              std::vector<std::string> &benchmarkCounters,
                                              const std::string &fileNamePrefix, int iterations) {
    if (partitionImplementation == Partition::RadixBitsAdaptive) {
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

            MABPL::runPartitionFunction(partitionImplementation, n, keys, radixBits);

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
void partitionBitsSweepBenchmark(const DataFile &dataFile, const std::vector<Partition> &partitionImplementations,
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

                std::cout << "Running radixBits = " << radixBits << ", iteration = " << iteration + 1;
                std::cout << ", implementation = ";
                std::cout << getPartitionName(partitionImplementations[j]) << "... ";

                cycles = *Counters::getInstance().readSharedEventSet();

                MABPL::runPartitionFunction(partitionImplementations[j],
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
void partitionSweepBenchmark(DataSweep &dataSweep, const std::vector<Partition> &partitionImplementations,
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

                MABPL::runPartitionFunction(partitionImplementations[j], n, keys, radixBits);

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



#endif //MABPL_PARTITIONCYCLESBENCHMARKIMPLEMENTATION_H
