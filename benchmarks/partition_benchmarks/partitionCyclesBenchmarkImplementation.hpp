#ifndef HAQP_PARTITIONCYCLESBENCHMARKIMPLEMENTATION_HPP
#define HAQP_PARTITIONCYCLESBENCHMARKIMPLEMENTATION_HPP

#include <limits>

#include "utilities/dataHelpers.hpp"
#include "utilities/papiHelpers.hpp"
#include "partitionCyclesBenchmark.hpp"
#include "data_generation/dataFiles.hpp"


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



#endif //HAQP_PARTITIONCYCLESBENCHMARKIMPLEMENTATION_HPP
