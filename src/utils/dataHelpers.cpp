#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <cassert>
#include <memory>

#include "dataHelpers.h"

LoadedData::LoadedData(const DataFile &_dataFile) : dataFile(_dataFile) {
    data = std::make_unique<int[]>(dataFile.getNumElements());
    dataFile.loadDataIntoMemory(data.get());
}

LoadedData &LoadedData::getInstance(const DataFile &dataFile) {
    static LoadedData instance(dataFile);
    return instance;
}

int* LoadedData::getData() const {
    return data.get();
}

const DataFile& LoadedData::getDataFile() const {
    return dataFile;
}

void displayDistribution(const DataFile& dataFile) {
    std::vector<int> counts(101, 0);
    int numElements = dataFile.getNumElements();
    int* inputData = LoadedData::getInstance(dataFile).getData();
    for (int i = 0; i < numElements; ++i) {
        counts[inputData[i]]++;
    }
    for (int i = 0; i <= 100; ++i) {
        std::cout << i << "%: " << static_cast<float>(counts[i]) / static_cast<float>(numElements) << std::endl;
    }
}

void writeHeadersAndTableToCSV(std::vector<std::string>& headers,
                               std::vector<std::vector<long_long>>  values,
                               std::string& filePath) {
    assert(headers.size() == values[0].size());

    std::ofstream file(filePath);

    for (size_t i = 0; i < headers.size(); ++i) {
        file << headers[i];
        if (i != headers.size() - 1) {
            file << ",";
        }
    }
    file << "\n";

    if (file.is_open()) {
        for (const auto& row : values) {
            for (size_t i = 0; i < row.size(); ++i) {
                file << row[i];
                if (i != row.size() - 1) {
                    file << ",";
                }
            }
            file << "\n";
        }
        file.close();
    } else {
        std::cerr << "Failed to open file: " << filePath << "\n";
    }
}

