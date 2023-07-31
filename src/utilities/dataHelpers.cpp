#include <iostream>
#include <fstream>
#include <string>
#include <vector>
#include <cassert>
#include <cmath>

#include "dataHelpers.h"

LoadedData::LoadedData(const DataFile &dataFile) : data(nullptr), dataFile(&dataFile) {
    loadData();
}

void LoadedData::loadData() {
    data = new int[dataFile->getNumElements()];
    dataFile->loadDataIntoMemory(data);
}

LoadedData &LoadedData::getInstance(const DataFile &requestedDataFile) {
    static LoadedData instance(requestedDataFile);

    if (requestedDataFile.getFileName() != instance.getDataFile().getFileName()) {
        delete[] instance.data;
        instance.dataFile = &requestedDataFile;
        instance.loadData();
    }

    return instance;
}

int* LoadedData::getData() const {
    return data;
}

const DataFile& LoadedData::getDataFile() const {
    return *dataFile;
}

void displayDistribution(const DataFile &dataFile) {
    std::vector<int> counts(101, 0);
    auto numElements = dataFile.getNumElements();
    auto inputData = LoadedData::getInstance(dataFile).getData();
    for (auto i = 0; i < numElements; ++i) {
        counts[inputData[i]]++;
    }
    for (auto i = 0; i <= 100; ++i) {
        std::cout << i << "%: " << static_cast<float>(counts[i]) / static_cast<float>(numElements) << std::endl;
    }
}

void writeDataFileToCSV(const DataFile &dataFile) {
    auto data = LoadedData::getInstance(dataFile).getData();
    auto numElements = dataFile.getNumElements();

    std::string fileName = FilePaths::getInstance().getDataFilesFolderPath() + dataFile.getFileName() + ".csv";
    std::ofstream file(fileName);

    if (file.is_open()) {
        for (auto i = 0; i < numElements; ++i) {
            file << data[i];

            // Add comma except for the last element
            if (i < numElements - 1) {
                file << ",\n";
            }
        }

        file.close();
        std::cout << "CSV file created successfully: " << fileName << std::endl;
    } else {
        std::cout << "Unable to create the CSV file: " << fileName << std::endl;
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

void writeHeadersAndTableToCSV(std::vector<std::string>& headers,
                               std::vector<std::vector<double>>  values,
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
