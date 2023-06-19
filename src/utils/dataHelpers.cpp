#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <cassert>
#include <memory>
#include <cmath>

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

void generateLogDistribution(int numPoints, double minValue, double maxValue, std::vector<float> &points) {
    for (int i = 0; i < numPoints; ++i) {
        double t = i / static_cast<double>(numPoints - 1); // Normalized parameter between 0 and 1
        double value = std::pow(10.0, t * (std::log10(maxValue) - std::log10(minValue)) + std::log10(minValue));
        points.push_back(static_cast<float>(value));
    }
}

void generateLinearDistribution(int numPoints, double minValue, double maxValue, std::vector<float> &points) {
    double step = (maxValue - minValue) / (numPoints - 1);
    for (int i = 0; i < numPoints; i++) {
        double value = minValue + i * step;
        points.push_back(static_cast<float>(value));
    }
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

void writeDataFileToCSV(const DataFile& dataFile) {
    int* data = LoadedData::getInstance(dataFile).getData();
    int numElements = dataFile.getNumElements();

    std::string fileName = outputFilePath + dataFilesFolder + dataFile.getFileName() + ".csv";
    std::ofstream file(fileName);

    if (file.is_open()) {
        for (int i = 0; i < numElements; ++i) {
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

void copyArray(const int* source, int* destination, int size) {
    for (int i = 0; i < size; i++) {
        destination[i] = source[i];
    }
}

