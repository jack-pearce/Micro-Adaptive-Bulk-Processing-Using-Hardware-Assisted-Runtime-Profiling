#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <cassert>

#include "dataHelpers.h"

int countElements(const std::string& filePath) {
    std::ifstream inputFile(filePath);

    if (!inputFile) {
        std::cerr << "Failed to open the file." << std::endl;
        exit(1);
    }

    std::string line;
    int index = 0;
    while (std::getline(inputFile, line)) {
        std::istringstream iss(line);
        std::string value;

        while (std::getline(iss, value, ',')) {
            index++;
        }
    }

    inputFile.close();
    return index;
}

void loadDataToVector(const std::string& filePath, std::vector<int>& data) {
    std::ifstream inputFile(filePath);

    if (!inputFile) {
        std::cerr << "Failed to open the file." << std::endl;
        exit(1);
    }

    std::string line;
    while (std::getline(inputFile, line)) {
        std::istringstream iss(line);
        std::string value;

        while (std::getline(iss, value, ',')) {
            try {
                int intValue = std::stoi(value);
                data.push_back(intValue);
            } catch (const std::exception& e) {
                std::cerr << "Failed to convert value to int: " << value << std::endl;
            }
        }
    }

    inputFile.close();
}

int loadDataToArray(const std::string& filePath, int *data) {
    std::ifstream inputFile(filePath);

    if (!inputFile) {
        std::cerr << "Failed to open the file." << std::endl;
        exit(1);
    }

    std::string line;
    int index = 0;
    while (std::getline(inputFile, line)) {
        std::istringstream iss(line);
        std::string value;

        while (std::getline(iss, value, ',')) {
            try {
                int intValue = std::stoi(value);
                data[index] = intValue;
                index++;
            } catch (const std::exception& e) {
                std::cerr << "Failed to convert value to int: " << value << std::endl;
            }
        }
    }

    inputFile.close();
    return index;
}

void displayDistribution(const std::vector<int>& data) {
    std::vector<int> counts(101, 0);
    int elements = static_cast<int>(data.size());
    for (int element : data) {
        counts[element]++;
    }
    for (int i = 0; i <= 100; ++i) {
        std::cout << i << "%: " << static_cast<float>(counts[i]) / static_cast<float>(elements) << std::endl;
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

