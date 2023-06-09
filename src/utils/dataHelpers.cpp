#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

#include "dataHelpers.h"

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

