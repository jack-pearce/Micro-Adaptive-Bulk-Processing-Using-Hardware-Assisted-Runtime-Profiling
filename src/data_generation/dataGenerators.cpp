#include <iostream>
#include <fstream>
#include <random>
#include <string>

#include "dataGenerators.h"

void generateUniformDistributionCSV(const std::string& filePath, int numValues) {
    unsigned int seed = 1;
    std::mt19937 gen(seed);

    int lowerBound = 1;
    int upperBound = 100;

    std::uniform_int_distribution<int> distribution(lowerBound, upperBound);

    std::ofstream outputFile(filePath);

    if (!outputFile) {
        std::cerr << "Failed to open the file." << std::endl;
        exit(1);
    }

    for (int i = 0; i < numValues; ++i) {
        outputFile << distribution(gen) << std::endl;
    }

    outputFile.close();
}