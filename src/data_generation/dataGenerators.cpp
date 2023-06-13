#include <iostream>
#include <fstream>
#include <random>
#include <string>

#include "dataGenerators.h"

void generateUniformDistributionCSV(const std::string& filePath, int n) {
    std::cout << "Generating csv datafile... ";
    std::cout.flush();

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

    for (int i = 0; i < n; ++i) {
        outputFile << distribution(gen) << std::endl;
    }

    outputFile.close();
    std::cout << "Complete" << std::endl;
}