#include <iostream>
#include <fstream>
#include <random>
#include <string>

#include "dataGenerators.h"
#include "dataFiles.h"


void generateUniformDistributionCSV(const std::string& fileName, int n) {
    std::cout << "Generating csv datafile... ";
    std::cout.flush();

    unsigned int seed = 1;
    std::mt19937 gen(seed);

    int lowerBound = 1;
    int upperBound = 100;

    std::uniform_int_distribution<int> distribution(lowerBound, upperBound);

    std::ofstream outputFile(inputFilePath + fileName);

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

void generateVaryingSelectivityCSV(const std::string& fileName, int n, int minimum) {
    std::cout << "Generating csv datafile... ";
    std::cout.flush();

    std::ofstream outputFile(inputFilePath + fileName);

    if (!outputFile) {
        std::cerr << "Failed to open the file." << std::endl;
        exit(1);
    }

    int numberOfDiscreteSections = 10;
    int elementsPerSection = n / (numberOfDiscreteSections * minimum);

    unsigned int seed = 1;
    std::mt19937 gen(seed);

    bool increasing = true;
    int lowerBound = 1;
    int upperBound = 100;

    for (int i = 0; i < numberOfDiscreteSections; ++i) {
        for (int j = 0; j < (minimum); ++j) {
            if (j > 0) {
                if (increasing) {
                    lowerBound += 1;
                } else {
                    lowerBound -= 1;
                }
            }

            std::uniform_int_distribution<int> distribution(lowerBound, upperBound);
            for (int k = 0; k < elementsPerSection; ++k) {
                outputFile << distribution(gen) << std::endl;
            }
        }
        increasing = !increasing;
    }

    outputFile.close();
    std::cout << "Complete" << std::endl;
}


void generateStepSelectivityCSV(const std::string& fileName, int n, int step) {
    std::cout << "Generating csv datafile... ";
    std::cout.flush();

    std::ofstream outputFile(inputFilePath + fileName);

    if (!outputFile) {
        std::cerr << "Failed to open the file." << std::endl;
        exit(1);
    }

    int numberOfDiscreteSections = 10;
    int elementsPerSection = n / numberOfDiscreteSections;

    unsigned int seed = 1;
    std::mt19937 gen(seed);

    bool onStep = false;
    int lowerBound = 1;
    int upperBound;

    for (int i = 0; i < numberOfDiscreteSections; ++i) {
        if (onStep) {
            upperBound = step;
        } else {
            upperBound = 100;
        }

        std::uniform_int_distribution<int> distribution(lowerBound, upperBound);
        for (int k = 0; k < elementsPerSection; ++k) {
            outputFile << distribution(gen) << std::endl;
        }

        onStep = !onStep;
    }

    outputFile.close();
    std::cout << "Complete" << std::endl;
}