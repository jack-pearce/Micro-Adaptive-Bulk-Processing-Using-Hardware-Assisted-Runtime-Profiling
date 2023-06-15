#include <iostream>
#include <random>

#include "dataGenerators.h"


void generateUniformDistributionInMemory(int *data, int n) {
    std::cout << "Generating data in memory... ";
    std::cout.flush();

    unsigned int seed = 1;
    std::mt19937 gen(seed);

    int lowerBound = 1;
    int upperBound = 100;

    std::uniform_int_distribution<int> distribution(lowerBound, upperBound);

    int index = 0;
    for (int i = 0; i < n; ++i) {
        data[index++] = distribution(gen);
    }

    std::cout << "Complete" << std::endl;
}

void generateVaryingSelectivityInMemory(int *data, int n, int minimum, int numberOfDiscreteSections) {
    std::cout << "Generating data in memory... ";
    std::cout.flush();

    int elementsPerSection = n / (numberOfDiscreteSections * minimum);

    unsigned int seed = 1;
    std::mt19937 gen(seed);

    bool increasing = true;
    int lowerBound = 1;
    int upperBound = 100;

    int index = 0;
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
                data[index++] = distribution(gen);
            }
        }
        increasing = !increasing;
    }

    std::cout << "Complete" << std::endl;
}

void generateStepSelectivityInMemory(int *data, int n, int step, int numberOfDiscreteSections) {
    std::cout << "Generating data in memory... ";
    std::cout.flush();

    int elementsPerSection = n / numberOfDiscreteSections;

    unsigned int seed = 1;
    std::mt19937 gen(seed);

    bool onStep = false;
    int lowerBound = 1;
    int upperBound;

    int index = 0;
    for (int i = 0; i < numberOfDiscreteSections; ++i) {
        if (onStep) {
            upperBound = step;
        } else {
            upperBound = 100;
        }

        std::uniform_int_distribution<int> distribution(lowerBound, upperBound);
        for (int k = 0; k < elementsPerSection; ++k) {
            data[index++] = distribution(gen);
        }

        onStep = !onStep;
    }

    std::cout << "Complete" << std::endl;
}
