#include <iostream>
#include <random>

#include "dataGenerators.h"


void generateUniformDistributionInMemory(int *data, int n) {
    std::cout << "Generating data in memory... ";
    std::cout.flush();

    unsigned int seed = 9999;
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

void generateUnequalStepSelectivityInMemory(int *data, int n, int step, int numberOfDiscreteSections, int sectionRatio) {
    std::cout << "Generating data in memory... ";
    std::cout.flush();

    int elementsPerSection = n / numberOfDiscreteSections;

    int elementsPerStepSection = elementsPerSection * sectionRatio / (1 + sectionRatio);
    int elementsPerNoStepSection = elementsPerSection - elementsPerStepSection;

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
        if (onStep) {
            for (int k = 0; k < elementsPerStepSection; ++k) {
                data[index++] = distribution(gen);
            }
        } else {
            for (int k = 0; k < elementsPerNoStepSection; ++k) {
                data[index++] = distribution(gen);
            }

        }

        onStep = !onStep;
    }

    std::cout << "Complete" << std::endl;
}

void generatePartiallySortedInMemory(int *data, int n, int numRepeats, float percentageRandom) {
    std::cout << "Generating data in memory... ";
    std::cout.flush();

    int sectionSize = 100 * numRepeats;
    int sections = n / (sectionSize);
    int elementsToShufflePerSection = static_cast<int>(
            0.5 * (percentageRandom / 100.0) * static_cast<float>(sectionSize));

//    std::cout << sectionSize << " section size" << std::endl;
//    std::cout << sections << " sections" << std::endl;
//    std::cout << elementsToShufflePerSection << " elements to shuffle per section" << std::endl;

    unsigned int seed = 1;
    std::mt19937 gen(seed);

    bool increasing = true;
    int lowerBound = 1;
    int upperBound = 100;

    int value;
    int index = 0;
    for (int i = 0; i < sections; ++i) {
        value = increasing ? lowerBound : upperBound;
        for (int j = 0; j < 100; ++j) {
            for (int k = 0; k < numRepeats; ++k) {
                data[index++] = value;
            }
            value = increasing ? value + 1 : value - 1;
        }
        increasing = !increasing;

        for (int swapCount = 0; swapCount < elementsToShufflePerSection; ++swapCount) {
            std::uniform_int_distribution<int> dis(1, sectionSize);
            std::swap(data[index - dis(gen)], data[index - dis(gen)]);

//            int index1 = dis(gen);
//            int index2 = dis(gen);
//            std::swap(data[index - index1], data[index - index2]);
//            std::cout << "Swapped indexes: " << index1 << ", " << index2 << std::endl;
        }

/*        for (int x = 0; x < 100; ++x) {
            for (int y = 0; y < numRepeats; ++y) {
                std::cout << data[index - sectionSize + (x * numRepeats) + y] << ", ";
            }
            std::cout << std::endl;
        }*/
    }

    std::cout << "Complete" << std::endl;
}

