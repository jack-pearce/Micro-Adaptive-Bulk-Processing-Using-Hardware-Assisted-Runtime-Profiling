#include <iostream>
#include <random>
#include <set>

#include "dataGenerators.h"


void generateUniformDistributionInMemory(int *data, int n, int upperBound) {
    std::cout << "Generating data in memory... ";
    std::cout.flush();

    unsigned int seed = 1;
    std::mt19937 gen(seed);

    auto lowerBound = 1;

    std::uniform_int_distribution<int> distribution(lowerBound, upperBound);

    auto index = 0;
    for (auto i = 0; i < n; ++i) {
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
    auto lowerBound = 1;
    auto upperBound = 100;

    auto index = 0;
    for (auto i = 0; i < numberOfDiscreteSections; ++i) {
        for (auto j = 0; j < minimum; ++j) {
            if (j > 0) {
                if (increasing) {
                    lowerBound += 1;
                } else {
                    lowerBound -= 1;
                }
            }

            std::uniform_int_distribution<int> distribution(lowerBound, upperBound);
            for (auto k = 0; k < elementsPerSection; ++k) {
                data[index++] = distribution(gen);
            }
        }
        increasing = !increasing;
    }

    std::cout << "Complete" << std::endl;
}

void generateUpperStepSelectivityInMemory(int *data, int n, int step, int numberOfDiscreteSections) {
    std::cout << "Generating data in memory... ";
    std::cout.flush();

    int elementsPerSection = n / numberOfDiscreteSections;

    unsigned int seed = 1;
    std::mt19937 gen(seed);

    bool onStep = false;
    auto lowerBound = 1;
    int upperBound;

    auto index = 0;
    for (auto i = 0; i < numberOfDiscreteSections; ++i) {
        if (onStep) {
            upperBound = step;
        } else {
            upperBound = 100;
        }

        std::uniform_int_distribution<int> distribution(lowerBound, upperBound);
        for (auto k = 0; k < elementsPerSection; ++k) {
            data[index++] = distribution(gen);
        }

        onStep = !onStep;
    }

    std::cout << "Complete" << std::endl;
}


void generateLowerStepSelectivityInMemory(int *data, int n, int step, int numberOfDiscreteSections) {
    std::cout << "Generating data in memory... ";
    std::cout.flush();

    int elementsPerSection = n / numberOfDiscreteSections;

    unsigned int seed = 1;
    std::mt19937 gen(seed);

    bool onStep = false;
    int lowerBound;
    auto upperBound = 100;

    auto index = 0;
    for (auto i = 0; i < numberOfDiscreteSections; ++i) {
        if (onStep) {
            lowerBound = step;
        } else {
            lowerBound = 1;
        }

        std::uniform_int_distribution<int> distribution(lowerBound, upperBound);
        for (auto k = 0; k < elementsPerSection; ++k) {
            data[index++] = distribution(gen);
        }

        onStep = !onStep;
    }

    std::cout << "Complete" << std::endl;
}


void generateUnequalLowerStepSelectivityInMemory(int *data, int n, int step, int numberOfDiscreteSections, int sectionRatio) {
    std::cout << "Generating data in memory... ";
    std::cout.flush();

    int elementsPerSection = n / numberOfDiscreteSections;

    auto elementsPerStepSection = elementsPerSection * sectionRatio / (1 + sectionRatio);
    auto elementsPerNoStepSection = elementsPerSection - elementsPerStepSection;

    unsigned int seed = 1;
    std::mt19937 gen(seed);

    bool onStep = false;
    int lowerBound;
    auto upperBound = 100;

    auto index = 0;
    for (auto i = 0; i < (2 * numberOfDiscreteSections); ++i) {
        if (onStep) {
            lowerBound = step;
        } else {
            lowerBound = 1;
        }

        std::uniform_int_distribution<int> distribution(lowerBound, upperBound);
        if (onStep) {
            for (auto k = 0; k < elementsPerStepSection; ++k) {
                data[index++] = distribution(gen);
            }
        } else {
            for (auto k = 0; k < elementsPerNoStepSection; ++k) {
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
//    std::cout << elementsToShufflePerSection << " pairs to shuffle per section" << std::endl;

    unsigned int seed = 1;
    std::mt19937 gen(seed);

    bool increasing = true;
    auto lowerBound = 1;
    auto upperBound = 100;

    int value;
    auto index = 0;
    for (auto i = 0; i < sections; ++i) {
        value = increasing ? lowerBound : upperBound;
        for (auto j = 0; j < 100; ++j) {
            for (auto k = 0; k < numRepeats; ++k) {
                data[index++] = value;
            }
            value = increasing ? value + 1 : value - 1;
        }
        increasing = !increasing;

        std::set<int> selectedIndexes = {};
        int index1, index2;
        for (auto swapCount = 0; swapCount < elementsToShufflePerSection; ++swapCount) {
            std::uniform_int_distribution<int> dis(1, sectionSize);

            index1 = dis(gen);
            while (selectedIndexes.count(index1) > 0) {
                index1 = dis(gen);
            }

            index2 = dis(gen);
            while (selectedIndexes.count(index2) > 0) {
                index2 = dis(gen);
            }

            selectedIndexes.insert(index1);
            selectedIndexes.insert(index2);
            std::swap(data[index - index1], data[index - index2]);

//            std::cout << "Swapped indexes: " << index1 << ", " << index2 << std::endl;
        }

/*        for (int x = 0; x < 100; ++x) {
            for (int y = 0; y < numRepeats; ++y) {
                std::cout << data[index - sectionSize + (x * numRepeats) + y] << std::endl;
            }
        }*/
    }

    std::cout << "Complete" << std::endl;
}

