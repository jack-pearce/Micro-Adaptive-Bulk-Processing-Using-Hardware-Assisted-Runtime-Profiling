#include <iostream>
#include <random>
#include <set>
#include <cassert>

#include "dataGeneration.h"

namespace MABPL {

void copyArray(const int *source, int *destination, int size) {
    for (auto i = 0; i < size; i++) {
        destination[i] = source[i];
    }
}

void generateUniformDistributionInMemory(int *data, int n, int upperBound) {
    if (upperBound == n) {
        generateRandomisedUniqueValuesInMemory(data, n);
        return;
    }

    unsigned int seed = 1;
    std::mt19937 gen(seed);

    auto lowerBound = 1;

    std::uniform_int_distribution<int> distribution(lowerBound, upperBound);

    for (auto i = 0; i < n; ++i) {
        data[i] = distribution(gen);
    }
}

void generateRandomisedUniqueValuesInMemory(int *data, int n) {
    // Fisher–Yates shuffle

    std::cout.flush();

    for (int i = 1; i <= n; ++i) {
        data[i - 1] = i;
    }

    unsigned int seed = 1;
    std::mt19937 gen(seed);
    std::uniform_int_distribution<> dis(1, 1);
    int j;

    for (int i = n - 1; i >= 0; --i) {
        dis = std::uniform_int_distribution<>(0, i);
        j = dis(gen);
        std::swap(data[i], data[j]);
    }
}

inline int scaleNumberLogarithmically(int number, int startingUpperBound, int targetUpperBound) {
    double scaledValue = log(number) / log(startingUpperBound);
    double scaledNumber = pow(targetUpperBound, scaledValue);
    return std::round(scaledNumber);
}

void generateUniformDistributionWithSetCardinalityInMemory(int *data, int n, int upperBound, int cardinality) {
    assert(cardinality <= upperBound);

    if (cardinality == 1) {
        for (auto i = 0; i < n; ++i) {
            data[i] = upperBound;
        }
        return;
    }

    if (cardinality == n) {
        generateRandomisedUniqueValuesInMemory(data, n);
        return;
    }

    unsigned int seed = 1;
    std::mt19937 gen(seed);

    std::uniform_int_distribution<int> distribution(1, cardinality);

    for (auto i = 0; i < n; ++i) {
        data[i] = scaleNumberLogarithmically(distribution(gen), cardinality, upperBound);
    }
}

}