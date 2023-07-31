#ifndef MABPL_DATAGENERATIONIMPLEMENTATION_H
#define MABPL_DATAGENERATIONIMPLEMENTATION_H

#include <iostream>
#include <random>
#include <set>
#include <cassert>

namespace MABPL {

template <typename T>
void copyArray(const T *source, T *destination, int size) {
    for (auto i = 0; i < size; i++) {
        destination[i] = source[i];
    }
}

template <typename T>
void generateUniformDistributionInMemory(T *data, int n, int upperBound) {
    static_assert(std::is_integral<T>::value, "Must be an integer type");
    if (upperBound == n) {
        generateRandomisedUniqueValuesInMemory(data, n);
        return;
    }

    unsigned int seed = 1;
    std::mt19937 gen(seed);

    auto lowerBound = 1;

    std::uniform_int_distribution<T> distribution(lowerBound, upperBound);

    for (auto i = 0; i < n; ++i) {
        data[i] = distribution(gen);
    }
}

template <typename T>
void generateRandomisedUniqueValuesInMemory(T *data, int n) {
    // Fisher–Yates shuffle

    static_assert(std::is_integral<T>::value, "Must be an integer type");

    for (T i = 1; i <= n; ++i) {
        data[i - 1] = i;
    }

    unsigned int seed = 1;
    std::mt19937 gen(seed);
    std::uniform_int_distribution<int> dis(1, 1);
    int j;

    for (int i = n - 1; i >= 0; --i) {
        dis = std::uniform_int_distribution<int>(0, i);
        j = dis(gen);
        std::swap(data[i], data[j]);
    }
}

template <typename T>
inline T scaleValueLogarithmically(T number, int startingUpperBound, int targetUpperBound) {
    static_assert(std::is_integral<T>::value, "Must be an integer type");
    double scaledValue = log(number) / log(startingUpperBound);
    double scaledNumber = pow(targetUpperBound, scaledValue);
    return std::round(scaledNumber);
}

template <typename T>
void generateUniformDistributionWithSetCardinalityInMemory(T *data, int n, int upperBound, int cardinality) {
    assert(cardinality <= upperBound);
    static_assert(std::is_integral<T>::value, "Must be an integer type");

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

    std::uniform_int_distribution<T> distribution(1, cardinality);

    for (auto i = 0; i < n; ++i) {
        data[i] = scaleValueLogarithmically(distribution(gen), cardinality, upperBound);
    }
}

template <typename T>
void generatePartiallySortedInMemory(T *data, int n, int numRepeats, float percentageRandom) {
    int sectionSize = 100 * numRepeats;
    int sections = n / (sectionSize);
    int elementsToShufflePerSection = static_cast<int>(
            0.5 * (percentageRandom / 100.0) * static_cast<float>(sectionSize));

    unsigned int seed = 1;
    std::mt19937 gen(seed);

    bool increasing = true;
    auto lowerBound = 1;
    auto upperBound = 100;

    T value;
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
        }
    }
}



}

#endif //MABPL_DATAGENERATIONIMPLEMENTATION_H
