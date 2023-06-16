#include <algorithm>
#include <iostream>

#include "select.h"
#include "../utils/papiHelpers.h"
#include "papi.h"


int selectBranch(int n, const int *inputData, int *selection, int threshold) {
    int k = 0;
    for (int i = 0; i < n; ++i) {
        if (inputData[i] <= threshold) {
            selection[k++] = i;
        }
    }
    return k;
}

int selectPredication(int n, const int *inputData, int *selection, int threshold) {
    int k = 0;
    for (int i = 0; i < n; ++i) {
        selection[k] = i;
        k += (inputData[i] <= threshold);
    }
    return k;
}

inline void performAdaption(int (*&selectFunctionPtr)(int, const int *, int *, int),
                            const long_long *counterValues,
                            float lowerCrossoverSelectivity,
                            float upperCrossoverSelectivity,
                            float lowerBranchCrossoverBranchMisses,
                            float m,
                            float selectivity) {

    if (__builtin_expect(static_cast<float>(counterValues[0]) >
                         (((selectivity - lowerCrossoverSelectivity) * m) + lowerBranchCrossoverBranchMisses)
                         && selectFunctionPtr == selectBranch, false))
        selectFunctionPtr = selectPredication;

    if (__builtin_expect((selectivity < lowerCrossoverSelectivity
                         || selectivity > upperCrossoverSelectivity)
                         && selectFunctionPtr == selectPredication, false))
        selectFunctionPtr = selectBranch;
}

int selectAdaptive(int n, const int *inputData, int *selection, int threshold) {
    int tuplesPerAdaption = 50000; // down to 10 000
    float lowerCrossoverSelectivity = 0.03; // Could use a tuning function to identify these cross-over points
    float upperCrossoverSelectivity = 0.98; // Could use a tuning function to identify these cross-over points

    // Equations below are only valid at the extreme ends of selectivity
    // Y intercept of number of branch misses (at lower cross-over selectivity)
    float lowerBranchCrossoverBranchMisses = lowerCrossoverSelectivity * static_cast<float>(tuplesPerAdaption);
    float upperBranchCrossoverBranchMisses = (1 - upperCrossoverSelectivity) * static_cast<float>(tuplesPerAdaption);

    // Gradient of number of branch misses between lower cross-over selectivity and upper cross-over selectivity
    float m = (upperBranchCrossoverBranchMisses - lowerBranchCrossoverBranchMisses) /
            (upperCrossoverSelectivity - lowerCrossoverSelectivity);

    int k = 0;
    int tuplesToProcess;
    int selected;
    SelectFunctionPtr selectFunctionPtr = selectBranch;

    std::vector<std::string> counters = {"PERF_COUNT_HW_BRANCH_MISSES"};
    long_long *counterValues = Counters::getInstance().getEvents(counters);

    while (n > 0) {
        tuplesToProcess = std::min(n, tuplesPerAdaption);
        Counters::getInstance().readEventSet();
        selected = selectFunctionPtr(tuplesToProcess, inputData, selection, threshold);
        Counters::getInstance().readEventSet();
        n -= tuplesToProcess;
        inputData += tuplesToProcess;
        selection += selected;
        k += selected;
        performAdaption(selectFunctionPtr, counterValues, lowerCrossoverSelectivity, upperCrossoverSelectivity,
                        lowerBranchCrossoverBranchMisses, m,
                        static_cast<float>(selected) / static_cast<float>(tuplesPerAdaption));
    }

    return k;
}

void setSelectFuncPtr(SelectFunctionPtr &selectFunctionPtr, SelectImplementation selectImplementation) {
    switch(selectImplementation) {
        case SelectImplementation::Branch:
            selectFunctionPtr = selectBranch;
            break;
        case SelectImplementation::Predication:
            selectFunctionPtr = selectPredication;
            break;
        case SelectImplementation::Adaptive:
            selectFunctionPtr = selectAdaptive;
            break;
        default:
            std::cout << "Invalid selection of 'Select' implementation!" << std::endl;
            exit(1);
    }
}

std::string getName(SelectImplementation selectImplementation) {
    switch(selectImplementation) {
        case SelectImplementation::Branch:
            return "Select_Branch";
        case SelectImplementation::Predication:
            return "Select_Predication";
        case SelectImplementation::Adaptive:
            return "Select_Adaptive";
        default:
            std::cout << "Invalid selection of 'Select' implementation!" << std::endl;
            exit(1);
    }
}