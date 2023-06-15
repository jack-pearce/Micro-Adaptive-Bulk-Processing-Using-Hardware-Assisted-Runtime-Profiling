#include <algorithm>
#include <iostream>

#include "select.h"
#include "hardwareTuning.h"
#include "../utils/papiHelpers.h"
#include "papi.h"


int selectBranch(int n, const int *inputData, int *selection, int threshold) {
//    std::cout << "Select branch called" << std::endl;
    int k = 0;
    for (int i = 0; i < n; ++i) {
        if (inputData[i] <= threshold) {
            selection[k++] = i;
        }
    }
    return k;
}

int selectPredication(int n, const int *inputData, int *selection, int threshold) {
//    std::cout << "Select predication called" << std::endl;
    int k = 0;
    for (int i = 0; i < n; ++i) {
        selection[k] = i;
        k += (inputData[i] <= threshold);
    }
    return k;
}

inline void performAdaption(int (*&selectFunctionPtr)(int, const int *, int *, int),
                            const long_long *counterValues,
                            int tuplesPerAdaption,
                            float lowerPredicateCrossoverL1dMisses,
                            float upperPredicateCrossoverL1dMisses,
                            float lowerBranchCrossoverRetiredInstructions,
                            float upperBranchCrossoverRetiredInstructions,
                            int iteration) {

/*    if (__builtin_expect(static_cast<float>(counterValues[0]) > lowerBranchCrossoverRetiredInstructions
                         && static_cast<float>(counterValues[0]) < upperBranchCrossoverRetiredInstructions
                         && selectFunctionPtr == selectBranch, false)) {
        selectFunctionPtr = selectPredication;
//        std::cout << "Switch made to predication" << std::endl;
    }*/
    if (__builtin_expect((static_cast<float>(counterValues[1]) < lowerPredicateCrossoverL1dMisses
                          || static_cast<float>(counterValues[1]) > upperPredicateCrossoverL1dMisses)
                         && selectFunctionPtr == selectPredication, false)) {
        selectFunctionPtr = selectBranch;
        std::cout << "Switch made to branch on chunk " << iteration << std::endl;
        std::cout << counterValues[1] << std::endl;
    }
}

int selectAdaptive(int n, const int *inputData, int *selection, int threshold) {
    int tuplesPerAdaption = 50000;
    float lowerPredicateCrossoverSelectivity = 0.04;
    float upperPredicateCrossoverSelectivity = 0.99;
    float lowerBranchCrossoverSelectivity = 0.04;
    float upperBranchCrossoverSelectivity = 0.98;

    int elementsPerL1dCacheLine = getL1cacheLineSize() / sizeof (int);
    float lowerPredicateCrossoverL1dMisses =
            (tuplesPerAdaption + (tuplesPerAdaption * lowerPredicateCrossoverSelectivity)) / elementsPerL1dCacheLine;
    float upperPredicateCrossoverL1dMisses =
            (tuplesPerAdaption + (tuplesPerAdaption * upperPredicateCrossoverSelectivity)) / elementsPerL1dCacheLine;
    float lowerBranchCrossoverRetiredInstructions =
            (tuplesPerAdaption * 5) + (tuplesPerAdaption * 3 * lowerBranchCrossoverSelectivity);
    float upperBranchCrossoverRetiredInstructions =
            (tuplesPerAdaption * 5) + (tuplesPerAdaption * 3 * upperBranchCrossoverSelectivity);

    std::cout << lowerPredicateCrossoverL1dMisses << std::endl;
    std::cout << upperPredicateCrossoverL1dMisses << std::endl;


    int k = 0;
    int tuplesToProcess;
    int selected;
    SelectFunctionPtr selectFunctionPtr = selectPredication;

    std::vector<std::string> counters = {"INSTRUCTION_RETIRED",
                                         "PERF_COUNT_HW_CACHE_L1D"};
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
        performAdaption(selectFunctionPtr, counterValues, tuplesPerAdaption, lowerPredicateCrossoverL1dMisses,
                        upperPredicateCrossoverL1dMisses, lowerBranchCrossoverRetiredInstructions,
                        upperBranchCrossoverRetiredInstructions,
                        5000 - (n / tuplesPerAdaption));
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