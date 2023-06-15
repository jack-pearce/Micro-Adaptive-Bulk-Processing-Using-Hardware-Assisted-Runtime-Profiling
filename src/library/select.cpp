#include <algorithm>
#include <iostream>

#include "select.h"
#include "hardwareTuning.h"
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

int selectAdaptive(int n, const int *inputData, int *selection, int threshold) {
    int tuplesPerAdaption = 10000;
    double lowerPredicateCrossoverSelectivity = 0.04 * tuplesPerAdaption;
    double upperPredicateCrossoverSelectivity = 0.98 * tuplesPerAdaption;
    double lowerBranchCrossoverSelectivity = 0.04 * tuplesPerAdaption;
    double upperBranchCrossoverSelectivity = 0.98 * tuplesPerAdaption;

    int k = 0;
    int tuplesToProcess;
    int selected;
    SelectFunctionPtr selectFunctionPtr = selectPredication;

    while (n > 0) {
        tuplesToProcess = std::min(n, tuplesPerAdaption);
        selected = selectFunctionPtr(tuplesToProcess, inputData, selection, threshold);
        n -= tuplesToProcess;
        inputData += tuplesToProcess;
        selection += selected;
        k += selected;

        if (__builtin_expect(selected > lowerBranchCrossoverSelectivity
                             && selected < upperBranchCrossoverSelectivity
                             && selectFunctionPtr == selectBranch, false))
            selectFunctionPtr = selectPredication;
        if (__builtin_expect((selected < lowerPredicateCrossoverSelectivity
                             || selected > upperPredicateCrossoverSelectivity)
                             && selectFunctionPtr == selectPredication, false))
            selectFunctionPtr = selectBranch;
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