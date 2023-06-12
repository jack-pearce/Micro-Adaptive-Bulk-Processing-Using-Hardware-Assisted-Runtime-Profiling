#include <algorithm>
#include <iostream>

#include "select.h"


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

/*void selectPredication_2(int n, int *&inputData, int *&selection, int threshold, int &k) {
    for (int i = 0; i < n; ++i) {
        *selection = *inputData;
        selection += (*inputData) <= threshold;
        k += (*inputData)++ <= threshold;
    }
}*/

void performAdaption(SelectFunctionPtr &selectFunctionPtr) {
    // stop counters and read counters

    // do analysis

    // update selectFunctionPtr if necessary

    // reset counters
}

int selectAdaptive(int n, const int *inputData, int *selection, int threshold) {
    int tuplesPerAdaption = 2500;
    int k = 0;
    int tuplesToProcess;
    int selected;

    // initialise performance counters

    // start counters

    SelectFunctionPtr selectFunctionPtr = selectPredication;

    while (n > 0) {
        tuplesToProcess = std::min(n, tuplesPerAdaption);
        selected = selectFunctionPtr(tuplesToProcess, inputData, selection, threshold);
        n -= tuplesToProcess;
        inputData += tuplesToProcess;
        selection += selected;
        k += selected;
        performAdaption(selectFunctionPtr);
    }

    // counter teardown

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
