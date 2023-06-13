#include <algorithm>
#include <iostream>

#include "select.h"
#include "../utils/papiHelpers.h"


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

inline void performAdaption(SelectFunctionPtr &selectFunctionPtr,
                            int eventSet, long_long counterValues[], int tuplesPerAdaption) {

    if (__builtin_expect(PAPI_read(eventSet, counterValues) != PAPI_OK, false))
        exit(1);

    if (__builtin_expect(counterValues[0] / tuplesPerAdaption > 8 && selectFunctionPtr == selectBranch, false))
        selectFunctionPtr = selectPredication;

    if (__builtin_expect(static_cast<float>(tuplesPerAdaption) / static_cast<float>(counterValues[1]) < 8.3
                         && selectFunctionPtr == selectPredication, false))
        selectFunctionPtr = selectBranch;

    if (__builtin_expect(PAPI_reset(eventSet) != PAPI_OK, false))
        exit(1);
}

int selectAdaptive(int n, const int *inputData, int *selection, int threshold) {
    int tuplesPerAdaption = 10000;
    int k = 0;
    int tuplesToProcess;
    int selected;
    SelectFunctionPtr selectFunctionPtr = selectPredication;

    std::vector<std::string> counters = {"UNHALTED_CORE_CYCLES",
                                         "L1-DCACHE-LOAD-MISSES"};

    int eventSet;
    long_long counterValues[counters.size()];
    eventSet = initialisePAPIandCreateEventSet(counters);

    while (n > 0) {
        tuplesToProcess = std::min(n, tuplesPerAdaption);
        selected = selectFunctionPtr(tuplesToProcess, inputData, selection, threshold);
        n -= tuplesToProcess;
        inputData += tuplesToProcess;
        selection += selected;
        k += selected;
        performAdaption(selectFunctionPtr, eventSet, counterValues, tuplesPerAdaption);
    }

    PAPI_destroy_eventset(&eventSet);
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