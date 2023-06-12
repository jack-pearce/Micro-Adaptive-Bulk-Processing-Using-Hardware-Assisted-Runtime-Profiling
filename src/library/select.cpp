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

inline void performAdaption(SelectFunctionPtr &selectFunctionPtr, int eventSet, long_long counterValues[]) {
    if (PAPI_read(eventSet, counterValues) != PAPI_OK)
        exit(1);

    // do analysis

    // update selectFunctionPtr if necessary

    if (PAPI_reset(eventSet) != PAPI_OK)
        exit(1);
}

int selectAdaptive(int n, const int *inputData, int *selection, int threshold) {
    int tuplesPerAdaption = 10000;
    int k = 0;
    int tuplesToProcess;
    int selected;
    SelectFunctionPtr selectFunctionPtr = selectPredication;

    std::vector<std::string> counters = {"PERF_COUNT_HW_CPU_CYCLES",
                                         "INSTRUCTION_RETIRED",
                                         "BRANCH_INSTRUCTIONS_RETIRED",
                                         "MISPREDICTED_BRANCH_RETIRED",
                                         "LLC_MISSES",
                                         "PERF_COUNT_HW_CACHE_MISSES"};
    long_long counterValues[counters.size()];
    int eventSet = initialisePAPIandCreateEventSet(counters);
    int returnValue = PAPI_start(eventSet);
    if (returnValue != PAPI_OK) {
        std::cerr << "PAPI could not start counting events!" << std::endl;
        std::cerr << "Error code: " << returnValue << std::endl;
        exit(1);
    }

    while (n > 0) {
        tuplesToProcess = std::min(n, tuplesPerAdaption);
        selected = selectFunctionPtr(tuplesToProcess, inputData, selection, threshold);
        n -= tuplesToProcess;
        inputData += tuplesToProcess;
        selection += selected;
        k += selected;
        performAdaption(selectFunctionPtr, eventSet, counterValues);
    }

    teardownPAPI(eventSet, counterValues);

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
