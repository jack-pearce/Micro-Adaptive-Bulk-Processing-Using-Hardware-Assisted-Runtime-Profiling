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

inline void performAdaption(int (*&selectFunctionPtr)(int, const int *, int *, int), const long_long *counterValues,
                            int tuplesPerAdaption) {

//    std::cout << static_cast<float>(counterValues[0]) / static_cast<float>(tuplesPerAdaption) << " cycles per tuple" << std::endl;
//    std::cout << static_cast<float>(tuplesPerAdaption) / static_cast<float>(counterValues[1]) << " tuples per L1 dcache load miss" << std::endl;

    if (__builtin_expect(static_cast<float>(counterValues[0]) / static_cast<float>(tuplesPerAdaption) > 8.8
                         && selectFunctionPtr == selectBranch, false)) {
        selectFunctionPtr = selectPredication;
//        std::cout << "Switch made to predication" << std::endl;
    }

    if (__builtin_expect(static_cast<float>(tuplesPerAdaption) / static_cast<float>(counterValues[1]) < 8.0
                         && selectFunctionPtr == selectPredication, false)) {
        selectFunctionPtr = selectBranch;
//        std::cout << "Switch made to branch" << std::endl;
    }
}

int selectAdaptive(int n, const int *inputData, int *selection, int threshold) {
    int tuplesPerAdaption = 10000;
    int k = 0;
    int tuplesToProcess;
    int selected;
    SelectFunctionPtr selectFunctionPtr = selectPredication;

    std::vector<std::string> counters = {"UNHALTED_CORE_CYCLES",
                                         "L1-DCACHE-LOAD-MISSES"};
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
        performAdaption(selectFunctionPtr, counterValues, tuplesPerAdaption);
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