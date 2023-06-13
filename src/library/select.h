#ifndef MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_SELECT_H
#define MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_SELECT_H

#include <string>

int selectBranch(int n, const int *inputData, int *selection, int threshold);
int selectPredication(int n, const int *inputData, int *selection, int threshold);
int selectAdaptive(int n, const int *inputData, int *selection, int threshold);

enum SelectImplementation {
    Branch, Predication, Adaptive
};

typedef int (*SelectFunctionPtr)(int n, const int *inputData, int *selection, int threshold);

void setSelectFuncPtr(SelectFunctionPtr &selectFunctionPtr, SelectImplementation selectImplementation);
std::string getName(SelectImplementation selectImplementation);

#endif //MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_SELECT_H
