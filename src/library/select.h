#ifndef MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_SELECT_H
#define MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_SELECT_H

#include <string>

int selectIndexesBranch(int n, const int *inputData, int *selection, int threshold);
int selectIndexesPredication(int n, const int *inputData, int *selection, int threshold);
int selectIndexesAdaptive(int n, const int *inputData, int *selection, int threshold);

int selectValuesBranch(int n, const int *inputData, int *selection, int threshold);
int selectValuesPredication(int n, const int *inputData, int *selection, int threshold);
int selectValuesVectorized(int n, const int *inputData, int *selection, int threshold);
int selectValuesAdaptive(int n, const int *inputData, int *selection, int threshold);

enum SelectImplementation {
    IndexesBranch, IndexesPredication, IndexesAdaptive,
    ValuesBranch, ValuesPredication, ValuesVectorized, ValuesAdaptive
};

typedef int (*SelectFunctionPtr)(int n, const int *inputData, int *selection, int threshold);

void setSelectFuncPtr(SelectFunctionPtr &selectFunctionPtr, SelectImplementation selectImplementation);
std::string getName(SelectImplementation selectImplementation);

#endif //MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_SELECT_H
