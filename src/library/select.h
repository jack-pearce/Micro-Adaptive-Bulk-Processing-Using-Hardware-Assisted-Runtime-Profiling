#ifndef MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_SELECT_H
#define MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_SELECT_H

#include <string>

enum SelectImplementation {
    IndexesBranch, IndexesPredication, IndexesAdaptive,
    ValuesBranch, ValuesPredication, ValuesVectorized, ValuesAdaptive
};

int selectIndexesBranch(int n, const int *inputFilter, int *selection, int threshold);
int selectIndexesPredication(int n, const int *inputFilter, int *selection, int threshold);
int selectIndexesAdaptive(int n, const int *inputFilter, int *selection, int threshold);

int selectValuesBranch(int n, const int *inputData, const int *inputFilter, int *selection, int threshold);
int selectValuesPredication(int n, const int *inputData, const int *inputFilter, int *selection, int threshold);
int selectValuesVectorized(int n, const int *inputData, const int *inputFilter, int *selection, int threshold);
int selectValuesAdaptive(int n, const int *inputData, const int *inputFilter, int *selection, int threshold);

typedef int (*SelectIndexesFunctionPtr)(int n, const int *inputData, int *selection, int threshold);
typedef int (*SelectValuesFunctionPtr)(int n, const int *inputData, const int* inputFilter, int *selection, int threshold);

void setSelectValuesFuncPtr(SelectValuesFunctionPtr &selectValueFunctionPtr, SelectImplementation selectImplementation);
void setSelectIndexesFuncPtr(SelectIndexesFunctionPtr &selectIndexesFunctionPtr, SelectImplementation selectImplementation);

int runSelectFunction(SelectImplementation selectImplementation,
                      int n, const int *inputData, const int *inputFilter, int *selection, int threshold);

std::string getSelectName(SelectImplementation selectImplementation);

#endif //MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_SELECT_H
