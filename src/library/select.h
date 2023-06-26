#ifndef MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_SELECT_H
#define MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_SELECT_H

#include <string>
#include <iostream>


enum SelectImplementation {
    IndexesBranch, IndexesPredication, IndexesAdaptive,
    ValuesBranch, ValuesPredication, ValuesVectorized, ValuesAdaptive
};

std::string getSelectName(SelectImplementation selectImplementation);


template<typename T>
using SelectIndexesFunctionPtr = int (*)(int, const T*, int*, T);

template<typename T1, typename T2>
using SelectValuesFunctionPtr = int (*)(int, const T2*, const T1*, int*, T1);


template<typename T>
int selectIndexesBranch(int n, const T *inputFilter, int *selection, T threshold);

template<typename T>
int selectIndexesPredication(int n, const T *inputFilter, int *selection, T threshold);

template<typename T>
int selectIndexesAdaptive(int n, const T *inputFilter, int *selection, T threshold);


template<typename T1, typename T2>
int selectValuesBranch(int n, const T2 *inputData, const T1 *inputFilter, T2 *selection, T1 threshold);

template<typename T1, typename T2>
int selectValuesPredication(int n, const T2 *inputData, const T1 *inputFilter, T2 *selection, T1 threshold);

template<typename T1, typename T2>
int selectValuesVectorized(int n, const T2 *inputData, const T1 *inputFilter, T2 *selection, T1 threshold);

template<typename T1, typename T2>
int selectValuesAdaptive(int n, const T2 *inputData, const T1 *inputFilter, T2 *selection, T1 threshold);


template<typename T1, typename T2>
int runSelectFunction(SelectImplementation selectImplementation,
                      int n, const T2 *inputData, const T1 *inputFilter, T2 *selection, T1 threshold);


#include "selectImpl.h"

#endif //MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_SELECT_H
