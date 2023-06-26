#ifndef MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_SELECT_H
#define MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_SELECT_H

#include <string>
#include <iostream>
#include <vector>


enum SelectIndexesChoice {
    IndexesBranch,
    IndexesPredication
};

enum SelectValuesChoice {
    ValuesBranch,
    ValuesPredication,
    ValuesVectorized
};

enum Select {
    ImplementationIndexesBranch,
    ImplementationIndexesPredication,
    ImplementationIndexesAdaptive,
    ImplementationValuesBranch,
    ImplementationValuesPredication,
    ImplementationValuesVectorized,
    ImplementationValuesAdaptive
};

std::string getSelectName(Select selectImplementation);


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
int runSelectFunction(Select selectImplementation,
                      int n, const T2 *inputData, const T1 *inputFilter, T2 *selection, T1 threshold);


#include "selectImplementation.h"

#endif //MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_SELECT_H
