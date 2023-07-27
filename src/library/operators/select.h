#ifndef MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_SELECT_H
#define MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_SELECT_H

#include <string>
#include <iostream>
#include <vector>


namespace MABPL {

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
    ImplementationIndexesAdaptiveParallel,
    ImplementationValuesBranch,
    ImplementationValuesPredication,
    ImplementationValuesVectorized,
    ImplementationValuesAdaptive,
    ImplementationValuesAdaptiveParallel
};

std::string getSelectName(Select selectImplementation);


template<typename T>
int selectIndexesBranch(int endIndex, const T *inputFilter, int *selection, T threshold, int startIndex = 0);

template<typename T>
int selectIndexesPredication(int endIndex, const T *inputFilter, int *selection, T threshold, int startIndex = 0);

template<typename T>
int selectIndexesAdaptive(int n, const T *inputFilter, int *selection, T threshold);

template<typename T>
int selectIndexesAdaptiveParallel(int n, const T *inputFilter, int *selection, T threshold, int dop = 2);


template<typename T1, typename T2>
int selectValuesBranch(int endIndex, const T2 *inputData, const T1 *inputFilter, T2 *selection,
                       T1 threshold, int startIndex = 0);

template<typename T1, typename T2>
int selectValuesPredication(int endIndex, const T2 *inputData, const T1 *inputFilter, T2 *selection,
                            T1 threshold, int startIndex = 0);

template<typename T1, typename T2>
int selectValuesVectorized(int endIndex, const T2 *inputData, const T1 *inputFilter, T2 *selection,
                           T1 threshold, int startIndex = 0);

template<typename T1, typename T2>
int selectValuesAdaptive(int n, const T2 *inputData, const T1 *inputFilter, T2 *selection, T1 threshold);

template<typename T1, typename T2>
int selectValuesAdaptiveParallel(int n, const T2 *inputData, const T1 *inputFilter, T2 *selection, T1 threshold,
                                 int dop = 2);


template<typename T1, typename T2>
int runSelectFunction(Select selectImplementation,
                      int n, const T2 *inputData, const T1 *inputFilter, T2 *selection, T1 threshold, int dop = 2);

}

#include "selectImplementation.h"

#endif //MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_SELECT_H
