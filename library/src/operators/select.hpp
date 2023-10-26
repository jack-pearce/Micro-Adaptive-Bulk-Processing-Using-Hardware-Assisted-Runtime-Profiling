#ifndef MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_SELECT_H
#define MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_SELECT_H

#include <string>
#include <iostream>
#include <vector>


namespace HAQP {

enum SelectIndexes {
    IndexesBranch,
    IndexesPredication
};

enum SelectValues {
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


template<typename T1, typename T2>
int runSelectFunction(Select selectImplementation,
                      int n, const T2 *inputData, const T1 *inputFilter, T2 *selection, T1 threshold, int dop = 2);

}

#include "selectImplementation.hpp"

#endif //MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_SELECT_H
