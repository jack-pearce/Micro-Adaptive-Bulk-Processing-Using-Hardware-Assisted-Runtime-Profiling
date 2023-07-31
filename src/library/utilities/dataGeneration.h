#ifndef MABPL_DATAGENERATION_H
#define MABPL_DATAGENERATION_H

namespace MABPL {

template <typename T>
void copyArray(const T *source, T *destination, int size);

template <typename T>
void generateUniformDistributionInMemory(T *data, int n, int upperBound);

template <typename T>
void generateRandomisedUniqueValuesInMemory(T *data, int n);

template <typename T>
inline int scaleNumberLogarithmically(T number, int startingUpperBound, int targetUpperBound);

template <typename T>
void generateUniformDistributionWithSetCardinalityInMemory(T *data, int n, int upperBound, int cardinality);

template <typename T>
void generatePartiallySortedInMemory(T *data, int n, int numRepeats, float percentageRandom);

}

#include "dataGenerationImplementation.h"

#endif //MABPL_DATAGENERATION_H
