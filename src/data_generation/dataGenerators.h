#ifndef MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATA_GENERATOR_H
#define MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATA_GENERATOR_H


template <typename T>
void generateVaryingSelectivityInMemory(T *data, int n, int minimum, int numberOfDiscreteSections);

template <typename T>
void generateUpperStepSelectivityInMemory(T *data, int n, int step, int numberOfDiscreteSections);

template <typename T>
void generateLowerStepSelectivityInMemory(T *data, int n, int step, int numberOfDiscreteSections);

template <typename T>
void generateLowerStepSelectivityInMemoryLengthOfSection(T *data, int n, int step, int sectionLength);

template <typename T>
void generateUnequalLowerStepSelectivityInMemory(T *data, int n, int step, int numberOfDiscreteSections,
                                                 float percentStepSection);

template <typename T>
void generatePartiallySortedInMemory(T *data, int n, int numRepeats, float percentageRandom);


template <typename T>
void generateUniformDistributionInMemory(T *data, int n, int upperBound);

template <typename T>
inline T scaleNumberLinearly(int number, int startingUpperBound, int targetUpperBound);

template <typename T>
inline T scaleNumberLogarithmically(T number, int startingUpperBound, int targetUpperBound);

template <typename T>
void generateUniqueValuesRandomisedInMemory(T *data, int n);

template <typename T>
void generateUniformDistributionInMemoryWithSetCardinality(T *data, int n, int upperBound, int cardinality);

template <typename T>
void generateUniformDistributionInMemoryWithSetCardinalityClustered(T *data, int n, int upperBound,
                                                                    int cardinality, int spreadInCluster);
template <typename T>
void generateUniformDistributionInMemoryWithTwoCardinalitySections(T *data, int n, int upperBound,
                                                                   int cardinalitySectionOne, int cardinalitySectionTwo,
                                                                   float fractionSectionTwo);

template <typename T>
void generateUniformDistributionInMemoryWithMultipleTwoCardinalitySections(T *data, int n, int upperBound,
                                                                           int cardinalitySectionOne,
                                                                           int cardinalitySectionTwo,
                                                                           float fractionSectionTwo,
                                                                           int numSections);


#include "dataGeneratorsImplementation.h"

#endif //MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATA_GENERATOR_H
