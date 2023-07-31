#ifndef MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATA_GENERATOR_H
#define MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATA_GENERATOR_H


void generateVaryingSelectivityInMemory(int *data, int n, int minimum, int numberOfDiscreteSections);
void generateUpperStepSelectivityInMemory(int *data, int n, int step, int numberOfDiscreteSections);
void generateLowerStepSelectivityInMemory(int *data, int n, int step, int numberOfDiscreteSections);
void generateUnequalLowerStepSelectivityInMemory(int *data, int n, int step, int numberOfDiscreteSections, float percentStepSection);
void generatePartiallySortedInMemory(int *data, int n, int numRepeats, float percentageRandom);

void generateUniformDistributionInMemory(int *data, int n, int upperBound);
void generateUniformDistributionInMemory(int64_t *data, int n, int upperBound);
void generateUniqueValuesRandomisedInMemory(int *data, int n);
void generateUniqueValuesRandomisedInMemory(int64_t *data, int n);
void generateUniformDistributionInMemoryWithSetCardinality(int *data, int n, int upperBound, int cardinality);
void generateUniformDistributionInMemoryWithSetCardinality(int64_t *data, int n, int upperBound, int cardinality);
void generateUniformDistributionInMemoryWithSetCardinalityClustered(int *data, int n, int upperBound, int cardinality, int spreadInCluster);
void generateUniformDistributionInMemoryWithTwoCardinalitySections(int *data, int n, int upperBound, int cardinalitySectionOne, int cardinalitySectionTwo, float fractionSectionTwo);
void generateUniformDistributionInMemoryWithMultipleTwoCardinalitySections(int *data, int n, int upperBound, int cardinalitySectionOne, int cardinalitySectionTwo, float fractionSectionTwo, int numSections);

#endif //MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATA_GENERATOR_H
