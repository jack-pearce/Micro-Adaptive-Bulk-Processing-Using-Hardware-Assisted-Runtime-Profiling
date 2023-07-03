#ifndef MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATA_GENERATOR_H
#define MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATA_GENERATOR_H

void generateUniformDistributionInMemory(int *data, int n, int upperBound);
void generateUniformDistributionInMemoryWithSetCardinality(int *data, int n, int upperBound, int cardinality);
void generateUniformDistributionInMemoryWithSetCardinalityClustered(int *data, int n, int upperBound, int cardinality, int spreadInCluster);
void generateVaryingSelectivityInMemory(int *data, int n, int minimum, int numberOfDiscreteSections);
void generateUpperStepSelectivityInMemory(int *data, int n, int step, int numberOfDiscreteSections);
void generateLowerStepSelectivityInMemory(int *data, int n, int step, int numberOfDiscreteSections);
void generateUnequalLowerStepSelectivityInMemory(int *data, int n, int step, int numberOfDiscreteSections, int sectionRatio);
void generatePartiallySortedInMemory(int *data, int n, int numRepeats, float percentageRandom);

#endif //MICRO_ADAPTIVE_BULK_PROCESSING_LIBRARY_DATA_GENERATOR_H
